package store

import (
	"context"
	"fmt"
)

// autoscale.go：ADR-0041 并发自动弹性——app 级策略（PG 权威，Redis 不存策略）。
//
// 策略跟 app 走（改策略不产生新 generation，区别于 deployment 固化字段）。
// 写者纪律（§1 写序/TOCTOU）：
//   - autoscaler 一律经 SetAppReplicasCAS 条件 UPDATE（optimistic CAS），
//     RowsAffected=0 即“策略已变/手动接管/其他写者”，本拍跳过绝不覆盖；
//   - 手动 scale 经 TakeoverScale 单条 UPDATE（desired + enabled=false 原子接管）。

// AutoscalePolicy 是 apps 行上的弹性策略（与 App 内联字段一一对应）。
type AutoscalePolicy struct {
	Enabled           bool
	MinReplicas       int
	MaxReplicas       int
	TargetConcurrency int
	ScaleDownDelaySec int
	PanicThreshold    float64
}

// DefaultAutoscalePolicy 返回列默认值对应的策略（= 当前手动行为，零回归）。
func DefaultAutoscalePolicy() AutoscalePolicy {
	return AutoscalePolicy{
		Enabled:           false,
		MinReplicas:       1,
		MaxReplicas:       10,
		TargetConcurrency: 20,
		ScaleDownDelaySec: 120,
		PanicThreshold:    2.0,
	}
}

// autoscaleBounds 是 ADR-0041 §1 边界的单一事实表（Validate 与 clamp 共用，
// 数值变更只改一处；错误文案与钳制语义不变）。
var autoscaleBounds = struct {
	minReplicas       [2]int
	maxReplicas       [2]int
	targetConcurrency [2]int
	scaleDownDelaySec [2]int
	panicThreshold    [2]float64
}{
	minReplicas:       [2]int{0, 100},
	maxReplicas:       [2]int{1, 100},
	targetConcurrency: [2]int{1, 256},
	scaleDownDelaySec: [2]int{30, 600},
	panicThreshold:    [2]float64{1.5, 5.0},
}

// clampBound 把 v 钳制到 [lo,hi]（整数/浮点通用，避免每字段手写双分支）。
func clampBound[T int | float64](v, lo, hi T) T {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// ValidateAutoscalePolicy 校验策略（API 层 400 + store 侧写入前双保险）。
// 约束（ADR-0041 §1）：0<=min<=max<=100 且 max>=1；1<=target<=256；
// 30<=delay<=600；1.5<=panic<=5.0。
func ValidateAutoscalePolicy(p AutoscalePolicy) error {
	if p.MinReplicas < autoscaleBounds.minReplicas[0] || p.MaxReplicas < autoscaleBounds.minReplicas[0] {
		return fmt.Errorf("min_replicas and max_replicas must be >= 0")
	}
	if p.MaxReplicas < autoscaleBounds.maxReplicas[0] {
		return fmt.Errorf("max_replicas must be >= 1 (max=0 would pin an enabled app at zero replicas)")
	}
	if p.MaxReplicas > autoscaleBounds.maxReplicas[1] {
		return fmt.Errorf("max_replicas must be <= 100")
	}
	if p.MinReplicas > p.MaxReplicas {
		return fmt.Errorf("min_replicas must be <= max_replicas")
	}
	if p.TargetConcurrency < autoscaleBounds.targetConcurrency[0] || p.TargetConcurrency > autoscaleBounds.targetConcurrency[1] {
		return fmt.Errorf("target_concurrency must be in [1,256]")
	}
	if p.ScaleDownDelaySec < autoscaleBounds.scaleDownDelaySec[0] || p.ScaleDownDelaySec > autoscaleBounds.scaleDownDelaySec[1] {
		return fmt.Errorf("scale_down_delay_sec must be in [30,600]")
	}
	if p.PanicThreshold < autoscaleBounds.panicThreshold[0] || p.PanicThreshold > autoscaleBounds.panicThreshold[1] {
		return fmt.Errorf("panic_threshold must be in [1.5,5.0]")
	}
	return nil
}

// clampAutoscalePolicy 防脏行：读时钳制到合法域（DB 无 CHECK 约束，
// 历史/手工行可能越界；决策循环读此口径，不信任裸列值）。
func clampAutoscalePolicy(p AutoscalePolicy) AutoscalePolicy {
	p.MinReplicas = clampBound(p.MinReplicas, autoscaleBounds.minReplicas[0], autoscaleBounds.minReplicas[1])
	p.MaxReplicas = clampBound(p.MaxReplicas, autoscaleBounds.maxReplicas[0], autoscaleBounds.maxReplicas[1])
	if p.MinReplicas > p.MaxReplicas {
		p.MinReplicas = p.MaxReplicas
	}
	p.TargetConcurrency = clampBound(p.TargetConcurrency, autoscaleBounds.targetConcurrency[0], autoscaleBounds.targetConcurrency[1])
	p.ScaleDownDelaySec = clampBound(p.ScaleDownDelaySec, autoscaleBounds.scaleDownDelaySec[0], autoscaleBounds.scaleDownDelaySec[1])
	p.PanicThreshold = clampBound(p.PanicThreshold, autoscaleBounds.panicThreshold[0], autoscaleBounds.panicThreshold[1])
	return p
}

// GetAutoscalePolicy 读 app 策略（返回钳制后值；app 不存在 → ErrNotFound）。
func (s *Store) GetAutoscalePolicy(ctx context.Context, appID string) (AutoscalePolicy, error) {
	app, err := s.GetApp(ctx, appID)
	if err != nil {
		return AutoscalePolicy{}, err
	}
	if app == nil {
		return AutoscalePolicy{}, ErrNotFound
	}
	return app.Autoscale(), nil
}

// SetAutoscalePolicy 全量替换策略（PUT 语义；不改 desired_replicas，
// 下一拍决策收敛）。已删除 app 拒绝（与 SetAppReplicas 同纪律）。
func (s *Store) SetAutoscalePolicy(ctx context.Context, appID string, p AutoscalePolicy) error {
	if err := ValidateAutoscalePolicy(p); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE apps SET autoscale_enabled=$2, min_replicas=$3,
		max_replicas=$4, target_concurrency=$5, scale_down_delay_sec=$6,
		panic_threshold=$7, updated_at=now()
		WHERE id=$1 AND deleted_at IS NULL`,
		appID, p.Enabled, p.MinReplicas, p.MaxReplicas, p.TargetConcurrency,
		p.ScaleDownDelaySec, p.PanicThreshold)
	if err != nil {
		return fmt.Errorf("set autoscale policy: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if a, gerr := s.GetApp(ctx, appID); gerr == nil && a != nil && a.Deleted {
			return ErrAppDeleted
		}
		return ErrNotFound
	}
	return nil
}

// SetAppReplicasCAS 是 autoscaler 唯一的 desired 写入口（乐观 CAS）。
// 仅当行仍启用 autoscale 且 desired 仍为 expect 时写入 want；
// 返回 false = “策略已变/手动接管/其他写者”，调用方本拍跳过并计数，绝不覆盖。
func (s *Store) SetAppReplicasCAS(ctx context.Context, appID string, want, expect int) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE apps SET desired_replicas=$3, updated_at=now()
		WHERE id=$1 AND deleted_at IS NULL AND autoscale_enabled AND desired_replicas=$2`,
		appID, expect, want)
	if err != nil {
		return false, fmt.Errorf("cas set replicas: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// TakeoverScale 是手动 scale 的原子接管：单条 UPDATE 同时写 desired_replicas
// 并置 autoscale_enabled=false。autoscaler 的 CAS 永远撞不上接管后的行
// （enabled 已关），接管值不会被覆盖。
func (s *Store) TakeoverScale(ctx context.Context, appID string, replicas int) error {
	tag, err := s.pool.Exec(ctx, `UPDATE apps SET desired_replicas=$2, autoscale_enabled=false,
		updated_at=now() WHERE id=$1 AND deleted_at IS NULL`, appID, replicas)
	if err != nil {
		return fmt.Errorf("takeover scale: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if a, gerr := s.GetApp(ctx, appID); gerr == nil && a != nil && a.Deleted {
			return ErrAppDeleted
		}
		return ErrNotFound
	}
	return nil
}
