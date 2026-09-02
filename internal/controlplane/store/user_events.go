// user_events.go：v1.2-F（v1.2-plan §9）append-only 租户事件。
//
// 红线：details 只存脱敏摘要（状态、原因分类、计数）；不得包含 secret
// 值/键名、command env、文件正文、slot IP/netns 等 agent 本地细节。
// 所有事件必须有 project attribution。
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// UserEvent 是一条租户可见事件。
type UserEvent struct {
	ID        int64           `json:"id"`
	At        time.Time       `json:"at"`
	ProjectID string          `json:"project_id"`
	AppID     string          `json:"app_id,omitempty"`
	MachineID string          `json:"machine_id,omitempty"`
	Type      string          `json:"type"`
	Details   json.RawMessage `json:"details,omitempty"`
}

// UserEventFilter 是 /v1/events 的查询面（keyset 分页：before = 上一页
// 最小 id；0 = 最新一页）。
type UserEventFilter struct {
	ProjectID string
	AppID     string
	MachineID string
	Type      string
	Before    int64
	Since     time.Time
	Limit     int
}

// 用户事件类型（稳定契约；前端/CLI 依赖字符串）。
const (
	UserEventMachineCreated      = "machine.created"
	UserEventMachineDeleted      = "machine.deleted"
	UserEventMachineExpired      = "machine.expired"
	UserEventMachineRestarted    = "machine.restarted"
	UserEventMachineRestartBlock = "machine.restart_blocked"
	UserEventRolloutUpdated      = "rollout.updated"
	UserEventSecretDelivered     = "secret.delivered"
	// UserEventSecretCreateRejected（R2 评审 P0）：master key 缺失导致的
	// fail-closed 拒绝（未创建 VM），与 quota 拒绝区分开以便针对性告警。
	UserEventSecretCreateRejected = "secret.create_rejected"
	UserEventQuotaRejected        = "quota.rejected"
	UserEventRateLimitRejected    = "ratelimit.rejected"
	UserEventSessionRejected      = "session.rejected"
)

// RecordUserEvent 追加一条租户事件（fire-and-forget：错误只返回给调用方
// 决定是否记录日志，不得阻塞业务路径）。
func (s *Store) RecordUserEvent(ctx context.Context, ev UserEvent) error {
	if ev.ProjectID == "" || ev.Type == "" {
		return fmt.Errorf("user event requires project_id and type")
	}
	if len(ev.Details) == 0 {
		ev.Details = json.RawMessage(`{}`)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO user_events (at, project_id, app_id, machine_id, type, details)
		VALUES (now(), $1, $2, $3, $4, $5::jsonb)`,
		ev.ProjectID, ev.AppID, ev.MachineID, ev.Type, []byte(ev.Details))
	if err != nil {
		return fmt.Errorf("record user event: %w", err)
	}
	return nil
}

// ListUserEvents keyset 分页查询（id 降序 = 最新在前）。
func (s *Store) ListUserEvents(ctx context.Context, f UserEventFilter) ([]UserEvent, error) {
	if f.Limit <= 0 || f.Limit > 1000 {
		f.Limit = 200
	}
	q := `SELECT id, at, project_id, app_id, machine_id, type, details
		FROM user_events WHERE project_id=$1`
	args := []any{f.ProjectID}
	n := 2
	if f.AppID != "" {
		q += fmt.Sprintf(" AND app_id=$%d", n)
		args = append(args, f.AppID)
		n++
	}
	if f.MachineID != "" {
		q += fmt.Sprintf(" AND machine_id=$%d", n)
		args = append(args, f.MachineID)
		n++
	}
	if f.Type != "" {
		q += fmt.Sprintf(" AND type=$%d", n)
		args = append(args, f.Type)
		n++
	}
	if !f.Since.IsZero() {
		q += fmt.Sprintf(" AND at >= $%d", n)
		args = append(args, f.Since)
		n++
	}
	if f.Before > 0 {
		q += fmt.Sprintf(" AND id < $%d", n)
		args = append(args, f.Before)
		n++
	}
	q += fmt.Sprintf(" ORDER BY id DESC LIMIT $%d", n)
	args = append(args, f.Limit)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list user events: %w", err)
	}
	defer rows.Close()
	var out []UserEvent
	for rows.Next() {
		var ev UserEvent
		if err := rows.Scan(&ev.ID, &ev.At, &ev.ProjectID, &ev.AppID, &ev.MachineID,
			&ev.Type, &ev.Details); err != nil {
			return nil, fmt.Errorf("scan user event: %w", err)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// DeleteUserEventsOlderThan 保留期清理（controller 周期调用），返回删除行数。
func (s *Store) DeleteUserEventsOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM user_events WHERE at < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("delete old user events: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ---------------------------------------------------------------------------
// 引用感知 GC（v1.2-plan §9 GC）：digest 首见时间的持久记账。
// ---------------------------------------------------------------------------

// GCMarkSeen 记录节点缓存里出现过某 digest（首见时间固定，存在即幂等）。
func (s *Store) GCMarkSeen(ctx context.Context, nodeID string, digests []string) error {
	if len(digests) == 0 {
		return nil
	}
	// 逐条 upsert（ON CONFLICT 保持 first_seen 不变）。批量走 COPY 收益
	// 不大（节点 digest 数量 ~百级）。
	for _, d := range digests {
		if _, err := s.pool.Exec(ctx, `
			INSERT INTO gc_seen_digests (node_id, digest, first_seen)
			VALUES ($1, $2, now()) ON CONFLICT DO NOTHING`, nodeID, d); err != nil {
			return fmt.Errorf("gc mark seen: %w", err)
		}
	}
	return nil
}

// GCFirstSeen 批量返回 digest 的首见时间（缺失的跳过）。
func (s *Store) GCFirstSeen(ctx context.Context, nodeID string, digests []string) (map[string]time.Time, error) {
	out := map[string]time.Time{}
	for _, d := range digests {
		var ts time.Time
		err := s.pool.QueryRow(ctx,
			`SELECT first_seen FROM gc_seen_digests WHERE node_id=$1 AND digest=$2`,
			nodeID, d).Scan(&ts)
		if err == nil {
			out[d] = ts
		}
	}
	return out, nil
}

// GCPurgeNode 删除节点下所有 first_seen 记录（节点摘除时调用，防止表无界）。
func (s *Store) GCPurgeNode(ctx context.Context, nodeID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM gc_seen_digests WHERE node_id=$1`, nodeID)
	return err
}

// GCRootImages 返回引用感知 GC 的 root 镜像引用集合（v1.2-F）：
// active/preparing deployment、在途 rollout 的 from/to deployment、
// 在途 create op 的 image_ref。
func (s *Store) GCRootImages(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT coalesce(ref, '') FROM (
			SELECT image_ref AS ref FROM deployments
				WHERE status IN ('PREPARING','ACTIVE')
			UNION ALL
			-- LEFT JOIN deliberately emits an empty root when either rollout
			-- generation is missing; the controller treats it as unsafe.
			SELECT d.image_ref AS ref FROM rollouts r
				CROSS JOIN LATERAL (VALUES (r.from_generation), (r.to_generation)) AS g(generation)
				LEFT JOIN deployments d ON d.app_id = r.app_id AND d.generation = g.generation
				WHERE r.status IN ('PREPARING','CUTOVER','ROLLING_BACK')
			UNION ALL
			SELECT o.request->'spec'->>'image_ref' AS ref FROM operations o
				WHERE o.kind='create' AND o.status IN ('PENDING','CLAIMED')
			UNION ALL
			-- v1.2-F：期望存活 machine 的镜像（standalone machine 与
			-- SUPERSEDED deployment 的存量副本；restart/standby 恢复依赖）。
			SELECT image_ref AS ref FROM machines
				WHERE desired_state IN ('CREATED','RUNNING')
		) t`)
	if err != nil {
		return nil, fmt.Errorf("gc root images: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return nil, fmt.Errorf("scan gc root: %w", err)
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}
