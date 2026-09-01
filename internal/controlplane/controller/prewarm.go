// prewarm.go：v1.4-C（docs/v1.4-plan.md §7）显式镜像预热派发。
// prewarm operation（kind=image_prewarm）逐节点 PullImage；重复派发只补
// 未完成节点（PullImage 本身幂等），leader 切换由 outbox 重试收敛。
package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PrewarmRequest 是 image_prewarm operation 的持久化请求（控制面内部
// JSON；targets 在入队时解析为显式节点集合，重试不重新解析）。
type PrewarmRequest struct {
	ImageRef string   `json:"image_ref"`
	Digest   string   `json:"digest"`
	Targets  []string `json:"targets"`
}

// prewarmPuller 是 dispatch 依赖的最小 agent 能力（生产实现 =
// agentclient.Client）。
type prewarmPuller interface {
	PullImage(ctx context.Context, imageRef string) (*pb.PullImageResponse, error)
}

// processPrewarm 派发 prewarm：对每个未完成目标节点 PullImage；瞬时失败
// 让 op 重试（已完成目标跳过），确定性失败记为该节点 FAILED。全部目标
// 终态后 operation 落账，结果携带逐节点明细。
func (c *Controller) processPrewarm(ctx context.Context, op store.Operation) error {
	return c.dispatchPrewarm(ctx, op, func(nodeID string) prewarmPuller {
		if client := c.clientForNodeID(nodeID); client != nil {
			return client
		}
		return nil
	})
}

func (c *Controller) dispatchPrewarm(ctx context.Context, op store.Operation, resolve func(string) prewarmPuller) error {
	var req PrewarmRequest
	if err := json.Unmarshal(op.Request, &req); err != nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
	if req.ImageRef == "" || req.Digest == "" {
		err := fmt.Errorf("prewarm request missing image_ref/digest")
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
	targets, err := c.store.ListPrewarmTargets(ctx, op.ID)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		return c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, "prewarm has no targets")
	}
	outstanding := false
	attempted := 0
	const maxTargetsPerClaim = 4
	for _, target := range targets {
		if target.Status != "PENDING" {
			continue
		}
		if !target.DeadlineAt.IsZero() && !target.DeadlineAt.After(time.Now()) {
			terminal, aerr := c.store.RecordPrewarmTargetAttemptFailure(ctx, op.ID, target.NodeID, "prewarm deadline exceeded")
			if aerr != nil {
				return aerr
			}
			if terminal {
				c.metrics.Inc("firepaas_prewarm_targets_total", map[string]string{"result": "failed"}, 1)
			}
			continue
		}
		// A prewarm shares the generic operation queue. Bound slow PullImage RPCs
		// per claim so a large request cannot monopolize the reconcile loop.
		if attempted >= maxTargetsPerClaim {
			outstanding = true
			continue
		}
		attempted++
		puller := resolve(target.NodeID)
		if puller == nil {
			terminal, aerr := c.store.RecordPrewarmTargetAttemptFailure(ctx, op.ID, target.NodeID, "node unreachable")
			if aerr != nil {
				return aerr
			}
			if terminal {
				c.metrics.Inc("firepaas_prewarm_targets_total", map[string]string{"result": "failed"}, 1)
			} else {
				outstanding = true
			}
			continue
		}
		// 单节点拉取限时（与其它派发一致），避免长拉取超出 CLAIMED 租约被
		// stale 回收后双派发重复下载。
		pullCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
		resp, err := puller.PullImage(pullCtx, req.ImageRef)
		cancel()
		if err != nil {
			switch status.Code(err) {
			case codes.InvalidArgument, codes.NotFound, codes.Unimplemented, codes.PermissionDenied, codes.Unauthenticated:
				// 确定性失败：该节点终态 FAILED，其余节点继续。
				if serr := c.store.SetPrewarmTargetStatus(ctx, op.ID, target.NodeID, "FAILED", err.Error()); serr != nil {
					return serr
				}
				c.metrics.Inc("firepaas_prewarm_targets_total",
					map[string]string{"result": "failed"}, 1)
				c.recordEvent(ctx, "prewarm", "", op.ID, target.NodeID, "pull failed: "+err.Error(), nil)
			default:
				terminal, aerr := c.store.RecordPrewarmTargetAttemptFailure(ctx, op.ID, target.NodeID, err.Error())
				if aerr != nil {
					return aerr
				}
				if terminal {
					c.metrics.Inc("firepaas_prewarm_targets_total", map[string]string{"result": "failed"}, 1)
				} else {
					outstanding = true
				}
			}
			continue
		}
		if serr := c.store.SetPrewarmTargetStatus(ctx, op.ID, target.NodeID, "SUCCEEDED", ""); serr != nil {
			return serr
		}
		if uerr := c.store.UpsertImageSize(ctx, req.Digest, int64(resp.GetSizeMib())); uerr != nil {
			slog.Warn("prewarm record image size", "digest", req.Digest, "error", uerr)
		}
		c.metrics.Inc("firepaas_prewarm_targets_total",
			map[string]string{"result": "succeeded"}, 1)
	}
	if outstanding {
		return fmt.Errorf("prewarm has outstanding targets; retrying")
	}
	// 全部目标终态：落账并携带逐节点结果（coverage/事件摘要来源）。
	targets, err = c.store.ListPrewarmTargets(ctx, op.ID)
	if err != nil {
		return err
	}
	summary := map[string]any{"digest": req.Digest, "targets": targets}
	raw, _ := json.Marshal(summary)
	succeeded, failed := 0, 0
	for _, t := range targets {
		if t.Status == "SUCCEEDED" {
			succeeded++
		} else if t.Status == "FAILED" {
			failed++
		}
	}
	details, _ := json.Marshal(map[string]any{"operation_id": op.ID, "digest": req.Digest,
		"succeeded": succeeded, "failed": failed})
	return c.store.CompletePrewarmWithEvent(ctx, op.ID, raw, store.UserEvent{
		ProjectID: op.ProjectID, Type: "image.prewarm.completed", Details: details,
	})
}
