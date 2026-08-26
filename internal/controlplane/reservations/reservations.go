// Package reservations 实现 M2b 的 Redis Lua 预约（ADR-0014）：只保障短时并发
// 与项目配额，不把 deployment 当唯一键；业务幂等仍由 PG 唯一索引保障。
//
// 键布局：
//
//	resv:node:{node_id}    hash{pending_vcpu, pending_mem_mib} 在途承诺
//	resv:project:{project} hash{pending_vcpu, pending_mem_mib} 项目在途承诺
//	resv:op:{operation_id} 预约记录（TTL，含 node/project/vcpu/mem）
//
// 生命周期：Acquire（原子 Lua，幂等）→ 派发 agent → Commit/Fail（释放
// pending）。TTL 过期由 rebuildLeases 兜底：PruneStaleOps 清非活跃 op 键，
// Reset 清零全部 hash 后按活跃 op 重新 Acquire——修复旧实现“op 键先过期、
// hash 增量永不释放”的节点假满泄漏（P2-2）。
package reservations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// 语义化错误（调用方按此决策）。
var (
	ErrProjectQuota = errors.New("project quota exceeded")
	ErrNodeCapacity = errors.New("node capacity exceeded")
	ErrNotHeld      = errors.New("reservation not held")
)

// Record 是 resv:op 键里的预约记录。
type Record struct {
	NodeID    string `json:"node_id"`
	ProjectID string `json:"project_id"`
	VCPU      int64  `json:"vcpu"`
	MemMib    int64  `json:"mem_mib"`
}

// Manager 封装预约脚本。
type Manager struct {
	rdb     *redis.Client
	ttl     time.Duration
	acquire *redis.Script
	release *redis.Script
	reset   *redis.Script
}

// New 构造 Manager。ttl<=0 时默认 120s。
func New(rdb *redis.Client, ttl time.Duration) *Manager {
	if ttl <= 0 {
		ttl = 120 * time.Second
	}
	return &Manager{
		rdb: rdb,
		ttl: ttl,
		acquire: redis.NewScript(`
local node_key = KEYS[1]
local project_key = KEYS[2]
local op_key = KEYS[3]
local vcpu = tonumber(ARGV[1])
local mem = tonumber(ARGV[2])
local node_vcpu_total = tonumber(ARGV[3])
local node_mem_total = tonumber(ARGV[4])
local project_vcpu_quota = tonumber(ARGV[5])
local project_mem_quota = tonumber(ARGV[6])
local ttl = tonumber(ARGV[7])
local node_id = ARGV[8]
local project_id = ARGV[9]

-- 幂等（P3-1）：同 opID 同参数的重复 Acquire 直接成功，不双计 pending。
-- 若重试换了节点（旧 Release 失败的残留），先从旧 hash 扣回再走全流程。
local raw = redis.call('GET', op_key)
if raw then
  local ok, prev = pcall(cjson.decode, raw)
  if ok and prev.node_id == node_id and prev.project_id == project_id
      and tonumber(prev.vcpu) == vcpu and tonumber(prev.mem_mib) == mem then
    return 1
  end
  if ok then
    redis.call('HINCRBY', 'resv:node:' .. prev.node_id, 'pending_vcpu', -tonumber(prev.vcpu))
    redis.call('HINCRBY', 'resv:node:' .. prev.node_id, 'pending_mem_mib', -tonumber(prev.mem_mib))
    redis.call('HINCRBY', 'resv:project:' .. prev.project_id, 'pending_vcpu', -tonumber(prev.vcpu))
    redis.call('HINCRBY', 'resv:project:' .. prev.project_id, 'pending_mem_mib', -tonumber(prev.mem_mib))
    redis.call('DEL', op_key)
  end
end

local pending_vcpu = tonumber(redis.call('HGET', node_key, 'pending_vcpu') or '0')
local pending_mem = tonumber(redis.call('HGET', node_key, 'pending_mem_mib') or '0')
-- 节点侧硬上限（与 scheduler 同一语义的保守双保险；CPU 超售 R=4，内存不超售）。
if pending_vcpu + vcpu > node_vcpu_total * 4 then return -3 end
if pending_mem + mem > node_mem_total then return -4 end

local proj_vcpu = tonumber(redis.call('HGET', project_key, 'pending_vcpu') or '0')
local proj_mem = tonumber(redis.call('HGET', project_key, 'pending_mem_mib') or '0')
if proj_vcpu + vcpu > project_vcpu_quota then return -1 end
if proj_mem + mem > project_mem_quota then return -2 end

local data = {node_id=node_id, project_id=project_id, vcpu=vcpu, mem_mib=mem}
redis.call('HINCRBY', node_key, 'pending_vcpu', vcpu)
redis.call('HINCRBY', node_key, 'pending_mem_mib', mem)
redis.call('HINCRBY', project_key, 'pending_vcpu', vcpu)
redis.call('HINCRBY', project_key, 'pending_mem_mib', mem)
redis.call('SET', op_key, cjson.encode(data), 'EX', ttl)
return 1`),
		release: redis.NewScript(`
local node_key = KEYS[1]
local project_key = KEYS[2]
local op_key = KEYS[3]
local raw = redis.call('GET', op_key)
if not raw then return 0 end
local ok, data = pcall(cjson.decode, raw)
if not ok then redis.call('DEL', op_key) return 1 end
redis.call('HINCRBY', node_key, 'pending_vcpu', -data.vcpu)
redis.call('HINCRBY', node_key, 'pending_mem_mib', -data.mem_mib)
redis.call('HINCRBY', project_key, 'pending_vcpu', -data.vcpu)
redis.call('HINCRBY', project_key, 'pending_mem_mib', -data.mem_mib)
redis.call('DEL', op_key)
return 1`),
		reset: redis.NewScript(`
-- P2-2 全量重建：清零全部 node/project hash，然后从存活的 resv:op 键
-- 原子重放在途承诺。修复两类泄漏：
--   a) op 键 TTL 先过期、hash 增量永久残留（节点假满）；
--   b) 重建不得依赖重新 Acquire——Acquire 的幂等早退会跳过 hash 入账。
local n = 0
local nodes = redis.call('KEYS', 'resv:node:*')
for i, k in ipairs(nodes) do redis.call('DEL', k) n = n + 1 end
local projects = redis.call('KEYS', 'resv:project:*')
for i, k in ipairs(projects) do redis.call('DEL', k) n = n + 1 end
local ops = redis.call('KEYS', 'resv:op:*')
for i, k in ipairs(ops) do
  local raw = redis.call('GET', k)
  if raw then
    local ok, d = pcall(cjson.decode, raw)
    if ok and d.node_id and d.vcpu then
      redis.call('HINCRBY', 'resv:node:' .. d.node_id, 'pending_vcpu', tonumber(d.vcpu))
      redis.call('HINCRBY', 'resv:node:' .. d.node_id, 'pending_mem_mib', tonumber(d.mem_mib))
      redis.call('HINCRBY', 'resv:project:' .. d.project_id, 'pending_vcpu', tonumber(d.vcpu))
      redis.call('HINCRBY', 'resv:project:' .. d.project_id, 'pending_mem_mib', tonumber(d.mem_mib))
    end
  end
end
return n`),
	}
}

func nodeKey(nodeID string) string       { return "resv:node:" + nodeID }
func projectKey(projectID string) string { return "resv:project:" + projectID }
func opKey(opID string) string           { return "resv:op:" + opID }

// Acquire 原子预约：检查节点硬上限与项目配额后记账并写 TTL 记录。
// 幂等（P3-1）：同 opID、同参数的重复调用直接成功；换节点的重试会先把
// 旧预约从原节点/项目 hash 扣回。
func (m *Manager) Acquire(ctx context.Context, opID, nodeID, projectID string,
	vcpu, memMib uint64, nodeVCPUTotal, nodeMemTotal uint64,
	projectQuotaVCPU, projectQuotaMem uint64) error {

	res, err := m.acquire.Run(ctx, m.rdb,
		[]string{nodeKey(nodeID), projectKey(projectID), opKey(opID)},
		vcpu, memMib, nodeVCPUTotal, nodeMemTotal,
		projectQuotaVCPU, projectQuotaMem,
		int64(m.ttl.Seconds()), nodeID, projectID).Int()
	if err != nil {
		return fmt.Errorf("reservation acquire %s: %w", opID, err)
	}
	switch res {
	case 1:
		return nil
	case -1, -2:
		return ErrProjectQuota
	case -3, -4:
		return ErrNodeCapacity
	default:
		return fmt.Errorf("reservation acquire %s: unexpected script result %d", opID, res)
	}
}

// Commit 成功落账：释放 pending（allocated 由 20s ServiceInfo 校正，ADR-0002）。
func (m *Manager) Commit(ctx context.Context, opID string) error {
	return m.releasePending(ctx, opID)
}

// Release 失败/重排时释放预约。
func (m *Manager) Release(ctx context.Context, opID string) error {
	return m.releasePending(ctx, opID)
}

func (m *Manager) releasePending(ctx context.Context, opID string) error {
	rec, err := m.Get(ctx, opID)
	if err != nil {
		return err
	}
	if rec == nil {
		return ErrNotHeld
	}
	_, err = m.release.Run(ctx, m.rdb,
		[]string{nodeKey(rec.NodeID), projectKey(rec.ProjectID), opKey(opID)}).Int()
	if err != nil {
		return fmt.Errorf("reservation release %s: %w", opID, err)
	}
	return nil
}

// Get 读取预约记录；不存在返回 nil。
func (m *Manager) Get(ctx context.Context, opID string) (*Record, error) {
	raw, err := m.rdb.Get(ctx, opKey(opID)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get reservation %s: %w", opID, err)
	}
	var rec Record
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, fmt.Errorf("parse reservation %s: %w", opID, err)
	}
	return &rec, nil
}

// PendingByNode 返回每个节点的在途预约承诺（调度器可参考；PG 推导为主）。
func (m *Manager) PendingByNode(ctx context.Context) (map[string][2]int64, error) {
	out := map[string][2]int64{}
	iter := m.rdb.Scan(ctx, 0, "resv:node:*", 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		nodeID := key[len("resv:node:"):]
		vals, err := m.rdb.HMGet(ctx, key, "pending_vcpu", "pending_mem_mib").Result()
		if err != nil {
			return nil, fmt.Errorf("hmget %s: %w", key, err)
		}
		var v [2]int64
		if vals[0] != nil {
			v[0] = redisInt64(vals[0])
		}
		if vals[1] != nil {
			v[1] = redisInt64(vals[1])
		}
		out[nodeID] = v
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan reservations: %w", err)
	}
	return out, nil
}

// PruneStaleOps 删除不在 activeOps 集合中的 resv:op 记录（P2-2）。
// 仅删 op 键、不动 hash：hash 是派生状态，由随后的 Reset + 重新 Acquire
// 全量重建。返回删除的键数。activeOps 来自 PG 在途（PENDING/CLAIMED）
// create 操作集合。
func (m *Manager) PruneStaleOps(ctx context.Context, activeOps map[string]bool) (int, error) {
	released := 0
	iter := m.rdb.Scan(ctx, 0, "resv:op:*", 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		opID := key[len("resv:op:"):]
		if activeOps[opID] {
			continue
		}
		if err := m.rdb.Del(ctx, key).Err(); err != nil {
			return released, fmt.Errorf("prune stale op %s: %w", opID, err)
		}
		released++
	}
	if err := iter.Err(); err != nil {
		return released, fmt.Errorf("scan reservation ops: %w", err)
	}
	return released, nil
}

// Reset 全量重建预约 hash（P2-2）：原子执行“清零 node/project hash + 从
// 存活的 resv:op 键重放在途承诺”。必须在 PruneStaleOps 之后调用（否则
// 已死操作的增量会被重放回来），只在单写者（M2a leader）重建周期内执行。
// 返回清零的 hash 键数。
func (m *Manager) Reset(ctx context.Context) (int, error) {
	n, err := m.reset.Run(ctx, m.rdb, nil).Int()
	if err != nil {
		return 0, fmt.Errorf("rebuild reservation hashes: %w", err)
	}
	return n, nil
}

func redisInt64(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case string:
		var n int64
		fmt.Sscanf(t, "%d", &n)
		return n
	default:
		return 0
	}
}
