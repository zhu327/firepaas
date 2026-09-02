// Package reservations 实现 M2b 的 Redis Lua 预约（ADR-0014）：只保障短时并发
// 与项目配额，不把 deployment 当唯一键；业务幂等仍由 PG 唯一索引保障。
//
// v1.2-E（ADR-0035）：磁盘维度进预约（不超售）。旧记录（无 disk_mib 字段）
// 按 0 重放/释放，升级期安全。
//
// D-3（R2 加固）epoch 化键布局（旧实现用 KEYS 全量枚举，大键空间下阻塞
// Redis 单线程；现全部经 active-epoch 指针 + 每 epoch 的成员集合访问，全程
// 无 KEYS/SCAN）：
//
//	resv:active                  当前 epoch 指针（string，缺失视为 "0"）
//	resv:epoch_seq               epoch 序号计数器（Reset 内 INCR 产生新 epoch）
//	resv:{epoch}:node:{node_id}     hash{pending_*} 节点在途承诺
//	resv:{epoch}:project:{project}  hash{pending_*} 项目在途承诺
//	resv:{epoch}:op:{operation_id}  预约记录（TTL）
//	resv:{epoch}:nodes              set：本 epoch 触及过的 node_id
//	resv:{epoch}:projects           set：本 epoch 触及过的 project
//	resv:{epoch}:ops                set：本 epoch 在途 op_id
//
// 生命周期：Acquire（原子 Lua，幂等；脚本内先读 resv:active 决定落在哪个
// epoch）→ 派发 agent → Commit/Fail（释放 pending）。TTL 过期由
// rebuildLeases 兜底（PruneStaleOps 清非活跃 op → Reset 全量重建）。
//
// Reset 语义重写（D-3）：不再"清零全部 hash + 按存活 op 键重放"，而是在一
// 个原子 Lua 里：INCR epoch_seq 得新 epoch → 从旧 epoch 的 ops 集合把存活
// op 记录重放进新 epoch（hash 重建 + op 键以剩余 TTL 复制）→ SET
// resv:active 切换指针 → 给旧 epoch 的全部键挂 TTL 让其自然过期。快照与
// 指针切换在同一原子脚本内，因此在途 Acquire 只有两种有序结果：先于
// Reset 执行（看到旧指针，其 op 已在旧 ops 集合里 → 被完整重放进新
// epoch，不丢不重）或后于 Reset（看到新指针，直接落新 epoch）——不存在
// "切换与快照之间落进旧 epoch 却被漏重放"的窗口。
//
// 旧 epoch TTL 取预约租约 TTL（m.ttl）而非 rebuild 间隔：旧 epoch 里每个
// hash 增量都对应一条创建时 TTL ≤ m.ttl 的 op 记录，指针切换后残存增量
// 的活期不超过 m.ttl——m.ttl 是覆盖残留窗口的最小充分值。旧 epoch 过期
// 是新 epoch 记账的纯后台清理，两 epoch 键空间完全隔离，互不影响口径。
//
// 升级期兼容：epoch 化前的旧布局键（resv:node:* 等无前缀）不再被任何读写
// 路径访问；其 hash 键无 TTL 会成为死键（基数有界：节点数×项目数），不做
// 在线迁移（迁死键反而需要引入 SCAN）。新代码从空投影自然收敛。
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

const (
	activeKey   = "resv:active"
	epochSeqKey = "resv:epoch_seq"
)

// Record 是 resv:{epoch}:op 键里的预约记录。
type Record struct {
	NodeID    string `json:"node_id"`
	ProjectID string `json:"project_id"`
	VCPU      int64  `json:"vcpu"`
	MemMib    int64  `json:"mem_mib"`
	DiskMib   int64  `json:"disk_mib"` // v1.2-E；旧记录缺省 0
	Machines  int64  `json:"machines"` // project machine-concurrency 在途承诺
}

// Manager 封装预约脚本。
type Manager struct {
	rdb     *redis.Client
	ttl     time.Duration
	acquire *redis.Script
	release *redis.Script
	reset   *redis.Script
	get     *redis.Script
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
-- D-3：脚本内原子读 active epoch，预约落在读取时刻的 epoch（指针切换
-- 与 Reset 快照同脚本原子，要么被重放要么直接落新 epoch，见包注释）。
local active = redis.call('GET', KEYS[1]) or '0'
local pfx = 'resv:' .. active .. ':'
local vcpu = tonumber(ARGV[1])
local mem = tonumber(ARGV[2])
local node_vcpu_total = tonumber(ARGV[3])
local node_mem_total = tonumber(ARGV[4])
local project_vcpu_quota = tonumber(ARGV[5])
local project_mem_quota = tonumber(ARGV[6])
local ttl = tonumber(ARGV[7])
local node_id = ARGV[8]
local project_id = ARGV[9]
-- v1.2-E（ADR-0035）：磁盘维度（ARGV 10-12 追加，兼容旧调用方省略）。
local disk = tonumber(ARGV[10] or '0')
local node_disk_total = tonumber(ARGV[11] or '0')
local project_disk_quota = tonumber(ARGV[12] or '0')
local machines = tonumber(ARGV[13] or '0')
local project_machine_quota = tonumber(ARGV[14] or '0')
-- 契约 C-3：节点 CPU 超售比由调用方传入（与调度器 BestOfKConfig.R 同源），
-- 缺省兼容历史硬编码 4。
local cpu_oversubscribe = tonumber(ARGV[15] or '4')
local op_id = ARGV[16]

local node_key = pfx .. 'node:' .. node_id
local project_key = pfx .. 'project:' .. project_id
local op_key = pfx .. 'op:' .. op_id

-- 幂等（P3-1）：同 opID 同参数的重复 Acquire 直接成功，不双计 pending。
-- 若重试换了节点（旧 Release 失败的残留），先从旧 hash（同一 epoch）扣回
-- 再走全流程。
local raw = redis.call('GET', op_key)
if raw then
  local ok, prev = pcall(cjson.decode, raw)
  if ok and prev.node_id == node_id and prev.project_id == project_id
      and tonumber(prev.vcpu) == vcpu and tonumber(prev.mem_mib) == mem
      and tonumber(prev.disk_mib or 0) == disk
      and tonumber(prev.machines or 0) == machines then
    return 1
  end
  if ok then
    local pv = tonumber(prev.vcpu)
    local pm = tonumber(prev.mem_mib)
    local pd = tonumber(prev.disk_mib or 0)
    local pc = tonumber(prev.machines or 0)
    redis.call('HINCRBY', pfx .. 'node:' .. prev.node_id, 'pending_vcpu', -pv)
    redis.call('HINCRBY', pfx .. 'node:' .. prev.node_id, 'pending_mem_mib', -pm)
    redis.call('HINCRBY', pfx .. 'node:' .. prev.node_id, 'pending_disk_mib', -pd)
    redis.call('HINCRBY', pfx .. 'project:' .. prev.project_id, 'pending_vcpu', -pv)
    redis.call('HINCRBY', pfx .. 'project:' .. prev.project_id, 'pending_mem_mib', -pm)
    redis.call('HINCRBY', pfx .. 'project:' .. prev.project_id, 'pending_disk_mib', -pd)
    redis.call('HINCRBY', pfx .. 'project:' .. prev.project_id, 'pending_machines', -pc)
    redis.call('SREM', pfx .. 'ops', op_id)
    redis.call('DEL', op_key)
  end
end

local pending_vcpu = tonumber(redis.call('HGET', node_key, 'pending_vcpu') or '0')
local pending_mem = tonumber(redis.call('HGET', node_key, 'pending_mem_mib') or '0')
local pending_disk = tonumber(redis.call('HGET', node_key, 'pending_disk_mib') or '0')
-- 节点侧硬上限（与 scheduler 同一语义的保守双保险；CPU 超售比为入参，内存/磁盘不超售）。
if pending_vcpu + vcpu > node_vcpu_total * cpu_oversubscribe then return -3 end
if pending_mem + mem > node_mem_total then return -4 end
if node_disk_total > 0 and pending_disk + disk > node_disk_total then return -5 end

local proj_vcpu = tonumber(redis.call('HGET', project_key, 'pending_vcpu') or '0')
local proj_mem = tonumber(redis.call('HGET', project_key, 'pending_mem_mib') or '0')
local proj_disk = tonumber(redis.call('HGET', project_key, 'pending_disk_mib') or '0')
local proj_machines = tonumber(redis.call('HGET', project_key, 'pending_machines') or '0')
if proj_vcpu + vcpu > project_vcpu_quota then return -1 end
if proj_mem + mem > project_mem_quota then return -2 end
if project_disk_quota > 0 and proj_disk + disk > project_disk_quota then return -6 end
if project_machine_quota > 0 and proj_machines + machines > project_machine_quota then return -7 end

local data = {node_id=node_id, project_id=project_id, vcpu=vcpu, mem_mib=mem, disk_mib=disk, machines=machines}
redis.call('HINCRBY', node_key, 'pending_vcpu', vcpu)
redis.call('HINCRBY', node_key, 'pending_mem_mib', mem)
redis.call('HINCRBY', node_key, 'pending_disk_mib', disk)
redis.call('HINCRBY', project_key, 'pending_vcpu', vcpu)
redis.call('HINCRBY', project_key, 'pending_mem_mib', mem)
redis.call('HINCRBY', project_key, 'pending_disk_mib', disk)
redis.call('HINCRBY', project_key, 'pending_machines', machines)
-- 成员集合供 Reset/PendingByNode/PruneStaleOps 无 KEYS/SCAN 枚举本 epoch。
redis.call('SADD', pfx .. 'nodes', node_id)
redis.call('SADD', pfx .. 'projects', project_id)
redis.call('SADD', pfx .. 'ops', op_id)
redis.call('SET', op_key, cjson.encode(data), 'EX', ttl)
return 1`),
		release: redis.NewScript(`
local active = redis.call('GET', KEYS[1]) or '0'
local pfx = 'resv:' .. active .. ':'
local op_id = ARGV[1]
local op_key = pfx .. 'op:' .. op_id
local raw = redis.call('GET', op_key)
if not raw then return 0 end
local ok, data = pcall(cjson.decode, raw)
if not ok then redis.call('DEL', op_key) redis.call('SREM', pfx .. 'ops', op_id) return 1 end
redis.call('HINCRBY', pfx .. 'node:' .. data.node_id, 'pending_vcpu', -data.vcpu)
redis.call('HINCRBY', pfx .. 'node:' .. data.node_id, 'pending_mem_mib', -data.mem_mib)
redis.call('HINCRBY', pfx .. 'node:' .. data.node_id, 'pending_disk_mib', -(tonumber(data.disk_mib or 0)))
redis.call('HINCRBY', pfx .. 'project:' .. data.project_id, 'pending_vcpu', -data.vcpu)
redis.call('HINCRBY', pfx .. 'project:' .. data.project_id, 'pending_mem_mib', -data.mem_mib)
redis.call('HINCRBY', pfx .. 'project:' .. data.project_id, 'pending_disk_mib', -(tonumber(data.disk_mib or 0)))
redis.call('HINCRBY', pfx .. 'project:' .. data.project_id, 'pending_machines', -(tonumber(data.machines or 0)))
redis.call('SREM', pfx .. 'ops', op_id)
redis.call('DEL', op_key)
return 1`),
		reset: redis.NewScript(`
-- D-3 epoch 切换（单写者重建；原子）：
--   1) INCR epoch_seq 分配新 epoch；
--   2) 从旧 epoch 的 ops 集合把存活 op 记录重放进新 epoch（重建 hash +
--      成员集合，op 键以剩余 TTL 复制——不延长租约）；
--   3) SET resv:active 切换指针；
--   4) 旧 epoch 全部键挂 TTL 自然过期（不得 KEYS/SCAN 全库）。
-- 快照（步骤 2）与切换（步骤 3）同脚本原子：在途 Acquire 要么先于本脚本
-- （op 已在旧 ops 集合 → 完整重放，不丢不重），要么后于本脚本（直接落新
-- epoch）——见包注释与 D-3 契约。
local old_active = redis.call('GET', KEYS[1]) or '0'
local old = 'resv:' .. old_active .. ':'
local epoch = redis.call('INCR', KEYS[2])
local new = 'resv:' .. epoch .. ':'
local ttl = tonumber(ARGV[1])

local ops = redis.call('SMEMBERS', old .. 'ops')
for i, op in ipairs(ops) do
  local raw = redis.call('GET', old .. 'op:' .. op)
  if raw then
    local ok, d = pcall(cjson.decode, raw)
    if ok and d.node_id and d.vcpu then
      local dd = tonumber(d.disk_mib or 0)
      local dc = tonumber(d.machines or 0)
      redis.call('HINCRBY', new .. 'node:' .. d.node_id, 'pending_vcpu', tonumber(d.vcpu))
      redis.call('HINCRBY', new .. 'node:' .. d.node_id, 'pending_mem_mib', tonumber(d.mem_mib))
      redis.call('HINCRBY', new .. 'node:' .. d.node_id, 'pending_disk_mib', dd)
      redis.call('HINCRBY', new .. 'project:' .. d.project_id, 'pending_vcpu', tonumber(d.vcpu))
      redis.call('HINCRBY', new .. 'project:' .. d.project_id, 'pending_mem_mib', tonumber(d.mem_mib))
      redis.call('HINCRBY', new .. 'project:' .. d.project_id, 'pending_disk_mib', dd)
      redis.call('HINCRBY', new .. 'project:' .. d.project_id, 'pending_machines', dc)
      redis.call('SADD', new .. 'nodes', d.node_id)
      redis.call('SADD', new .. 'projects', d.project_id)
      redis.call('SADD', new .. 'ops', op)
      local pttl = redis.call('PTTL', old .. 'op:' .. op)
      if pttl <= 0 then pttl = ttl * 1000 end
      redis.call('SET', new .. 'op:' .. op, raw, 'PX', pttl)
    end
  end
end

redis.call('SET', KEYS[1], tostring(epoch))

-- 旧 epoch 自我过期：TTL = 预约租约 TTL，覆盖指针切换前 Acquire 残存
-- 增量的最大存活窗口（理由见包注释）。旧的 hash 里可能含 op 键先过期的
-- 泄漏增量——重放只按存活 op 记录记账，泄漏不会带入新 epoch。
local n = 0
for i, nid in ipairs(redis.call('SMEMBERS', old .. 'nodes')) do
  redis.call('EXPIRE', old .. 'node:' .. nid, ttl)
  n = n + 1
end
for i, pid in ipairs(redis.call('SMEMBERS', old .. 'projects')) do
  redis.call('EXPIRE', old .. 'project:' .. pid, ttl)
  n = n + 1
end
for i, op in ipairs(ops) do
  redis.call('EXPIRE', old .. 'op:' .. op, ttl)
end
redis.call('EXPIRE', old .. 'nodes', ttl)
redis.call('EXPIRE', old .. 'projects', ttl)
redis.call('EXPIRE', old .. 'ops', ttl)
return n`),
		get: redis.NewScript(`
local active = redis.call('GET', KEYS[1]) or '0'
return redis.call('GET', 'resv:' .. active .. ':op:' .. ARGV[1])`),
	}
}

// epochPfx 返回指定 epoch 的键前缀（仅测试与只读路径使用）。
func epochPfx(epoch string) string { return "resv:" + epoch + ":" }

// activeEpoch 读取当前 epoch 指针（缺失 = "0" 引导态）。
func (m *Manager) activeEpoch(ctx context.Context) (string, error) {
	v, err := m.rdb.Get(ctx, activeKey).Result()
	if err == redis.Nil {
		return "0", nil
	}
	if err != nil {
		return "", fmt.Errorf("get active reservation epoch: %w", err)
	}
	return v, nil
}

// defaultNodeCPUOversubscribe 是历史硬编码的节点 CPU 超售比，作为 Acquire
// 存量调用方的兼容默认。机器放置必须走 AcquireR 显式传入与调度器一致的 R
// （契约 C-3），否则双保险与硬准入对“节点满”的判定会发散。
const defaultNodeCPUOversubscribe = 4.0

// Acquire 原子预约：检查节点硬上限与项目配额后记账并写 TTL 记录。
// v1.2-E（ADR-0035）：diskMib 为有效磁盘承诺；nodeDiskTotal/projectQuotaDisk
// 为 0 表示该维度不限（旧节点/未配置配额的兼容语义）。节点 CPU 超售比取
// 兼容默认 4（vcpu=0 的预约沿用历史语义）。
func (m *Manager) Acquire(ctx context.Context, opID, nodeID, projectID string,
	vcpu, memMib, diskMib uint64, nodeVCPUTotal, nodeMemTotal, nodeDiskTotal uint64,
	projectQuotaVCPU, projectQuotaMem, projectQuotaDisk uint64, projectMachineQuotaOpt ...uint64,
) error {
	return m.AcquireR(ctx, opID, nodeID, projectID, vcpu, memMib, diskMib,
		nodeVCPUTotal, nodeMemTotal, nodeDiskTotal,
		projectQuotaVCPU, projectQuotaMem, projectQuotaDisk,
		defaultNodeCPUOversubscribe, projectMachineQuotaOpt...)
}

// AcquireR 与 Acquire 相同，但节点 CPU 超售比由调用方显式给出（契约 C-3：
// 必须与调度器 BestOfKConfig.R 同源；<=0 时退到兼容默认 4）。
func (m *Manager) AcquireR(ctx context.Context, opID, nodeID, projectID string,
	vcpu, memMib, diskMib uint64, nodeVCPUTotal, nodeMemTotal, nodeDiskTotal uint64,
	projectQuotaVCPU, projectQuotaMem, projectQuotaDisk uint64, cpuOversubscribe float64,
	projectMachineQuotaOpt ...uint64,
) error {
	if cpuOversubscribe <= 0 {
		cpuOversubscribe = defaultNodeCPUOversubscribe
	}
	var projectMachineQuota uint64
	machines := uint64(1)
	if len(projectMachineQuotaOpt) > 0 {
		projectMachineQuota = projectMachineQuotaOpt[0]
	}
	if len(projectMachineQuotaOpt) > 1 {
		machines = projectMachineQuotaOpt[1]
	}
	res, err := m.acquire.Run(ctx, m.rdb,
		[]string{activeKey},
		vcpu, memMib, nodeVCPUTotal, nodeMemTotal,
		projectQuotaVCPU, projectQuotaMem,
		int64(m.ttl.Seconds()), nodeID, projectID,
		diskMib, nodeDiskTotal, projectQuotaDisk, machines, projectMachineQuota, cpuOversubscribe,
		opID).Int()
	if err != nil {
		return fmt.Errorf("reservation acquire %s: %w", opID, err)
	}
	switch res {
	case 1:
		return nil
	case -1, -2, -6, -7:
		return ErrProjectQuota
	case -3, -4, -5:
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
	_, err = m.release.Run(ctx, m.rdb, []string{activeKey}, opID).Int()
	if err != nil {
		return fmt.Errorf("reservation release %s: %w", opID, err)
	}
	return nil
}

// Get 读取预约记录；不存在返回 nil。读取经 get 脚本在 active epoch 内
// 原子解析，不会把指针切换读成半态。
func (m *Manager) Get(ctx context.Context, opID string) (*Record, error) {
	raw, err := m.get.Run(ctx, m.rdb, []string{activeKey}, opID).Text()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get reservation %s: %w", opID, err)
	}
	var rec Record
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return nil, fmt.Errorf("parse reservation %s: %w", opID, err)
	}
	return &rec, nil
}

// PendingByNode 返回每个节点的在途预约承诺（调度器可参考；PG 推导为主）。
// D-3：经 active epoch 的 nodes 成员集合枚举，不 KEYS/SCAN。
func (m *Manager) PendingByNode(ctx context.Context) (map[string][2]int64, error) {
	out := map[string][2]int64{}
	epoch, err := m.activeEpoch(ctx)
	if err != nil {
		return nil, err
	}
	pfx := epochPfx(epoch)
	nodeIDs, err := m.rdb.SMembers(ctx, pfx+"nodes").Result()
	if err != nil {
		return nil, fmt.Errorf("list reservation nodes: %w", err)
	}
	for _, nodeID := range nodeIDs {
		key := pfx + "node:" + nodeID
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
	return out, nil
}

// PruneStaleOps 删除不在 activeOps 集合中的 op 记录（P2-2）。
// 仅删 op 键、不动 hash：hash 是派生状态，由随后的 Reset（epoch 切换）
// 按存活 op 记录全量重建。返回删除的键数。activeOps 来自 PG 在途
// （PENDING/CLAIMED）create 操作集合。
// D-3：只作用于"先行 active epoch"（本方法调用时的指针值）。与 Reset
// 都在单写者（M2a leader）重建周期内顺序调用，两步之间无并发 Acquire
// 跨指针的一致性问题；即使多走一个周期，漏删的陈旧 op 也随租约 TTL 自愈。
func (m *Manager) PruneStaleOps(ctx context.Context, activeOps map[string]bool) (int, error) {
	released := 0
	epoch, err := m.activeEpoch(ctx)
	if err != nil {
		return 0, err
	}
	pfx := epochPfx(epoch)
	opIDs, err := m.rdb.SMembers(ctx, pfx+"ops").Result()
	if err != nil {
		return 0, fmt.Errorf("list reservation ops: %w", err)
	}
	for _, opID := range opIDs {
		if activeOps[opID] {
			continue
		}
		pipe := m.rdb.Pipeline()
		pipe.Del(ctx, pfx+"op:"+opID)
		pipe.SRem(ctx, pfx+"ops", opID)
		if _, err := pipe.Exec(ctx); err != nil {
			return released, fmt.Errorf("prune stale op %s: %w", opID, err)
		}
		released++
	}
	return released, nil
}

// Reset 全量重建预约 hash（D-3 epoch 切换）：原子执行"分配新 epoch → 从
// 旧 epoch 存活 op 记录重放新 epoch 账 → 切换 active 指针 → 旧 epoch 键挂
// 租约 TTL 自然过期"。必须在 PruneStaleOps 之后调用（否则已死操作的增量
// 会被重放回来），只在单写者（M2a leader）重建周期内执行。
// 返回为过期的旧 epoch hash 键数（观测口径沿用旧实现的"清理键数"语义）。
func (m *Manager) Reset(ctx context.Context) (int, error) {
	n, err := m.reset.Run(ctx, m.rdb,
		[]string{activeKey, epochSeqKey}, int64(m.ttl.Seconds())).Int()
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
		_, _ = fmt.Sscanf(t, "%d", &n)
		return n
	default:
		return 0
	}
}
