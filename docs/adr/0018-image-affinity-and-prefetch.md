# ADR-0018:调度器镜像缓存亲和与部署时预取

状态:已接受(2026-08-28)
依据:对照评审(docs/fp.md §6.1/§8.1)提出的 image 亲和 δ 罚项与部署 prefetch;
M0 实测未缓存冷启动 p95 7.6s(40MB 镜像)是当前部署体验短板;M3 遗留的"镜像
LRU/预热"DEFERRED 项尚未闭环。现有代码基础:proto 已有 `PullImage`/`ListImages`
RPC,M5.1 的 imagepolicy 已用 ListImages 做大小校验,capacity-model 已定义镜像
缓存 LRU 预算与磁盘水位守护。

## 决策

### 1. 镜像缓存状态进入节点视图

- proto `ServiceInfoResponse` 增加 `repeated string cached_image_digests`
  (digest-pinned,LRU 序,上限截断如 512 条;agent 由 `ListImages` 派生);
- PG `nodes` 增加 `image_cache jsonb`,随 nodemanager 既有 20s ServiceInfo sync
  落库;scheduler 的 `Node` 视图同步扩展。

### 2. 亲和是打分项,不是硬过滤

score 增加 `WeightImage · imageMiss(n, digest)`:目标镜像 digest 在节点缓存内
=0,不在=1。维持 ADR-0009 的"先过滤后打分"管线——镜像亲和**只出现在打分层**,
任何节点都不因镜像未缓存而被过滤(否则新镜像永远无候选)。`WeightImage` 默认 0.5,
与 R/K/α/WeightCPU 同一配置面,可热调。

v1.1 不做按镜像大小/带宽加权的精确成本模型(δ 为常数罚项);仿真器(tools/sim)
增加"缓存命中的节点在同等资源分下必被选中"断言。

### 3. 部署预取(prefetch)

- 时机:rollout 创建后、PREPARING 派发 create 前,控制面向 **top-K 候选节点**
  (按当前 score 排序,K 默认 3)异步下发 `PullImage(digest)`;
- 语义:**尽力而为**——失败/超时不阻塞 rollout、不计入重试预算,只记调度事件
  (`prefetch_failed`);成功与否不影响正确性(副本仍可能落在未缓存节点,冷拉取
  路径不变);
- 预算约束:prefetch 尊重节点镜像 LRU 预算与磁盘水位(capacity-model 既有语义,
  agent 侧 imageretention GC 为兜底);不做节点间镜像互传(P2P 分发明确不做)。

### 4. 与既有机制的边界

- 镜像 digest 解析沿用 M5.1 准入(digest-pinned、registry allowlist)——prefetch
  的输入就是准入通过的 digest,不引入第二套校验;
- R3 换代重建同样吃到亲和(重建请求携带 digest,自然倾向原节点或已缓存节点);
  origin-node 优先语义(ADR-0002)保持不变,亲和仅在 origin 不可达后生效。

## 理由

1. 未缓存冷启动是实测短板,而"镜像在哪个节点热就放哪"是 e2b 模板缓存亲和
   (Best-of-K 同源算法)在生产验证过的思路,移植成本集中在 Node 视图与一个
   罚项;
2. prefetch 把镜像拉取从**请求路径**(create 时冷拉)移到**部署路径**(并行预取),
   是不改数据面的唯一杠杆;
3. 全链路复用既有 RPC 与准入,无新组件、无新协议。

## 后果

- ServiceInfo 体量增加(512×~70B≈35KB 上限,20s 一次可接受;超限截断 LRU 序
  尾部并记 gauge);
- 调度事件新增 `image_miss_penalty`/`prefetch_*` 类别,进入 scheduler_events 审计;
- e2e 新增:未缓存镜像部署断言落点偏好、预取后 READY 时长对比基线、磁盘水位
  下不突破预算;
- v1.1 关闭 M3 遗留"registry LRU/预热"项(共享 registry 生产部署物仍是运维项,
  不在本 ADR 范围)。
