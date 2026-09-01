# ADR-0019:实例标识响应头与调试钉扎(edge 轻量可观测契约)

状态:已接受(2026-08-28)
依据:对照评审(docs/fp.md §7.5 的 fly-instance-id 响应头与 fp-replay 语义)。
现状:多副本 app 的排障无法回答"这个请求命中了哪个副本",也无法把调试会话
钉在指定实例上复现问题。

## 决策

### 1. 响应头:命中实例标识

edge 在代理响应上设置 `X-Firepaas-Machine-ID: <machine_id>`(命中 backend 的
machine id;可附带 `X-Firepaas-Execution-ID`)。覆盖所有路径:正常转发、
serve-stale、403 后 invalidate+重试后的最终命中。命名与现有 `X-Firepaas-Stale`
风格一致。

注意:edge→agent proxy 方向已存在**请求头** `X-Firepaas-Machine-ID`(路由寻址用);
本决策增加的是 edge→客户端方向的**响应头**,同名不同向,不冲突,但文档必须
显式区分两个方向的语义。

### 2. 请求头:调试钉扎

客户端请求携带 `X-Firepaas-Pin-Machine: <machine_id>` 时:

- 该 machine 在当前 route 的 **eligible 集合**(非 draining 且 readiness 可服务,
  与正常选择同一过滤)内 → 直选该 backend,跳过负载均衡算法(含 V1.1-C 的
  least-inflight);钉扎优先于并发控制的选择层,但仍受 hard 上限约束;
- 不在 eligible 集合(不存在/已换代/NOT_READY/draining)→ **404** 并附说明头,
  显式失败。选 404 而非 503:503 语义保留给"路由可达但平台侧不可用",钉错
  id 属于客户端错误,必须可区分。

**这是调试契约,不是负载均衡契约**:平台不承诺钉扎请求的路由稳定性(实例换代后
404,客户端重新取 id);文档与 CLI 帮助中明确标注。不引入鉴权(钉扎等价于
客户端反复重试直到命中,无越权面)。

### 3. 使用闭环

`curl -si https://app.internal | grep X-Firepaas-Machine-ID` 取得实例 id →
后续请求带 `X-Firepaas-Pin-Machine` 复现该实例上的问题。fpctl 增加便捷参数
(`fpctl app curl --pin` 形态可选,v1.1 以文档为准)。

## 理由

1. 成本极低(纯 edge 内部,一行响应头 + 一个请求头分支),却直接补齐多副本
   排障的"最后一公里";
2. 钉扎是 fly.io fp-replay/fly-instance-id 组合在 HTTP 语义下的最小等价物
   (fp-replay 的跨 edge 重放依赖多 edge 集群拓扑,当前 DNS 轮询形态下无必要,
   记录为未来多 edge 时的扩展点);
3. 404 显式失败避免"钉错实例却静默落到别的副本"造成的调试误导。

## 后果

- edge 新增钉扎命中计数(`firepaas_edge_pin_hits_total`)与钉扎 404 计数;
- e2e 新增:钉扎请求 100% 命中(响应头校验)、钉错 id 404、standby 实例钉扎
  触发唤醒(与 ADR-0017 语义组合);
- 文档(runbook)增加"多副本排障流程"小节;
- 本契约与 V1.1-C 的并发控制共存时,钉扎路径不计入 least-inflight 选择,但
  inflight 计数仍累计(该实例的真实负载可见)。
