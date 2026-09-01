# ADR-0020:edge 并发控制(hard 拒绝线)与 least-inflight 后端选择

状态:已接受(2026-08-28)
依据:对照评审(docs/fp.md §7.5,对齐 fly-proxy 的 soft/hard 并发语义)。现状:
edge 对 eligible backends 做确定性 round-robin(`counter % len(eligible)`),单副本
变慢时持续接收 1/N 流量且无过载拒绝手段;per-hostname 令牌桶只防外部洪泛,
不反映实例真实负载。

## 决策

### 1. per-machine inflight 计数(edge 内存)

- edge 为每个 backend machine 维护原子 inflight 计数:请求进入 eligible 选择后
  +1,响应完成(含流式/WS 连接关闭)后 -1;
- 计数是 **edge 本地视角**(多 edge 各自计数,与 token 缓存同形态):每个 edge
  只见自己转发的份额,fly.io 的并发控制同样是 per-edge 语义。文档标注该局限;
  跨 edge 全局并发是 v1.2+ 的分布式计数问题,不进 v1.1。

### 2. 后端选择:least-inflight + 随机抖动

eligible 集合内按 inflight 升序选择,相同 inflight 随机抖动(打散同分);
替换现有 round-robin 计数器。**soft 语义由此天然承担**:busy 实例自动靠后,
不再接收新请求偏好——这正是 fly "soft limit 满则分给其他实例"的实现形态,
无需独立参数。

### 3. hard 拒绝线

- `FIREPAAS_EDGE_HARD_CONCURRENCY`(默认 256/machine):当**最闲的** eligible
  backend inflight 也 ≥ hard → 503 + `Retry-After` + 计数器(受控降级,与
  serve-stale 超窗 503 同风格);
- hard 是全局配置起步(env);**app 级 soft/hard 配置明确延后 v1.2**(需要
  catalog 契约扩展,单独立项);
- 钉扎请求(ADR-0019)跳过选择但**不豁免 hard**:钉扎目标超限同样 503,
  防止调试流量打死实例。

### 4. 观测

- `/metrics` 新增:`firepaas_edge_backend_inflight`(per machine gauge,随
  route 缓存失效清理防泄漏)、`firepaas_edge_hard_rejected_total`、
  选择耗时可忽略(内存原子操作)。

## 理由

1. round-robin 的"公平"在实例异构(慢盘/慢依赖)时是反模式:least-inflight 是
   无外部依赖的最小修正;
2. fly 的 soft/hard 二段语义中,soft 的本质是"偏好更闲实例"——least-inflight
   已完整表达;hard 的本质是"显式过载失败优于隐性雪崩"——一个阈值一个 503
   即可。两者都不需要 app 级配置就能交付核心价值,故 v1.1 不动 catalog 契约;
3. inflight 是请求生命周期维度(长连接持续占用),比连接速率更接近真实负载,
   且实现只是计数器包装。

## 后果

- edge 选择算法变更需回归:M4 的 e2e(serve-stale/403 重试/token)全绿是闸门;
- WS/SSE 长连接会长期占用 inflight——这是预期语义(真实负载),e2e 增加断言
  连接关闭后计数归零;
- 慢副本场景 e2e:注入延迟副本,断言流量分配显著偏离 1/N 偏向快副本;
- 多 edge 环境下 hard 阈值语义按"每 edge 各自 N"理解,runbook 说明集群容量
  上限 = N × edge 数(近似,忽略入口不均);
- 未来 app 级 soft/hard(catalog 扩展)与跨 edge 全局计数均以本 ADR 的
  per-edge 语义为基线演进。
