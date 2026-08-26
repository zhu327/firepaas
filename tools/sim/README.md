# tools/sim：调度仿真器（M2.6 已落地）

目的：在真实集群之外，用虚拟节点验证 Best-of-K 放置质量与记账不变量。

运行：`make sim`（等价 `go run ./tools/sim -n 100000 -seed 42`）。

断言不变量：

1. 过滤先于打分：ScoreHook 观测到的候选必须已通过全部过滤。
2. 硬准入不破：任何放置后 allocated ≤ R·capacity（CPU R=4，内存 R=1.0）。
3. 无重复逻辑副本：machine_id 唯一。
4. 失联节点不再入选。
5. DEPLOYMENT 反亲和：候选充足时副本落点 distinct（候选不足记降级事件）。

场景：5 个虚拟节点（4 compute + 1 control 测 node_pool 过滤）、30 个
deployment、随机请求流、每 500 轮注入节点失联、每 10 轮释放一台 machine。
任何断言违反立即以非零退出并打印违反轮次与上下文。

参考：e2b 的 placement benchmark（`infra/packages/api/internal/orchestrator/placement/placement_benchmark_test.go`）。
