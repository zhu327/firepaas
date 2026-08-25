# tools/sim:调度仿真器(P2.5)

目的:在接入真实集群前,用注入的节点指标验证 Best-of-K 放置质量与记账不变量。

输入:场景 JSON(节点数、容量、负载曲线、创建请求流、故障事件)

断言不变量:

1. 稳态 `allocated <= R * capacity`(任何时刻)
2. 无重复 machine_id
3. 放置偏差:最忙/最闲节点 committed 差 < 20%
4. 注入节点失联后,新放置不再落在该节点
5. API/agent 崩溃注入后,对账循环 2 分钟内收敛

参考:e2b 的 placement benchmark(`infra/packages/api/internal/orchestrator/placement/placement_benchmark_test.go`)。
