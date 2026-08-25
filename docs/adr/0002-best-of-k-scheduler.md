# ADR-0002:VM 放置采用移植的 Best-of-K 算法,先单写者后多写者

状态:已接受(2026-08-25)
依据:可行性分析第 3.2 节与附录 D.2

## 决策

1. 控制面实现 Best-of-K 放置:
   `score = (req + allocated + pending + α·usage) / (R·capacity)`,
   默认 R=4、K=3、α=0.5,CPU/内存分维度打分,内存超售独立系数(默认 1.0)。
2. 重试语义:ResourceExhausted 换节点、硬错误排除、最多 3 次;resume 优先 origin node。
3. 记账:in-progress 计入 pending → 成功后 optimistic add → 20s ServiceInfo 校正。
4. 分两步落地:M2a 单写者(leader election 保证同一时刻只有一个 API 实例放置);
   M2b 多写者(Redis Lua 预约 + execution_id CAS)。

## 理由

- 算法有 e2b 现成实现与测试可移植(`infra/packages/api/internal/orchestrator/placement/`)。
- 单写者先行可消灭 80% 双记账场景,同时保留完整打分/重试逻辑,风险最小。
- agent 侧以真实 cgroup/进程状态做准入硬校验,与调度器软决策形成双保险。

## 后果

- 控制面包含一个需要严格测试的调度组件(单元 + 仿真 + 混沌)。
- API 横向扩容依赖 Redis 预约完成(M2b),在此之前 API 为单写者部署。
