# ADR-0001:Nomad 只编排基础设施,不调度用户 VM

状态:已接受(2026-08-25)
依据:可行性分析第 3.1 节

## 决策

- Nomad 负责:部署 agent(system job + raw_exec)、control-plane/edge(service job)、
  Redis/可观测等基础设施作业;native service registration 提供节点发现;
  autoscaler 插件对齐作业副本与节点池。
- **用户 VM 不由 Nomad 调度**,由控制面自研 Best-of-K 调度器选节点后,
  通过 gRPC 让节点 agent 创建 Firecracker microVM。

## 理由

1. e2b-dev/infra 生产验证了该模式(`iac/modules/job-orchestrator/jobs/orchestrator.hcl`
   的 `type="system"` + `raw_exec`,sandbox 全部由 orchestrator 进程内管理)。
2. VM 放置需要快照/镜像缓存亲和、自定义超售分、ResourceExhausted 快速重试等语义,
   Nomad 原生调度器表达不了。
3. Nomad 社区 Firecracker driver 成熟度不足,且不支持 snapshot/restore。

## 后果

- 需要自研并维护调度器与对账循环(成本可控,见 ADR-0002)。
- agent 是 Nomad system job,意味着"每节点恰好一个 agent"由 Nomad 保证。
