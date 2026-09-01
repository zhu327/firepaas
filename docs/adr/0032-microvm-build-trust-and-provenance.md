# ADR-0032：MicroVM Build 的信任边界、队列与 Provenance

状态：提议（v2）
依赖：ADR-0024、ADR-0027、ADR-0031。

## 背景

从源码/Dockerfile 构建会执行不可信构建步骤、接触 registry 凭证和 secret，并产生高 CPU/IO 波动。不能把 hypeman 的单机内存队列直接暴露为平台服务。

## 决策

1. build、attempt、worker 和结果 digest 是 PG 业务事实；队列持久化，leader/worker 重启后可重派，attempt 使用幂等键。
2. build worker 使用独立 node pool；每个 attempt 在一次性 microVM 中运行 rootless BuildKit，默认无 host privileged 能力。
3. source 通过 artifact plane 上传，API 不缓冲正文；输出写 OCI registry，并只以 digest 进入 deployment。
4. registry credential 为短时 repository-scoped token；build secret 仅经 one-shot guest channel，禁止写入 layer、cache、日志和 provenance。
5. build egress 必须受 ADR-0027 policy 约束；基础镜像使用 digest，mutable tag 只在注册时解析一次。
6. cache 以 project/trust-domain 隔离；跨租户共享只允许经过安全评审的不可变 public base。
7. provenance 至少记录 source、Dockerfile、base、output digest，BuildKit/runtime 版本、policy generation 和时间；生成 SBOM，签名作为后续增强。
8. project 配额覆盖并发、CPU、内存、磁盘、输出大小和最长构建时间。

## 理由

构建是供应链执行面，必须拥有独立信任边界、持久状态和可追溯输出，不能只作为 deploy handler 内的子进程。

## 后果

- 增加 build API/controller、专用节点池和 artifact/registry 集成；
- 不自研 Dockerfile frontend、Git hosting 或完整 CI；
- build 成功不自动部署生产；
- 需要恶意 source、cache poisoning、secret canary、worker crash 和 build storm 测试。
