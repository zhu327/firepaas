# ADR-0029：Node-local volume 状态所有权、locality 与 attachment fencing

状态：已接受（v1.3）
补充：ADR-0003、ADR-0009、ADR-0021。

## 背景

agent proto 已有 VolumeMount，hypeman 有本地 volume primitive，但 firepaas 没有 volume 业务事实、attachment fencing、放置约束或节点故障语义。

## 决策

1. PG 是 volume 和 attachment 业务事实权威；agent 是本地 materialization/attachment 观测权威。
2. 本 ADR 首先交付通用 volume/attachment 框架与 `LOCAL_RW`：单写、硬钉 origin node。`DATASET_RO` 的导入、seal、多挂载和 CoW 生命周期由 ADR-0030 单独定义。
3. attachment 绑定 machine/execution；所有 attach/detach 带 generation/operation fencing，旧 execution 不得卸载新代卷。
4. 调度过滤顺序中，volume locality 位于资源和反亲和之前；locality 与反亲和冲突时 locality 胜出并记录降级事件。
5. LOCAL_RW 所在节点失联时 volume 进入 `UNAVAILABLE`；不得在其他节点创建空同名卷或把 machine 当无状态重建。
6. 有 LOCAL_RW attachment 的 machine 不适用现有 delete-then-recreate evacuate；必须显式 detach、等待节点恢复或由操作者接受数据丢失。
7. LOCAL_RW 首版只支持空卷创建和已有本地卷登记，不开放 archive 导入。
8. volume 容量进入 project quota、scheduler reservation 和 agent 磁盘硬准入。

## 理由

本地卷首先是调度和故障语义，而不是 CRUD。未建模 locality 就开放挂载会破坏现有无状态重建与 evacuate 不变量。

## 后果

- 修改 ADR-0009 的过滤顺序和 ADR-0021 的驱离限制；
- 不支持 RW 多挂载、热挂载、在线 resize、跨节点恢复或 durability SLA；
- 节点故障必须向用户返回明确 unavailable 状态；
- v2 content plane 在该模型上增加 durable versions，而非替换 attachment fencing。
