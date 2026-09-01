# ADR-0031：Volume、Snapshot 与 Template 的内容寻址工件面

状态：提议（v2）
补充：ADR-0003、ADR-0028、ADR-0029。

## 背景

v1.x snapshot 和 volume 以节点本地状态为主，无法在节点永久丢失后恢复，也无法安全支撑多集群。需要把 durable content 与本地 materialization 分离。

## 决策

1. PG 保存 artifact manifest、version、lineage、reference、replica、operation 和 encryption key version；S3-compatible object storage 保存不可变内容；agent 本地只保存可丢缓存；Redis 只保存短期传输租约。
2. artifact 类型包括 volume version、filesystem snapshot、memory snapshot 和 template，类型间不共享未经验证的解释语义。
3. 内容以 chunk/object digest 寻址；manifest 包含有序引用、总大小、checksum、压缩、父 lineage 和 compatibility metadata。
4. READY 必须晚于全部对象上传、checksum 验证和 manifest 原子发布。半成品永不对 attach/restore 可见。
5. 上传下载可续传；token 短时并绑定 project、artifact、operation、direction 和 audience；主 API 不代理大文件。
6. 删除先 tombstone，再等待引用和传输 lease 归零，最后清理对象。引用计数用于快速路径，周期 mark-and-sweep 用于校验与修复。
7. 对象存储是 durable content 权威，但不是 deployment/machine 业务状态权威；后者仍在 PG。
8. 加密、key version、checksum 和来源进入 manifest；日志不得包含 token 或对象密钥。

## 理由

统一工件生命周期和传输协议可以支持跨节点恢复与多集群复制，同时不改变各资源的业务状态机。

## 后果

- 需要独立 content service/SDK、对象存储故障模型和大对象基准；
- node-local hypeman 格式需通过 versioned adapter 导入/导出；
- 不承诺跨集群 RW block coherence；
- v2 build、portable snapshot 和 multi-cluster 都依赖本 ADR。
