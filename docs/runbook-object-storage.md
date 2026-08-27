# Runbook：对象存储恢复（MinIO/artifacts）

## 平台责任边界（单机实验室形态）

- **registry**（:5000）：唯一"半权威"镜像来源；镜像 digest 由 firepaas 部署时
  解析并在 hypeman 侧缓存。registry 卷丢失 → 在途 VM 不受影响，新建 VM 失败。
- **MinIO**（:9000）：实验室存放镜像 manifest 缓存/实验工件。恢复目标 = 条目
  一致（`scripts/lab/minio-backup-rehearsal.sh` 断言）。
- **VM 快照**：**node-local**（hypeman guest 目录）——不进对象存储；
  节点丢失即快照丢失，契约上是"restore 失败 → 冷启动重建"（M4.5 已实现该降级）。

## 备份

```bash
sudo bash scripts/lab/pg-backup.sh            # PG 全库
sudo bash scripts/lab/minio-backup-rehearsal.sh  # MinIO 卷镜像
```

备份保留策略：PG 7 份（脚本内置）；MinIO 排练目录手动轮转
（`/var/lib/firepaas-p0/backups/minio-rehearsal`）。

## 恢复

1. registry：`docker compose -f iac/dev/docker-compose.yaml up -d registry` 后
   `mc mirror backup dir aliyun?`——实验室最简路径为重新推送 ontime 等测试镜像
   （registry 内容可重建；生产形态用 registry 自身的 GC/备份）。
2. MinIO：反向 `docker cp` 排练目录 → 容器 `/data`，重启 minio 容器。
3. PG：见 `scripts/lab/pg-restore-rehearsal.sh`；生产恢复流程 = 停 API →
   restore 至新库 → 改 DSN → 起 API → 显式重投影（`POST /v1/system/reprojections`）。

## 演练节奏

每月一次 backup+rehearsal；72h soak 期间按 `scripts/lab/soak-m5.sh` 快照节奏
附带备份断言。
