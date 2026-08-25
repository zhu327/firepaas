# tests/e2e

端到端测试(P1.5 开始填充),场景与 mvp-plan.md 的用户故事对齐:

- U1 创建/部署/域名访问
- U2 滚动替换零错误切换
- U3 scale 3 + 故障重建
- U4 scale-to-zero 唤醒
- v1.1：node-local 卷持久化（不属于首个无状态 MVP）
- U6 多 project 隔离

约定:每个场景输出 `scenario`、`steps`、`assertions`,可重复运行,幂等。

最小 harness(一键拉起 dev 集群 + U1 冒烟脚本)随 M1 工程基线交付;各里程碑验收场景在此之上逐步落地,不靠手工。
