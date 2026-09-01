# Runbook：host hardening（实验室/生产）

审计入口：`sudo bash scripts/lab/host-hardening-check.sh`（**只读**，绝不写
系统配置）。输出 PASS/WARN/FAIL + 本次结论；WARN 不会覆盖之前的 FAIL。sshd 检查
使用 `sshd -T` 的生效配置（含 Include/Match），无法取得生效配置时只 WARN，不以
原始 `sshd_config` 产生错误结论。凭证扫描仅计数、不输出匹配内容，且不再截断候选
文件列表。

## 实验室形态（当前执行结果）

- 内核随机化/链接保护/ip_forward=1（slot 数据面必需）/dev/kvm 权限通过。
- WARN 项（实验室可接受，迁移生产前关闭）：sshd PermitRootLogin、
  docker.sock 归 docker 组、SUID 面大小。
- 任何 FAIL（如 `/dev/kvm` 缺失）必须当日解决——firepaas 不可运行。审计脚本的
  退出码仅代表 FAIL；WARN 须记录风险接受或修复计划。

## 生产实施清单（每次装机）

1. `kernel.randomize_va_space=2`、`fs.protected_hardlinks/symlinks=1`。
2. sshd：`PermitRootLogin no` + 密钥登录；审计登录事件。
3. docker.sock 仅 root 或专门的构建机用户组。
4. systemd `/etc/sysctl.d/99-firepaas.conf`：
   `net.ipv4.ip_forward=1 net.ipv4.conf.all.rp_filter=1`。
5. 明文凭证纪律：API_TOKEN/主密钥只经环境注入；日志/历史扫描钩入审计
   （hardening 脚本 §5 已有扫描段，接 cron.failure 即告警）。
6. 主密钥轮换/KMS：由 mvp-plan §8 遗留项在 v1.1 落（ADR-0010 已留口）。

## 违规响应

审计 FAIL/WARN 项产生时按标签逐条归档至 `scripts/lab/results/m5/`，修复后
重跑脚本并把 diff 记录进治理变更单。对 firepaas 数据面（ip_forward/kvm/
netns 权限）相关的回归，回滚优先级最高，且必须跑 e2e-m5 F 段零泄漏。
