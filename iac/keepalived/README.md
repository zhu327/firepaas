# keepalived VIP（M4，ADR-0011）

edge 入口高可用：两个 control 节点间 keepalived 托管唯一 VIP，泛域名
`*.<domain>` 只解析 VIP。DNS 轮询保留为无二层环境的降级路径。

## 组件

- `keepalived-edge.conf.tmpl`：双节点 keepalived 配置模板（占位符见文件头注释）。
- 健康探测：`track_script` curl 本节点 edge `:80/healthz`（edge 挂 → 放弃 VIP，
  流量漂移到对端）。

## 部署步骤（每节点）

```bash
sudo apt install -y keepalived   # 或等价包管理
sudo cp iac/keepalived/keepalived-edge.conf.tmpl /etc/keepalived/keepalived.conf
# 编辑占位符：__VIP__ / __INTERFACE__ / __PEER_IP__ / __NODE_IP__ /
#            __NODE_PRIORITY__（150/100）/ __INITIAL_STATE__（MASTER/BACKUP）/
#            __VRRP_PASSWORD__ / __EDGE_CHECK_PORT__
sudo systemctl enable --now keepalived

# 验证（MASTER 节点）：
ip addr show <INTERFACE> | grep <VIP>       # VIP 应在 MASTER 上
curl -s http://<VIP>/healthz                 # 经 VIP 的明文探针（308 前置）
```

DNS：`*.<domain>` 与 `<domain>` 的 A 记录全部指向 VIP。

## 客户端信任链（运维前置，ADR-0011）

- 实验室：`scripts/lab/gen-certs.sh` 签发的内部 CA——客户端预置
  `scripts/lab/certs/ca.crt`（curl 用法：`curl --cacert ca.crt https://<app>.<domain>/`）。
- 生产：step-ca ACME 按需签发泛域名证书（Caddy 集成），根证书仍由组织
  配置管理（Ansible/MDM）统一预置。**证书有效但客户端不信任 = 部署失败**，
  此项列在 Onboarding 清单里，不是平台功能。

## 降级路径

- 无二层环境（VRRP 组播与 unicast 均不可用）：不部署 keepalived，保留
  DNS 轮询形态（`*.<domain>` → 各 edge 实例地址）。接受单 edge 故障时
  部分解析失败（ADR-0011 决策表）。
- 多 edge 实例（>2）：每实例独立 TokenClient 内存缓存（凭 execution-bound
  语义各自拉取，无共享状态）；VIP 集群与实例数无关（keepalived 只管 VIP
  归属，流量由 DNS/VIP 进入后仍按 hostname 均衡）。

## 已知边界（单机实验室未覆盖）

- VIP 漂移的端到端演练（kill MASTER keepalived / 拔 edge 进程）需要双
  control 节点，标记 DEFERRED-MULTI-NODE。
- VRRP 通告经明文 auth_pass 仅防误配置，不防恶意注入——受信内部网络
  边界假设（与 mvp-plan §2.3 一致）。
