# ADR-0022:多端口 services 模型(hostname × port 路由)

状态:已接受(2026-08-28)
依据:对照评审(docs/fp.md §3.2/§7.5 的 fly `[[services]]` 模型)。现状:
deployments 只有单一 `port` 字段;catalog 的 route key 已经是
`route:{hostname}:{port}`(hostname+port 维度天然存在),但 hostidx 索引限定
"每 hostname 一个活跃端口"(M1 语义)。需求:一个 app 同时暴露 HTTP 主服务与
附加端口(gRPC、内部管理端点、TCP 类服务)。

## 决策

### 1. 数据模型

- `deployments.port` → `services jsonb`:`[{name, internal_port}]`,主 service
  (第一条)继承现有语义(路由默认端口、readiness 探针目标);
- 迁移:存量单 port 转一条 `[{name: "default", internal_port: <port>}]`;
- v1.1 **裁剪**:不做 fly 的 handler 矩阵(http/tls/tcp 语义)、不做
  per-service 公网端口映射与 dedicated IP、不做 per-service 并发配置。所有
  service 共享 app 的 hostname、TLS 与并发控制。

### 2. 契约

- proto:`MachineSpec.services`(repeated `ServiceSpec{name, internal_port}`);
  保留 `network.ingress_port` 兼容读——单端口声明行为完全不变(旧客户端/旧
  agent 兼容);
- catalog:hostidx 从 `hostname → port` 演进为 `hostname → [ports]` 集合;
  每 service 发布一条 `route:{hostname}:{public_port}`,backend 的 `AppPort` =
  该 service 的 `internal_port`。public_port 默认 = internal_port(v1.1 不做
  端口映射改写);
- edge→proxy 请求头:`X-Firepaas-App-Port: <internal_port>`(edge 从命中的
  route 取 AppPort 设置);**头缺失 = 旧行为**(proxy 转发到 spec 声明的单端口),
  向后兼容;
- agent proxy:端口白名单从单一 ingress port 扩为 services 集合;未声明端口
  一律拒绝(白名单语义与 M4 一致)。

### 3. edge 监听形态

- 附加端口监听由 `FIREPAAS_EDGE_EXTRA_PORTS` 声明(逗号列表/区间,默认关闭);
- 80/443 的请求按 (hostname, 目标端口) 查路由:主 service 在 80/443(现有
  行为),附加 service 在其声明端口;
- 未声明端口访问 → 404(与权威 miss 语义一致,P2-8);

### 4. readiness(v1.1 限制)

每 machine 仍**单一探针**(指向主 service,ADR-0008 不变);附加 service 的
readiness 跟随主 service。理由:per-service 独立探针需要 health tracker 按
service 维度扩展(agent 契约与状态机改动),与多端口路由本身解耦,记录为
v1.2 候选;主 service 未 READY 时整个 machine 摘除,是保守正确的方向。

### 5. 交互语义

- **auto-standby(ADR-0017)**:conntrack 的 IgnoreDestinationPorts 由平台注入
  "非主 service 端口无入站时仍可入睡"的判定基础?否——conntrack 观察的是真实
  目标端口,附加端口的入站连接天然计入活跃;平台只在探针源排除(ADR-0017
  决策 3)上注入。附加 service 空闲不阻止主 service 判定,符合直觉;
- **evacuate/滚动发布**:machine 级操作与端口无关,天然覆盖全部 services;
- **并发控制(ADR-0020)**:inflight 按 machine 维度(所有端口合计),per-service
  计数是 v1.2 候选。

## 理由

1. route key 的端口维度已存在(`route:{hostname}:{port}`),扩展集中在 hostidx
   与 edge/proxy 的端口传递——契约变更面收敛;
2. 单探针 + 共享并发 + 无 handler 矩阵的裁剪让 v1.1 只交付"多端口可路由"的
   核心价值,TCP 透传/SNI/证书分 service 等深水区明确延后;
3. `X-Firepaas-App-Port` 头缺失即旧行为的设计使 edge/proxy 可以独立滚动升级
   (旧 proxy 先上、新 edge 后上,或反之)。

## 后果

- migrations:deployments.services;PG routes/route_backends 逻辑不变(端口
  维度已在);
- catalog hostidx 演进需要 controller/edge 同版本发布;投影重建路径(FLUSHALL
  演练,M4 验收 F 步)重跑;
- API/CLI:deploy 接受重复 `--port`/`--port name=8081`;fpctl status 展示
  services;存量单端口 app 零迁移成本;
- e2e 新增:双端口 app 分别路由、未声明端口 404、proxy 拒绝未声明端口、
  单端口 app 全量回归(e2e-m3/m4/m5);
- 每条 service 一条 route 意味着投影条目数 = app 数 × service 数,容量影响
  可忽略(内部规模)。
