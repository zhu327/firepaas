# tools/cli:firepaas CLI(P3 提供)

MVP 命令集(与用户故事对齐):

```
firepaas login <api-url> <token>
firepaas projects list
firepaas apps create <name>
firepaas apps deploy <app> --image registry.local/nginx:1.27
firepaas apps scale <app> <n>
firepaas apps logs <app> [-f]
firepaas apps exec <app> -- <cmd>
firepaas apps env set <app> KEY=VAL
firepaas apps delete <app>
firepaas images list / pull
firepaas volumes create / attach
```

实现:直接消费控制面 OpenAPI(P3.4 与 API 一起交付),CLI 不做独立状态。
