// Command api 是 firepaas 控制面入口。
//
// 目标形态(见 docs/mvp-plan.md):
//   - REST:projects / apps / deployments / machines / images / volumes / secrets
//   - 自研 Best-of-K 调度器(control-plane/internal/scheduler)
//   - Postgres 持久实体 + Redis 运行态/路由目录
//   - 内部 gRPC 供 edge-proxy 调用 ResumeMachine(自动唤醒)
//
// 当前为骨架,尚未接线。
package main

import "fmt"

func main() {
	fmt.Println("firepaas control-plane api: skeleton, see docs/mvp-plan.md")
}
