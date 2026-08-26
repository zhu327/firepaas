// Command edge-proxy 是 firepaas 的边缘路由。
//
// 目标形态(见 docs/mvp-plan.md P3):
//   - 终止 TLS(复用 hypeman 的 Caddy 集成)
//   - hostname 解析 -> Redis routing catalog -> 节点 agent proxy -> slot -> VM
//   - paused machine 收到流量时调用控制面 gRPC ResumeMachine 自动唤醒
//
// 当前为骨架,尚未接线。
package main

import "fmt"

func main() {
	fmt.Println("firepaas edge-proxy: skeleton, see docs/mvp-plan.md")
}
