// Command agentd 是 firepaas 的节点数据面 agent。
//
// 目标形态(见 docs/mvp-plan.md P1):
//   - 从 hypeman 抽取 lib/hypervisor、lib/instances、lib/images、lib/snapshot、lib/network
//   - 暴露 gRPC:InfoService / MachineService / ImageService(protos/agent/v1/agent.proto)
//   - 每节点一个,由 Nomad system job 以 raw_exec 运行(root)
//
// 当前为骨架,尚未接线。
package main

import "fmt"

func main() {
	fmt.Println("firepaas agentd: skeleton, see docs/mvp-plan.md")
}
