// Command sim 是 M2.6 调度仿真器（mvp-plan §6.6）：用虚拟节点对
// Best-of-K 管线做 10 万次放置断言。
//
// 断言（任何一条违反立即以非零退出）：
//  1. 过滤先于打分：ScoreHook 观测到的候选必须已通过全部过滤。
//  2. 硬准入不破：任何放置后 allocated ≤ R·capacity。
//  3. 无重复逻辑副本：machine_id 唯一。
//  4. 失联节点不再入选。
//  5. DEPLOYMENT 反亲和：候选充足时副本落点 distinct。
//
// 用法：make sim 或 go run ./tools/sim -n 100000 -seed 42
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"

	agentv1 "github.com/zhu327/firepaas/internal/contracts/agentv1"
	"github.com/zhu327/firepaas/internal/scheduler"
)

type simNode struct {
	node       scheduler.Node
	allocVCPU  uint64
	allocMem   uint64
	allocDisk  uint64 // v1.2-E：磁盘承诺记账（与调度器硬过滤同语义）
	lostUntil  int64  // 失联到该迭代为止（0 = 未失联）
	placements int
}

type placedMachine struct {
	disk            uint64 // v1.2-E
	id, nodeID, dep string
	vcpu, mem       uint64
}

func main() {
	n := flag.Int("n", 100000, "放置次数")
	seed := flag.Int64("seed", 42, "随机种子")
	flag.Parse()

	if err := run(*n, *seed); err != nil {
		log.Fatal(err)
	}
}

func run(n int, seed int64) error {
	rnd := rand.New(rand.NewSource(seed))

	nodes := []*simNode{
		mk("n1", "compute", 16, 64<<10),
		mk("n2", "compute", 32, 128<<10),
		mk("n3", "compute", 64, 256<<10),
		mk("n4", "compute", 32, 128<<10),
		mk("n5", "control", 8, 32<<10), // 错误 pool：测 node_pool 过滤
	}
	deployments := map[string]map[string]int{}
	for i := 0; i < 30; i++ {
		deployments[fmt.Sprintf("dep-%02d", i)] = map[string]int{}
	}

	var scoredPassed []string
	placer := scheduler.New(scheduler.DefaultBestOfKConfig(), scheduler.Options{
		ScoreHook: func(nodeID string) { scoredPassed = append(scoredPassed, nodeID) },
	})

	var placed, rejected, degraded, deleted int
	live := map[string]placedMachine{}

	for i := 0; i < n; i++ {
		// 周期性注入节点失联/恢复。
		if i%500 == 0 {
			idx := rnd.Intn(len(nodes) - 1) // 保留 n5 给 pool 过滤测试
			if nodes[idx].lostUntil == 0 {
				nodes[idx].lostUntil = int64(i + 1 + rnd.Intn(300))
			}
		}

		// 10% 迭代释放一台 machine（模拟 delete 收敛）。
		if i%10 == 0 && len(live) > 0 {
			keys := make([]string, 0, len(live))
			for k := range live {
				keys = append(keys, k)
			}
			m := live[keys[rnd.Intn(len(keys))]]
			releaseNode(nodes, m)
			delete(live, m.id)
			if m.dep != "" {
				if deployments[m.dep][m.nodeID] <= 1 {
					delete(deployments[m.dep], m.nodeID)
				} else {
					deployments[m.dep][m.nodeID]--
				}
			}
			deleted++
			continue
		}

		req := scheduler.Request{
			VCPU:    uint64(1 + rnd.Intn(4)),
			MemMib:  uint64(512 * (1 + rnd.Intn(8))),
			DiskMib: uint64(agentv1.DefaultDiskMib + 1024*rnd.Intn(16)), // v1.2-E：磁盘维度
			Pool:    "compute",
			Labels:  map[string]string{"arch": "x86_64"},
		}
		if rnd.Intn(10) < 8 {
			req.DeploymentID = fmt.Sprintf("dep-%02d", rnd.Intn(30))
			req.AntiAffinity = true
			req.ExistingDeploymentNodes = map[string]bool{}
			for nd := range deployments[req.DeploymentID] {
				req.ExistingDeploymentNodes[nd] = true
			}
		}

		views := make([]scheduler.Node, 0, len(nodes))
		excludedByLoss := map[string]bool{}
		for _, sn := range nodes {
			n := sn.node
			n.CPUAllocated = sn.allocVCPU
			n.MemAllocated = sn.allocMem
			n.DiskAllocated = sn.allocDisk
			if sn.lostUntil > 0 && int64(i) < sn.lostUntil {
				n.Status = scheduler.StatusUnhealthy
				excludedByLoss[n.ID] = true
			}
			views = append(views, n)
		}

		scoredPassed = nil
		machineID := fmt.Sprintf("m-%06d", i)
		pl, err := placer.Place(req, views, rnd)
		if err != nil {
			rejected++ // 全部过滤拒绝（预期：无节点可放）
			continue
		}
		placed++

		// 断言 4：失联节点不得入选。
		if excludedByLoss[pl.NodeID] {
			return fmt.Errorf("iter %d: lost node %s selected", i, pl.NodeID)
		}

		// 断言 5：反亲和——排除集外有候选时，落点必须 distinct。
		if req.AntiAffinity && req.DeploymentID != "" {
			depNodes := deployments[req.DeploymentID]
			if depNodes[pl.NodeID] > 0 {
				outside := 0
				for _, v := range views {
					if v.Status == scheduler.StatusHealthy &&
						v.Pool == req.Pool &&
						labelsMatch(v.Labels, req.Labels) &&
						!req.ExistingDeploymentNodes[v.ID] &&
						canFitNode(v, req.VCPU, req.MemMib, req.DiskMib) {
						outside++
					}
				}
				if outside > 0 {
					return fmt.Errorf("iter %d: anti-affinity violated: %s already hosts %s, %d alternatives available; existing=%v events=%v",
						i, pl.NodeID, req.DeploymentID, outside, req.ExistingDeploymentNodes, pl.Events)
				}
				degraded++ // 候选不足：合法降级（ADR-0009）
			}
		}

		// 断言 1：被采样的候选必须全部通过过滤。
		viewByID := map[string]scheduler.Node{}
		for _, v := range views {
			viewByID[v.ID] = v
		}
		for _, scored := range scoredPassed {
			v, ok := viewByID[scored]
			if !ok {
				return fmt.Errorf("iter %d: scored unknown node %s", i, scored)
			}
			if err := checkPassed(v, req, excludedByLoss); err != nil {
				return fmt.Errorf("iter %d: filter-before-score violated for %s: %v", i, scored, err)
			}
		}

		// 记账 + 断言 2（硬准入）。
		chosen := findByID(nodes, pl.NodeID)
		chosen.allocVCPU += req.VCPU
		chosen.allocMem += req.MemMib
		chosen.allocDisk += req.DiskMib
		chosen.placements++
		if float64(chosen.allocVCPU) > float64(chosen.node.CPUTotal)*4 {
			return fmt.Errorf("iter %d: node %s cpu overcommit %d > 4x%d", i, chosen.node.ID, chosen.allocVCPU, chosen.node.CPUTotal)
		}
		if chosen.allocMem > chosen.node.MemTotalMib {
			return fmt.Errorf("iter %d: node %s mem overcommit %d > %d", i, chosen.node.ID, chosen.allocMem, chosen.node.MemTotalMib)
		}
		if chosen.allocDisk > chosen.node.DiskTotalMib {
			return fmt.Errorf("iter %d: node %s disk overcommit %d > %d", i, chosen.node.ID, chosen.allocDisk, chosen.node.DiskTotalMib)
		}
		if _, dup := live[machineID]; dup {
			return fmt.Errorf("iter %d: duplicate machine id %s", i, machineID)
		}
		live[machineID] = placedMachine{id: machineID, nodeID: chosen.node.ID, dep: req.DeploymentID, vcpu: req.VCPU, mem: req.MemMib, disk: req.DiskMib}
		if req.DeploymentID != "" {
			deployments[req.DeploymentID][chosen.node.ID]++
		}
	}

	fmt.Printf("PASS: %d placements, %d rejected (no candidates), %d anti-affinity degradations, %d deletes, %d live machines\n",
		placed, rejected, degraded, deleted, len(live))
	for _, sn := range nodes {
		fmt.Printf("  node %-4s pool=%-8s placements=%-6d vcpu=%3d/%3d mem=%5d/%5d MiB\n",
			sn.node.ID, sn.node.Pool, sn.placements, sn.allocVCPU, sn.node.CPUTotal, sn.allocMem, sn.node.MemTotalMib)
	}
	fmt.Println("assertions: filter-before-score OK, hard admission OK, unique machines OK, lost-node exclusion OK, anti-affinity distinct OK")
	return nil
}

func releaseNode(nodes []*simNode, m placedMachine) {
	for _, sn := range nodes {
		if sn.node.ID == m.nodeID {
			if sn.allocVCPU >= m.vcpu {
				sn.allocVCPU -= m.vcpu
			}
			if sn.allocMem >= m.mem {
				sn.allocMem -= m.mem
				sn.allocDisk -= m.disk
			}
			return
		}
	}
}

func findByID(nodes []*simNode, id string) *simNode {
	for _, sn := range nodes {
		if sn.node.ID == id {
			return sn
		}
	}
	return nil
}

func mk(id, pool string, cpu, memMiB uint64) *simNode {
	return &simNode{node: scheduler.Node{
		ID: id, Pool: pool, Labels: map[string]string{"arch": "x86_64"},
		Status: scheduler.StatusHealthy, CPUTotal: cpu, MemTotalMib: memMiB,
		DiskTotalMib: 1024 * 1024, // v1.2-E：1TiB/节点（磁盘维度进硬过滤）
	}}
}

func canFitNode(n scheduler.Node, vcpu, mem, disk uint64) bool {
	return float64(n.CPUAllocated+vcpu) <= float64(n.CPUTotal)*4 &&
		n.MemAllocated+mem <= n.MemTotalMib &&
		(n.DiskTotalMib == 0 || n.DiskAllocated+disk <= n.DiskTotalMib)
}

func labelsMatch(nodeLabels, want map[string]string) bool {
	for k, v := range want {
		if nodeLabels[k] != v {
			return false
		}
	}
	return true
}

// checkPassed 校验一个被采样打分的节点确实通过了全部过滤。
func checkPassed(n scheduler.Node, req scheduler.Request, lost map[string]bool) error {
	if n.Status != scheduler.StatusHealthy {
		return fmt.Errorf("unhealthy node scored (status=%s)", n.Status)
	}
	if lost[n.ID] {
		return fmt.Errorf("lost node scored")
	}
	if n.Pool != req.Pool {
		return fmt.Errorf("pool mismatch")
	}
	for k, v := range req.Labels {
		if n.Labels[k] != v {
			return fmt.Errorf("label %s=%s mismatch", k, v)
		}
	}
	if !canFitNode(n, req.VCPU, req.MemMib, req.DiskMib) {
		return fmt.Errorf("resource filter violated")
	}
	return nil
}
