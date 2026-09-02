package state

import (
	"github.com/zhu327/firepaas/shared/pkg/durablewrite"
)

// writeFileDurable 按崩溃安全序列持久化 data 到 path：
// 写 temp → fsync(temp) → close → rename → fsync(dir)。成功返回意味着数据
// 与目录项都已落盘。实现收敛在 shared/pkg/durablewrite（评审 P2 去重：
// ledger/creds/slot 原先各持一份同纪律副本）。
func writeFileDurable(path, what string, data []byte) error {
	return durablewrite.WriteFileAtomic(path, what, data)
}
