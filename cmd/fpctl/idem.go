// idem.go：mutation 幂等键透传（v1.5）。
//
// 服务端 prewarm/pin/unpin 按 project × Idempotency-Key 去重：相同 key +
// 相同规范化 intent 返回原结果，相同 key + 不同 intent 返回 409 冲突。
// CLI 约定：
//   - mutation 子命令接受 --idempotency-key；为空则自动生成并打印到 stderr
//     （超时/断线后用同一 key 重试即安全重放，不会重复创建资源）。
//   - 也可用 FP_IDEMPOTENCY_KEY 环境变量预置（脚本批量场景）。
package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"
)

func idemKeyFlag(fs *flag.FlagSet) *string {
	return fs.String("idempotency-key", "", "幂等键（为空则自动生成并打印到 stderr；重试请复用同一 key）")
}

func resolveIdemKey(flagVal string) string {
	if flagVal != "" {
		return flagVal
	}
	if v := strings.TrimSpace(os.Getenv("FP_IDEMPOTENCY_KEY")); v != "" {
		return v
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	key := "fpctl-" + hex.EncodeToString(b[:])
	fmt.Fprintf(os.Stderr, "idempotency-key: %s\n", key)
	return key
}

// 幂等键经 doRequest(idemKey) 透传（key 为空则不发头），调用点直调 doRequest。
