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
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
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

// doIdem 与 do 同语义，额外透传 Idempotency-Key 请求头（key 为空则不发）。
func doIdem(method, path string, body, out any, key string) error {
	addr := os.Getenv("FP_API_ADDR")
	if addr == "" {
		addr = "http://127.0.0.1:8080"
	}
	token := os.Getenv("FP_API_TOKEN")
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, strings.TrimRight(addr, "/")+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %s (%s)", method, path, resp.Status, strings.TrimSpace(string(raw)))
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	fmt.Println(string(raw))
	return nil
}
