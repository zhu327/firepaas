// Command ontime 是 M5.2 guest-clock 探针：HTTP 回显 VM 内 wall clock 与
// 启动至今秒数（/proc/uptime 读不到时的降级）。用于量测 firecracker
// snapshot 恢复后的时钟漂移（mvp-plan §9.2）。
//
// v1.1：新增 /slow?ms=N（受控延迟响应）——edge 并发控制（least-inflight /
// hard limit）与 inflight 生命周期断言的驱动端点。
package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var started = time.Now()

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	// M5 e2e：双端口监听（80 + 8080）兼容 proxy 的端口钉扎不同版本。
	go func() {
		// 第二个端口失败不影响主端口（端口占用时静默退化）。
		extra := "80"
		if port == "80" {
			extra = "8080"
		}
		_ = http.ListenAndServe(":"+extra, nil)
	}()
	// v1.1 e2e（ADR-0022）：EXTRA_PORTS 附加监听（逗号分隔，最多 4 个）——
	// 多端口 services 的端到端验收（edge 附加端口段 → guest 声明端口）。
	for _, p := range strings.Split(os.Getenv("EXTRA_PORTS"), ",") {
		p = strings.TrimSpace(p)
		if p == "" || p == port {
			continue
		}
		go func(p string) { _ = http.ListenAndServe(":"+p, nil) }(p)
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"epoch_ms":%d,"uptime_s":%d}`,
			time.Now().UnixMilli(), int64(time.Since(started).Seconds()))
	})
	// v1.2-B（ADR-0024）：/env?k=NAME —— 回显 entrypoint 进程环境变量。
	// one-shot secret canary 探针：secret 经 guest init 从 tmpfs 注入
	// entrypoint 环境；平台链路（PG/agent 落盘/hypeman metadata）不得出现明文。
	http.HandleFunc("/env", func(w http.ResponseWriter, r *http.Request) {
		k := r.URL.Query().Get("k")
		fmt.Fprintf(w, `{"value":%q}`, os.Getenv(k))
	})
	// v1.1：/slow?ms=N —— 睡 N 毫秒后回 JSON（edge inflight/hard 并发测试）。
	http.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		if ms < 0 || ms > 60000 {
			ms = 1000
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
		fmt.Fprintf(w, `{"slept_ms":%d,"epoch_ms":%d}`,
			ms, time.Now().UnixMilli())
	})
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
