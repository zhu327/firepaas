// Command ontime 是 M5.2 guest-clock 探针：HTTP 回显 VM 内 wall clock 与
// 启动至今秒数（/proc/uptime 读不到时的降级）。用于量测 firecracker
// snapshot 恢复后的时钟漂移（mvp-plan §9.2）。
package main

import (
	"fmt"
	"net/http"
	"os"
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
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"epoch_ms":%d,"uptime_s":%d}`,
			time.Now().UnixMilli(), int64(time.Since(started).Seconds()))
	})
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
