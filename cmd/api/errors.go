// errors.go：HTTP 错误输出收口（生产就绪 P0——控制面错误映射收敛 + P1 panic 防护）。
//
// 规则：
//   - 5xx 响应体固定文案，绝不回吐 PG/内部错误原文（内部地址、SQL、约束名
//     等都属于泄露面）；完整错误只进服务端日志（含 route 便于定位）。
//   - DB/连接不可用类错误 → 503（对客户端是可重试信号，区别于业务 500）。
//   - 404 只能由 handler 在确证 not-found（store.ErrNotFound / nil 行）时显式
//     写出；helper 不做 404 猜测。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgconn"
)

// isDBUnavailable 判断错误是否属于「PG 依赖不可用」类（连接失败/超时），
// 与业务错误区分：这类错误对客户端是 503 可重试信号。
// 注意：PG 返回的 statement_timeout（57014 PgError）说明 DB 可达，不在此类。
func isDBUnavailable(err error) bool {
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}
	// pgconn.Timeout 覆盖 pgx 内部超时包装；context deadline（statement/连接
	// 超时经 r.Context() 传导）同样说明当前依赖不可用而非业务错误。
	return pgconn.Timeout(err) || errors.Is(err, context.DeadlineExceeded)
}

// writeInternalErr 是 5xx 的唯一出口：log 全文 + 固定文案响应。
func writeInternalErr(w http.ResponseWriter, r *http.Request, err error) {
	slog.Error("request failed",
		"route", r.Pattern, "method", r.Method, "path", r.URL.Path, "error", err)
	if isDBUnavailable(err) {
		writeErr(w, 503, "service temporarily unavailable")
		return
	}
	writeErr(w, 500, "internal error")
}

// writeUnavailableErr 用于已知是「下游依赖不可用」语义的失败（如 runtime
// 网关/agent 拨号失败）：细节进日志，body 固定 503 文案。
func writeUnavailableErr(w http.ResponseWriter, r *http.Request, err error) {
	slog.Error("upstream unavailable",
		"route", r.Pattern, "method", r.Method, "path", r.URL.Path, "error", err)
	writeErr(w, 503, "service temporarily unavailable")
}

// recoverMiddleware 兜住 handler panic：log 全文，客户端拿到固定 500，
// 进程继续服务（单请求 panic 不升级为进程崩溃）。
func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if p := recover(); p != nil {
				slog.Error("handler panic",
					"route", r.Pattern, "method", r.Method, "path", r.URL.Path, "panic", p)
				// 已写出的响应无法体面收口（如流式端点半途中断）；流式场景
				// 的 panic 本就不会走到可补救的状态，此处 best-effort 写 500。
				writeErr(w, 500, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
