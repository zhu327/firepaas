package httpapi

// pagination.go：列表端点的统一分页参数解析（limit + 不透明 keyset cursor）。
//
// 约定（与 listEvents/limit 口径对齐）：limit 缺省 200、上限 1000；cursor 是
// 上一页的 next_cursor（本页从其之后开始）；响应体带 next_cursor（空 = 末页）。
// 非法 limit（非数字/<=0/超上限）→ 缺省/钳制，不 400（与 listEvents 一致，
// 避免把调页参数变成可用性故障）；cursor 原样透传，store 层对未知 cursor 400。

import (
	"net/http"
	"strconv"
)

// defaultListLimit 是缺省页大小；maxListLimit 是上限。
const (
	defaultListLimit = 200
	maxListLimit     = 1000
)

// parseListParams 解析 limit/cursor 查询参数（r 为 nil 防御返回缺省）。
func parseListParams(r *http.Request) (limit int, cursor string) {
	limit = defaultListLimit
	if r == nil {
		return limit, ""
	}
	q := r.URL.Query()
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	return limit, q.Get("cursor")
}
