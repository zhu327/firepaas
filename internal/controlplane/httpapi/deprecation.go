package httpapi

// deprecation.go：公开 API 弃用政策的机器可读表达（P1：此前无 deprecation
// 字段/政策，调用方无法发现“还能用但别再用”的端点）。
//
// 政策（docs/openapi.yaml 头部同步声明）：
//   - 弃用 ≠ 删除：弃用端点保持全功能至少两个 minor 版本；删除前先经本注册表
//     公告 Sunset 日期，删除版本提前一版在 capabilities/deprecations 公示；
//   - 机器信号：RFC 8594 Deprecation 头（值为弃用生效时间戳）+ Sunset 头
//     （值为计划删除日期，RFC 7231 HTTP-date）+ Link 头 rel="successor-version"
//     指向替代端点；响应体不加字段（避免破坏严格客户端）；
//   - 注册表是本文件唯一的弃用登记点：新增弃用只改 deprecatedEndpoints，
//     handler 在入口调 markDeprecated。
//
// 当前登记：
//   - POST /v1/machines（M2 单机直建，自动派生 app/deployment，绕过 rollout
//     状态机）：替代为 POST /v1/apps + POST /v1/apps/{id}/deployments。

import (
	"net/http"
)

// deprecation 是一条弃用公告。
type deprecation struct {
	// since 是弃用生效日期（Deprecation 头值，RFC 7231 HTTP-date 形式）。
	since string
	// sunset 是计划删除日期（Sunset 头值）；空 = 暂未计划删除。
	sunset string
	// successor 是替代端点（Link 头 rel="successor-version"）。
	successor string
}

// deprecatedEndpoints 是方法 + 路径模式 → 公告（key 与 Register 的
// "METHOD /path" 口径一致，路径用 r.Pattern 的无方法部分比对）。
var deprecatedEndpoints = map[string]deprecation{
	"POST /v1/machines": {
		since:     "Tue, 22 Sep 2026 00:00:00 GMT",
		sunset:    "",
		successor: "/v1/apps",
	},
}

// deprecationFor 返回 pattern（如 "POST /v1/machines"）的公告（无则 nil）。
func deprecationFor(pattern string) *deprecation {
	if d, ok := deprecatedEndpoints[pattern]; ok {
		return &d
	}
	return nil
}

// markDeprecated 为弃用端点写机器可读头（幂等；非弃用 pattern 无操作）。
// 调用方传 r.Pattern（net/http 路由模式，含方法前缀，如 "POST /v1/machines"）。
func markDeprecated(w http.ResponseWriter, pattern string) {
	d := deprecationFor(pattern)
	if d == nil {
		return
	}
	w.Header().Set("Deprecation", d.since)
	if d.sunset != "" {
		w.Header().Set("Sunset", d.sunset)
	}
	if d.successor != "" {
		w.Header().Set("Link", "<"+d.successor+`>; rel="successor-version"`)
	}
}
