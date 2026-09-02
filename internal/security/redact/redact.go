// Package redact 实现审计/日志的敏感字段黑名单（ADR-0010 §2 / mvp-plan §8.1）。
//
// 黑名单按 key 子串匹配（大小写/下划线不敏感），覆盖 M4 已知敏感字段：
// secret_env、secret_value、value_ciphertext、dek_wrapped、proxy_credential、
// traffic_token、authorization。宁可误杀（值打码），不可漏杀（明文落日志）。
package redact

import (
	"encoding/json"
	"regexp"
	"strings"
)

// sensitive 片段表：任一片段命中即视为敏感。
var sensitive = []string{
	"secret_env", "secret_value", "secretenv", "ciphertext", "cipher_text",
	"wrapped_dek", "wrappeddek", "wrapped", "proxy_credential",
	"proxycredential", "traffic_token", "traffictoken", "authorization",
	"password", "api_key", "bearer",
	// v1.4（ADR-0030）：dataset 来源 URL 原文不得进入响应/日志/事件；
	// 摘要（source_url_digest）不受影响。
	"source_url", "sourceurl",
}

// sensitiveExactKeys 是 normalize 后整键精确匹配的黑名单。与 substring 表
// 互补：token/credential/secret/password 词干太短，子串匹配会误伤合法元
// 数据键（计数器 tokens、secret_refs 里的 secret_id/secret 名引用、
// source_url_digest），因此只做精确整键匹配。已核对的 in-repo 影响面：
//   - protojson/op 载荷里的 bare "token"（快照/capability token）、
//     bare "credential"/"secret" 键均为敏感受体，打码是期望行为；
//   - cmd/api GET traffic-token 响应的 "token" 键不经本包（直接写响应体，
//     是凭证的单向交付端点），不受影响；
//   - HTTP SecretRef 请求体的 "secret"（仅 secret 名引用）不经本包路径；
//   - "password" 本已在 substring 表中，此处仅为防御性精确键冗余。
var sensitiveExactKeys = map[string]bool{
	"token":      true,
	"credential": true,
	"secret":     true,
	"password":   true,
}

var (
	urlPattern = regexp.MustCompile(`(?i)https?://[^\s"'<>]+`)
	// Error strings commonly flatten structured fields as key=value or key: value.
	// Keep the key for diagnosis while removing its value.
	sensitiveAssignmentPattern = regexp.MustCompile(
		`(?i)(source[_-]?url|authorization|password|api[_-]?key|bearer|token)\s*[:=]\s*[^\s,;]+`,
	)
)

// RedactText removes URL-bearing and common flattened secret values from
// unstructured errors before they are returned by operation/wait APIs.
func RedactText(text string) string {
	text = urlPattern.ReplaceAllString(text, "[REDACTED_URL]")
	return sensitiveAssignmentPattern.ReplaceAllStringFunc(text, func(match string) string {
		if i := strings.IndexAny(match, ":="); i >= 0 {
			return match[:i+1] + "[REDACTED]"
		}
		return "[REDACTED]"
	})
}

// IsSensitive 判断字段名是否命中黑名单。
func IsSensitive(key string) bool {
	n := normalize(key)
	if n == "sourceurldigest" {
		return false
	}
	for _, s := range sensitive {
		if strings.Contains(n, s) {
			return true
		}
	}
	return sensitiveExactKeys[n]
}

// RedactMap 返回打码后的拷贝：命中黑名单的键值替换为 "[REDACTED]""。
// 嵌套结构递归处理：对象里的对象、数组、以及数组里的对象（如 repeated
// proto 字段的 protojson 展开）都必须过同一个黑名单。
func RedactMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		if IsSensitive(k) {
			out[k] = "[REDACTED]"
			continue
		}
		out[k] = redactValue(v)
	}
	return out
}

// redactValue 递归处理任意 JSON 值：map 走 RedactMap，数组逐元素递归，
// 标量原样返回。
func redactValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return RedactMap(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = redactValue(e)
		}
		return out
	default:
		return v
	}
}

// RedactHeaders 打码 HTTP 头集合（审计中间件用）。
func RedactHeaders(h map[string][]string) map[string]any {
	m := make(map[string]any, len(h))
	for k, vs := range h {
		if IsSensitive(k) || k == "Authorization" || k == "Cookie" {
			m[k] = "[REDACTED]"
			continue
		}
		m[k] = vs
	}
	return m
}

// normalize 去掉分隔符并小写，使 secretEnv / secret-env / SECRET_ENV 同判。
func normalize(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch r {
		case '-', '_', '.', ' ':
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// RedactJSONBytes 对 jsonb/JSON 原始字节做整树脱敏（M5.3 operation trace
// 用：operations.request/result 可能含 secret_env/proxy_credential）。
// 顶层数组同样结构化处理（protojson 均为对象，但防御性覆盖任意合法 JSON）；
// 仅解析失败（非法 JSON）时才退回到保守的逐键正则替换，不放行原文。
func RedactJSONBytes(raw []byte) []byte {
	if len(raw) == 0 {
		return []byte("{}")
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		// 非法 JSON：逐个敏感键兜底替换。camelCase（trafficToken /
		// proxyCredential / secretEnv）经由 sensitive 表中的无分隔符形态
		// + (?i) 覆盖；bares 精确键（"token" 等）带引号匹配，不会误伤
		// "traffic_token" 之类的复合键（substring 形态已单独列出）。
		sanitized := raw
		for _, key := range sensitive {
			re := regexp.MustCompile(`(?i)"` + regexp.QuoteMeta(key) + `"\s*:\s*"[^"]*"`)
			sanitized = re.ReplaceAll(sanitized, []byte(`"`+key+`":"[REDACTED]"`))
		}
		for key := range sensitiveExactKeys {
			re := regexp.MustCompile(`(?i)"` + regexp.QuoteMeta(key) + `"\s*:\s*"[^"]*"`)
			sanitized = re.ReplaceAll(
				sanitized,
				[]byte(`"`+key+`":"[REDACTED]"`),
			)
		}
		return sanitized
	}
	out, err := json.Marshal(redactValue(v))
	if err != nil {
		return []byte("{}")
	}
	return out
}
