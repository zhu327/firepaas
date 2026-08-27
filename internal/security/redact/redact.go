// Package redact 实现审计/日志的敏感字段黑名单（ADR-0010 §2 / mvp-plan §8.1）。
//
// 黑名单按 key 子串匹配（大小写/下划线不敏感），覆盖 M4 已知敏感字段：
// secret_env、secret_value、value_ciphertext、dek_wrapped、proxy_credential、
// traffic_token、authorization。宁可误杀（值打码），不可漏杀（明文落日志）。
package redact

import (
	"strings"
)

// sensitive 片段表：任一片段命中即视为敏感。
var sensitive = []string{
	"secret_env", "secret_value", "secretenv", "ciphertext", "cipher_text",
	"wrapped_dek", "wrappeddek", "wrapped", "proxy_credential",
	"proxycredential", "traffic_token", "traffictoken", "authorization",
	"password", "api_key", "bearer",
}

// IsSensitive 判断字段名是否命中黑名单。
func IsSensitive(key string) bool {
	n := normalize(key)
	for _, s := range sensitive {
		if strings.Contains(n, s) {
			return true
		}
	}
	return false
}

// RedactMap 返回打码后的拷贝：命中黑名单的键值替换为 "[REDACTED]"。
func RedactMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		if IsSensitive(k) {
			out[k] = "[REDACTED]"
			continue
		}
		if sub, ok := v.(map[string]any); ok {
			out[k] = RedactMap(sub)
			continue
		}
		out[k] = v
	}
	return out
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
