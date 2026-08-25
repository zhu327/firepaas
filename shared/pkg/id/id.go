// Package id 提供 firepaas 的 ID 生成与校验(MVP 用 CUID2/ULID,后续可换 UUIDv7)。
package id

import "crypto/rand"

// New 生成一个新的机器/部署 ID。P1 接入 CUID2 库后替换实现。
func New() string {
	const hex = "0123456789abcdef"
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	out := make([]byte, 0, 26)
	for i := 0; i < len(b); i++ {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out = append(out, '-')
		}
		out = append(out, hex[b[i]>>4], hex[b[i]&0x0f])
	}
	return string(out)
}

// Valid 校验 ID 格式(MVP 只要求非空,契约冻结时收紧)。
func Valid(s string) bool {
	return s != ""
}
