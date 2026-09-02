package traffic

import (
	"encoding/hex"
	"strings"
	"testing"
)

func testKey(t *testing.T, fill byte) []byte {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = fill
	}
	return key
}

func TestNewSignerRejectsShortKeys(t *testing.T) {
	t.Parallel()
	for _, n := range []int{0, 1, 16, 31} {
		if _, err := NewSigner(make([]byte, n)); err == nil {
			t.Fatalf("key of %d bytes must be rejected", n)
		} else if !strings.Contains(err.Error(), "32") {
			t.Fatalf("error must state the >=32 requirement, got %v", err)
		}
	}
	for _, n := range []int{32, 48, 64} {
		if _, err := NewSigner(make([]byte, n)); err != nil {
			t.Fatalf("key of %d bytes must be accepted: %v", n, err)
		}
	}
}

// 同一密钥 + 同一 (machine, execution) 必须收敛到同一 credential：controller
// 重试（ACK 丢失补账）依赖该确定性命中 agent ledger 幂等。
func TestTokenDeterministicAcrossSigners(t *testing.T) {
	t.Parallel()
	s1, err := NewSigner(testKey(t, 0xAA))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := NewSigner(testKey(t, 0xAA))
	if err != nil {
		t.Fatal(err)
	}
	a, b := s1.Token("m1", "e1"), s2.Token("m1", "e1")
	if a != b {
		t.Fatalf("same key must derive the same credential: %q != %q", a, b)
	}
	if a != s1.Token("m1", "e1") {
		t.Fatal("repeated derivation must be stable")
	}
}

// Credential 是 32 个十六进制字符（128bit HMAC 截断），且不含密钥。
func TestTokenFormat(t *testing.T) {
	t.Parallel()
	s, err := NewSigner(testKey(t, 0x01))
	if err != nil {
		t.Fatal(err)
	}
	tok := s.Token("m1", "e1")
	if len(tok) != 32 {
		t.Fatalf("token length=%d want 32", len(tok))
	}
	if _, err := hex.DecodeString(tok); err != nil {
		t.Fatalf("token must be lowercase hex: %v", err)
	}
}

// execution 绑定：machine/execution 任一变化、credential 必须变化；agent 侧
// 摘要比对（state.Creds）据此拒绝错误 machine/execution 重放的 token。
// 本包无内嵌过期语义（轮换 = execution 替换、删除撤销），过期/篡改拒绝
// 发生在 agent 摘要比对侧，不在本包 API 内。
func TestTokenBoundToMachineAndExecution(t *testing.T) {
	t.Parallel()
	s, err := NewSigner(testKey(t, 0x02))
	if err != nil {
		t.Fatal(err)
	}
	base := s.Token("m1", "e1")
	for _, tc := range []struct{ machine, execution string }{
		{"m2", "e1"},
		{"m1", "e2"},
		{"m2", "e2"},
		{"m1", ""},
		{"", "e1"},
		{"m1/e1", ""}, // 拼接歧义输入也必须与 "m1"/"e1" 区分。
	} {
		if got := s.Token(tc.machine, tc.execution); got == base {
			t.Fatalf("Token(%q,%q) must differ from Token(%q,%q)", tc.machine, tc.execution, "m1", "e1")
		}
	}
}

// 密钥不同（轮换后）同 (machine, execution) 派生出不同 credential。
func TestTokenDifferentKeysDiffer(t *testing.T) {
	t.Parallel()
	s1, err := NewSigner(testKey(t, 0x03))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := NewSigner(testKey(t, 0x04))
	if err != nil {
		t.Fatal(err)
	}
	if s1.Token("m1", "e1") == s2.Token("m1", "e1") {
		t.Fatal("different keys must derive different credentials")
	}
}

// edge→agent proxy 的 credential 请求头是 wire 契约，改名即断链。
func TestHeaderCredentialContract(t *testing.T) {
	t.Parallel()
	if HeaderCredential != "X-Firepaas-Credential" {
		t.Fatalf("HeaderCredential=%q", HeaderCredential)
	}
}
