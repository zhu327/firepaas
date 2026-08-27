package secrets

import (
	"bytes"
	"encoding/base64"
	"testing"
)

func testKey(t *testing.T) string {
	t.Helper()
	raw := bytes.Repeat([]byte{0x42}, 32)
	return base64.StdEncoding.EncodeToString(raw)
}

func TestSealOpenRoundtrip(t *testing.T) {
	m, err := NewManager(testKey(t))
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := m.Seal([]byte("hunter2"), "dev", "db-password", 3)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed.Ciphertext, []byte("hunter2")) {
		t.Fatal("plaintext leaked into ciphertext")
	}
	pt, err := m.Open(sealed, "dev", "db-password", 3)
	if err != nil || string(pt) != "hunter2" {
		t.Fatalf("roundtrip: %v %q", err, pt)
	}
}

// AAD 绑定：同一密文换身份/版本打开必须失败（防行间互换/重放）。
func TestSealAadBinding(t *testing.T) {
	m, _ := NewManager(testKey(t))
	sealed, _ := m.Seal([]byte("v"), "dev", "s1", 1)
	if _, err := m.Open(sealed, "dev", "s2", 1); err == nil {
		t.Fatal("open under wrong name must fail")
	}
	if _, err := m.Open(sealed, "prod", "s1", 1); err == nil {
		t.Fatal("open under wrong project must fail")
	}
	if _, err := m.Open(sealed, "dev", "s1", 2); err == nil {
		t.Fatal("open under wrong version must fail")
	}
}

// 每次加密 DEK 独立随机，同值不同批次密文不同。
func TestSealUniquePerCall(t *testing.T) {
	m, _ := NewManager(testKey(t))
	a, _ := m.Seal([]byte("same"), "p", "n", 1)
	b, _ := m.Seal([]byte("same"), "p", "n", 1)
	if bytes.Equal(a.Ciphertext, b.Ciphertext) {
		t.Fatal("ciphertext must be randomized per call")
	}
}

func TestBadMasterKey(t *testing.T) {
	if _, err := NewManager(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 16))); err == nil {
		t.Fatal("16-byte key must be rejected")
	}
	if _, err := NewManager("not-base64!!!"); err == nil {
		t.Fatal("bad base64 must be rejected")
	}
}
