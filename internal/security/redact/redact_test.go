package redact

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRedactJSONBytesObjectTree(t *testing.T) {
	raw := []byte(`{"spec":{"secret_env":{"TOKEN":"supersecret"},"env":{"A":"b"},
		"proxy_credential":"fp-cred","image":"x@sha256:aaaa"}}`)
	out := RedactJSONBytes(raw)
	s := string(out)
	if strings.Contains(s, "supersecret") {
		t.Fatalf("secret_env value leaked: %s", s)
	}
	if strings.Contains(s, "fp-cred") {
		t.Fatalf("proxy_credential leaked: %s", s)
	}
	// 非敏感字段保留。
	if !strings.Contains(s, `"image":"x@sha256:aaaa"`) {
		t.Fatalf("non-sensitive field dropped: %s", s)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("output not valid JSON: %v", err)
	}
}

func TestRedactJSONBytesNestedValueObjects(t *testing.T) {
	// 值为对象的敏感键（而非字符串）也必须整树打码。
	raw := []byte(`{"secret_env":{"TOKEN":"v1","DB":"v2"},"traffic_token":{"k":"v"}}`)
	out := RedactJSONBytes(raw)
	s := string(out)
	if strings.Contains(s, "v1") || strings.Contains(s, "v2") {
		t.Fatalf("nested object values leaked: %s", s)
	}
}

func TestRedactJSONBytesNonObjectFallback(t *testing.T) {
	// 数组/非法 JSON：正则兜底路径——宁打码勿漏明文。
	raw := []byte(`[{"secret_env":"should-not-appear"}]`)
	out := RedactJSONBytes(raw)
	if strings.Contains(string(out), "should-not-appear") {
		t.Fatalf("fallback path leaked: %s", out)
	}
}

func TestSourceURLDigestIsSafeMetadata(t *testing.T) {
	if IsSensitive("source_url_digest") {
		t.Fatal("source_url_digest must remain available for safe correlation")
	}
	if !IsSensitive("source_url") {
		t.Fatal("source_url must remain sensitive")
	}
}

func TestRedactTextRemovesURLsAndFlattenedSecrets(t *testing.T) {
	input := `redirect to https://target.internal/path?sig=secret source_url=https://source.internal/a token=abc`
	got := RedactText(input)
	for _, secret := range []string{"target.internal", "source.internal", "sig=secret", "token=abc"} {
		if strings.Contains(got, secret) {
			t.Fatalf("RedactText leaked %q: %s", secret, got)
		}
	}
}

func TestRedactJSONBytesEmpty(t *testing.T) {
	if got := string(RedactJSONBytes(nil)); got != "{}" {
		t.Fatalf("nil input should map to {}, got %q", got)
	}
}
