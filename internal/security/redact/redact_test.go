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

// F：bare 精确键 token/credential/secret/password——normalize 后整键匹配，
// 不做子串匹配（tokens、secret_id、source_url_digest 等合法元数据键不受影响）。
func TestExactBareKeys(t *testing.T) {
	for _, key := range []string{"token", "Token", "TOKEN", "credential", "secret", "password"} {
		if !IsSensitive(key) {
			t.Fatalf("bare key %q must be sensitive", key)
		}
	}
	for _, key := range []string{
		"tokens", "token_count", "secret_id", "secret_refs", "secretId",
		"source_url_digest", "credentials_file",
	} {
		if IsSensitive(key) {
			t.Fatalf("metadata key %q must NOT be redacted", key)
		}
	}
	raw := []byte(`{"snapshot":{"token":"cap-token-123","snapshot_id":"s1"}}`)
	out := string(RedactJSONBytes(raw))
	if strings.Contains(out, "cap-token-123") {
		t.Fatalf("bare token key leaked: %s", out)
	}
	if !strings.Contains(out, `"snapshot_id":"s1"`) {
		t.Fatalf("non-sensitive key dropped: %s", out)
	}
}

// F：camelCase 键（protojson 缺省输出）在对象树路径打码。
func TestCamelCaseKeysInObjectTree(t *testing.T) {
	raw := []byte(`{
		"trafficToken":"ttp-1",
		"proxyCredential":"pc-1",
		"secretEnv":{"DB_PASSWORD":"pw1"},
		"env":{"PLAIN":"keep"}
	}`)
	out := string(RedactJSONBytes(raw))
	for _, leaked := range []string{"ttp-1", "pc-1", "pw1"} {
		if strings.Contains(out, leaked) {
			t.Fatalf("camelCase value leaked: %s", out)
		}
	}
	if !strings.Contains(out, `"PLAIN":"keep"`) {
		t.Fatalf("non-sensitive camelCase-adjacent key dropped: %s", out)
	}
}

// F：数组内的对象键同样过黑名单（嵌套数组 + 顶层数组的结构化路径）。
func TestArrayOfObjectsNesting(t *testing.T) {
	raw := []byte(`{"items":[{"trafficToken":"a1","name":"keep1"},[{"secret":"a2","ok":1}],"plain"]}`)
	out := string(RedactJSONBytes(raw))
	for _, leaked := range []string{"a1", "a2"} {
		if strings.Contains(out, leaked) {
			t.Fatalf("array-nested secret leaked: %s", out)
		}
	}
	if !strings.Contains(out, `"keep1"`) || !strings.Contains(out, `"plain"`) {
		t.Fatalf("non-sensitive array content dropped: %s", out)
	}

	// 顶层数组原先是 regexp 兑底路径，现在必须结构化整树脱敏。
	top := []byte(`[{"proxyCredential":"b1"},{"secretEnv":{"K":"b2"}},{"ok":"keep"}]`)
	topOut := string(RedactJSONBytes(top))
	if strings.Contains(topOut, "b1") || strings.Contains(topOut, "b2") {
		t.Fatalf("top-level array leaked: %s", topOut)
	}
	if !strings.Contains(topOut, `"keep"`) {
		t.Fatalf("top-level array safe content dropped: %s", topOut)
	}
}

// F：非法 JSON 的兑底路径覆盖 camelCase 与 bare 精确键。
func TestNonObjectFallbackCamelCaseAndBareKeys(t *testing.T) {
	raw := []byte(`{"trafficToken":"camel-1",broken`) // 非法 JSON
	out := string(RedactJSONBytes(raw))
	if strings.Contains(out, "camel-1") {
		t.Fatalf("fallback leaked camelCase value: %s", out)
	}
	raw = []byte(`{"token":"bare-1",broken`)
	out = string(RedactJSONBytes(raw))
	if strings.Contains(out, "bare-1") {
		t.Fatalf("fallback leaked bare token value: %s", out)
	}
}

func TestRedactJSONBytesEmpty(t *testing.T) {
	if got := string(RedactJSONBytes(nil)); got != "{}" {
		t.Fatalf("nil input should map to {}, got %q", got)
	}
}
