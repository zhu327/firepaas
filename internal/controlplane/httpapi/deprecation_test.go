package httpapi

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMarkDeprecated(t *testing.T) {
	w := httptest.NewRecorder()
	markDeprecated(w, "POST /v1/machines")
	if got := w.Header().Get("Deprecation"); got == "" {
		t.Fatal("missing Deprecation header")
	}
	if got := w.Header().Get("Link"); got != `</v1/apps>; rel="successor-version"` {
		t.Fatalf("Link = %q", got)
	}
	// 非弃用路由无操作。
	w2 := httptest.NewRecorder()
	markDeprecated(w2, "GET /v1/machines")
	if got := w2.Header().Get("Deprecation"); got != "" {
		t.Fatalf("unexpected Deprecation = %q", got)
	}
	// 注册表 key 必须与 Register 的路由模式一致（契约漂移即失败）。
	if deprecationFor("POST /v1/machines") == nil {
		t.Fatal("POST /v1/machines must be registered deprecated")
	}
}

func TestScaleETagRoundTrip(t *testing.T) {
	ts := "2026-09-22 10:00:00+00"
	if got := scaleETag(3, ts); got != `"replicas-3-2026-09-22T10:00:00+00"` {
		t.Fatalf("etag = %q", got)
	}
	// RFC 7232 etagc 不允许空格。
	if strings.Contains(scaleETag(3, ts), " ") {
		t.Fatal("etag must not contain a space")
	}
	// 服务端签发形态（'T' 分隔）与旧空格形态都可解析。
	for _, in := range []string{`"replicas-3-2026-09-22T10:00:00+00"`, `"replicas-3-2026-09-22 10:00:00+00"`} {
		n, gotTS, err := parseScaleIfMatch(in)
		if err != nil || n != 3 || !strings.Contains(gotTS, "2026-09-22") {
			t.Fatalf("parse(%s) = %d %q %v", in, n, gotTS, err)
		}
	}
	for _, bad := range []string{
		`"rev-3"`, `"replicas-x"`, `"replicas--1"`, `"replicas-3"`,
		`"replicas-3-not-a-time"`, "replicas-3-" + ts, "",
	} {
		if _, _, err := parseScaleIfMatch(bad); err == nil {
			t.Fatalf("bad If-Match %q accepted", bad)
		}
	}
}
