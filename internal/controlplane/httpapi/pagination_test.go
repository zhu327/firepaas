package httpapi

import (
	"net/http/httptest"
	"testing"
)

func TestParseListParams(t *testing.T) {
	limit, cursor := parseListParams(nil)
	if limit != 200 || cursor != "" {
		t.Fatalf("nil = %d %q", limit, cursor)
	}
	r := httptest.NewRequest("GET", "/v1/machines?limit=50&cursor=abc", nil)
	if limit, cursor := parseListParams(r); limit != 50 || cursor != "abc" {
		t.Fatalf("got %d %q", limit, cursor)
	}
	// 非法 limit → 缺省；超上限 → 钳制（不 400）。
	r = httptest.NewRequest("GET", "/v1/machines?limit=bogus", nil)
	if limit, _ := parseListParams(r); limit != 200 {
		t.Fatalf("bogus limit = %d", limit)
	}
	r = httptest.NewRequest("GET", "/v1/machines?limit=99999", nil)
	if limit, _ := parseListParams(r); limit != 1000 {
		t.Fatalf("over limit = %d", limit)
	}
	r = httptest.NewRequest("GET", "/v1/machines?limit=-5", nil)
	if limit, _ := parseListParams(r); limit != 200 {
		t.Fatalf("negative limit = %d", limit)
	}
}
