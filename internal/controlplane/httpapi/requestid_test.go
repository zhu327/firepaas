// requestid_test.go：review L6 —— 入站 X-Request-Id 的长度/字符集约束。
package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequestIDMiddlewareSanitizesInboundID(t *testing.T) {
	cases := []struct {
		name    string
		inbound string
		keep    bool
	}{
		{name: "valid id kept", inbound: "abc-123_X.Y", keep: true},
		{name: "missing generates", inbound: "", keep: false},
		{name: "too long regenerates", inbound: strings.Repeat("a", 129), keep: false},
		{name: "space regenerates", inbound: "abc def", keep: false},
		{name: "newline regenerates", inbound: "abc\ndef", keep: false},
		{name: "non-ascii regenerates", inbound: "请求-id", keep: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var seen string
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen = requestIDFrom(r.Context())
			})
			req := httptest.NewRequest(http.MethodGet, "/v1/machines", nil)
			if tc.inbound != "" {
				req.Header.Set("X-Request-Id", tc.inbound)
			}
			rec := httptest.NewRecorder()
			requestIDMiddleware(next).ServeHTTP(rec, req)

			got := rec.Header().Get("X-Request-Id")
			if got == "" {
				t.Fatal("response must carry X-Request-Id")
			}
			if got != seen {
				t.Fatalf("context id %q != response header %q", seen, got)
			}
			if tc.keep {
				if got != tc.inbound {
					t.Fatalf("valid inbound id must be preserved: got %q want %q", got, tc.inbound)
				}
				return
			}
			if got == tc.inbound {
				t.Fatalf("invalid inbound id must be replaced, got %q", got)
			}
			if !validRequestID(got) || len(got) != 16 {
				t.Fatalf("generated id %q must be 16 hex chars", got)
			}
		})
	}
}
