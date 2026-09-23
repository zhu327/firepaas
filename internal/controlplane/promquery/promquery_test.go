package promquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func fakeProm(t *testing.T, body string, status int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !r.URL.Query().Has("query") {
			t.Errorf("missing query param")
		}
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, body)
	}))
}

func TestQueryFirstSample(t *testing.T) {
	srv := fakeProm(t, `{"status":"success","data":{"result":[{"value":[123,"0.042"]}]}}`, 200)
	defer srv.Close()
	c := &Client{BaseURL: srv.URL}
	v, err := c.Query(context.Background(), `up`)
	if err != nil || v != 0.042 {
		t.Fatalf("v=%v err=%v", v, err)
	}
}

func TestQueryNoData(t *testing.T) {
	srv := fakeProm(t, `{"status":"success","data":{"result":[]}}`, 200)
	defer srv.Close()
	c := &Client{BaseURL: srv.URL}
	if _, err := c.Query(context.Background(), `up`); !errors.Is(err, ErrNoData) {
		t.Fatalf("want ErrNoData, got %v", err)
	}
}

func TestQueryFailureModes(t *testing.T) {
	// 未配置地址。
	if _, err := (&Client{}).Query(context.Background(), `up`); err == nil {
		t.Fatal("empty base URL must fail")
	}
	// 非 200。
	srv := fakeProm(t, `overload`, 503)
	defer srv.Close()
	if _, err := (&Client{BaseURL: srv.URL}).Query(context.Background(), `up`); err == nil {
		t.Fatal("503 must fail")
	}
	// error 状态。
	srv2 := fakeProm(t, `{"status":"error","error":"bad query"}`, 200)
	defer srv2.Close()
	if _, err := (&Client{BaseURL: srv2.URL}).Query(context.Background(), `up{`); err == nil {
		t.Fatal("error status must fail")
	}
}
