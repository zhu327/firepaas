package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWebhookScaleUpNotifier(t *testing.T) {
	var got ScaleUpSignal
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &got)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	n := &WebhookScaleUpNotifier{URL: srv.URL, Token: "s3cret"}
	sig := ScaleUpSignal{Pool: "compute", AppID: "app", OperationID: "op-1", VCPU: 2, Reason: "no candidates"}
	if err := n.NotifyScaleUp(context.Background(), sig); err != nil {
		t.Fatal(err)
	}
	if got.Pool != "compute" || got.OperationID != "op-1" {
		t.Fatalf("signal = %+v", got)
	}
	if auth != "Bearer s3cret" {
		t.Fatalf("auth = %q", auth)
	}
}

func TestWebhookScaleUpNotifierFailureModes(t *testing.T) {
	// nil 与空 URL = 禁用，不报错。
	var nilN *WebhookScaleUpNotifier
	if err := nilN.NotifyScaleUp(context.Background(), ScaleUpSignal{}); err != nil {
		t.Fatal(err)
	}
	if err := (&WebhookScaleUpNotifier{}).NotifyScaleUp(context.Background(), ScaleUpSignal{}); err != nil {
		t.Fatal(err)
	}
	// 非 2xx 报错（调用方只记日志，见 signalNodeScaleUp）。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()
	if err := (&WebhookScaleUpNotifier{URL: srv.URL}).NotifyScaleUp(context.Background(), ScaleUpSignal{}); err == nil {
		t.Fatal("500 must surface")
	}
	// 不可达。
	if err := (&WebhookScaleUpNotifier{URL: "http://127.0.0.1:1"}).NotifyScaleUp(context.Background(), ScaleUpSignal{}); err == nil {
		t.Fatal("unreachable must surface")
	}
}
