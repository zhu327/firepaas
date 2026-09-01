package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type flushRecorder struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (w *flushRecorder) Flush() {
	w.flushed = true
	w.ResponseRecorder.Flush()
}

func TestAuditRecorderPreservesFlusher(t *testing.T) {
	underlying := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	wrapped := auditMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("audit middleware hid http.Flusher")
		}
		flusher.Flush()
	}))
	wrapped.ServeHTTP(underlying, httptest.NewRequest(http.MethodGet, "/stream", nil))
	if !underlying.flushed {
		t.Fatal("flush was not forwarded")
	}
}
