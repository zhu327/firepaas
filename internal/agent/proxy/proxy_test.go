package proxy

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/kernel/hypeman/lib/images"
	"github.com/kernel/hypeman/lib/instances"
)

type testInstances struct {
	instance *instances.Instance
	err      error
}

func (m *testInstances) CreateInstance(context.Context, instances.CreateInstanceRequest) (*instances.Instance, error) {
	return nil, errors.New("unused")
}
func (m *testInstances) ListInstances(context.Context, *instances.ListInstancesFilter) ([]instances.Instance, error) {
	return nil, nil
}
func (m *testInstances) GetInstance(context.Context, string) (*instances.Instance, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.instance, nil
}
func (m *testInstances) DeleteInstance(context.Context, string) error { return nil }
func (m *testInstances) StandbyInstance(context.Context, string, instances.StandbyInstanceRequest) (*instances.Instance, error) {
	return nil, errors.New("unused")
}
func (m *testInstances) RestoreInstance(context.Context, string) (*instances.Instance, error) {
	return nil, errors.New("unused")
}

// Keep images imported only through the adapter's interface surface explicit;
// proxy endpoint tests do not exercise image operations.
var _ machine.ImageManager = (*testImages)(nil)

type testImages struct{}

func (*testImages) CreateImage(context.Context, images.CreateImageRequest) (*images.Image, error) {
	return nil, nil
}
func (*testImages) WaitForReady(context.Context, string) error         { return nil }
func (*testImages) ListImages(context.Context) ([]images.Image, error) { return nil, nil }
func (*testImages) DeleteImage(context.Context, string) error          { return nil }

func proxyRequest() *http.Request {
	r := httptest.NewRequest(http.MethodGet, "http://agent/", nil)
	r.Header.Set(HeaderMachineID, "m1")
	r.Header.Set(HeaderExecutionID, "e1")
	return r
}

func TestProxyMarksEndpointResolution502Retryable(t *testing.T) {
	p := New(machine.New(&testInstances{err: errors.New("instance gone")}, &testImages{}, nil, nil))
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, proxyRequest())

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
	if got := rr.Header().Get(HeaderRetryable); got != retryableValue {
		t.Fatalf("retry marker = %q, want %q", got, retryableValue)
	}
}

func TestProxyMarksGuestTransport502Retryable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	listener.Close() // preserve a known-unused endpoint for the proxy transport.
	if err != nil {
		t.Fatal(err)
	}
	inst := &instances.Instance{StoredMetadata: instances.StoredMetadata{
		IP: "127.0.0.1", Tags: map[string]string{
			"firepaas/execution_id": "e1", "firepaas/port": port,
		},
	}}
	p := New(machine.New(&testInstances{instance: inst}, &testImages{}, nil, nil))
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, proxyRequest())
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
	if got := rr.Header().Get(HeaderRetryable); got != retryableValue {
		t.Fatalf("retry marker = %q, want %q", got, retryableValue)
	}
}

func TestProxyDoesNotForwardOrTrustRetryHeaderFromWorkload(t *testing.T) {
	guestSawInternalHeader := false
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		guestSawInternalHeader = r.Header.Get(HeaderRetryable) != ""
		w.Header().Set(HeaderRetryable, retryableValue)
		http.Error(w, "workload failure", http.StatusBadGateway)
	}))
	defer guest.Close()

	// Use the server's host as the endpoint. The proxy's URL construction adds
	// http itself, so this is intentionally an HTTP guest server.
	inst := &instances.Instance{StoredMetadata: instances.StoredMetadata{
		IP: "127.0.0.1", Tags: map[string]string{"firepaas/execution_id": "e1"},
	}}
	// httptest exposes a dynamic port; split it through the instance port tag.
	_, port, err := net.SplitHostPort(guest.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	inst.Tags["firepaas/port"] = port
	p := New(machine.New(&testInstances{instance: inst}, &testImages{}, nil, nil))

	r := proxyRequest()
	r.Header.Set(HeaderRetryable, retryableValue)
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, r)
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
	if guestSawInternalHeader {
		t.Fatal("internal retry marker was forwarded to workload")
	}
	if got := rr.Header().Get(HeaderRetryable); got != "" {
		t.Fatalf("workload-forged retry marker leaked to edge: %q", got)
	}
}
