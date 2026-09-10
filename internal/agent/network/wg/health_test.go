package wg

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

type stubRunner struct {
	output string
	err    error
	calls  int
}

func (s *stubRunner) Run(_ context.Context, _, _ string, _ ...string) (string, error) {
	s.calls++
	return s.output, s.err
}

func TestClassifyHandshakeAge(t *testing.T) {
	now := time.Now().UTC()
	cases := []struct {
		name string
		last time.Time
		want PeerState
	}{
		{"never", time.Time{}, PeerUnknown},
		{"fresh", now.Add(-10 * time.Second), PeerHealthy},
		{"suspect", now.Add(-100 * time.Second), PeerSuspect},
		{"failed", now.Add(-10 * time.Minute), PeerFailed},
		{"future clock skew", now.Add(time.Minute), PeerHealthy},
	}
	for _, c := range cases {
		if got := ClassifyHandshakeAge(c.last, now); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}

func TestParseLatestHandshakes(t *testing.T) {
	now := time.Now().UTC()
	recent := now.Add(-20 * time.Second).Unix()
	old := now.Add(-time.Hour).Unix()
	out := fmt.Sprintf("cGsta2V5MQ== %d\ncGsta2V5Mg== %d\ncGsta2V5Mw== 0\nnot-a-line\n", recent, old)
	parsed := ParseLatestHandshakes(out, now)
	if len(parsed) != 3 {
		t.Fatalf("parsed %d peers, want 3 (dirty line skipped)", len(parsed))
	}
	if parsed["cGsta2V5MQ=="].State != PeerHealthy {
		t.Errorf("recent peer = %s, want healthy", parsed["cGsta2V5MQ=="].State)
	}
	if parsed["cGsta2V5Mg=="].State != PeerFailed {
		t.Errorf("old peer = %s, want failed", parsed["cGsta2V5Mg=="].State)
	}
	if parsed["cGsta2V5Mw=="].State != PeerUnknown {
		t.Errorf("never peer = %s, want unknown", parsed["cGsta2V5Mw=="].State)
	}
}

func TestManagerPeerHealthUsesRunner(t *testing.T) {
	recent := time.Now().UTC().Add(-5 * time.Second).Unix()
	stub := &stubRunner{output: fmt.Sprintf("cGsta2V5MQ== %d\n", recent)}
	m, err := New(Options{KeyDir: t.TempDir(), Runner: stub})
	if err != nil {
		t.Fatal(err)
	}
	health, err := m.PeerHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	h, ok := health["cGsta2V5MQ=="]
	if !ok {
		t.Fatal("missing peer in health map")
	}
	if h.State != PeerHealthy {
		t.Fatalf("state = %s, want healthy", h.State)
	}
	if stub.calls != 1 {
		t.Fatalf("runner calls = %d, want 1", stub.calls)
	}
	if !strings.Contains(h.Pubkey, "cGsta2V5MQ==") {
		t.Fatalf("pubkey = %q", h.Pubkey)
	}
}
