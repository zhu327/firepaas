package main

import "testing"

// v1.4-C：prewarm/pin 只接受 digest-pinned 引用或裸 digest。
func TestPrewarmDigestFromRef(t *testing.T) {
	digest := "sha256:" + makeHex(64)
	for _, ref := range []string{
		"registry.local/app@" + digest,
		digest,
	} {
		got, ok := prewarmDigestFromRef(ref)
		if !ok || got != digest {
			t.Fatalf("prewarmDigestFromRef(%q) = %q, %v", ref, got, ok)
		}
	}
	for _, ref := range []string{
		"registry.local/app:latest",
		"registry.local/app@sha256:short",
		"sha256:UPPERCASE",
		"",
		"app@sha256:" + makeHex(63),
	} {
		if got, ok := prewarmDigestFromRef(ref); ok {
			t.Fatalf("prewarmDigestFromRef(%q) = %q, true; want rejection", ref, got)
		}
	}
}

func makeHex(n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte("0123456789abcdef"[i%16])
	}
	return string(out)
}
