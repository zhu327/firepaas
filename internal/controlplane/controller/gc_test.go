package controller

import (
	"strings"
	"testing"
	"time"
)

func TestParsePinnedImageDigestFailClosed(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, ref := range []string{digest, "registry.example/app@" + digest} {
		got, ok := parsePinnedImageDigest(ref)
		if !ok || got != digest {
			t.Fatalf("parsePinnedImageDigest(%q) = %q, %v", ref, got, ok)
		}
	}
	for _, ref := range []string{"", "registry.example/app:latest", "sha256:short", "app@sha256:xyz"} {
		if got, ok := parsePinnedImageDigest(ref); ok {
			t.Fatalf("parsePinnedImageDigest(%q) = %q, true; want failure", ref, got)
		}
	}
}

func TestValidateReportedCacheFailsClosed(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	valid := map[string]gcCachedImage{digest: {ref: "repo/app@" + digest, sizeMib: 123}}
	if err := validateReportedCache([]string{digest}, valid); err != nil {
		t.Fatal(err)
	}
	if err := validateReportedCache([]string{"not-a-digest"}, map[string]gcCachedImage{}); err == nil {
		t.Fatal("unparseable cache reference must fail closed")
	}
	if err := validateReportedCache([]string{digest}, map[string]gcCachedImage{}); err == nil {
		t.Fatal("unresolved cache digest must fail closed")
	}
	if err := validateReportedCache([]string{digest}, map[string]gcCachedImage{digest: {ref: "repo/app@" + digest}}); err == nil {
		t.Fatal("unknown size_mib must fail closed")
	}
}

func TestGCDeletionBudgetUsesFractionBeforeConversion(t *testing.T) {
	if got := gcDeletionBudgetMib(1000, 0.85, 0.70); got != 150 {
		t.Fatalf("budget = %d MiB, want 150", got)
	}
	if got := gcDeletionBudgetMib(1000, 0.69, 0.70); got != 0 {
		t.Fatalf("budget below low watermark = %d, want 0", got)
	}
	if got := gcConsumeBudgetMib(150, 40); got != 110 {
		t.Fatalf("budget after observed 40 MiB image = %d, want 110", got)
	}
}

func TestDefaultScrubConfigIsOffAndBudgeted(t *testing.T) {
	cfg := DefaultScrubConfig()
	if cfg.Enabled || !validScrubConfig(cfg) || cfg.Budget != 1 {
		t.Fatalf("unsafe scrub defaults: %+v", cfg)
	}
}

func TestDefaultGCConfigIsOff(t *testing.T) {
	cfg := DefaultGCConfig()
	if cfg.Mode != "off" || !validGCConfig(cfg) || cfg.MinAge != time.Hour {
		t.Fatalf("unsafe default GC config: %+v", cfg)
	}
}

func TestInvalidGCConfigFailsValidation(t *testing.T) {
	cfg := DefaultGCConfig()
	cfg.Mode = "delete-now"
	if validGCConfig(cfg) {
		t.Fatal("unknown GC mode must be rejected")
	}
	cfg = DefaultGCConfig()
	cfg.LowWater = cfg.HighWater
	if validGCConfig(cfg) {
		t.Fatal("low watermark >= high watermark must be rejected")
	}
}
