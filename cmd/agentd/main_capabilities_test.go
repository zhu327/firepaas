package main

import (
	"slices"
	"testing"

	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/zhu327/firepaas/internal/capabilities"
)

func TestAgentFeatureIDsSecretCapabilityFollowsSafeMode(t *testing.T) {
	for _, mode := range []string{machine.SecretInjectionOff, machine.SecretInjectionUnsafePersistedEnv, "unknown"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("FIREPAAS_AGENT_FEATURE_IDS", capabilities.SecretOneShotV1)
			if got := agentFeatureIDs(mode, nil); slices.Contains(got, capabilities.SecretOneShotV1) {
				t.Fatalf("mode %q must not advertise %s: %v", mode, capabilities.SecretOneShotV1, got)
			}
		})
	}

	t.Setenv("FIREPAAS_AGENT_FEATURE_IDS", "")
	if got := agentFeatureIDs(machine.SecretInjectionOneShot, nil); !slices.Contains(
		got,
		capabilities.SecretOneShotV1,
	) {
		t.Fatalf("safe one-shot mode must advertise %s: %v", capabilities.SecretOneShotV1, got)
	}
}

func TestAgentFeatureIDsOverrideOnlyReducesCapabilities(t *testing.T) {
	t.Setenv("FIREPAAS_AGENT_FEATURE_IDS", capabilities.GuestLogsV1+","+capabilities.SecretOneShotV1+",invalid")
	got := agentFeatureIDs(machine.SecretInjectionOneShot, nil)
	want := []string{capabilities.GuestLogsV1, capabilities.SecretOneShotV1}
	if !slices.Equal(got, want) {
		t.Fatalf("features = %v, want %v", got, want)
	}
}

func TestAgentFeatureIDsVolumeRequiresAssembly(t *testing.T) {
	t.Setenv("FIREPAAS_AGENT_FEATURE_IDS", "")
	if slices.Contains(agentFeatureIDs(machine.SecretInjectionOneShot, nil), capabilities.VolumeLocalRWV1) {
		t.Fatal("volume capability advertised without volume manager")
	}
	got := agentFeatureIDs(machine.SecretInjectionOneShot, nil, true)
	if !slices.Contains(got, capabilities.VolumeLocalRWV1) || !slices.Contains(got, capabilities.VolumeDatasetROV1) {
		t.Fatal("assembled volume manager must advertise LOCAL_RW and DATASET_RO capabilities")
	}
	// v1.4-A：per-execution CoW overlay 未过验收（hypeman capability、磁盘
	// admission、cleanup、真机 e2e），装配了 volume manager 也不得广告。
	if slices.Contains(got, capabilities.VolumeDatasetOverlayV1) {
		t.Fatal("dataset overlay capability must not be advertised until CoW passes acceptance")
	}
	if !slices.Contains(got, capabilities.SnapshotMemoryV1) ||
		!slices.Contains(got, capabilities.SnapshotFilesystemV1) {
		t.Fatal("agent must advertise hypeman snapshot capabilities")
	}
}

func TestAgentFeatureIDsEgressRequiresAssembly(t *testing.T) {
	t.Setenv("FIREPAAS_AGENT_FEATURE_IDS", "")
	// 未装配 egress 时绝不报告 egress 能力（fail closed）。
	for _, f := range agentFeatureIDs(machine.SecretInjectionOneShot, nil) {
		if f == capabilities.EgressCidrV1 || f == capabilities.EgressDomainV1 {
			t.Fatalf("egress capability %s must not be advertised without assembly", f)
		}
	}
	// 装配后可报告，且仍受环境变量减法约束。
	t.Setenv("FIREPAAS_AGENT_FEATURE_IDS", capabilities.EgressCidrV1)
	got := agentFeatureIDs(
		machine.SecretInjectionOneShot,
		[]string{capabilities.EgressCidrV1, capabilities.EgressDomainV1},
	)
	if !slices.Contains(got, capabilities.EgressCidrV1) || slices.Contains(got, capabilities.EgressDomainV1) {
		t.Fatalf("egress capabilities mismatch: %v", got)
	}
}
