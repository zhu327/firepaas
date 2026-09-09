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
			if got := agentFeatureIDs(mode, nil, "", ""); slices.Contains(got, capabilities.SecretOneShotV1) {
				t.Fatalf("mode %q must not advertise %s: %v", mode, capabilities.SecretOneShotV1, got)
			}
		})
	}

	t.Setenv("FIREPAAS_AGENT_FEATURE_IDS", "")
	if got := agentFeatureIDs(machine.SecretInjectionOneShot, nil, "", ""); !slices.Contains(
		got,
		capabilities.SecretOneShotV1,
	) {
		t.Fatalf("safe one-shot mode must advertise %s: %v", capabilities.SecretOneShotV1, got)
	}
}

func TestAgentFeatureIDsOverrideOnlyReducesCapabilities(t *testing.T) {
	t.Setenv("FIREPAAS_AGENT_FEATURE_IDS", capabilities.GuestLogsV1+","+capabilities.SecretOneShotV1+",invalid")
	got := agentFeatureIDs(machine.SecretInjectionOneShot, nil, "", "")
	want := []string{capabilities.GuestLogsV1, capabilities.SecretOneShotV1}
	if !slices.Equal(got, want) {
		t.Fatalf("features = %v, want %v", got, want)
	}
}

func TestAgentFeatureIDsVolumeRequiresAssembly(t *testing.T) {
	t.Setenv("FIREPAAS_AGENT_FEATURE_IDS", "")
	if slices.Contains(agentFeatureIDs(machine.SecretInjectionOneShot, nil, "", ""), capabilities.VolumeLocalRWV1) {
		t.Fatal("volume capability advertised without volume manager")
	}
	got := agentFeatureIDs(machine.SecretInjectionOneShot, nil, "", "", true)
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
	for _, f := range agentFeatureIDs(machine.SecretInjectionOneShot, nil, "", "") {
		if f == capabilities.EgressCidrV1 || f == capabilities.EgressDomainV1 {
			t.Fatalf("egress capability %s must not be advertised without assembly", f)
		}
	}
	// 装配后可报告，且仍受环境变量减法约束。
	t.Setenv("FIREPAAS_AGENT_FEATURE_IDS", capabilities.EgressCidrV1)
	got := agentFeatureIDs(
		machine.SecretInjectionOneShot,
		[]string{capabilities.EgressCidrV1, capabilities.EgressDomainV1},
		"",
		"",
	)
	if !slices.Contains(got, capabilities.EgressCidrV1) || slices.Contains(got, capabilities.EgressDomainV1) {
		t.Fatalf("egress capabilities mismatch: %v", got)
	}
}

// TestAgentFeatureIDsNetworkCapabilityIsExclusive：ADR-0040 §12 二选一上报
// （network.ebpf.v1 / network.nftfallback.v1）：装配侧只传一个值；空串 =
// 不上报（ebpf 数据面落地前虚报会误导调度硬过滤）。
func TestAgentFeatureIDsNetworkCapabilityIsExclusive(t *testing.T) {
	t.Setenv("FIREPAAS_AGENT_FEATURE_IDS", "")
	if got := agentFeatureIDs(machine.SecretInjectionOneShot, nil, "", ""); slices.Contains(
		got, capabilities.NetworkEbpfV1) || slices.Contains(got, capabilities.NetworkNftFallbackV1) {
		t.Fatalf("no network capability must be advertised when unset: %v", got)
	}
	got := agentFeatureIDs(machine.SecretInjectionOneShot, nil, capabilities.NetworkEbpfV1, "")
	if !slices.Contains(got, capabilities.NetworkEbpfV1) ||
		slices.Contains(got, capabilities.NetworkNftFallbackV1) {
		t.Fatalf("ebpf exclusivity violated: %v", got)
	}
	got = agentFeatureIDs(machine.SecretInjectionOneShot, nil, capabilities.NetworkNftFallbackV1, "")
	if !slices.Contains(got, capabilities.NetworkNftFallbackV1) ||
		slices.Contains(got, capabilities.NetworkEbpfV1) {
		t.Fatalf("nft-fallback exclusivity violated: %v", got)
	}
}

// TestAgentFeatureIDsMeshRequiresExplicitMode（ADR-0040 §12，W3）：
// mesh.eastwest.v1 只在显式传入时广告（调用方保证 ebpf 可用且 eastwest 模式）；
// 空串 = 不上报，mesh 服务永不调度到该节点。
func TestAgentFeatureIDsMeshRequiresExplicitMode(t *testing.T) {
	t.Setenv("FIREPAAS_AGENT_FEATURE_IDS", "")
	if got := agentFeatureIDs(machine.SecretInjectionOneShot, nil, capabilities.NetworkEbpfV1, ""); slices.Contains(
		got, capabilities.MeshEastWestV1) {
		t.Fatalf("mesh capability must not be advertised when unset: %v", got)
	}
	got := agentFeatureIDs(machine.SecretInjectionOneShot, nil,
		capabilities.NetworkEbpfV1, capabilities.MeshEastWestV1)
	if !slices.Contains(got, capabilities.MeshEastWestV1) ||
		!slices.Contains(got, capabilities.NetworkEbpfV1) {
		t.Fatalf("mesh mode must advertise both mesh and ebpf: %v", got)
	}
}
