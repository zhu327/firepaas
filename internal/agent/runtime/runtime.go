// Package runtime 装配 agentd 所需的 hypeman lib 组件。
// 只 import hypeman/lib/*；配置经 lib/config（类型别名入口）读取。
package runtime

import (
	"fmt"

	"github.com/c2h5oh/datasize"
	"github.com/kernel/hypeman/lib/config"
	"github.com/kernel/hypeman/lib/devices"
	"github.com/kernel/hypeman/lib/hypervisor"
	"github.com/kernel/hypeman/lib/hypervisor/cloudhypervisor"
	"github.com/kernel/hypeman/lib/hypervisor/firecracker"
	"github.com/kernel/hypeman/lib/images"
	"github.com/kernel/hypeman/lib/instances"
	"github.com/kernel/hypeman/lib/network"
	"github.com/kernel/hypeman/lib/paths"
	"github.com/kernel/hypeman/lib/system"
	"github.com/kernel/hypeman/lib/volumes"
	"go.opentelemetry.io/otel"
)

// Set 是 agentd 需要的一组 hypeman 管理器。
type Set struct {
	Paths     *paths.Paths
	Images    images.Manager
	System    system.Manager
	Network   network.Manager
	Volumes   volumes.Manager
	Instances instances.Manager
}

// LoadConfig 从 CONFIG_PATH 加载 hypeman 兼容配置。
func LoadConfig() (*config.Config, error) {
	cfg, err := config.Load("")
	if err != nil {
		return nil, fmt.Errorf("load hypeman config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate hypeman config: %w", err)
	}
	return cfg, nil
}

// Assemble 构造 runtime Set。不启动后台服务，只做依赖装配。
func Assemble(cfg *config.Config) (*Set, error) {
	p := paths.New(cfg.DataDir)
	meter := otel.GetMeterProvider().Meter("firepaas-agent")
	tracer := otel.GetTracerProvider().Tracer("firepaas-agent")

	if cfg.Hypervisor.FirecrackerBinaryPath != "" {
		firecracker.SetCustomBinaryPath(cfg.Hypervisor.FirecrackerBinaryPath)
	}
	if err := cloudhypervisor.SetDefaultVersion(cfg.Hypervisor.CloudHypervisorDefaultVersion); err != nil {
		return nil, fmt.Errorf("set cloud-hypervisor default version: %w", err)
	}

	imageMgr, err := images.NewManager(p, cfg.Limits.MaxConcurrentBuilds, meter)
	if err != nil {
		return nil, fmt.Errorf("image manager: %w", err)
	}
	systemMgr := system.NewManager(p)
	networkMgr := network.NewManager(p, cfg, meter)
	deviceMgr := devices.NewManager(p)
	volumeMgr := volumes.NewManager(p, 0, meter) // M1 不限制总卷容量

	var maxOverlaySize datasize.ByteSize
	if cfg.Limits.MaxOverlaySize != "" && cfg.Limits.MaxOverlaySize != "0" {
		if err := maxOverlaySize.UnmarshalText([]byte(cfg.Limits.MaxOverlaySize)); err != nil {
			return nil, fmt.Errorf("parse limits.max_overlay_size: %w", err)
		}
	}
	var maxMemoryPerInstance int64
	if cfg.Limits.MaxMemoryPerInstance != "" && cfg.Limits.MaxMemoryPerInstance != "0" {
		var mem datasize.ByteSize
		if err := mem.UnmarshalText([]byte(cfg.Limits.MaxMemoryPerInstance)); err != nil {
			return nil, fmt.Errorf("parse limits.max_memory_per_instance: %w", err)
		}
		maxMemoryPerInstance = int64(mem)
	}

	limits := instances.ResourceLimits{
		MaxVcpusPerInstance:  cfg.Limits.MaxVcpusPerInstance,
		MaxOverlaySize:       int64(maxOverlaySize),
		MaxMemoryPerInstance: maxMemoryPerInstance,
	}
	instanceMgr, err := instances.NewManagerWithConfigE(
		p,
		imageMgr,
		systemMgr,
		networkMgr,
		deviceMgr,
		volumeMgr,
		limits,
		hypervisor.Type(cfg.Hypervisor.Default),
		instances.SnapshotPolicy{},
		instances.ManagerConfig{},
		meter,
		tracer,
	)
	if err != nil {
		return nil, fmt.Errorf("instance manager: %w", err)
	}

	return &Set{
		Paths:     p,
		Images:    imageMgr,
		System:    systemMgr,
		Network:   networkMgr,
		Volumes:   volumeMgr,
		Instances: instanceMgr,
	}, nil
}
