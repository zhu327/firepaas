// Package appcommand implements transport-independent application mutations.
package appcommand

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/zhu327/firepaas/internal/capabilities"
	agentv1 "github.com/zhu327/firepaas/internal/contracts/agentv1"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"github.com/zhu327/firepaas/shared/pkg/id"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	ErrAppNotFound             = errors.New("app not found")
	ErrNoActiveDeployment      = errors.New("no active deployment")
	ErrInvalidIntent           = errors.New("invalid deployment intent")
	ErrInvalidActiveDeployment = errors.New("invalid active deployment")
	ErrRolloutBusy             = errors.New("rollout already in progress for this app")
)

// Store is the narrow persistence port used by a deployment command.
type Store interface {
	GetApp(context.Context, string) (*store.App, error)
	ActiveDeploymentForApp(context.Context, string) (*store.Deployment, error)
	GetSecretMeta(context.Context, string, string, *int64) (*store.SecretMeta, error)
	DeployApp(context.Context, store.Deployment, store.Rollout, int64) error
}

// ImageValidator validates and canonicalizes an image reference.
type ImageValidator interface {
	Validate(string) (string, error)
}

type Service struct {
	Name         string
	InternalPort int
}

type HealthCheck struct {
	Type               string
	Target             string
	IntervalSeconds    uint32
	TimeoutSeconds     uint32
	UnhealthyThreshold uint32
}

type AutoStandby struct {
	Enabled                bool
	IdleTimeoutSeconds     uint32
	IgnoreDestinationPorts []uint32
}

type EgressPolicy struct {
	Mode              string
	AllowedCIDRs      []string
	DeniedCIDRs       []string
	AllowedDomains    []string
	MaxTCPConnections uint32
	AuditAll          bool
}

// Intent is a transport-neutral request to create an immutable deployment.
type Intent struct {
	AppID        string
	ProjectID    string
	Image        string
	VCPU         int64
	MemMIB       int64
	Port         int
	Services     []Service
	Strategy     string
	Env          map[string]string
	NodePool     string
	Labels       map[string]string
	AntiAffinity string
	HealthCheck  *HealthCheck
	SecretRefs   map[string]store.SecretRef
	AutoStandby  *AutoStandby
	Egress       *EgressPolicy
	// InheritAll selects the secret-binding update semantics: placement and
	// strategy are inherited in addition to ordinary deployment fields.
	InheritAll bool
	// ReadActiveFirst preserves the secret-ref endpoint's historical lookup order.
	ReadActiveFirst bool
}

type Result struct {
	AppID        string
	DeploymentID string
	Generation   int64
	RolloutID    string
	Status       string
}

type Command struct {
	store  Store
	images ImageValidator
	newID  func() string
}

func New(st Store, images ImageValidator) *Command {
	return &Command{store: st, images: images, newID: id.New}
}

// Execute validates the intent, derives the next immutable deployment and
// starts its rollout with one atomic persistence transition.
func (c *Command) Execute(ctx context.Context, in Intent) (Result, error) {
	var app *store.App
	var active *store.Deployment
	var err error
	if in.ReadActiveFirst {
		active, err = c.store.ActiveDeploymentForApp(ctx, in.AppID)
		if err != nil {
			return Result{}, err
		}
		if active == nil {
			return Result{}, ErrNoActiveDeployment
		}
	}
	app, err = c.store.GetApp(ctx, in.AppID)
	if err != nil {
		return Result{}, err
	}
	if app == nil {
		return Result{}, ErrAppNotFound
	}
	if active == nil {
		active, err = c.store.ActiveDeploymentForApp(ctx, in.AppID)
		if err != nil {
			return Result{}, err
		}
		if active == nil {
			return Result{}, ErrNoActiveDeployment
		}
	}

	if in.Image == "" {
		in.Image = active.ImageRef
	} else {
		normalized, err := c.images.Validate(in.Image)
		if err != nil {
			return Result{}, invalid(err)
		}
		in.Image = normalized
	}
	if in.SecretRefs == nil {
		in.SecretRefs = active.SecretRefs
	}
	in.SecretRefs, err = c.validateSecretRefs(ctx, in.ProjectID, in.SecretRefs)
	if err != nil {
		return Result{}, invalid(err)
	}

	generation := app.Generation + 1
	deployment, err := prepare(active, in, generation)
	if err != nil {
		return Result{}, err
	}
	deployment.ID = fmt.Sprintf("dep-%s-%d", app.ID, generation)
	deployment.AppID = app.ID
	deployment.Status = "PREPARING"
	rolloutID := "rollout-" + c.newID()
	rollout := store.Rollout{ID: rolloutID, AppID: app.ID, FromGeneration: active.Generation, ToGeneration: generation}
	if err := c.store.DeployApp(ctx, deployment, rollout, generation); err != nil {
		if errors.Is(err, store.ErrRolloutBusy) {
			return Result{}, ErrRolloutBusy
		}
		return Result{}, err
	}
	return Result{
		AppID:        app.ID,
		DeploymentID: deployment.ID,
		Generation:   generation,
		RolloutID:    rolloutID,
		Status:       "PREPARING",
	}, nil
}

func invalid(err error) error { return fmt.Errorf("%w: %v", ErrInvalidIntent, err) }

func (c *Command) validateSecretRefs(
	ctx context.Context,
	projectID string,
	refs map[string]store.SecretRef,
) (map[string]store.SecretRef, error) {
	out := make(map[string]store.SecretRef, len(refs))
	for varName, ref := range refs {
		if varName == "" {
			return nil, fmt.Errorf("secret ref %q: var name is required", varName)
		}
		if ref.Secret == "" && ref.Version == nil {
			continue
		}
		if ref.Secret == "" {
			return nil, fmt.Errorf("secret ref %q: secret name is required", varName)
		}
		if _, err := c.store.GetSecretMeta(ctx, projectID, ref.Secret, ref.Version); err != nil {
			return nil, fmt.Errorf("secret %q (ref %q) not found", ref.Secret, varName)
		}
		out[varName] = ref
	}
	return out, nil
}

func prepare(active *store.Deployment, in Intent, generation int64) (store.Deployment, error) {
	// R2 评审：负值显式拒绝（0 走继承；负值此前会静默落库成非法 spec）。
	if in.VCPU < 0 || in.MemMIB < 0 {
		return store.Deployment{}, invalid(errors.New("vcpu and mem_mib must be >= 0"))
	}
	if in.VCPU == 0 {
		in.VCPU = active.VCPU
	}
	if in.MemMIB == 0 {
		in.MemMIB = active.MemMIB
	}
	if in.Services == nil && in.Port == 0 {
		if len(active.Services) > 0 {
			in.Services = make([]Service, len(active.Services))
			for i, svc := range active.Services {
				in.Services[i] = Service{Name: svc.Name, InternalPort: svc.InternalPort}
			}
		} else {
			in.Port = active.Port
		}
	}
	if in.Env == nil {
		in.Env = active.Env
	}

	services, port, err := resolveServices(in.Services, in.Port)
	if err != nil {
		return store.Deployment{}, invalid(err)
	}
	strategyInput := in.Strategy
	if in.InheritAll && strategyInput == "" {
		strategyInput = active.EffectiveStrategy()
	}
	strategy, err := resolveStrategy(strategyInput)
	if err != nil {
		return store.Deployment{}, invalid(err)
	}

	healthCheck, err := marshalHealthCheck(in.HealthCheck)
	if err != nil {
		return store.Deployment{}, invalid(err)
	}
	if in.HealthCheck == nil && len(active.HealthCheck) > 0 && string(active.HealthCheck) != "null" {
		healthCheck = append(json.RawMessage(nil), active.HealthCheck...)
	}
	placement, err := marshalPlacement(in.NodePool, in.Labels, in.AntiAffinity)
	if err != nil {
		return store.Deployment{}, invalid(err)
	}
	if in.InheritAll {
		placement = append(json.RawMessage(nil), active.Placement...)
	}

	autoJSON, auto, err := resolveAutoStandby(in.AutoStandby, active.AutoStandby, in.InheritAll)
	if err != nil {
		return store.Deployment{}, err
	}
	if len(in.SecretRefs) > 0 && auto != nil && auto.Enabled {
		return store.Deployment{}, invalid(
			errors.New(
				"secret_refs cannot be combined with enabled auto_standby: secret executions forbid memory snapshots (ADR-0024)",
			),
		)
	}
	egress, err := resolveEgress(in.Egress, active.EgressPolicy)
	if err != nil {
		return store.Deployment{}, err
	}
	egressJSON, err := marshalEgress(egress, generation)
	if err != nil {
		return store.Deployment{}, invalid(err)
	}

	return store.Deployment{
		Generation: generation, ImageRef: in.Image, VCPU: in.VCPU, MemMIB: in.MemMIB,
		Port: port, Services: services, Strategy: strategy, Env: in.Env, SecretRefs: in.SecretRefs,
		Placement: placement, HealthCheck: healthCheck, AutoStandby: autoJSON,
		RequiredFeatures: requiredFeatures(in.SecretRefs), EgressPolicy: egressJSON,
	}, nil
}

func resolveServices(services []Service, port int) ([]store.ServiceSpec, int, error) {
	if len(services) == 0 {
		if port != 0 && (port < 1 || port > 65535) {
			return nil, 0, errors.New("port must be in [1,65535]")
		}
		return nil, port, nil
	}
	if len(services) > 8 {
		return nil, 0, errors.New("services supports at most 8 entries in v1.1")
	}
	if port != 0 && port != services[0].InternalPort {
		return nil, 0, fmt.Errorf("port conflicts with services[0].internal_port (%d)", services[0].InternalPort)
	}
	out := make([]store.ServiceSpec, 0, len(services))
	seenPort := map[int]bool{}
	seenName := map[string]bool{}
	for i, svc := range services {
		if svc.InternalPort < 1 || svc.InternalPort > 65535 {
			return nil, 0, fmt.Errorf("services[%d].internal_port must be in [1,65535]", i)
		}
		if seenPort[svc.InternalPort] {
			return nil, 0, fmt.Errorf("services[%d].internal_port %d duplicated", i, svc.InternalPort)
		}
		name := svc.Name
		if name == "" {
			name = fmt.Sprintf("svc-%d", svc.InternalPort)
		}
		if seenName[name] {
			return nil, 0, fmt.Errorf("services[%d].name %q duplicated", i, name)
		}
		seenPort[svc.InternalPort], seenName[name] = true, true
		out = append(out, store.ServiceSpec{Name: name, InternalPort: svc.InternalPort})
	}
	return out, out[0].InternalPort, nil
}

func resolveStrategy(strategy string) (string, error) {
	switch strategy {
	case "", "bluegreen":
		return "bluegreen", nil
	case "rolling":
		return "rolling", nil
	default:
		return "", errors.New("strategy must be bluegreen or rolling")
	}
}

func marshalHealthCheck(check *HealthCheck) (json.RawMessage, error) {
	if check == nil {
		return nil, nil
	}
	var typ pb.HealthCheckSpec_Type
	switch strings.ToUpper(check.Type) {
	case "HTTP":
		typ = pb.HealthCheckSpec_HTTP
	case "TCP":
		typ = pb.HealthCheckSpec_TCP
	case "":
		return nil, nil
	default:
		return nil, errors.New("health_check.type must be http or tcp")
	}
	raw, err := protojson.Marshal(&pb.HealthCheckSpec{
		Type: typ, Target: check.Target,
		IntervalSeconds: check.IntervalSeconds, TimeoutSeconds: check.TimeoutSeconds,
		UnhealthyThreshold: check.UnhealthyThreshold,
	})
	return json.RawMessage(raw), err
}

func marshalPlacement(nodePool string, labels map[string]string, antiAffinity string) (json.RawMessage, error) {
	aa := pb.PlacementConstraints_NONE
	if antiAffinity == "DEPLOYMENT" {
		aa = pb.PlacementConstraints_DEPLOYMENT
	}
	raw, err := protojson.Marshal(&pb.PlacementConstraints{NodePool: nodePool, Labels: labels, AntiAffinity: aa})
	return json.RawMessage(raw), err
}

func marshalAutoStandby(policy *AutoStandby) (json.RawMessage, error) {
	if policy == nil || !policy.Enabled {
		return nil, nil
	}
	if policy.IdleTimeoutSeconds < 5 {
		return nil, errors.New("auto_standby.idle_timeout_seconds must be >= 5 when enabled")
	}
	for _, p := range policy.IgnoreDestinationPorts {
		if p == 0 || p > 65535 {
			return nil, fmt.Errorf("auto_standby.ignore_destination_ports entry %d out of range", p)
		}
	}
	raw, err := protojson.Marshal(
		&pb.AutoStandbyPolicy{
			Enabled:                true,
			IdleTimeoutSeconds:     policy.IdleTimeoutSeconds,
			IgnoreDestinationPorts: policy.IgnoreDestinationPorts,
		},
	)
	return json.RawMessage(raw), err
}

func resolveAutoStandby(policy *AutoStandby, raw json.RawMessage, strict bool) (json.RawMessage, *AutoStandby, error) {
	if policy != nil {
		encoded, err := marshalAutoStandby(policy)
		if err != nil {
			return nil, nil, invalid(err)
		}
		return encoded, policy, nil
	}
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil, nil
	}
	var inherited pb.AutoStandbyPolicy
	if err := protojson.Unmarshal(raw, &inherited); err != nil {
		if strict {
			return nil, nil, fmt.Errorf("%w: decode active auto_standby: %v", ErrInvalidActiveDeployment, err)
		}
		return nil, nil, nil
	}
	return append(
			json.RawMessage(nil),
			raw...), &AutoStandby{
			Enabled:                inherited.GetEnabled(),
			IdleTimeoutSeconds:     inherited.GetIdleTimeoutSeconds(),
			IgnoreDestinationPorts: append([]uint32(nil), inherited.GetIgnoreDestinationPorts()...),
		}, nil
}

func resolveEgress(policy *EgressPolicy, raw json.RawMessage) (*EgressPolicy, error) {
	if policy != nil || len(raw) == 0 || string(raw) == "null" {
		return policy, nil
	}
	var inherited pb.EgressPolicySpec
	if err := protojson.Unmarshal(raw, &inherited); err != nil {
		return nil, fmt.Errorf("%w: active deployment has invalid egress policy", ErrInvalidActiveDeployment)
	}
	if err := agentv1.ValidateEgressPolicy(&inherited); err != nil {
		return nil, fmt.Errorf("%w: active deployment has invalid egress policy", ErrInvalidActiveDeployment)
	}
	return &EgressPolicy{
		Mode:              strings.ToLower(strings.TrimPrefix(inherited.GetMode().String(), "MODE_")),
		AllowedCIDRs:      append([]string(nil), inherited.GetAllowedCidrs()...),
		DeniedCIDRs:       append([]string(nil), inherited.GetDeniedCidrs()...),
		AllowedDomains:    append([]string(nil), inherited.GetAllowedDomains()...),
		MaxTCPConnections: inherited.GetMaxTcpConnections(),
		AuditAll:          inherited.GetAuditAll(),
	}, nil
}

func marshalEgress(policy *EgressPolicy, generation int64) (json.RawMessage, error) {
	if policy == nil {
		return nil, nil
	}
	var mode pb.EgressPolicySpec_Mode
	switch strings.ToLower(policy.Mode) {
	case "", "unrestricted":
		mode = pb.EgressPolicySpec_UNRESTRICTED
	case "deny_all":
		mode = pb.EgressPolicySpec_DENY_ALL
	case "allowlist":
		mode = pb.EgressPolicySpec_ALLOWLIST
	default:
		return nil, errors.New("egress.mode must be unrestricted, deny_all or allowlist")
	}
	if policy.MaxTCPConnections > 65535 {
		return nil, errors.New("egress.max_tcp_connections must be <= 65535")
	}
	domains := make([]string, 0, len(policy.AllowedDomains))
	for _, domain := range policy.AllowedDomains {
		normalized, err := agentv1.NormalizeEgressDomain(domain)
		if err != nil {
			return nil, err
		}
		domains = append(domains, normalized)
	}
	spec := &pb.EgressPolicySpec{
		Mode:              mode,
		AllowedCidrs:      policy.AllowedCIDRs,
		DeniedCidrs:       policy.DeniedCIDRs,
		AllowedDomains:    domains,
		MaxTcpConnections: policy.MaxTCPConnections,
		PolicyGeneration:  uint64(generation),
		AuditAll:          policy.AuditAll,
	}
	if err := agentv1.ValidateEgressPolicy(spec); err != nil {
		return nil, err
	}
	raw, err := protojson.Marshal(spec)
	return json.RawMessage(raw), err
}

func requiredFeatures(refs map[string]store.SecretRef) []string {
	if len(refs) == 0 {
		return nil
	}
	return []string{capabilities.SecretOneShotV1}
}
