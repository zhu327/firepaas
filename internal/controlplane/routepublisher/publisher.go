// Package routepublisher owns rollout-aware route derivation and the ordered
// PostgreSQL-to-Redis publication workflow. PostgreSQL is always synchronized
// before the rebuildable Redis projection is replaced or pruned.
package routepublisher

import (
	"context"
	"fmt"
	"sort"

	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// Store is the narrow PostgreSQL port needed by a complete route rebuild.
type Store interface {
	ActiveRouteMachines(context.Context) ([]store.Machine, error)
	ListActiveRollouts(context.Context) ([]store.Rollout, error)
	ListDeployments(context.Context, string) ([]store.Deployment, error)
	SyncRoutes(context.Context, []store.RouteRow) error
}

// Catalog is the narrow Redis projection port needed by a complete rebuild.
type Catalog interface {
	ReplaceHostRoutes(context.Context, string, []catalog.HostRoute, int) error
	PruneRoutes(context.Context, map[string]bool, map[string]bool) error
}

// Publisher is the controller's sole route writer.
type Publisher struct {
	store           Store
	catalog         Catalog
	defaultAppPort  int
	legacyProxyAddr string
}

func New(st Store, cat Catalog, defaultAppPort int, legacyProxyAddr string) *Publisher {
	return &Publisher{store: st, catalog: cat, defaultAppPort: defaultAppPort, legacyProxyAddr: legacyProxyAddr}
}

// Input is the complete in-memory snapshot consumed by deterministic derivation.
type Input struct {
	Machines        []store.Machine
	Deployments     []store.Deployment
	Rollouts        []store.Rollout
	ProxyByNode     map[string]string
	DefaultAppPort  int
	LegacyProxyAddr string
}

// Projection contains PostgreSQL route facts and the primary-port information
// needed to publish the equivalent Redis representation.
type Projection struct {
	Routes       []store.RouteRow
	PrimaryPorts map[string]int
}

// Rebuild loads route inputs, derives one projection without I/O, commits the
// PostgreSQL authority, then replaces and prunes Redis. A Redis failure is
// returned without undoing or reinterpreting the PostgreSQL result.
func (p *Publisher) Rebuild(ctx context.Context, proxyByNode map[string]string) error {
	machines, err := p.store.ActiveRouteMachines(ctx)
	if err != nil {
		return err
	}
	rollouts, err := p.store.ListActiveRollouts(ctx)
	if err != nil {
		return err
	}

	apps := make(map[string]bool)
	for _, m := range machines {
		apps[m.AppID] = true
	}
	appIDs := make([]string, 0, len(apps))
	for appID := range apps {
		appIDs = append(appIDs, appID)
	}
	sort.Strings(appIDs)
	var deployments []store.Deployment
	for _, appID := range appIDs {
		deps, err := p.store.ListDeployments(ctx, appID)
		if err != nil {
			return err
		}
		deployments = append(deployments, deps...)
	}

	projection := Derive(Input{
		Machines: machines, Deployments: deployments, Rollouts: rollouts,
		ProxyByNode: proxyByNode, DefaultAppPort: p.defaultAppPort,
		LegacyProxyAddr: p.legacyProxyAddr,
	})
	if err := p.store.SyncRoutes(ctx, projection.Routes); err != nil {
		return err
	}
	return p.publishRedis(ctx, projection)
}

// Derive applies readiness, rollout, generation, and multiport policy without I/O.
func Derive(in Input) Projection {
	rolloutByApp := make(map[string]*store.Rollout, len(in.Rollouts))
	for i := range in.Rollouts {
		rolloutByApp[in.Rollouts[i].AppID] = &in.Rollouts[i]
	}
	depGen := make(map[string]int64, len(in.Deployments))
	depServices := make(map[string][]store.ServiceSpec, len(in.Deployments))
	depStrategy := make(map[string]string, len(in.Deployments))
	toDepByApp := make(map[string]string)
	for i := range in.Deployments {
		dep := &in.Deployments[i]
		depGen[dep.ID] = dep.Generation
		depServices[dep.ID] = dep.EffectiveServices()
		depStrategy[dep.ID] = dep.EffectiveStrategy()
		if rollout := rolloutByApp[dep.AppID]; rollout != nil && dep.Generation == rollout.ToGeneration {
			toDepByApp[dep.AppID] = dep.ID
		}
	}

	type appOrdinal struct {
		app     string
		ordinal int
	}
	cutOrdinals := make(map[appOrdinal]bool)
	for _, m := range in.Machines {
		if toDepByApp[m.AppID] == m.DeploymentID && machineServing(m) {
			cutOrdinals[appOrdinal{m.AppID, m.ReplicaOrdinal}] = true
		}
	}

	type routeKey struct {
		hostname string
		port     int
	}
	grouped := make(map[routeKey]*store.RouteRow)
	primaryPorts := make(map[string]int)
	for _, m := range in.Machines {
		port := m.IngressPort
		if port == 0 {
			port = in.DefaultAppPort
		}
		services := depServices[m.DeploymentID]
		if len(services) == 0 {
			services = []store.ServiceSpec{{Name: "default", InternalPort: port}}
		}
		proxy := in.ProxyByNode[m.NodeID]
		if proxy == "" {
			proxy = in.LegacyProxyAddr
		}
		generation := depGen[m.DeploymentID]
		draining := false
		if rollout := rolloutByApp[m.AppID]; rollout != nil {
			rolling := depStrategy[toDepByApp[m.AppID]] == "rolling" && rollout.Status == "PREPARING"
			switch rollout.Status {
			case "PREPARING":
				if rolling {
					cut := cutOrdinals[appOrdinal{m.AppID, m.ReplicaOrdinal}]
					if generation == rollout.ToGeneration {
						draining = !cut
					} else {
						draining = cut
					}
				} else {
					draining = generation != rollout.FromGeneration
				}
			case "CUTOVER":
				draining = generation != rollout.ToGeneration
			case "ROLLING_BACK":
				draining = generation != rollout.FromGeneration
			}
		}
		// Rollout cut decisions are derived from the complete machine snapshot
		// above, but neither authoritative route rows nor Redis may contain an
		// execution that is unready or draining.
		if !machineServing(m) || draining || proxy == "" {
			continue
		}
		for serviceIndex, service := range services {
			key := routeKey{m.Hostname, service.InternalPort}
			route := grouped[key]
			if route == nil {
				route = &store.RouteRow{Hostname: m.Hostname, Port: service.InternalPort, AppID: m.AppID}
				grouped[key] = route
			}
			if serviceIndex == 0 && (primaryPorts[m.Hostname] == 0 || service.InternalPort < primaryPorts[m.Hostname]) {
				primaryPorts[m.Hostname] = service.InternalPort
			}
			route.Backends = append(route.Backends, store.RouteBackendRow{
				MachineID: m.ID, ExecutionID: m.CurrentExecutionID,
				NodeProxyEndpoint: proxy, AppPort: service.InternalPort, Weight: 100,
				Readiness: m.ObservedReadiness,
			})
			if generation > route.Generation {
				route.Generation = generation
			}
		}
	}

	routes := make([]store.RouteRow, 0, len(grouped))
	for _, route := range grouped {
		sort.Slice(route.Backends, func(i, j int) bool {
			if route.Backends[i].MachineID != route.Backends[j].MachineID {
				return route.Backends[i].MachineID < route.Backends[j].MachineID
			}
			return route.Backends[i].ExecutionID < route.Backends[j].ExecutionID
		})
		routes = append(routes, *route)
	}
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Hostname != routes[j].Hostname {
			return routes[i].Hostname < routes[j].Hostname
		}
		return routes[i].Port < routes[j].Port
	})
	return Projection{Routes: routes, PrimaryPorts: primaryPorts}
}

func (p *Publisher) publishRedis(ctx context.Context, projection Projection) error {
	keepRoutes := make(map[string]bool, len(projection.Routes))
	keepHosts := make(map[string]bool, len(projection.Routes))
	hostRoutes := make(map[string][]catalog.HostRoute)
	for _, route := range projection.Routes {
		keepRoutes[fmt.Sprintf("route:%s:%d", route.Hostname, route.Port)] = true
		keepHosts[route.Hostname] = true
		converted := catalog.Route{RouteGeneration: route.Generation, Backends: make([]catalog.Backend, 0, len(route.Backends))}
		for _, backend := range route.Backends {
			converted.Backends = append(converted.Backends, catalog.Backend{
				MachineID: backend.MachineID, ExecutionID: backend.ExecutionID,
				NodeProxyEndpoint: backend.NodeProxyEndpoint, AppPort: backend.AppPort,
				Readiness: backend.Readiness, Weight: backend.Weight, Draining: backend.Draining,
			})
		}
		hostRoutes[route.Hostname] = append(hostRoutes[route.Hostname], catalog.HostRoute{Port: route.Port, Route: converted})
	}
	hostnames := make([]string, 0, len(hostRoutes))
	for hostname := range hostRoutes {
		hostnames = append(hostnames, hostname)
	}
	sort.Strings(hostnames)
	for _, hostname := range hostnames {
		if err := p.catalog.ReplaceHostRoutes(ctx, hostname, hostRoutes[hostname], projection.PrimaryPorts[hostname]); err != nil {
			return err
		}
	}
	return p.catalog.PruneRoutes(ctx, keepRoutes, keepHosts)
}

func machineServing(m store.Machine) bool {
	if m.ObservedState != "RUNNING" && m.ObservedState != "PAUSED" {
		return false
	}
	return m.ObservedReadiness == "READY" || m.ObservedReadiness == "UNCONFIGURED"
}
