// Command agentctl 是 agent gRPC 的开发调试客户端（M1 用；CLI 正式产品形态在 M3）。
// 使用: agentctl <info|create|list|delete> [flags]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/zhu327/firepaas/internal/security/mtls"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// stringSlice 支持重复标志（标准 flag 无 StringArray）。
type stringSlice []string

// version 由 release 构建经 -ldflags -X main.version=... 注入。
var version = "dev"

func (s *stringSlice) String() string     { return strings.Join(*s, ",") }
func (s *stringSlice) Set(v string) error { *s = append(*s, v); return nil }

// fencing 是 create/delete 共用的幂等围栏三元组（machine + generation + operation）。
type fencing struct {
	machineID  string
	generation uint64
	operation  string
}

func addFencingFlags(fs *flag.FlagSet, f *fencing) {
	fs.StringVar(&f.machineID, "machine-id", "", "stable machine id")
	fs.Uint64Var(&f.generation, "generation", 1, "fencing generation")
	fs.StringVar(&f.operation, "operation", "", "fencing operation id (required)")
}

func (f *fencing) validate() error {
	if f.machineID == "" || f.operation == "" {
		return fmt.Errorf("-machine-id and -operation are required")
	}
	return nil
}

// parseSecretEnv 解析可重复的 KEY=VALUE secret（行为不变：空 key/value 均拒绝）。
func parseSecretEnv(secrets []string) (map[string]string, error) {
	if len(secrets) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(secrets))
	for _, kv := range secrets {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("-secret must be KEY=VALUE, got %q", kv)
		}
		out[parts[0]] = parts[1]
	}
	return out, nil
}

func main() {
	addr := flag.String("addr", "127.0.0.1:5108", "agent gRPC address")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("agentctl", version)
		return
	}
	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var opts []grpc.DialOption
	certFile, keyFile, caFile := os.Getenv(
		"FIREPAAS_AGENT_TLS_CERT",
	), os.Getenv(
		"FIREPAAS_AGENT_TLS_KEY",
	), os.Getenv(
		"FIREPAAS_AGENT_TLS_CA",
	)
	if certFile != "" && keyFile != "" && caFile != "" {
		tlsConf, err := mtls.ClientConfig(certFile, keyFile, caFile, "agentd")
		if err != nil {
			fatal(err)
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.NewClient(*addr, opts...)
	if err != nil {
		fatal(err)
	}
	defer func() { _ = conn.Close() }()

	switch args[0] {
	case "info":
		resp, err := pb.NewInfoServiceClient(conn).ServiceInfo(ctx, &emptypb.Empty{})
		if err != nil {
			fatal(err)
		}
		print(resp)
	case "list":
		fs := flag.NewFlagSet("list", flag.ExitOnError)
		project := fs.String("project", "", "filter by project_id")
		_ = fs.Parse(args[1:])
		resp, err := pb.NewMachineServiceClient(conn).ListMachines(ctx, &pb.ListMachinesRequest{ProjectId: *project})
		if err != nil {
			fatal(err)
		}
		print(resp)
	case "create":
		fs := flag.NewFlagSet("create", flag.ExitOnError)
		var fence fencing
		addFencingFlags(fs, &fence)
		image := fs.String("image", "docker.io/library/nginx:alpine", "OCI image ref")
		vcpus := fs.Uint64("vcpus", 1, "vcpus")
		mem := fs.Uint64("mem-mib", 512, "memory MiB")
		project := fs.String("project", "dev", "project id")
		app := fs.String("app", "demo", "app id")
		deployment := fs.String("deployment", "demo-1", "deployment id")
		execution := fs.String("execution", "exec-1", "execution id")
		hostname := fs.String("hostname", "", "route hostname (spec.hostname)")
		port := fs.Uint64("port", 0, "ingress port (spec.network.ingress_port)")
		proxyCredential := fs.String("proxy-credential", "", "execution-bound proxy credential")
		secretLeaseID := fs.String("secret-lease-id", "", "one-shot secret delivery lease id")
		var secrets stringSlice
		fs.Var(&secrets, "secret", "secret env KEY=VALUE (repeatable); value must not echo in response")
		_ = fs.Parse(args[1:])
		if err := fence.validate(); err != nil {
			fatal(err)
		}
		spec := &pb.MachineSpec{
			ProjectId:    *project,
			AppId:        *app,
			DeploymentId: *deployment,
			ExecutionId:  *execution,
			ImageRef:     *image,
			Vcpu:         *vcpus,
			MemMib:       *mem,
			Placement:    &pb.PlacementConstraints{AntiAffinity: pb.PlacementConstraints_NONE},
		}
		if *hostname != "" {
			spec.Hostname = *hostname
		}
		if *port != 0 {
			spec.Network = &pb.NetworkSpec{IngressPort: *port}
		}
		secretEnv, err := parseSecretEnv(secrets)
		if err != nil {
			fatal(err)
		}
		req := &pb.CreateMachineRequest{
			MachineId:       fence.machineID,
			Generation:      fence.generation,
			OperationId:     fence.operation,
			Spec:            spec,
			ProxyCredential: *proxyCredential,
			SecretLeaseId:   *secretLeaseID,
			SecretEnv:       secretEnv,
		}
		resp, err := pb.NewMachineServiceClient(conn).CreateMachine(ctx, req)
		if err != nil {
			fatal(err)
		}
		print(resp)
	case "delete":
		fs := flag.NewFlagSet("delete", flag.ExitOnError)
		var fence fencing
		addFencingFlags(fs, &fence)
		execution := fs.String("execution", "", "execution id")
		_ = fs.Parse(args[1:])
		if err := fence.validate(); err != nil {
			fatal(err)
		}
		req := &pb.DeleteMachineRequest{
			MachineId:   fence.machineID,
			ExecutionId: *execution,
			Generation:  fence.generation,
			OperationId: fence.operation,
		}
		if _, err := pb.NewMachineServiceClient(conn).DeleteMachine(ctx, req); err != nil {
			fatal(err)
		}
		fmt.Println("deleted")
	default:
		usage()
		os.Exit(2)
	}
}

func print(m proto.Message) {
	raw, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(m)
	if err != nil {
		fatal(err)
	}
	fmt.Println(string(raw))
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "agentctl:", err)
	os.Exit(1)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: agentctl [-addr 127.0.0.1:5108] <info|create|list|delete> [flags]")
}
