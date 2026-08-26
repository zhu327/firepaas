// Command agentctl 是 agent gRPC 的开发调试客户端（M1 用；CLI 正式产品形态在 M3）。
// 使用: agentctl <info|create|list|delete> [flags]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	pb "github.com/example/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:5108", "agent gRPC address")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()

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
		machineID := fs.String("machine-id", "", "stable machine id")
		image := fs.String("image", "docker.io/library/nginx:alpine", "OCI image ref")
		vcpus := fs.Uint64("vcpus", 1, "vcpus")
		mem := fs.Uint64("mem-mib", 512, "memory MiB")
		project := fs.String("project", "dev", "project id")
		app := fs.String("app", "demo", "app id")
		deployment := fs.String("deployment", "demo-1", "deployment id")
		execution := fs.String("execution", "exec-1", "execution id")
		generation := fs.Uint64("generation", 1, "fencing generation")
		operation := fs.String("operation", "", "fencing operation id (required)")
		_ = fs.Parse(args[1:])
		if *machineID == "" || *operation == "" {
			fatal(fmt.Errorf("-machine-id and -operation are required"))
		}
		req := &pb.CreateMachineRequest{
			MachineId:   *machineID,
			Generation:  *generation,
			OperationId: *operation,
			Spec: &pb.MachineSpec{
				ProjectId:    *project,
				AppId:        *app,
				DeploymentId: *deployment,
				ExecutionId:  *execution,
				ImageRef:     *image,
				Vcpu:         *vcpus,
				MemMib:       *mem,
			},
		}
		resp, err := pb.NewMachineServiceClient(conn).CreateMachine(ctx, req)
		if err != nil {
			fatal(err)
		}
		print(resp)
	case "delete":
		fs := flag.NewFlagSet("delete", flag.ExitOnError)
		machineID := fs.String("machine-id", "", "machine id")
		execution := fs.String("execution", "", "execution id")
		generation := fs.Uint64("generation", 1, "fencing generation")
		operation := fs.String("operation", "", "fencing operation id (required)")
		_ = fs.Parse(args[1:])
		if *machineID == "" || *operation == "" {
			fatal(fmt.Errorf("-machine-id and -operation are required"))
		}
		req := &pb.DeleteMachineRequest{
			MachineId:   *machineID,
			ExecutionId: *execution,
			Generation:  *generation,
			OperationId: *operation,
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
