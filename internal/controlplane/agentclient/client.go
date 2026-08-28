// Package agentclient 是控制面到 agent 的 gRPC 客户端（M1 单节点固定地址）。
// M2 改为 Nomad native discovery + 节点连接池。
package agentclient

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/example/firepaas/internal/security/mtls"
	pb "github.com/example/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client 封装 Machine/Info 两个服务。
type Client struct {
	conn     *grpc.ClientConn
	addr     string
	Machines pb.MachineServiceClient
	Info     pb.InfoServiceClient
	Images   pb.ImageServiceClient
}

// Dial 连接单节点 agent。mTLS 是唯一正式形态（ADR-0006/0014）：必须设置
// FIREPAAS_AGENT_TLS_CERT/KEY/CA，缺失即失败（fail-closed，P3-5）；仅
// 显式设置 FIREPAAS_AGENT_TLS_ALLOW_INSECURE=true（本地开发）时才允许
// 明文连接，避免环境遗漏时静默降级为无认证 RPC。
func Dial(addr string) (*Client, error) {
	var opts []grpc.DialOption
	certFile, keyFile, caFile := os.Getenv("FIREPAAS_AGENT_TLS_CERT"), os.Getenv("FIREPAAS_AGENT_TLS_KEY"), os.Getenv("FIREPAAS_AGENT_TLS_CA")
	if certFile != "" && keyFile != "" && caFile != "" {
		tlsConf, err := mtls.ClientConfig(certFile, keyFile, caFile, "agentd")
		if err != nil {
			return nil, fmt.Errorf("agent mTLS config: %w", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)))
	} else if os.Getenv("FIREPAAS_AGENT_TLS_ALLOW_INSECURE") == "true" {
		slog.Warn("agent connection running WITHOUT mTLS (dev only)")
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		return nil, fmt.Errorf("agent mTLS required: set FIREPAAS_AGENT_TLS_CERT/KEY/CA (or FIREPAAS_AGENT_TLS_ALLOW_INSECURE=true for dev)")
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial agent %s: %w", addr, err)
	}
	return &Client{
		conn:     conn,
		addr:     addr,
		Machines: pb.NewMachineServiceClient(conn),
		Info:     pb.NewInfoServiceClient(conn),
		Images:   pb.NewImageServiceClient(conn),
	}, nil
}

// Addr 返回连接目标地址（nodemanager 判断是否需要重拨）。
func (c *Client) Addr() string { return c.addr }

// Close 关闭连接。
func (c *Client) Close() error { return c.conn.Close() }

// Create 调用 CreateMachine。
func (c *Client) Create(ctx context.Context, req *pb.CreateMachineRequest) (*pb.Machine, error) {
	resp, err := c.Machines.CreateMachine(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Machine, nil
}

// List 调用 ListMachines。
func (c *Client) List(ctx context.Context, projectID string) ([]*pb.Machine, error) {
	resp, err := c.Machines.ListMachines(ctx, &pb.ListMachinesRequest{ProjectId: projectID})
	if err != nil {
		return nil, err
	}
	return resp.Machines, nil
}

// Delete 调用 DeleteMachine。
func (c *Client) Delete(ctx context.Context, req *pb.DeleteMachineRequest) error {
	_, err := c.Machines.DeleteMachine(ctx, req)
	return err
}

// ServiceInfo 调用 InfoService。
func (c *Client) ServiceInfo(ctx context.Context) (*pb.ServiceInfoResponse, error) {
	return c.Info.ServiceInfo(ctx, &emptypb.Empty{})
}

// PullImage 调用 ImageService.PullImage（v1.1，ADR-0018 部署预取：尽力而为）。
func (c *Client) PullImage(ctx context.Context, imageRef string) (*pb.PullImageResponse, error) {
	return c.Images.PullImage(ctx, &pb.PullImageRequest{ImageRef: imageRef})
}

// Pause 调用 PauseMachine（M4.5 scale-to-zero）。
func (c *Client) Pause(ctx context.Context, machineID, executionID string, generation uint64, opID string) (*pb.Machine, error) {
	return c.Machines.PauseMachine(ctx, &pb.PauseMachineRequest{Operation: &pb.MachineOperationRequest{
		MachineId: machineID, ExecutionId: executionID, Generation: generation,
		OperationId: opID, ExpectedState: pb.MachineState_RUNNING,
	}})
}

// Resume 调用 ResumeMachine。
func (c *Client) Resume(ctx context.Context, machineID, executionID string, generation uint64, opID string) (*pb.Machine, error) {
	return c.Machines.ResumeMachine(ctx, &pb.ResumeMachineRequest{Operation: &pb.MachineOperationRequest{
		MachineId: machineID, ExecutionId: executionID, Generation: generation,
		OperationId: opID, ExpectedState: pb.MachineState_PAUSED,
	}})
}
