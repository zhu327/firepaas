// Package agentclient 是控制面到 agent 的 gRPC 客户端（M1 单节点固定地址）。
// M2 改为 Nomad native discovery + 节点连接池。
package agentclient

import (
	"context"
	"fmt"
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
	Machines pb.MachineServiceClient
	Info     pb.InfoServiceClient
}

// Dial 连接单节点 agent。设置 FIREPAAS_AGENT_TLS_CERT/KEY/CA 时启用 mTLS。
func Dial(addr string) (*Client, error) {
	var opts []grpc.DialOption
	certFile, keyFile, caFile := os.Getenv("FIREPAAS_AGENT_TLS_CERT"), os.Getenv("FIREPAAS_AGENT_TLS_KEY"), os.Getenv("FIREPAAS_AGENT_TLS_CA")
	if certFile != "" && keyFile != "" && caFile != "" {
		tlsConf, err := mtls.ClientConfig(certFile, keyFile, caFile, "agentd")
		if err != nil {
			return nil, fmt.Errorf("agent mTLS config: %w", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial agent %s: %w", addr, err)
	}
	return &Client{
		conn:     conn,
		Machines: pb.NewMachineServiceClient(conn),
		Info:     pb.NewInfoServiceClient(conn),
	}, nil
}

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
