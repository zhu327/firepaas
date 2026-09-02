// Package agentclient 是控制面到 agent 的 gRPC 客户端（M1 单节点固定地址）。
// M2 改为 Nomad native discovery + 节点连接池。
package agentclient

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/zhu327/firepaas/internal/security/mtls"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client 封装 Machine/Info 两个服务。
type Client struct {
	conn     *grpc.ClientConn
	addr     string
	certMgr  *mtls.CertManager
	Machines pb.MachineServiceClient
	Info     pb.InfoServiceClient
	Images   pb.ImageServiceClient
}

// NotAfterHook 在客户端证书每次成功加载（含热重载）后被调用，进程级
// 观测汇（契约 C-1）：cmd/api 把它接到 metrics registry 导出
// firepaas_tls_cert_not_after_seconds。只在进程装配期设置一次，
// 运行期不得改写（多个拨出的 Client 共享同一汇，语义等同）。
var NotAfterHook func(expiry time.Time)

// Dial 连接单节点 agent。mTLS 是唯一正式形态（ADR-0006/0014）：必须设置
// FIREPAAS_AGENT_TLS_CERT/KEY/CA，缺失即失败（fail-closed，P3-5）；仅
// 显式设置 FIREPAAS_AGENT_TLS_ALLOW_INSECURE=true（本地开发）时才允许
// 明文连接，避免环境遗漏时静默降级为无认证 RPC。
func Dial(addr string) (*Client, error) {
	var opts []grpc.DialOption
	certFile, keyFile, caFile := os.Getenv(
		"FIREPAAS_AGENT_TLS_CERT",
	), os.Getenv(
		"FIREPAAS_AGENT_TLS_KEY",
	), os.Getenv(
		"FIREPAAS_AGENT_TLS_CA",
	)
	var clientCertMgr *mtls.CertManager
	if certFile != "" && keyFile != "" && caFile != "" {
		cm, err := mtls.NewCertManager(certFile, keyFile,
			certReloadInterval(), slog.Default().With("component", "agentclient"), NotAfterHook)
		if err != nil {
			return nil, fmt.Errorf("agent mTLS cert: %w", err)
		}
		tlsConf, err := cm.ClientTLSConfig(caFile, "agentd")
		if err != nil {
			cm.Close()
			return nil, fmt.Errorf("agent mTLS config: %w", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)))
		clientCertMgr = cm
	} else if os.Getenv("FIREPAAS_AGENT_TLS_ALLOW_INSECURE") == "true" {
		slog.Warn("agent connection running WITHOUT mTLS (dev only)")
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		return nil, fmt.Errorf("agent mTLS required: set FIREPAAS_AGENT_TLS_CERT/KEY/CA (or FIREPAAS_AGENT_TLS_ALLOW_INSECURE=true for dev)")
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		if clientCertMgr != nil {
			clientCertMgr.Close()
		}
		return nil, fmt.Errorf("dial agent %s: %w", addr, err)
	}
	return &Client{
		conn:     conn,
		addr:     addr,
		certMgr:  clientCertMgr,
		Machines: pb.NewMachineServiceClient(conn),
		Info:     pb.NewInfoServiceClient(conn),
		Images:   pb.NewImageServiceClient(conn),
	}, nil
}

// Addr 返回连接目标地址（nodemanager 判断是否需要重拨）。
func (c *Client) Addr() string { return c.addr }

// RawConn 返回底层 gRPC 连接（派生 snapshot 等附属服务客户端用）。
func (c *Client) RawConn() *grpc.ClientConn { return c.conn }

// Close 关闭连接并停止证书热重载。
func (c *Client) Close() error {
	err := c.conn.Close()
	if c.certMgr != nil {
		c.certMgr.Close()
	}
	return err
}

// certReloadInterval 读取证书热重载周期（默认 1m；<=0 关闭热重载，仅用
// 启动时加载的证书）。非法值回退默认并告警。
func certReloadInterval() time.Duration {
	raw := os.Getenv("FIREPAAS_AGENT_TLS_CERT_RELOAD_INTERVAL")
	if raw == "" {
		return time.Minute
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		slog.Warn("invalid FIREPAAS_AGENT_TLS_CERT_RELOAD_INTERVAL, keeping default", "value", raw)
		return time.Minute
	}
	return d
}

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

// ListImages returns the node's ready local image cache entries.
func (c *Client) ListImages(ctx context.Context) ([]*pb.PullImageResponse, error) {
	resp, err := c.Images.ListImages(ctx, &pb.ListImagesRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Images, nil
}

// DeleteImage 调用 ImageService.DeleteImage（v1.2-F：引用感知 GC）。agent
// 在删除前独立检查本机 live instance 引用；not-found 按成功收敛。
func (c *Client) DeleteImage(ctx context.Context, imageRef string) error {
	_, err := c.Images.DeleteImage(ctx, &pb.DeleteImageRequest{ImageRef: imageRef})
	return err
}

func (c *Client) QuarantineImage(ctx context.Context, req *pb.QuarantineImageRequest) (*pb.ImageQuarantine, error) {
	return c.Images.QuarantineImage(ctx, req)
}

func (c *Client) ListImageQuarantines(ctx context.Context) ([]*pb.ImageQuarantine, error) {
	resp, err := c.Images.ListImageQuarantines(ctx, &pb.ListImageQuarantinesRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetQuarantines(), nil
}

func (c *Client) RollbackImageQuarantine(ctx context.Context, req *pb.ImageQuarantineActionRequest) error {
	_, err := c.Images.RollbackImageQuarantine(ctx, req)
	return err
}

func (c *Client) FinalizeImageQuarantine(ctx context.Context, req *pb.ImageQuarantineActionRequest) error {
	_, err := c.Images.FinalizeImageQuarantine(ctx, req)
	return err
}

// Pause 调用 PauseMachine（M4.5 scale-to-zero）。
func (c *Client) Pause(
	ctx context.Context,
	machineID, executionID string,
	generation uint64,
	opID string,
) (*pb.Machine, error) {
	return c.Machines.PauseMachine(ctx, &pb.PauseMachineRequest{Operation: &pb.MachineOperationRequest{
		MachineId: machineID, ExecutionId: executionID, Generation: generation,
		OperationId: opID, ExpectedState: pb.MachineState_RUNNING,
	}})
}

// Resume 调用 ResumeMachine。
func (c *Client) Resume(
	ctx context.Context,
	machineID, executionID string,
	generation uint64,
	opID string,
) (*pb.Machine, error) {
	return c.Machines.ResumeMachine(ctx, &pb.ResumeMachineRequest{Operation: &pb.MachineOperationRequest{
		MachineId: machineID, ExecutionId: executionID, Generation: generation,
		OperationId: opID, ExpectedState: pb.MachineState_PAUSED,
	}})
}

// Snapshots 是 agent SnapshotService 客户端（v1.3-B，ADR-0028）。
type SnapshotClient struct {
	client pb.SnapshotServiceClient
}

// NewSnapshotClient 构造 snapshot 客户端。
func NewSnapshotClient(conn *grpc.ClientConn) *SnapshotClient {
	return &SnapshotClient{client: pb.NewSnapshotServiceClient(conn)}
}

// CreateSnapshot 调用 agent 执行 checkpoint。
func (s *SnapshotClient) CreateSnapshot(ctx context.Context, req *pb.CreateSnapshotRequest) (*pb.SnapshotInfo, error) {
	resp, err := s.client.CreateSnapshot(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetSnapshot(), nil
}

// DeleteSnapshot 调用 agent 删除本地快照。
func (s *SnapshotClient) DeleteSnapshot(ctx context.Context, req *pb.DeleteSnapshotRequest) error {
	_, err := s.client.DeleteSnapshot(ctx, req)
	return err
}

// ListSnapshots 调用 agent 列出本地快照。返回完整响应以暴露 v1.4-B
// inventory 观测元数据（complete/generation/observed_at）。
func (s *SnapshotClient) ListSnapshots(
	ctx context.Context,
	machineID, snapshotID string,
) (*pb.ListSnapshotsResponse, error) {
	return s.client.ListSnapshots(ctx, &pb.ListSnapshotsRequest{MachineId: machineID, SnapshotId: snapshotID})
}

// ForkSnapshot 调用 agent fork 快照（v1.3-C）。
func (s *SnapshotClient) ForkSnapshot(
	ctx context.Context,
	req *pb.ForkSnapshotRequest,
) (*pb.ForkSnapshotResponse, error) {
	return s.client.ForkSnapshot(ctx, req)
}

// RestoreSnapshot 调用 agent restore 快照（v1.3-C rescue）。
func (s *SnapshotClient) RestoreSnapshot(
	ctx context.Context,
	req *pb.RestoreSnapshotRequest,
) (*pb.RestoreSnapshotResponse, error) {
	return s.client.RestoreSnapshot(ctx, req)
}

func (s *SnapshotClient) ScrubSnapshot(
	ctx context.Context,
	req *pb.ScrubSnapshotRequest,
) (*pb.ScrubSnapshotResponse, error) {
	return s.client.ScrubSnapshot(ctx, req)
}

// VolumeClient 是 agent VolumeService 客户端（v1.3-D，ADR-0029）。
type VolumeClient struct {
	client pb.VolumeServiceClient
}

// NewVolumeClient 构造 volume 客户端。
func NewVolumeClient(conn *grpc.ClientConn) *VolumeClient {
	return &VolumeClient{client: pb.NewVolumeServiceClient(conn)}
}

// CreateVolume 创建本地空卷。
func (v *VolumeClient) CreateVolume(
	ctx context.Context,
	req *pb.CreateVolumeRequest,
) (*pb.CreateVolumeResponse, error) {
	return v.client.CreateVolume(ctx, req)
}

// ImportDataset asks the agent to fetch an archive directly from object storage.
func (v *VolumeClient) ImportDataset(
	ctx context.Context,
	req *pb.ImportDatasetRequest,
) (*pb.CreateVolumeResponse, error) {
	return v.client.ImportDataset(ctx, req)
}

// ListVolumes 返回 agent 本地 materialization inventory（含 v1.4-B 观测
// 元数据）。
func (v *VolumeClient) ListVolumes(ctx context.Context) (*pb.ListVolumesResponse, error) {
	return v.client.ListVolumes(ctx, &pb.ListVolumesRequest{})
}

// DeleteVolume 删除本地卷。
func (v *VolumeClient) DeleteVolume(ctx context.Context, req *pb.DeleteVolumeRequest) error {
	_, err := v.client.DeleteVolume(ctx, req)
	return err
}

func (v *VolumeClient) QuarantineVolume(
	ctx context.Context,
	req *pb.QuarantineVolumeRequest,
) (*pb.VolumeQuarantine, error) {
	return v.client.QuarantineVolume(ctx, req)
}

func (v *VolumeClient) ListVolumeQuarantines(ctx context.Context) ([]*pb.VolumeQuarantine, error) {
	r, e := v.client.ListVolumeQuarantines(ctx, &pb.ListVolumeQuarantinesRequest{})
	if e != nil {
		return nil, e
	}
	return r.GetQuarantines(), nil
}

func (v *VolumeClient) RollbackVolumeQuarantine(ctx context.Context, req *pb.VolumeQuarantineActionRequest) error {
	_, e := v.client.RollbackVolumeQuarantine(ctx, req)
	return e
}

func (v *VolumeClient) FinalizeVolumeQuarantine(ctx context.Context, req *pb.VolumeQuarantineActionRequest) error {
	_, e := v.client.FinalizeVolumeQuarantine(ctx, req)
	return e
}

// AttachVolume 挂载卷。
func (v *VolumeClient) AttachVolume(ctx context.Context, req *pb.AttachVolumeRequest) (*pb.Machine, error) {
	return v.client.AttachVolume(ctx, req)
}

// DetachVolume 卸载卷。
func (v *VolumeClient) DetachVolume(ctx context.Context, req *pb.DetachVolumeRequest) (*pb.Machine, error) {
	return v.client.DetachVolume(ctx, req)
}
