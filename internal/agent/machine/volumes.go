// volumes.go：v1.3-D（ADR-0029）agent 侧 volume 适配。
// LOCAL_RW：hypeman volumes.Manager 空卷创建 + instances 挂载（execution
// fencing 由 server 层完成）。DATASET_RO 的 import/seal 见 v1.3-E。
package machine

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kernel/hypeman/lib/instances"
	"github.com/kernel/hypeman/lib/tags"
	"github.com/kernel/hypeman/lib/volumes"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

// volumeProvider 是 hypeman volumes.Manager 的能力子集。
type volumeProvider interface {
	CreateVolume(ctx context.Context, req volumes.CreateVolumeRequest) (*volumes.Volume, error)
	CreateVolumeFromArchive(
		ctx context.Context,
		req volumes.CreateVolumeFromArchiveRequest,
		archive io.Reader,
	) (*volumes.Volume, error)
	GetVolume(ctx context.Context, id string) (*volumes.Volume, error)
	DeleteVolume(ctx context.Context, id string) error
	ListVolumes(ctx context.Context) ([]volumes.Volume, error)
	TotalVolumeBytes(ctx context.Context) (int64, error)
}

// volumeAttacher 是 hypeman instances.Manager 的挂载能力子集。
type volumeAttacher interface {
	AttachVolume(
		ctx context.Context,
		id string,
		volumeID string,
		req instances.AttachVolumeRequest,
	) (*instances.Instance, error)
	DetachVolume(ctx context.Context, id string, volumeID string) (*instances.Instance, error)
	StopInstance(ctx context.Context, id string) (*instances.Instance, error)
	StartInstance(ctx context.Context, id string, req instances.StartInstanceRequest) (*instances.Instance, error)
}

// resolveVolumes 返回 hypeman volume/attach 能力（缺失 = 老上游/测试替身）。
func (a *Adapter) resolveVolumes() (volumeProvider, volumeAttacher, error) {
	at, ok := a.instances.(volumeAttacher)
	if a.volumes == nil || !ok {
		return nil, nil, ErrGuestOpsUnsupported
	}
	return a.volumes, at, nil
}

// SetVolumes（agentd 装配）注入 hypeman volumes.Manager。
func (a *Adapter) SetVolumes(v volumeProvider) { a.volumes = v }

const (
	tagDatasetDigest = "firepaas/dataset_digest"
	tagDatasetSealed = "firepaas/dataset_sealed"
)

var (
	ErrDatasetURL      = errors.New("dataset source URL is not allowed")
	ErrDatasetDigest   = errors.New("dataset archive digest mismatch")
	ErrDatasetArchive  = errors.New("unsafe dataset archive")
	ErrDatasetUnsealed = errors.New("dataset is not sealed")
)

// ImportDataset streams a bounded archive to a temporary file, verifies its
// compressed digest and independently validates every tar header before handing
// it to hypeman. Hypeman writes metadata only after the disk export succeeds;
// the immutable digest/sealed tags therefore become visible atomically.
func (a *Adapter) ImportDataset(ctx context.Context, req *pb.ImportDatasetRequest) (*pb.CreateVolumeResponse, error) {
	vp, _, err := a.resolveVolumes()
	if err != nil {
		return nil, err
	}
	if err := validateDatasetURL(req.GetSourceUrl(), req.GetAllowHttpLoopbackForTests()); err != nil {
		return nil, err
	}
	if req.GetMaxDownloadBytes() == 0 || req.GetMaxExpandedBytes() == 0 || req.GetMaxFiles() == 0 {
		return nil, fmt.Errorf("%w: limits must be positive", ErrDatasetArchive)
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(c context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			ips, err := net.DefaultResolver.LookupNetIP(c, "ip", host)
			if err != nil || len(ips) == 0 {
				return nil, ErrDatasetURL
			}
			for _, ip := range ips {
				// resolver 对 IPv4 字面量可能返回 IPv4-mapped IPv6（如
				// ::ffff:127.0.0.1）；那是解析产物，先 unmap 再按真实地址判。
				// 未 unmap 的入口（字面量 host 等）仍拒绝 4-in-6 形态。
				if !publicDatasetAddr(ip.Unmap(), req.GetAllowHttpLoopbackForTests()) {
					return nil, ErrDatasetURL
				}
			}
			return dialer.DialContext(c, network, net.JoinHostPort(ips[0].Unmap().String(), port))
		},
	}
	hc := &http.Client{
		Timeout:   15 * time.Minute,
		Transport: transport,
		CheckRedirect: func(next *http.Request, via []*http.Request) error {
			if err := validateDatasetURL(next.URL.String(), req.GetAllowHttpLoopbackForTests()); err != nil {
				// net/http's redirect error includes the target URL. Return a stable
				// sentinel here and replace the outer error below, rather than allowing
				// either the source or redirect target into logs or operation errors.
				return ErrDatasetURL
			}
			return nil
		},
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, req.GetSourceUrl(), nil)
	if err != nil {
		return nil, ErrDatasetURL
	}
	resp, err := hc.Do(httpReq)
	if err != nil {
		// url.Error includes request/redirect URLs. Do not wrap it: even a
		// different redirect host must never escape this boundary.
		return nil, fmt.Errorf("download dataset: %w", ErrDatasetURL)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download dataset: unexpected status %d", resp.StatusCode)
	}
	if resp.ContentLength > int64(req.GetMaxDownloadBytes()) {
		return nil, fmt.Errorf("%w: compressed size exceeds limit", ErrDatasetArchive)
	}
	// Include both stable coordinates. Components are reduced to a safe digest
	// so caller-controlled IDs cannot inject separators or unbounded names.
	f, err := os.CreateTemp("", datasetSpoolPattern(req.GetVolumeId(), req.GetOperationId()))
	if err != nil {
		return nil, fmt.Errorf("create import spool: %w", err)
	}
	name := f.Name()
	activeDatasetSpools.Store(name, struct{}{})
	defer activeDatasetSpools.Delete(name)
	defer func() { _ = os.Remove(name) }()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(resp.Body, int64(req.GetMaxDownloadBytes())+1))
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("download dataset: %w", err)
	}
	if n > int64(req.GetMaxDownloadBytes()) {
		_ = f.Close()
		return nil, fmt.Errorf("%w: compressed size exceeds limit", ErrDatasetArchive)
	}
	got := "sha256:" + hex.EncodeToString(h.Sum(nil))
	if got != strings.ToLower(req.GetExpectedDigest()) {
		_ = f.Close()
		return nil, fmt.Errorf("%w: got %s", ErrDatasetDigest, got)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := validateDatasetArchive(f, req.GetMaxExpandedBytes(), req.GetMaxFiles()); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	sizeGB := int((req.GetMaxExpandedBytes() + 1024*1024*1024 - 1) / (1024 * 1024 * 1024))
	if sizeGB < 1 {
		sizeGB = 1
	}
	v, err := vp.CreateVolumeFromArchive(
		ctx,
		volumes.CreateVolumeFromArchiveRequest{
			Id:     ptr(req.GetVolumeId()),
			SizeGb: sizeGB,
			Tags:   tags.Tags{tagDatasetDigest: got, tagDatasetSealed: "true"},
		},
		f,
	)
	_ = f.Close()
	if err != nil {
		return nil, fmt.Errorf("hypeman import dataset: %w", err)
	}
	return &pb.CreateVolumeResponse{
		VolumeId:      v.Id,
		SizeBytes:     uint64(v.SizeGb) * 1024 * 1024 * 1024,
		ContentDigest: got,
		Sealed:        true,
	}, nil
}

func ptr(s string) *string { return &s }

var activeDatasetSpools sync.Map

func datasetSpoolPattern(volumeID, operationID string) string {
	digest := func(value string) string {
		sum := sha256.Sum256([]byte(value))
		return hex.EncodeToString(sum[:8])
	}
	return fmt.Sprintf("firepaas-dataset-v%s-o%s-*.tar.gz", digest(volumeID), digest(operationID))
}

// CleanupStaleDatasetSpool removes dataset import spool leftovers from crashed
// imports (v1.4-D：agent 启动时调用)。在途导入由年龄保护：导入授权窗口最大
// 15 分钟，超过 maxAge 的 spool 文件一定属于已崩溃/过期的导入。
func CleanupStaleDatasetSpool(maxAge time.Duration) (int, error) {
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "firepaas-dataset-*"))
	if err != nil {
		return 0, err
	}
	if len(matches) == 0 {
		return 0, nil
	}
	cutoff := time.Now().Add(-maxAge)
	removed := 0
	var cleanupErr error
	for _, m := range matches {
		if _, active := activeDatasetSpools.Load(m); active {
			continue
		}
		fi, statErr := os.Stat(m)
		if statErr != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("stat dataset spool %q: %w", filepath.Base(m), statErr))
			continue
		}
		if !fi.ModTime().Before(cutoff) {
			continue
		}
		if removeErr := os.Remove(m); removeErr != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove dataset spool %q: %w", filepath.Base(m), removeErr))
			continue
		}
		removed++
	}
	return removed, cleanupErr
}

func validateDatasetURL(raw string, allowLoopback bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.Hostname() == "" {
		return ErrDatasetURL
	}
	// v1.4（ADR-0030）：仅接受无 userinfo/query/fragment 的 credential-free
	// 来源；query/fragment 可能携带签名或会话凭证，不得进入持久化事实或日志。
	if u.RawQuery != "" || u.Fragment != "" || u.RawFragment != "" {
		return ErrDatasetURL
	}
	if u.Scheme == "https" {
		return nil
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme == "http" && allowLoopback && ip != nil && ip.IsLoopback() {
		return nil
	}
	return ErrDatasetURL
}

// redactDatasetSource removes the source URL (and its host) from an error
// message so download failures can never leak the dataset origin into logs,
// events, or API error bodies (v1.4：错误正文只允许 URL 摘要).
func redactDatasetSource(rawURL string, err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	// 全 URL 必须最先替换：先替换 scheme://host 或 host 会破坏完整 URL 的
	// 匹配，让 path 部分残留在错误正文里。
	msg = strings.ReplaceAll(msg, rawURL, "[dataset-source]")
	if u, perr := url.Parse(rawURL); perr == nil && u.Host != "" {
		msg = strings.ReplaceAll(msg, u.Scheme+"://"+u.Host, "[dataset-source]")
		msg = strings.ReplaceAll(msg, u.Host, "[dataset-source-host]")
	}
	return errors.New(msg)
}

var datasetReservedPrefixes = func() []netip.Prefix {
	cidrs := []string{
		"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16",
		"172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24", "192.31.196.0/24", "192.52.193.0/24",
		"192.88.99.0/24", "192.168.0.0/16", "192.175.48.0/24", "198.18.0.0/15",
		"198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
		"::/128", "::1/128", "::ffff:0:0/96", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64",
		"2001::/23", "2001:db8::/32", "3fff::/20", "2620:4f:8000::/48", "fc00::/7", "fe80::/10", "ff00::/8",
	}
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, cidr := range cidrs {
		block, err := netip.ParsePrefix(cidr)
		if err != nil {
			panic(err)
		}
		out = append(out, block)
	}
	return out
}()

func publicDatasetAddr(addr netip.Addr, allowLoopback bool) bool {
	if !addr.IsValid() || addr.Is4In6() { // reject IPv4-mapped IPv6, not merely its mapped target
		return false
	}
	if allowLoopback && addr.IsLoopback() {
		return true
	}
	for _, prefix := range datasetReservedPrefixes {
		if prefix.Addr().BitLen() == addr.BitLen() && prefix.Contains(addr) {
			return false
		}
	}
	return addr.IsGlobalUnicast()
}

const (
	maxDatasetPathBytes = 4096
	maxDatasetPathDepth = 256
)

func validateDatasetArchive(r io.Reader, maxExpanded uint64, maxFiles uint32) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("%w: gzip: %v", ErrDatasetArchive, err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	var expanded uint64
	var entries uint64
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("%w: tar: %v", ErrDatasetArchive, err)
		}
		clean := path.Clean(h.Name)
		if h.Name == "" || len(h.Name) > maxDatasetPathBytes || strings.HasPrefix(h.Name, "/") || clean == ".." ||
			strings.HasPrefix(clean, "../") ||
			len(strings.Split(clean, "/")) > maxDatasetPathDepth {
			return fmt.Errorf("%w: invalid path", ErrDatasetArchive)
		}
		entries++ // directories also consume parser/filesystem work and count
		switch h.Typeflag {
		case tar.TypeReg:
		case tar.TypeDir:
		default:
			return fmt.Errorf("%w: archive entry type %d rejected", ErrDatasetArchive, h.Typeflag)
		}
		if entries > uint64(maxFiles) || h.Size < 0 || uint64(h.Size) > maxExpanded-expanded {
			return fmt.Errorf("%w: expanded limits exceeded", ErrDatasetArchive)
		}
		expanded += uint64(h.Size)
	}
	return nil
}

// CreateVolume 创建本地空卷（LOCAL_RW；v1.3-D 不支持 archive 导入）。
func (a *Adapter) CreateVolume(
	ctx context.Context,
	volumeID string,
	sizeBytes uint64,
) (*pb.CreateVolumeResponse, error) {
	vp, _, err := a.resolveVolumes()
	if err != nil {
		return nil, err
	}
	sizeGb := int(sizeBytes / (1024 * 1024 * 1024))
	if sizeGb < 1 {
		sizeGb = 1
	}
	_, err = vp.CreateVolume(ctx, volumes.CreateVolumeRequest{
		Id:     &volumeID,
		SizeGb: sizeGb,
	})
	if err != nil {
		return nil, fmt.Errorf("hypeman create volume: %w", err)
	}
	return &pb.CreateVolumeResponse{VolumeId: volumeID, SizeBytes: uint64(sizeGb) * 1024 * 1024 * 1024}, nil
}

// RecoverVolume resolves a create/import result from durable volume inventory.
func (a *Adapter) RecoverVolume(ctx context.Context, volumeID string) (*pb.CreateVolumeResponse, bool, error) {
	vp, _, err := a.resolveVolumes()
	if err != nil {
		return nil, false, err
	}
	v, err := vp.GetVolume(ctx, volumeID)
	if err != nil {
		// Managers differ in their not-found sentinel; listing is the stable
		// inventory fallback and avoids treating lookup errors as permission to create.
		list, listErr := vp.ListVolumes(ctx)
		if listErr != nil {
			return nil, false, fmt.Errorf("recover volume inventory: get: %v; list: %w", err, listErr)
		}
		for i := range list {
			if list[i].Id == volumeID {
				v = &list[i]
				break
			}
		}
		if v == nil {
			return nil, false, nil
		}
	}
	return &pb.CreateVolumeResponse{
		VolumeId: v.Id, SizeBytes: uint64(v.SizeGb) * 1024 * 1024 * 1024,
		ContentDigest: v.Tags[tagDatasetDigest], Sealed: v.Tags[tagDatasetSealed] == "true",
	}, true, nil
}

// VolumeAttachmentState observes attachment state by stable machine/execution/volume identity.
func (a *Adapter) VolumeAttachmentState(
	ctx context.Context,
	machineID, executionID, volumeID string,
) (*pb.Machine, bool, error) {
	inst, err := a.checkedVolumeInstance(ctx, machineID, executionID)
	if err != nil {
		return nil, false, err
	}
	for i := range inst.Volumes {
		if inst.Volumes[i].VolumeID == volumeID {
			return mapMachine(inst), true, nil
		}
	}
	return mapMachine(inst), false, nil
}

// AttachVolume performs an explicit cold restart. hypeman intentionally only
// changes an existing instance's disk plan while it is stopped; v1.3 does not
// claim hot-attach support.
func (a *Adapter) AttachVolume(ctx context.Context, req *pb.AttachVolumeRequest) (*pb.Machine, error) {
	vp, at, err := a.resolveVolumes()
	if err != nil {
		return nil, err
	}
	v, err := vp.GetVolume(ctx, req.GetVolumeId())
	if err != nil {
		return nil, fmt.Errorf("get volume: %w", err)
	}
	if digest := v.Tags[tagDatasetDigest]; digest != "" {
		if v.Tags[tagDatasetSealed] != "true" || !req.GetReadonly() {
			return nil, ErrDatasetUnsealed
		}
	}
	attachReq := instances.AttachVolumeRequest{
		MountPath: req.GetMountPath(), Readonly: req.GetReadonly(), Overlay: req.GetOverlay(),
		OverlaySize: int64(req.GetOverlaySizeBytes()),
	}
	return a.coldMutateVolume(ctx, at, req.GetMachineId(), req.GetExecutionId(), req.GetVolumeId(),
		func(c context.Context, id string) (*instances.Instance, error) {
			return at.AttachVolume(c, id, req.GetVolumeId(), attachReq)
		},
		func(c context.Context, id string) error {
			_, err := at.DetachVolume(c, id, req.GetVolumeId())
			return err
		})
}

// DetachVolume performs the same controlled cold restart and is idempotent.
func (a *Adapter) DetachVolume(ctx context.Context, req *pb.DetachVolumeRequest) (*pb.Machine, error) {
	_, at, err := a.resolveVolumes()
	if err != nil {
		return nil, err
	}
	inst, err := a.checkedVolumeInstance(ctx, req.GetMachineId(), req.GetExecutionId())
	if err != nil {
		return nil, err
	}
	var original *instances.VolumeAttachment
	for i := range inst.Volumes {
		if inst.Volumes[i].VolumeID == req.GetVolumeId() {
			copy := inst.Volumes[i]
			original = &copy
			break
		}
	}
	return a.coldMutateVolumeWithInstance(ctx, at, inst, req.GetMachineId(), req.GetVolumeId(),
		func(c context.Context, id string) (*instances.Instance, error) {
			return at.DetachVolume(c, id, req.GetVolumeId())
		},
		func(c context.Context, id string) error {
			if original == nil {
				return nil
			}
			_, err := at.AttachVolume(c, id, req.GetVolumeId(), instances.AttachVolumeRequest{
				MountPath: original.MountPath, Readonly: original.Readonly, Overlay: original.Overlay, OverlaySize: original.OverlaySize,
			})
			return err
		})
}

func (a *Adapter) checkedVolumeInstance(
	ctx context.Context,
	machineID, executionID string,
) (*instances.Instance, error) {
	inst, err := a.instances.GetInstance(ctx, machineID)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrMachineNotFound, machineID)
	}
	if inst.Tags[tagExecution] != executionID {
		return nil, fmt.Errorf(
			"%w: machine %s want %s got %s",
			ErrStaleExecution,
			machineID,
			executionID,
			inst.Tags[tagExecution],
		)
	}
	return inst, nil
}

func (a *Adapter) coldMutateVolume(
	ctx context.Context,
	at volumeAttacher,
	machineID, executionID, volumeID string,
	mutate func(context.Context, string) (*instances.Instance, error),
	rollback func(context.Context, string) error,
) (*pb.Machine, error) {
	inst, err := a.checkedVolumeInstance(ctx, machineID, executionID)
	if err != nil {
		return nil, err
	}
	return a.coldMutateVolumeWithInstance(ctx, at, inst, machineID, volumeID, mutate, rollback)
}

func (a *Adapter) coldMutateVolumeWithInstance(
	ctx context.Context,
	at volumeAttacher,
	inst *instances.Instance,
	machineID, volumeID string,
	mutate func(context.Context, string) (*instances.Instance, error),
	rollback func(context.Context, string) error,
) (*pb.Machine, error) {
	wasRunning := inst.State == instances.StateRunning || inst.State == instances.StateInitializing
	if !wasRunning && inst.State != instances.StateStopped {
		return nil, fmt.Errorf("volume cold restart requires running or stopped instance, got %s", inst.State)
	}
	if wasRunning {
		if _, err := at.StopInstance(ctx, inst.Id); err != nil {
			return nil, fmt.Errorf("stop before volume change: %w", err)
		}
	}
	changed, err := mutate(ctx, inst.Id)
	if err != nil {
		if wasRunning {
			compCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if _, startErr := a.startAndReattachSlot(compCtx, at, machineID, inst.Id); startErr != nil {
				return nil, errors.Join(
					fmt.Errorf("change volume %s: %w", volumeID, err),
					fmt.Errorf("restore source running state: %w", startErr),
				)
			}
		}
		return nil, fmt.Errorf("change volume %s: %w", volumeID, err)
	}
	if !wasRunning {
		return mapMachine(changed), nil
	}
	started, startErr := a.startAndReattachSlot(ctx, at, machineID, inst.Id)
	if startErr == nil {
		return mapMachine(started), nil
	}

	// Restore both the original attachment set and the source running state.
	compCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	_, stopErr := at.StopInstance(compCtx, inst.Id) // idempotent if start never launched
	rollbackErr := rollback(compCtx, inst.Id)
	_, restartErr := a.startAndReattachSlot(compCtx, at, machineID, inst.Id)
	return nil, errors.Join(fmt.Errorf("restart after volume change: %w", startErr),
		wrapVolumeCompensation("stop changed instance", stopErr),
		wrapVolumeCompensation("restore original attachment", rollbackErr),
		wrapVolumeCompensation("restore source running state", restartErr))
}

func (a *Adapter) startAndReattachSlot(
	ctx context.Context,
	at volumeAttacher,
	machineID, instanceID string,
) (*instances.Instance, error) {
	started, err := at.StartInstance(ctx, instanceID, instances.StartInstanceRequest{})
	if err != nil {
		return nil, err
	}
	if err := a.reattachSlot(ctx, machineID, started); err != nil {
		return nil, err
	}
	return started, nil
}

func wrapVolumeCompensation(action string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", action, err)
}

// DeleteVolume 删除本地卷（hypeman 拒绝被挂载中的卷）。产物不存在视为已删
// （幂等；v1.4-B MISSING 墓碑回收依赖此语义）。
func (a *Adapter) DeleteVolume(ctx context.Context, volumeID string) error {
	vp, _, err := a.resolveVolumes()
	if err != nil {
		return err
	}
	if err := vp.DeleteVolume(ctx, volumeID); err != nil {
		if errors.Is(err, volumes.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("hypeman delete volume: %w", err)
	}
	return nil
}

// ListVolumes 列出本地卷。
func (a *Adapter) ListVolumes(ctx context.Context) ([]*pb.VolumeInfo, error) {
	vp, _, err := a.resolveVolumes()
	if err != nil {
		return nil, err
	}
	list, err := vp.ListVolumes(ctx)
	if err != nil {
		return nil, fmt.Errorf("hypeman list volumes: %w", err)
	}
	out := make([]*pb.VolumeInfo, 0, len(list))
	for _, v := range list {
		mode := "LOCAL_RW"
		if v.Tags[tagDatasetDigest] != "" {
			mode = "DATASET_RO"
		}
		attachments := make([]*pb.VolumeAttachmentInfo, 0, len(v.Attachments))
		for _, att := range v.Attachments {
			// hypeman stores its instance ID here. firepaas machine IDs are used as
			// hypeman instance names by the adapter; execution is intentionally left
			// unknown rather than guessed from another authority.
			attachments = append(attachments, &pb.VolumeAttachmentInfo{
				MachineId: att.InstanceID,
				MountPath: att.MountPath, Readonly: att.Readonly,
			})
		}
		revisionInput := fmt.Sprintf(
			"%s\x00%d\x00%s\x00%t",
			v.Id,
			v.SizeGb,
			v.Tags[tagDatasetDigest],
			v.Tags[tagDatasetSealed] == "true",
		)
		revision := sha256.Sum256([]byte(revisionInput))
		out = append(out, &pb.VolumeInfo{
			Id: v.Id, SizeBytes: uint64(v.SizeGb) * 1024 * 1024 * 1024,
			ContentDigest: v.Tags[tagDatasetDigest], Sealed: v.Tags[tagDatasetSealed] == "true",
			Mode: mode, MetadataHealth: "HEALTHY", MaterializationRevision: hex.EncodeToString(revision[:]),
			Attachments: attachments,
		})
	}
	return out, nil
}
