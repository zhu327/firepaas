// volumes_loopback_test.go：v1.4 回归——loopback 测试例外的下载路径必须可达。
// 历史 bug：resolver 对 IPv4 字面量返回 IPv4-mapped IPv6，publicDatasetAddr
// 无条件拒绝 4-in-6，导致 allow_http_loopback_for_tests 永远失败。
package machine

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kernel/hypeman/lib/instances"
	"github.com/kernel/hypeman/lib/volumes"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

type loopbackVolumes struct {
	created   []volumes.CreateVolumeFromArchiveRequest
	deleted   []string
	deleteErr error
}

func (v *loopbackVolumes) CreateVolume(context.Context, volumes.CreateVolumeRequest) (*volumes.Volume, error) {
	return &volumes.Volume{Id: "vol-lb", SizeGb: 1}, nil
}

func (v *loopbackVolumes) CreateVolumeFromArchive(
	_ context.Context,
	req volumes.CreateVolumeFromArchiveRequest,
	_ io.Reader,
) (*volumes.Volume, error) {
	v.created = append(v.created, req)
	return &volumes.Volume{Id: *req.Id, SizeGb: req.SizeGb, Tags: req.Tags}, nil
}

func (v *loopbackVolumes) GetVolume(context.Context, string) (*volumes.Volume, error) {
	return nil, instances.ErrNotFound
}

func (v *loopbackVolumes) DeleteVolume(_ context.Context, id string) error {
	v.deleted = append(v.deleted, id)
	if v.deleteErr != nil {
		return v.deleteErr
	}
	return volumes.ErrNotFound
}

func (v *loopbackVolumes) ListVolumes(context.Context) ([]volumes.Volume, error) {
	return nil, nil
}

func (v *loopbackVolumes) TotalVolumeBytes(context.Context) (int64, error) { return 0, nil }

type loopbackInstances struct {
	fakeInstances
}

func (v *loopbackInstances) AttachVolume(
	context.Context,
	string,
	string,
	instances.AttachVolumeRequest,
) (*instances.Instance, error) {
	return nil, instances.ErrNotSupported
}

func (v *loopbackInstances) DetachVolume(context.Context, string, string) (*instances.Instance, error) {
	return nil, instances.ErrNotSupported
}

func (v *loopbackInstances) StopInstance(context.Context, string) (*instances.Instance, error) {
	return nil, instances.ErrNotSupported
}

func (v *loopbackInstances) StartInstance(
	context.Context,
	string,
	instances.StartInstanceRequest,
) (*instances.Instance, error) {
	return nil, instances.ErrNotSupported
}

func smallArchive(t *testing.T) []byte {
	t.Helper()
	var out bytes.Buffer
	gz := gzip.NewWriter(&out)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "data.bin", Typeflag: tar.TypeReg, Size: 3, Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func digestOf(t *testing.T, data []byte) string {
	t.Helper()
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// MISSING 墓碑回收：agent 本地产物不存在（hypeman ErrNotFound）时
// DeleteVolume 必须幂等成功，使 UNAVAILABLE/MISSING 卷可收敛 DELETED。
func TestDeleteVolumeMissingArtifactIsIdempotent(t *testing.T) {
	vp := &loopbackVolumes{}
	adapter := New(&loopbackInstances{}, &fakeImages{}, nil, nil)
	adapter.SetVolumes(vp)
	if err := adapter.DeleteVolume(context.Background(), "vol-gone"); err != nil {
		t.Fatalf("delete missing volume must converge: %v", err)
	}
	if len(vp.deleted) != 1 {
		t.Fatalf("delete calls = %d", len(vp.deleted))
	}
}

// loopback 测试例外必须能真实下载（hermetic e2e 的数据面回归）。
func TestImportDatasetLoopbackDownload(t *testing.T) {
	archive := smallArchive(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(archive)
	}))
	defer srv.Close()

	vp := &loopbackVolumes{}
	adapter := New(&loopbackInstances{}, &fakeImages{}, nil, nil)
	adapter.SetVolumes(vp)

	digest := digestOf(t, archive)
	resp, err := adapter.ImportDataset(context.Background(), &pb.ImportDatasetRequest{
		VolumeId: "vol-loopback", SourceUrl: srv.URL, ExpectedDigest: digest,
		MaxDownloadBytes: 1 << 20, MaxExpandedBytes: 1 << 20, MaxFiles: 10,
		AllowHttpLoopbackForTests: true,
	})
	if err != nil {
		t.Fatalf("loopback import: %v", err)
	}
	if !resp.GetSealed() || resp.GetContentDigest() != digest {
		t.Fatalf("sealed response = %+v", resp)
	}
	if len(vp.created) != 1 || vp.created[0].Tags[tagDatasetSealed] != "true" {
		t.Fatalf("volume creation = %+v", vp.created)
	}

	// 循环外生产语义不受影响：无 loopback 例外时 http 仍被拒绝。
	if _, err := adapter.ImportDataset(context.Background(), &pb.ImportDatasetRequest{
		VolumeId: "vol-loopback2", SourceUrl: srv.URL, ExpectedDigest: digest,
		MaxDownloadBytes: 1 << 20, MaxExpandedBytes: 1 << 20, MaxFiles: 10,
	}); err == nil {
		t.Fatal("http source without loopback exception must be rejected")
	}
}
