package machine

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/kernel/hypeman/lib/images"
	"github.com/kernel/hypeman/lib/volumes"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

func (a *Adapter) ImageQuarantineAvailable() bool {
	_, ok := a.images.(images.QuarantineManager)
	return ok
}

func (a *Adapter) VolumeQuarantineAvailable() bool {
	_, ok := a.volumes.(volumes.QuarantineManager)
	return ok
}

func (a *Adapter) QuarantineImage(ctx context.Context, req *pb.QuarantineImageRequest) (*pb.ImageQuarantine, error) {
	qm, ok := a.images.(images.QuarantineManager)
	if !ok {
		return nil, ErrGuestOpsUnsupported
	}
	if req.GetExpectedRevision() == "" || !strings.Contains(req.GetImageRef(), "@"+req.GetExpectedRevision()) {
		return nil, fmt.Errorf("image reference must be pinned to expected revision")
	}
	claim, err := qm.QuarantineImage(
		ctx,
		req.GetImageRef(),
		req.GetClaimId(),
		func(ctx context.Context, target images.ImageIdentity) error {
			if target.Digest != req.GetExpectedRevision() {
				return images.ErrClaimConflict
			}
			return a.guardImageReferences(ctx, target)
		},
	)
	if err != nil {
		return nil, err
	}
	return mapImageClaim(claim), nil
}

func (a *Adapter) ListImageQuarantines(ctx context.Context) ([]*pb.ImageQuarantine, error) {
	qm, ok := a.images.(images.QuarantineManager)
	if !ok {
		return nil, ErrGuestOpsUnsupported
	}
	claims, err := qm.ListImageQuarantines(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*pb.ImageQuarantine, 0, len(claims))
	for i := range claims {
		out = append(out, mapImageClaim(&claims[i]))
	}
	return out, nil
}

func mapImageClaim(c *images.QuarantineClaim) *pb.ImageQuarantine {
	return &pb.ImageQuarantine{
		ClaimId:       c.ClaimID,
		Token:         c.Token,
		Repository:    c.Image.Repository,
		Digest:        c.Image.Digest,
		State:         c.State,
		CreatedAtUnix: c.CreatedAt.Unix(),
	}
}

func (a *Adapter) RollbackImageQuarantine(ctx context.Context, id, token string) error {
	qm, ok := a.images.(images.QuarantineManager)
	if !ok {
		return ErrGuestOpsUnsupported
	}
	return qm.RollbackImageQuarantine(ctx, id, token)
}

func (a *Adapter) FinalizeImageQuarantine(ctx context.Context, id, token string) error {
	qm, ok := a.images.(images.QuarantineManager)
	if !ok {
		return ErrGuestOpsUnsupported
	}
	return qm.FinalizeImageQuarantine(ctx, id, token, a.guardImageReferences)
}

func (a *Adapter) guardImageReferences(ctx context.Context, target images.ImageIdentity) error {
	instancesList, err := a.instances.ListInstances(ctx, nil)
	if err != nil {
		return err
	}
	cached, err := a.images.ListImages(ctx)
	if err != nil {
		return err
	}
	for _, inst := range instancesList {
		ref := strings.TrimSpace(inst.Image)
		if ref == "" {
			return images.ErrInUse
		}
		if ref == target.Digest || strings.HasSuffix(ref, "@"+target.Digest) {
			return images.ErrInUse
		}
		resolved := false
		for _, img := range cached {
			if !imageReferenceMatches(img, ref) {
				continue
			}
			resolved = true
			if imageReferenceMatches(img, target.Digest) {
				return images.ErrInUse
			}
		}
		// A tag that cannot be resolved is unsafe: it may still name the
		// quarantined digest, which is intentionally absent from ListImages.
		if !resolved {
			return images.ErrInUse
		}
	}
	return nil
}

func (a *Adapter) QuarantineVolume(ctx context.Context, req *pb.QuarantineVolumeRequest) (*pb.VolumeQuarantine, error) {
	if req.GetMode() != "DATASET_RO" || !req.GetRebuildable() {
		return nil, errors.New("only rebuildable DATASET_RO may be quarantined")
	}
	qm, ok := a.volumes.(volumes.QuarantineManager)
	if !ok {
		return nil, ErrGuestOpsUnsupported
	}
	vp := a.volumes
	if vp == nil {
		return nil, ErrGuestOpsUnsupported
	}
	v, err := vp.GetVolume(ctx, req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	if v.Tags[tagDatasetDigest] == "" || v.Tags[tagDatasetSealed] != "true" || req.GetExpectedRevision() == "" ||
		v.Tags[tagDatasetDigest] != req.GetExpectedRevision() {
		return nil, errors.New("dataset revision or sealed metadata mismatch")
	}
	claim, err := qm.QuarantineVolume(ctx, req.GetVolumeId(), req.GetClaimId())
	if err != nil {
		return nil, err
	}
	return mapVolumeClaim(claim), nil
}

func mapVolumeClaim(c *volumes.QuarantineClaim) *pb.VolumeQuarantine {
	return &pb.VolumeQuarantine{
		ClaimId:       c.ClaimID,
		Token:         c.Token,
		VolumeId:      c.VolumeID,
		State:         c.State,
		CreatedAtUnix: c.CreatedAt.Unix(),
	}
}

func (a *Adapter) ListVolumeQuarantines(ctx context.Context) ([]*pb.VolumeQuarantine, error) {
	qm, ok := a.volumes.(volumes.QuarantineManager)
	if !ok {
		return nil, ErrGuestOpsUnsupported
	}
	cs, e := qm.ListVolumeQuarantines(ctx)
	if e != nil {
		return nil, e
	}
	out := make([]*pb.VolumeQuarantine, 0, len(cs))
	for i := range cs {
		out = append(out, mapVolumeClaim(&cs[i]))
	}
	return out, nil
}

func (a *Adapter) RollbackVolumeQuarantine(ctx context.Context, id, token string) error {
	qm, ok := a.volumes.(volumes.QuarantineManager)
	if !ok {
		return ErrGuestOpsUnsupported
	}
	return qm.RollbackVolumeQuarantine(ctx, id, token)
}

func (a *Adapter) FinalizeVolumeQuarantine(ctx context.Context, id, token string) error {
	qm, ok := a.volumes.(volumes.QuarantineManager)
	if !ok {
		return ErrGuestOpsUnsupported
	}
	return qm.FinalizeVolumeQuarantine(ctx, id, token)
}
