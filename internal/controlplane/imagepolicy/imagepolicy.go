// Package imagepolicy 是控制面侧的镜像引用策略（P1-2，M3 单机形态）：
//
//   - digest 校验：mutable tag 允许提交（部署创建时解析一次，hypeman 侧
//     存 ResolvedImage 后续不变），但 API 必须能识别 name@sha256:... 形态
//     并拒绝非法 digest；digest-pinned 引用优先。
//   - registry allowlist：环境变量 FIREPAAS_REGISTRY_ALLOWLIST（逗号分隔的
//     registry host 前缀，如 docker.io,registry.local）；空 = 全放行（实验室
//     默认），非空 = 引用的 registry 必须命中列表。
package imagepolicy

import (
	"fmt"
	"strings"

	"github.com/distribution/reference"
)

// Policy 校验镜像引用。
type Policy struct {
	// Allowlist 为空表示不限制。
	allowlist []string
	// requireDigest：拒绝 mutable tag（生产入口可开，实验室默认关）。
	requireDigest bool
}

// New 构造策略。allowlist 是逗号分隔的 registry host 列表（可含端口）。
func New(allowlistCSV string) *Policy {
	var allow []string
	for _, s := range strings.Split(allowlistCSV, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			allow = append(allow, strings.TrimSuffix(s, "/"))
		}
	}
	return &Policy{allowlist: allow}
}

// NewWithOptions 构造带 digest 要求的策略（M5.1）。
func NewWithOptions(allowlistCSV string, requireDigest bool) *Policy {
	p := New(allowlistCSV)
	p.requireDigest = requireDigest
	return p
}

// Validate 返回规范化后的引用与错误。接受 tag 或 digest 引用；digest 形态
// 校验 sha256 前缀与十六进制长度（64）。
func (p *Policy) Validate(imageRef string) (string, error) {
	ref, err := reference.ParseNormalizedNamed(imageRef)
	if err != nil {
		return "", fmt.Errorf("invalid image reference %q: %w", imageRef, err)
	}
	if p.requireDigest {
		if _, ok := ref.(reference.Canonical); !ok {
			return "", fmt.Errorf("image %q: digest-pinned reference required (FIREPAAS_IMAGE_REQUIRE_DIGEST)", imageRef)
		}
	}
	if canonical, ok := ref.(reference.Canonical); ok {
		d := canonical.Digest()
		if !strings.HasPrefix(d.String(), "sha256:") || len(d.Encoded()) != 64 {
			return "", fmt.Errorf("image %q: only sha256 digests are supported", imageRef)
		}
	}
	if len(p.allowlist) > 0 {
		domain := reference.Domain(ref)
		ok := false
		for _, a := range p.allowlist {
			if a == "docker.io" && (domain == "docker.io" || domain == "index.docker.io" || domain == "registry-1.docker.io") {
				ok = true
				break
			}
			if a == domain {
				ok = true
				break
			}
		}
		if !ok {
			return "", fmt.Errorf("registry %q not in allowlist (FIREPAAS_REGISTRY_ALLOWLIST)", domain)
		}
	}
	return reference.TagNameOnly(ref).String(), nil
}
