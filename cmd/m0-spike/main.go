// Command m0-spike 是 M0.4 的 agent adapter spike：不经 HTTP API，直接 import
// hypeman 的 lib（providers/instances/images）实际执行 Create/List/Delete，
// 用于验证“直接依赖 hypeman lib 抽取 agentd”的可行性与耦合点。
//
// 这不是 agentd 的正式实现；agentd 不允许 import cmd/api/config 与 providers
// 中携带的单机 API 耦合（见 agent/internal/README.md 设计红线）。
//
// 用法（root，需要 KVM/TAP；CONFIG_PATH 指向 scripts/lab/hypeman-p0.yaml）：
//
//	sudo env CONFIG_PATH=scripts/lab/hypeman-p0.yaml \
//	  go run ./cmd/m0-spike -image docker.io/library/nginx:alpine
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/kernel/hypeman/lib/images"
	"github.com/kernel/hypeman/lib/instances"
	"github.com/kernel/hypeman/lib/providers"
)

const (
	defaultSizeBytes = 512 * 1024 * 1024
)

func main() {
	imageRef := flag.String("image", "docker.io/library/nginx:alpine", "OCI image reference")
	instanceName := flag.String("name", "m0-spike", "instance name (lowercase letters/digits/dashes)")
	flag.Parse()

	if err := run(*imageRef, *instanceName); err != nil {
		fmt.Fprintln(os.Stderr, "m0-spike FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("m0-spike PASS: create/list/delete via hypeman lib")
}

func run(imageRef, instanceName string) error {
	ctx := context.Background()

	// 与 hypeman cmd/api 相同的装配路径（spike 专用）。
	cfg := providers.ProvideConfig()
	p := providers.ProvidePaths(cfg)

	imageMgr, err := providers.ProvideImageManager(p, cfg)
	if err != nil {
		return fmt.Errorf("image manager: %w", err)
	}
	systemMgr := providers.ProvideSystemManager(p)
	networkMgr := providers.ProvideNetworkManager(p, cfg)
	deviceMgr := providers.ProvideDeviceManager(p)
	volumeMgr, err := providers.ProvideVolumeManager(p, cfg)
	if err != nil {
		return fmt.Errorf("volume manager: %w", err)
	}
	instanceMgr, err := providers.ProvideInstanceManager(p, cfg, imageMgr, systemMgr, networkMgr, deviceMgr, volumeMgr)
	if err != nil {
		return fmt.Errorf("instance manager: %w", err)
	}

	// 1. image ready（若已缓存则直接 ready）
	if _, err := imageMgr.CreateImage(ctx, images.CreateImageRequest{Name: imageRef}); err != nil {
		// 已在队列中的情况允许继续等待
		fmt.Println("image create:", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if err := imageMgr.WaitForReady(waitCtx, imageRef); err != nil {
		return fmt.Errorf("image %s not ready: %w", imageRef, err)
	}

	// 2. create
	req := instances.CreateInstanceRequest{
		Name:           instanceName,
		Image:          imageRef,
		Size:           defaultSizeBytes,
		Vcpus:          1,
		NetworkEnabled: true,
	}
	created, err := instanceMgr.CreateInstance(ctx, req)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	fmt.Printf("created id=%s name=%s state=%s\n", created.Id, created.Name, created.State)

	// 3. list（按 name 过滤后确认存在）
	listed, err := instanceMgr.ListInstances(ctx, nil)
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}
	found := false
	for _, inst := range listed {
		if inst.Id == created.Id {
			found = true
			fmt.Printf("listed id=%s state=%s\n", inst.Id, inst.State)
		}
	}
	if !found {
		return fmt.Errorf("created instance %s not present in ListInstances", created.Id)
	}

	// 4. get
	got, err := instanceMgr.GetInstance(ctx, created.Id)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	fmt.Printf("get id=%s state=%s\n", got.Id, got.State)

	// 5. delete + verify
	if err := instanceMgr.DeleteInstance(ctx, created.Id); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	listedAfter, err := instanceMgr.ListInstances(ctx, nil)
	if err != nil {
		return fmt.Errorf("list after delete: %w", err)
	}
	for _, inst := range listedAfter {
		if inst.Id == created.Id {
			return fmt.Errorf("instance %s still present after DeleteInstance", created.Id)
		}
	}
	fmt.Println("delete verified: instance absent from ListInstances")
	return nil
}
