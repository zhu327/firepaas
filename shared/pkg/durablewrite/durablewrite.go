// Package durablewrite 提供进程无关的崩溃安全文件写入：write temp →
// fsync(temp) → close → rename → fsync(dir)。成功返回意味着数据与目录项
// 都已落盘。rename 前不 fsync 可能在掉电后留下空文件，重启时整份状态作废；
// agent 的 operation ledger / fences / proxy credential 摘要 / slot 状态均
// 依赖这一纪律，实现只此一份。
package durablewrite

import (
	"fmt"
	"os"
	"path/filepath"
)

// WriteFileAtomic 将 data 崩溃安全地写入 path：临时文件 0600（状态可能含
// 敏感摘要），父目录 0700。what 只用于错误文案定位。
func WriteFileAtomic(path, what string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create %s dir: %w", what, err)
	}
	tmp, err := os.OpenFile(path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open %s tmp: %w", what, err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("write %s tmp: %w", what, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("fsync %s tmp: %w", what, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("close %s tmp: %w", what, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("rename %s: %w", what, err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open %s dir: %w", what, err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync %s dir: %w", what, err)
	}
	return nil
}
