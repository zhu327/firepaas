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

// Deps 是崩溃注入/单测用的文件系统依赖面：默认 nil = os 实现。
// 生产调用 WriteFileAtomic 走默认面；单测用 WriteFileAtomicWithDeps
// 注入 fsync/rename 失败，覆盖 rename 前掉电、fsync 失败、tmp 残留。
type Deps struct {
	MkdirAll func(path string, perm os.FileMode) error
	OpenFile func(name string, flag int, perm os.FileMode) (*os.File, error)
	Rename   func(oldpath, newpath string) error
	Remove   func(name string) error
	OpenDir  func(name string) (*os.File, error)
	SyncFile func(f *os.File) error
	SyncDir  func(f *os.File) error
}

func (d Deps) mkdirAll(path string, perm os.FileMode) error {
	if d.MkdirAll != nil {
		return d.MkdirAll(path, perm)
	}
	return os.MkdirAll(path, perm)
}

func (d Deps) openFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	if d.OpenFile != nil {
		return d.OpenFile(name, flag, perm)
	}
	return os.OpenFile(name, flag, perm)
}

func (d Deps) rename(oldpath, newpath string) error {
	if d.Rename != nil {
		return d.Rename(oldpath, newpath)
	}
	return os.Rename(oldpath, newpath)
}

func (d Deps) remove(name string) error {
	if d.Remove != nil {
		return d.Remove(name)
	}
	return os.Remove(name)
}

func (d Deps) openDir(name string) (*os.File, error) {
	if d.OpenDir != nil {
		return d.OpenDir(name)
	}
	return os.Open(name)
}

func (d Deps) syncFile(f *os.File) error {
	if d.SyncFile != nil {
		return d.SyncFile(f)
	}
	return f.Sync()
}

func (d Deps) syncDir(f *os.File) error {
	if d.SyncDir != nil {
		return d.SyncDir(f)
	}
	return f.Sync()
}

// WriteFileAtomic 将 data 崩溃安全地写入 path：临时文件 0600（状态可能含
// 敏感摘要），父目录 0700。what 只用于错误文案定位。
func WriteFileAtomic(path, what string, data []byte) error {
	return WriteFileAtomicWithDeps(path, what, data, Deps{})
}

// WriteFileAtomicWithDeps 是可注入依赖的实现：d 零值 = os 默认面。
func WriteFileAtomicWithDeps(path, what string, data []byte, d Deps) error {
	dir := filepath.Dir(path)
	if err := d.mkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create %s dir: %w", what, err)
	}
	tmp, err := d.openFile(path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open %s tmp: %w", what, err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = d.remove(tmp.Name())
		return fmt.Errorf("write %s tmp: %w", what, err)
	}
	if err := d.syncFile(tmp); err != nil {
		_ = tmp.Close()
		_ = d.remove(tmp.Name())
		return fmt.Errorf("fsync %s tmp: %w", what, err)
	}
	if err := tmp.Close(); err != nil {
		_ = d.remove(tmp.Name())
		return fmt.Errorf("close %s tmp: %w", what, err)
	}
	if err := d.rename(tmp.Name(), path); err != nil {
		_ = d.remove(tmp.Name())
		return fmt.Errorf("rename %s: %w", what, err)
	}
	dirH, err := d.openDir(dir)
	if err != nil {
		return fmt.Errorf("open %s dir: %w", what, err)
	}
	defer func() { _ = dirH.Close() }()
	if err := d.syncDir(dirH); err != nil {
		return fmt.Errorf("fsync %s dir: %w", what, err)
	}
	return nil
}
