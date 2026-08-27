// Package secrets 实现 ADR-0010 的信封加密：
//
//	value_ciphertext = nonce(12B) || AES-256-GCM(DEK, plaintext)，AAD 绑定身份
//	dek_wrapped      = wnonce(12B) || AES-256-GCM(master, DEK)
//
// 主密钥来自部署注入（FIREPAAS_SECRETS_MASTER_KEY，base64 标准 32 字节），
// PG 中绝不出现明文；轮换与 KMS 化是 M5 工作项（key_version 已预留）。
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
)

const (
	// KeyVersion 当前主密钥版本。M5 轮换时引入多版本 keyring。
	KeyVersion = 1

	masterLen = 32
	dekLen    = 32
	nonceLen  = 12
)

// ErrBadMasterKey 主密钥缺失或长度不是 32 字节。
var ErrBadMasterKey = errors.New("secrets: master key must be 32 bytes base64")

// Manager 持有主密钥，提供 Seal/Open。
type Manager struct {
	gcm cipher.AEAD
}

// NewManager 由 base64 编码的 32 字节主密钥构造。
func NewManager(masterKeyB64 string) (*Manager, error) {
	raw, err := base64.StdEncoding.DecodeString(masterKeyB64)
	if err != nil || len(raw) != masterLen {
		return nil, ErrBadMasterKey
	}
	block, err := aes.NewCipher(raw)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Manager{gcm: gcm}, nil
}

// SealedValue 一条加密后的值（对应 secrets 行的两个 bytea 列）。
type SealedValue struct {
	Ciphertext []byte // nonce || ct
	WrappedDEK []byte // wnonce || wrapped DEK
}

// aad 将密文绑定到 (project, name, version)，防止行间密文互换/重放。
func aad(projectID, name string, version int64) []byte {
	return []byte(fmt.Sprintf("firepaas-secret:%s/%s/%d", projectID, name, version))
}

func sealWith(keyGcm cipher.AEAD, plaintext, aadBytes []byte) ([]byte, error) {
	nonce := make([]byte, nonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := keyGcm.Seal(nil, nonce, plaintext, aadBytes)
	return append(nonce, out...), nil
}

func openWith(keyGcm cipher.AEAD, blob, aadBytes []byte) ([]byte, error) {
	if len(blob) < nonceLen+1 {
		return nil, errors.New("secrets: ciphertext too short")
	}
	return keyGcm.Open(nil, blob[:nonceLen], blob[nonceLen:], aadBytes)
}

// Seal 加密一个值：随机 DEK 加密内容，主密钥包裹 DEK。
func (m *Manager) Seal(plaintext []byte, projectID, name string, version int64) (SealedValue, error) {
	dek := make([]byte, dekLen)
	if _, err := rand.Read(dek); err != nil {
		return SealedValue{}, err
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return SealedValue{}, err
	}
	dekGcm, err := cipher.NewGCM(block)
	if err != nil {
		return SealedValue{}, err
	}
	a := aad(projectID, name, version)
	ct, err := sealWith(dekGcm, plaintext, a)
	if err != nil {
		return SealedValue{}, err
	}
	wrapped, err := sealWith(m.gcm, dek, a)
	if err != nil {
		return SealedValue{}, err
	}
	return SealedValue{Ciphertext: ct, WrappedDEK: wrapped}, nil
}

// Open 解密：主密钥解开 DEK，DEK 解开内容；AAD 不匹配即失败。
func (m *Manager) Open(v SealedValue, projectID, name string, version int64) ([]byte, error) {
	a := aad(projectID, name, version)
	dek, err := openWith(m.gcm, v.WrappedDEK, a)
	if err != nil {
		return nil, fmt.Errorf("secrets: unwrap dek: %w", err)
	}
	if len(dek) != dekLen {
		return nil, errors.New("secrets: bad dek length")
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	dekGcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	pt, err := openWith(dekGcm, v.Ciphertext, a)
	if err != nil {
		return nil, fmt.Errorf("secrets: decrypt value: %w", err)
	}
	return pt, nil
}

// ConstantTimeEqual 供凭证比较等场景复用。
func ConstantTimeEqual(a, b []byte) bool { return subtle.ConstantTimeCompare(a, b) == 1 }
