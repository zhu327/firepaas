// sniff.go：v1.3-A（ADR-0027）首包嗅探——TCP/80 的 HTTP Host 与 TCP/443 的
// TLS ClientHello SNI。只读不写：解析过程保留已读字节供回放，不做 TLS 解密。
package egress

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
)

const (
	maxHTTPHeaderBytes = 8 * 1024
	maxClientHelloSize = 16 * 1024
)

// PeekResult 是嗅探结果：host（可为空）与从连接读到的前缀字节（回放用）。
type PeekResult struct {
	Host   string
	Prefix []byte
}

// ErrNoHostInfo 表示首包不含 Host/SNI（ECH、非 HTTP、畸形请求等）。
var ErrNoHostInfo = errors.New("no host/sni information")

// PeekHTTPHost 从 reader 读取一个 HTTP 请求头，提取 Host（大小写归一）。
// 无 Host（HTTP/1.0）返回 ErrNoHostInfo；prefix 包含读到的全部字节。
func PeekHTTPHost(r *bufio.Reader) (*PeekResult, error) {
	var buf bytes.Buffer
	host := ""
	line, err := readCRLFLine(r, &buf)
	if err != nil {
		return nil, err
	}
	// 请求行只用于格式校验；Host 必须来自 Host 头或绝对 URI。
	requestLine := strings.TrimSpace(line)
	if requestLine == "" || !strings.HasPrefix(requestLine, "GET ") && !strings.HasPrefix(requestLine, "POST ") &&
		!strings.HasPrefix(requestLine, "PUT ") && !strings.HasPrefix(requestLine, "HEAD ") &&
		!strings.HasPrefix(requestLine, "PATCH ") && !strings.HasPrefix(requestLine, "DELETE ") &&
		!strings.HasPrefix(requestLine, "OPTIONS ") && !strings.HasPrefix(requestLine, "CONNECT ") &&
		!strings.HasPrefix(requestLine, "TRACE ") {
		// 首行不是 HTTP 请求（可能非 HTTP 协议）：按无 Host 处理。
		return &PeekResult{Host: "", Prefix: buf.Bytes()}, ErrNoHostInfo
	}
	// 绝对 URI 形态（proxy 请求）: GET http://host/path HTTP/1.1
	if uri := strings.Fields(requestLine); len(uri) >= 2 && strings.HasPrefix(uri[1], "http") {
		if h, ok := hostFromURI(uri[1]); ok {
			host = h
		}
	}
	for {
		line, err = readCRLFLine(r, &buf)
		if err != nil {
			return nil, err
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			break // 头结束
		}
		if i := strings.IndexByte(trimmed, ':'); i > 0 {
			name := strings.ToLower(strings.TrimSpace(trimmed[:i]))
			if name == "host" {
				host = NormalizeHost(trimmed[i+1:])
			}
		}
		if buf.Len() > maxHTTPHeaderBytes {
			return nil, errors.New("http header too large")
		}
	}
	prefix := append([]byte(nil), buf.Bytes()...)
	if host == "" {
		return &PeekResult{Host: "", Prefix: prefix}, ErrNoHostInfo
	}
	return &PeekResult{Host: host, Prefix: prefix}, nil
}

func hostFromURI(uri string) (string, bool) {
	rest := uri
	if i := strings.Index(rest, "://"); i >= 0 {
		rest = rest[i+3:]
	}
	host := rest
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		host = rest[:i]
	}
	host = strings.TrimSuffix(host, ".")
	if host == "" {
		return "", false
	}
	return NormalizeHost(host), true
}

// readCRLFLine 读取一行（\r\n 或 \n），写入 buf。EOF 且 buf 空 → io.EOF。
func readCRLFLine(r *bufio.Reader, buf *bytes.Buffer) (string, error) {
	line, err := r.ReadString('\n')
	buf.WriteString(line)
	if err != nil && err != io.EOF {
		return "", err
	}
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	if err == io.EOF && line == "" && buf.Len() == 0 {
		return "", io.EOF
	}
	return line, nil
}

// PeekTLSSNI 读取 TLS 记录层 + ClientHello，提取 SNI 扩展（server_name）。
// 无 SNI（含 ECH 加密形态）返回 ErrNoHostInfo；prefix 含全部已读字节。
// 只处理单条 handshake record 的常见形态；超大/跨记录 ClientHello 按无 SNI。
func PeekTLSSNI(r *bufio.Reader) (*PeekResult, error) {
	var buf bytes.Buffer
	// 记录头: type(1) version(2) length(2)。
	hdr := make([]byte, 5)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	buf.Write(hdr)
	if hdr[0] != 0x16 { // handshake
		return &PeekResult{Host: "", Prefix: append([]byte(nil), buf.Bytes()...)}, ErrNoHostInfo
	}
	recLen := int(binary.BigEndian.Uint16(hdr[3:5]))
	if recLen > maxClientHelloSize {
		return &PeekResult{Host: "", Prefix: append([]byte(nil), buf.Bytes()...)}, ErrNoHostInfo
	}
	payload := make([]byte, recLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	buf.Write(payload)
	sni, ok := parseClientHelloSNI(payload)
	prefix := append([]byte(nil), buf.Bytes()...)
	if !ok || sni == "" {
		return &PeekResult{Host: "", Prefix: prefix}, ErrNoHostInfo
	}
	return &PeekResult{Host: NormalizeHost(sni), Prefix: prefix}, nil
}

// parseClientHelloSNI 从 handshake 消息体解析 SNI。返回 ok=false 表示无法
// 确定（畸形/ECH 等）。
func parseClientHelloSNI(payload []byte) (string, bool) {
	if len(payload) < 4 || payload[0] != 0x01 { // handshake type ClientHello
		return "", false
	}
	hsLen := int(payload[1])<<16 | int(payload[2])<<8 | int(payload[3])
	if hsLen+4 > len(payload) {
		return "", false
	}
	body := payload[4 : 4+hsLen]
	if len(body) < 2+2+32 {
		return "", false
	}
	off := 2  // legacy_version
	off += 32 // random
	if off+1 > len(body) {
		return "", false
	}
	sidLen := int(body[off])
	off++
	if off+sidLen > len(body) {
		return "", false
	}
	off += sidLen
	if off+2 > len(body) { // cipher suites
		return "", false
	}
	csLen := int(binary.BigEndian.Uint16(body[off : off+2]))
	off += 2 + csLen
	if off+1 > len(body) { // compression methods
		return "", false
	}
	compLen := int(body[off])
	off += 1 + compLen
	if off+2 > len(body) { // extensions length
		return "", false
	}
	extLen := int(binary.BigEndian.Uint16(body[off : off+2]))
	off += 2
	end := off + extLen
	if end > len(body) {
		return "", false
	}
	for off+4 <= end {
		extType := binary.BigEndian.Uint16(body[off : off+2])
		extSize := int(binary.BigEndian.Uint16(body[off+2 : off+4]))
		off += 4
		if off+extSize > end {
			break
		}
		if extType == 0x0000 { // server_name
			return parseServerNameExt(body[off : off+extSize])
		}
		off += extSize
	}
	return "", false
}

// parseServerNameExt 解析 server_name 扩展（RFC 6066 §3）。
func parseServerNameExt(ext []byte) (string, bool) {
	if len(ext) < 5 {
		return "", false
	}
	listLen := int(binary.BigEndian.Uint16(ext[0:2]))
	if listLen+2 > len(ext) {
		return "", false
	}
	list := ext[2 : 2+listLen]
	for i := 0; i+3 <= len(list); {
		nameType := list[i]
		nameLen := int(binary.BigEndian.Uint16(list[i+1 : i+3]))
		i += 3
		if i+nameLen > len(list) {
			break
		}
		if nameType == 0x00 { // host_name
			host := string(list[i : i+nameLen])
			if host != "" {
				return host, true
			}
		}
		i += nameLen
	}
	return "", false
}
