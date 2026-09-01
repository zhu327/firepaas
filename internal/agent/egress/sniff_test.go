package egress

import (
	"bufio"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestPeekHTTPHost(t *testing.T) {
	cases := []struct {
		name  string
		raw   string
		host  string
		noErr bool
	}{
		{"host header", "GET /path?q=1 HTTP/1.1\r\nHost: Example.com\r\nAccept: */*\r\n\r\n", "example.com", true},
		{"absolute uri", "GET http://Api.example.com/x HTTP/1.0\r\n\r\n", "api.example.com", true},
		{"no host http10", "GET / HTTP/1.0\r\n\r\n", "", false},
		{"no host http11", "GET / HTTP/1.1\r\nAccept: */*\r\n\r\n", "", false},
		{"case insensitive host", "POST / HTTP/1.1\r\nhOsT: wWw.Example.COM\r\n\r\n", "www.example.com", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			peek, err := PeekHTTPHost(bufio.NewReader(strings.NewReader(c.raw)))
			if c.noErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if peek.Host != c.host {
					t.Fatalf("host = %q, want %q", peek.Host, c.host)
				}
			} else if !errors.Is(err, ErrNoHostInfo) {
				t.Fatalf("want ErrNoHostInfo, got %v", err)
			}
			if got := string(peek.Prefix); got != c.raw {
				t.Fatalf("prefix must replay exactly: %q", got)
			}
		})
	}
	// 非 HTTP 协议按无 Host 处理。
	peek, err := PeekHTTPHost(bufio.NewReader(strings.NewReader("SSH-2.0-OpenSSH\r\n")))
	if !errors.Is(err, ErrNoHostInfo) {
		t.Fatalf("non-http must be ErrNoHostInfo, got %v", err)
	}
	if peek.Host != "" {
		t.Fatalf("host must be empty, got %q", peek.Host)
	}
}

// buildClientHello 构造带 SNI 的 ClientHello（最小可解析形态）。
func buildClientHello(t *testing.T, sni string, withEch bool) []byte {
	t.Helper()
	var body []byte
	body = append(body, 0x03, 0x03) // legacy_version
	random := make([]byte, 32)
	body = append(body, random...)
	body = append(body, 0x00)                   // session id length
	body = append(body, 0x00, 0x02, 0x13, 0x01) // cipher suites: TLS_AES_128_GCM_SHA256
	body = append(body, 0x01, 0x00)             // compression methods: null
	var ext []byte
	if sni != "" {
		name := []byte(sni)
		list := append([]byte{0x00}, byte(len(name)>>8), byte(len(name)))
		list = append(list, name...)
		sniExt := append([]byte{byte(len(list) >> 8), byte(len(list))}, list...)
		ext = append(ext, 0x00, 0x00, byte(len(sniExt)>>8), byte(len(sniExt)))
		ext = append(ext, sniExt...)
	}
	if withEch {
		// encrypted_client_hello(0xfe0d) 占位：无 SNI 时表示 SNI 被加密。
		ext = append(ext, 0xfe, 0x0d, 0x00, 0x00)
	}
	body = append(body, byte(len(ext)>>8), byte(len(ext)))
	body = append(body, ext...)
	hs := []byte{0x01}
	hs = append(hs, byte(len(body)>>16), byte(len(body)>>8), byte(len(body)))
	hs = append(hs, body...)
	rec := []byte{0x16, 0x03, 0x03}
	rec = append(rec, byte(len(hs)>>8), byte(len(hs)))
	rec = append(rec, hs...)
	return rec
}

func TestPeekTLSSNI(t *testing.T) {
	rec := buildClientHello(t, "example.com", false)
	peek, err := PeekTLSSNI(bufio.NewReader(strings.NewReader(string(rec))))
	if err != nil {
		t.Fatal(err)
	}
	if peek.Host != "example.com" {
		t.Fatalf("sni = %q", peek.Host)
	}
	if string(peek.Prefix) != string(rec) {
		t.Fatal("prefix must replay exactly")
	}
}

func TestPeekTLSSNICases(t *testing.T) {
	// 无 SNI。
	rec := buildClientHello(t, "", false)
	peek, err := PeekTLSSNI(bufio.NewReader(strings.NewReader(string(rec))))
	if !errors.Is(err, ErrNoHostInfo) || peek.Host != "" {
		t.Fatalf("no SNI must be ErrNoHostInfo, got %v host=%q", err, peek.Host)
	}
	// ECH 且无明文 SNI。
	rec = buildClientHello(t, "", true)
	if _, err := PeekTLSSNI(bufio.NewReader(strings.NewReader(string(rec)))); !errors.Is(err, ErrNoHostInfo) {
		t.Fatalf("ECH without SNI must be ErrNoHostInfo, got %v", err)
	}
	// 非 handshake 记录。
	rec = []byte{0x17, 0x03, 0x03, 0x00, 0x02, 0x01, 0x02}
	if _, err := PeekTLSSNI(bufio.NewReader(strings.NewReader(string(rec)))); !errors.Is(err, ErrNoHostInfo) {
		t.Fatalf("non-handshake must be ErrNoHostInfo, got %v", err)
	}
}

func TestParseClientHelloSNIGarbage(t *testing.T) {
	// 畸形长度/截断不 panic，返回 ok=false。
	if _, ok := parseClientHelloSNI([]byte{0x01, 0xff, 0xff, 0xff, 0x00}); ok {
		t.Fatal("truncated must not parse")
	}
	if _, ok := parseClientHelloSNI(nil); ok {
		t.Fatal("empty must not parse")
	}
	rec := buildClientHello(t, "example.com", false)
	truncated := rec[:len(rec)-3]
	if _, ok := parseClientHelloSNI(truncated[5:]); ok {
		t.Fatal("truncated record must not parse")
	}
	_ = binary.BigEndian // keep import honest for future record parsing
}
