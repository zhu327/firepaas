package mtls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type contextServerStream struct{ ctx context.Context }

func (s contextServerStream) SetHeader(metadata.MD) error  { return nil }
func (s contextServerStream) SendHeader(metadata.MD) error { return nil }
func (s contextServerStream) SetTrailer(metadata.MD)       {}
func (s contextServerStream) Context() context.Context     { return s.ctx }
func (s contextServerStream) SendMsg(any) error            { return nil }
func (s contextServerStream) RecvMsg(any) error            { return nil }

func tlsPeerContext(cn string) context.Context {
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: cn}}
	info := credentials.TLSInfo{State: tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{cert},
		VerifiedChains:   [][]*x509.Certificate{{cert}},
	}}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: info})
}

func TestStreamServerIdentityInterceptor(t *testing.T) {
	interceptor := StreamServerIdentityInterceptor([]string{"control-plane"})
	called := false
	handler := func(any, grpc.ServerStream) error { called = true; return nil }

	err := interceptor(nil, contextServerStream{ctx: tlsPeerContext("edge-proxy")}, nil, handler)
	if status.Code(err) != codes.PermissionDenied || called {
		t.Fatalf("wrong CN: code=%v called=%v", status.Code(err), called)
	}

	err = interceptor(nil, contextServerStream{ctx: tlsPeerContext("control-plane")}, nil, handler)
	if err != nil || !called {
		t.Fatalf("allowed CN: err=%v called=%v", err, called)
	}
}
