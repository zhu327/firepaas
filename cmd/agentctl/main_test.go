package main

import (
	"io"
	"os"
	"strings"
	"testing"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

// -secret 可重复标志的累积语义必须由 stringSlice 保证。
func TestStringSliceRepeatableFlag(t *testing.T) {
	var s stringSlice
	if s.String() != "" {
		t.Fatalf("empty slice String()=%q", s.String())
	}
	for _, kv := range []string{"A=1", "B=2", "C=3"} {
		if err := s.Set(kv); err != nil {
			t.Fatalf("Set(%q): %v", kv, err)
		}
	}
	if len(s) != 3 {
		t.Fatalf("Set must accumulate, len=%d", len(s))
	}
	if got := s.String(); got != "A=1,B=2,C=3" {
		t.Fatalf("String()=%q", got)
	}
}

// print 用 protojson 多行格式输出（CLI 的人类可读契约），不得静默退化为
// 单行或其他编码。
func TestPrintWritesMultilineProtoJSON(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	// 任何断言路径都必须恢复 stdout（print panic 不应污染其余测试）。
	t.Cleanup(func() { os.Stdout = old })
	print(&pb.ServiceInfoResponse{NodeId: "n1", ServiceVersion: "v0.0.0-test"})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	os.Stdout = old
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)
	if !strings.Contains(got, `"nodeId"`) || !strings.Contains(got, `"n1"`) {
		t.Fatalf("missing protojson field in output: %q", got)
	}
	if !strings.Contains(got, "\n") || !strings.Contains(got, "  ") {
		t.Fatalf("output must be multiline indented JSON: %q", got)
	}
	// protojson 默认省略零值字段（CLI 输出不应充斥空字段）。
	if strings.Contains(got, "serviceCommit") {
		t.Fatalf("zero-value fields must be omitted: %q", got)
	}
}
