package httpapi

import (
	"testing"
)

// parseEastWestRuleBody 是纯校验函数（无 DB）：坏输入必须拒绝，好输入归一化。
func TestParseEastWestRuleBody(t *testing.T) {
	valid := eastWestRuleBody{
		SrcProject: "p1", SrcApp: "web",
		DstProject: "p1", DstApp: "db", DstService: "pg",
		Ports: []uint32{5432, 5432, 6432}, // 输入重复端口归一化
	}
	got, err := parseEastWestRuleBody(valid)
	if err != nil {
		t.Fatalf("valid rule rejected: %v", err)
	}
	if len(got.Ports) != 2 || got.Ports[0] != 5432 || got.Ports[1] != 6432 {
		t.Fatalf("ports not deduped/ordered: %v", got.Ports)
	}
	if got.SrcApp != "web" || got.DstService != "pg" {
		t.Fatalf("identity mangled: %+v", got)
	}
	for name, mutate := range map[string]func(*eastWestRuleBody){
		"empty src_app":   func(b *eastWestRuleBody) { b.SrcApp = "" },
		"empty dst":       func(b *eastWestRuleBody) { b.DstProject = "" },
		"empty service":   func(b *eastWestRuleBody) { b.DstService = "" },
		"empty ports":     func(b *eastWestRuleBody) { b.Ports = nil },
		"port zero":       func(b *eastWestRuleBody) { b.Ports = []uint32{0} },
		"port over range": func(b *eastWestRuleBody) { b.Ports = []uint32{65536} },
	} {
		t.Run(name, func(t *testing.T) {
			b := valid
			mutate(&b)
			if _, err := parseEastWestRuleBody(b); err == nil {
				t.Fatal("expected validation failure")
			}
		})
	}
}
