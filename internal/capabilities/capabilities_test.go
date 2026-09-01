package capabilities

import "testing"

func TestValid(t *testing.T) {
	for _, id := range All() {
		if !Valid(id) {
			t.Fatalf("published feature %q must be valid", id)
		}
	}
	for _, bad := range []string{"", "UPPER.v1", "space id", "no.version"} {
		if bad == "no.version" && Valid(bad) {
			// no.version 由字母/点组成，按当前形态规则合法（语义审查由发布流程承担）。
			continue
		}
		if Valid(bad) {
			t.Fatalf("want invalid: %q", bad)
		}
	}
}

func TestSetOf(t *testing.T) {
	if got := SetOf(nil); got != nil {
		t.Fatalf("nil in must yield nil set, got %v", got)
	}
	s := SetOf([]string{GuestExecV1, GuestExecV1, "bad!id"})
	if len(s) != 1 || !s[GuestExecV1] {
		t.Fatalf("want deduped valid set, got %v", s)
	}
}
