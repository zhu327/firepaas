package env

import (
	"testing"
	"time"
)

func TestGetFallback(t *testing.T) {
	t.Setenv("FIREPAAS_TEST_GET_X", "")
	if got := Get("FIREPAAS_TEST_GET_X", "def"); got != "def" {
		t.Fatalf("empty → default, got %q", got)
	}
	t.Setenv("FIREPAAS_TEST_GET_X", "v")
	if got := Get("FIREPAAS_TEST_GET_X", "def"); got != "v" {
		t.Fatalf("nonempty passthrough, got %q", got)
	}
}

func TestBoolInvalidFallsBack(t *testing.T) {
	t.Setenv("FIREPAAS_TEST_BOOL_X", "notabool")
	if got := Bool("FIREPAAS_TEST_BOOL_X", true); got != true {
		t.Fatal("invalid bool must fall back to default")
	}
	t.Setenv("FIREPAAS_TEST_BOOL_X", " true ")
	if got := Bool("FIREPAAS_TEST_BOOL_X", false); got != true {
		t.Fatal("bool should trim whitespace")
	}
	t.Setenv("FIREPAAS_TEST_BOOL_X", "")
	if got := Bool("FIREPAAS_TEST_BOOL_X", true); got != true {
		t.Fatal("empty bool must fall back to default")
	}
}

func TestIntRejectsNonPositive(t *testing.T) {
	for _, raw := range []string{"abc", "0", "-3", "1.5"} {
		t.Setenv("FIREPAAS_TEST_INT_X", raw)
		if got := Int("FIREPAAS_TEST_INT_X", 7); got != 7 {
			t.Fatalf("Int(%q) = %d, want default 7", raw, got)
		}
	}
	t.Setenv("FIREPAAS_TEST_INT_X", "12")
	if got := Int("FIREPAAS_TEST_INT_X", 7); got != 12 {
		t.Fatalf("Int(12) = %d", got)
	}
}

func TestDurRejectsNonPositive(t *testing.T) {
	for _, raw := range []string{"bogus", "0s", "-1s"} {
		t.Setenv("FIREPAAS_TEST_DUR_X", raw)
		if got := Dur("FIREPAAS_TEST_DUR_X", time.Second); got != time.Second {
			t.Fatalf("Dur(%q) must fall back, got %v", raw, got)
		}
	}
	t.Setenv("FIREPAAS_TEST_DUR_X", "5s")
	if got := Dur("FIREPAAS_TEST_DUR_X", time.Second); got != 5*time.Second {
		t.Fatalf("Dur(5s) = %v", got)
	}
}

func TestFloatInvalidFallsBack(t *testing.T) {
	t.Setenv("FIREPAAS_TEST_FLOAT_X", "nan-value")
	if got := Float("FIREPAAS_TEST_FLOAT_X", 1.5); got != 1.5 {
		t.Fatalf("invalid float must fall back, got %v", got)
	}
	t.Setenv("FIREPAAS_TEST_FLOAT_X", "2.5")
	if got := Float("FIREPAAS_TEST_FLOAT_X", 1.5); got != 2.5 {
		t.Fatalf("Float(2.5) = %v", got)
	}
}
