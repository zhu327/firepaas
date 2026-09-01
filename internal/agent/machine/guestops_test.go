package machine

import "testing"

func TestValidateGuestFilePath(t *testing.T) {
	for _, ok := range []string{"/tmp/a.txt", "/run/firepaas/secrets/TOKEN", "/a/b/c"} {
		if err := ValidateGuestFilePath(ok); err != nil {
			t.Fatalf("want ok %q, got %v", ok, err)
		}
	}
	for _, bad := range []string{
		"", "relative/x", "/", "/a/../b", "/a/b/../../c", "/etc/../../etc/passwd",
	} {
		if err := ValidateGuestFilePath(bad); err == nil {
			t.Fatalf("want rejection for %q", bad)
		}
	}
}
