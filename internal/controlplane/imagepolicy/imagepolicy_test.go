package imagepolicy

import "testing"

func TestValidate(t *testing.T) {
	cases := []struct {
		name      string
		allowlist string
		ref       string
		wantErr   bool
	}{
		{"tag no allowlist", "", "nginx:alpine", false},
		{"digest valid", "", "docker.io/library/nginx@sha256:1f25fedd50aec27413031afb3a4f8ee4effcc9d843f6a76e81bfa92245ac5c06", false},
		{"digest bad hex", "", "nginx@sha256:short", true},
		{"digest bad algo", "", "nginx@sha512:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true},
		{"garbage", "", "not a valid ref !!", true},
		{"allowlist hit", "docker.io", "docker.io/library/nginx:alpine", false},
		{"allowlist hit short form", "docker.io", "nginx:alpine", false},
		{"allowlist miss", "registry.local", "nginx:alpine", true},
		{"allowlist custom reg", "registry.local,docker.io", "registry.local/team/app:v1", false},
	}
	for _, c := range cases {
		p := New(c.allowlist)
		_, err := p.Validate(c.ref)
		if (err != nil) != c.wantErr {
			t.Errorf("%s: Validate(%q) allowlist=%q err = %v, wantErr %v", c.name, c.ref, c.allowlist, err, c.wantErr)
		}
	}
}
