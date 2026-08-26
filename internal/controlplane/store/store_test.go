package store

import (
	"testing"
)

func TestJSONEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b []byte
		want bool
	}{
		{"same object different key order", []byte(`{"a":1,"b":"2"}`), []byte(`{"b":"2","a":1}`), true},
		{"protojson 64-bit ints as strings", []byte(`{"generation":"1","vcpu":"2"}`), []byte(`{"vcpu":"2","generation":"1"}`), true},
		{"different values", []byte(`{"a":1}`), []byte(`{"a":2}`), false},
		{"different types", []byte(`{"a":1}`), []byte(`{"a":"1"}`), false},
		{"missing key", []byte(`{"a":1}`), []byte(`{"a":1,"b":2}`), false},
		{"invalid json a", []byte(`{`), []byte(`{}`), false},
		{"invalid json b", []byte(`{}`), []byte(`}`), false},
	}
	for _, tc := range cases {
		if got := jsonEqual(tc.a, tc.b); got != tc.want {
			t.Errorf("%s: jsonEqual(%s, %s) = %v, want %v", tc.name, tc.a, tc.b, got, tc.want)
		}
	}
}
