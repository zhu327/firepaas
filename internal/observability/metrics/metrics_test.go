package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// P2-8：gauge family 清理——label 组合消失后旧序列不得残留。
func TestResetFamilyRemovesStaleSeries(t *testing.T) {
	r := New()
	r.Set("firepaas_machines_observed", map[string]string{"state": "RUNNING"}, 3)
	r.Set("firepaas_machines_observed", map[string]string{"state": "PAUSED"}, 1)
	r.Set("firepaas_nodes_total", nil, 2)

	r.ResetFamily("firepaas_machines_observed")
	r.Set("firepaas_machines_observed", map[string]string{"state": "RUNNING"}, 2)

	out := render(t, r)
	if strings.Contains(out, `state="PAUSED"`) {
		t.Fatalf("stale PAUSED series survived reset:\n%s", out)
	}
	if !strings.Contains(out, `firepaas_machines_observed{state="RUNNING"} 2`) {
		t.Fatalf("fresh series missing:\n%s", out)
	}
	if !strings.Contains(out, "firepaas_nodes_total 2") {
		t.Fatalf("other family must be untouched:\n%s", out)
	}
}

func render(t *testing.T, r *Registry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, nil)
	return rec.Body.String()
}
