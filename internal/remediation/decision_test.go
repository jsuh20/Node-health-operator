package remediation

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/jsuh20/node-health-operator/api/v1alpha1"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func nodeWith(conds ...corev1.NodeCondition) *corev1.Node {
	return &corev1.Node{Status: corev1.NodeStatus{Conditions: conds}}
}

func cond(t corev1.NodeConditionType, s corev1.ConditionStatus, since time.Time) corev1.NodeCondition {
	return corev1.NodeCondition{Type: t, Status: s, LastTransitionTime: metav1.NewTime(since)}
}

var rules = []v1alpha1.UnhealthyCondition{
	{Type: "Ready", Status: "False"},
	{Type: "Ready", Status: "Unknown"},
	{Type: "GPUHealthy", Status: "False"},
}

func TestEvaluateHealth(t *testing.T) {
	tests := []struct {
		name       string
		node       *corev1.Node
		wantBad    bool
		wantReason string
		wantSince  time.Time
	}{
		{"healthy", nodeWith(cond("Ready", "True", t0)), false, "", time.Time{}},
		{"not ready", nodeWith(cond("Ready", "False", t0)), true, "Ready=False", t0},
		{"unknown ready", nodeWith(cond("Ready", "Unknown", t0)), true, "Ready=Unknown", t0},
		{"gpu fault on a ready node",
			nodeWith(cond("Ready", "True", t0), cond("GPUHealthy", "False", t0.Add(time.Minute))),
			true, "GPUHealthy=False", t0.Add(time.Minute)},
		{"two faults: the older one wins",
			nodeWith(cond("Ready", "False", t0.Add(time.Minute)), cond("GPUHealthy", "False", t0)),
			true, "GPUHealthy=False", t0},
		{"unwatched condition is ignored", nodeWith(cond("DiskPressure", "True", t0)), false, "", time.Time{}},
		{"missing condition counts as healthy", nodeWith(), false, "", time.Time{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EvaluateHealth(tt.node, rules)
			if got.Unhealthy != tt.wantBad || got.Reason != tt.wantReason || !got.Since.Equal(tt.wantSince) {
				t.Errorf("got %+v, want unhealthy=%v reason=%q since=%v", got, tt.wantBad, tt.wantReason, tt.wantSince)
			}
		})
	}
}

func TestThresholdReached(t *testing.T) {
	bad := Health{Unhealthy: true, Since: t0}
	tests := []struct {
		name    string
		h       Health
		elapsed time.Duration
		want    bool
	}{
		{"healthy never triggers", Health{Since: t0}, time.Hour, false},
		{"transient blip is ignored", bad, 30 * time.Second, false},
		{"one tick before threshold", bad, 2*time.Minute - time.Nanosecond, false},
		{"exactly at threshold", bad, 2 * time.Minute, true},
		{"past threshold", bad, 10 * time.Minute, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ThresholdReached(tt.h, 2*time.Minute, t0.Add(tt.elapsed)); got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMaxConcurrent(t *testing.T) {
	tests := []struct {
		name  string
		v     intstr.IntOrString
		total int
		want  int
	}{
		{"count", intstr.FromInt32(2), 10, 2},
		{"percent", intstr.FromString("25%"), 8, 2},
		{"percent rounds down", intstr.FromString("25%"), 7, 1},
		{"tiny pool floors at 1", intstr.FromString("10%"), 5, 1},
		{"zero count floors at 1", intstr.FromInt32(0), 5, 1},
		{"no nodes means no slots", intstr.FromString("50%"), 0, 0},
		{"count above pool size", intstr.FromInt32(20), 4, 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MaxConcurrent(tt.v, tt.total)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}

	if _, err := MaxConcurrent(intstr.FromString("lots"), 5); err == nil {
		t.Error("expected an error for a non-percent string")
	}
}

func TestBackoff(t *testing.T) {
	initial, maxWait := 30*time.Second, 5*time.Minute
	tests := []struct {
		failures int32
		want     time.Duration
	}{
		{0, 0},
		{1, 30 * time.Second},
		{2, time.Minute},
		{3, 2 * time.Minute},
		{4, 4 * time.Minute},
		{5, 5 * time.Minute},  // capped
		{60, 5 * time.Minute}, // no overflow
	}
	for _, tt := range tests {
		if got := Backoff(tt.failures, initial, maxWait); got != tt.want {
			t.Errorf("Backoff(%d) = %v, want %v", tt.failures, got, tt.want)
		}
	}
}

func TestSelectToStart(t *testing.T) {
	c := func(name string, ageMin int) Candidate {
		return Candidate{Name: name, Health: Health{Unhealthy: true, Since: t0.Add(-time.Duration(ageMin) * time.Minute)}}
	}
	names := func(cs []Candidate) []string {
		out := make([]string, 0, len(cs))
		for _, x := range cs {
			out = append(out, x.Name)
		}
		return out
	}
	tests := []struct {
		name      string
		cands     []Candidate
		active    int
		max       int
		wantStart []string
		wantWait  []string
	}{
		{"room for all", []Candidate{c("a", 5), c("b", 3)}, 0, 3, []string{"a", "b"}, []string{}},
		{"oldest fault goes first", []Candidate{c("young", 1), c("old", 9), c("mid", 5)}, 0, 2,
			[]string{"old", "mid"}, []string{"young"}},
		{"cap already full", []Candidate{c("a", 5)}, 2, 2, []string{}, []string{"a"}},
		{"over cap (e.g. cap lowered) starts nothing", []Candidate{c("a", 5)}, 3, 1, []string{}, []string{"a"}},
		{"same age breaks tie by name", []Candidate{c("b", 5), c("a", 5)}, 0, 1, []string{"a"}, []string{"b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, wait := SelectToStart(tt.cands, tt.active, tt.max)
			if got := names(start); !equal(got, tt.wantStart) {
				t.Errorf("start = %v, want %v", got, tt.wantStart)
			}
			if got := names(wait); !equal(got, tt.wantWait) {
				t.Errorf("wait = %v, want %v", got, tt.wantWait)
			}
		})
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
