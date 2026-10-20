package remediation

import (
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/jsuh20/node-health-operator/api/v1alpha1"
)

// The functions in this file are pure: no API calls, no clock reads. That is
// what makes them easy to unit test (see decision_test.go).

// Health is the result of checking a node against a policy's rules.
type Health struct {
	Unhealthy bool
	Reason    string    // e.g. "Ready=Unknown"
	Since     time.Time // when that condition last changed (LastTransitionTime)
}

// EvaluateHealth checks the node's conditions against the policy rules. If
// several rules match, the one that has been unhealthy the longest wins.
//
// We use the condition's LastTransitionTime as the start of the problem. The
// kubelet / node controller sets it, it survives controller restarts, and it
// resets if the condition flips back to healthy (which is what makes
// hysteresis work: a blip restarts the clock).
func EvaluateHealth(node *corev1.Node, rules []v1alpha1.UnhealthyCondition) Health {
	var worst Health
	for _, cond := range node.Status.Conditions {
		for _, rule := range rules {
			if string(cond.Type) != rule.Type || string(cond.Status) != rule.Status {
				continue
			}
			since := cond.LastTransitionTime.Time
			if !worst.Unhealthy || since.Before(worst.Since) {
				worst = Health{
					Unhealthy: true,
					Reason:    fmt.Sprintf("%s=%s", cond.Type, cond.Status),
					Since:     since,
				}
			}
		}
	}
	return worst
}

// ThresholdReached reports whether an unhealthy condition has persisted for
// at least `threshold`. This is the hysteresis check.
func ThresholdReached(h Health, threshold time.Duration, now time.Time) bool {
	return h.Unhealthy && now.Sub(h.Since) >= threshold
}

// MaxConcurrent resolves the blast-radius cap to a node count.
//
// Percentages round DOWN (the safe direction), but the result is never below
// 1 when there is at least one node. Otherwise "10%" of a 5-node pool would be
// 0 and the operator could never fix anything. The tradeoff: in a tiny pool,
// 1 node may be more than the stated percentage.
func MaxConcurrent(v intstr.IntOrString, totalNodes int) (int, error) {
	if totalNodes == 0 {
		return 0, nil
	}
	n, err := intstr.GetScaledValueFromIntOrPercent(&v, totalNodes, false)
	if err != nil {
		return 0, fmt.Errorf("invalid maxConcurrent %q: %w", v.String(), err)
	}
	if n < 1 {
		n = 1
	}
	return n, nil
}

// Backoff returns the wait after `failures` failed attempts:
// initial, 2*initial, 4*initial, ... capped at max.
func Backoff(failures int32, initial, maxWait time.Duration) time.Duration {
	if failures < 1 {
		return 0
	}
	d := initial
	for i := int32(1); i < failures; i++ {
		d *= 2
		if d >= maxWait || d <= 0 { // d <= 0 guards against overflow
			return maxWait
		}
	}
	if d > maxWait {
		return maxWait
	}
	return d
}

// Candidate is an unhealthy node that has passed the hysteresis threshold
// and is not yet being remediated.
type Candidate struct {
	Name   string
	Health Health
}

// SelectToStart decides which candidates may start now, given how many nodes
// are already active (under remediation or Failed) and the cap. Nodes that
// have been sick the longest go first; ties break by name so the result is
// deterministic.
func SelectToStart(candidates []Candidate, active, maxConcurrent int) (start, wait []Candidate) {
	sorted := append([]Candidate(nil), candidates...) // copy; don't reorder the caller's slice
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].Health.Since.Equal(sorted[j].Health.Since) {
			return sorted[i].Health.Since.Before(sorted[j].Health.Since)
		}
		return sorted[i].Name < sorted[j].Name
	})

	free := maxConcurrent - active
	if free < 0 {
		free = 0
	}
	if free > len(sorted) {
		free = len(sorted)
	}
	return sorted[:free], sorted[free:]
}
