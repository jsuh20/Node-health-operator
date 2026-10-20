// Package metrics defines the operator's Prometheus metrics. They are
// registered with controller-runtime's registry, so they are served on the
// manager's existing /metrics endpoint next to the built-in controller metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// labelPolicy is on every metric: the RemediationPolicy name.
const labelPolicy = "policy"

// Result label values for RemediationsTotal.
const (
	ResultStarted   = "started"
	ResultSucceeded = "succeeded"
	ResultFailed    = "failed" // gave up after maxAttempts
)

var (
	// DetectionSeconds: fault start (condition LastTransitionTime) until the
	// controller first sees it. Measures watch/reaction latency.
	DetectionSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nodehealth_detection_seconds",
		Help:    "Time from a node condition turning unhealthy to the controller observing it.",
		Buckets: []float64{0.5, 1, 2, 5, 10, 20, 30, 60, 120},
	}, []string{labelPolicy})

	// TimeToRemediateSeconds: fault start until the node is healthy and back
	// in service. End to end, so it includes the hysteresis threshold.
	TimeToRemediateSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nodehealth_time_to_remediate_seconds",
		Help:    "Time from a node condition turning unhealthy to the node being healthy and uncordoned.",
		Buckets: prometheus.ExponentialBuckets(15, 2, 10), // 15s .. ~2h
	}, []string{labelPolicy})

	RemediationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nodehealth_remediations_total",
		Help: "Remediations by result (started, succeeded, failed).",
	}, []string{labelPolicy, "result"})

	AttemptFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nodehealth_remediation_attempt_failures_total",
		Help: "Failed remediation attempts (each one triggers a backoff or a final Failed).",
	}, []string{labelPolicy, "reason"})

	NodesUnderRemediation = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nodehealth_nodes_under_remediation",
		Help: "Nodes using the blast-radius budget (Draining, Rebooting, Backoff, Failed).",
	}, []string{labelPolicy})

	BlastRadiusLimit = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nodehealth_blast_radius_limit",
		Help: "maxConcurrent resolved to a node count.",
	}, []string{labelPolicy})

	UnhealthyNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nodehealth_unhealthy_nodes",
		Help: "Matching nodes with an unhealthy condition (any phase, including Pending).",
	}, []string{labelPolicy})
)

func init() {
	metrics.Registry.MustRegister(
		DetectionSeconds, TimeToRemediateSeconds, RemediationsTotal,
		AttemptFailuresTotal, NodesUnderRemediation, BlastRadiusLimit, UnhealthyNodes,
	)
}

// ForgetPolicy drops a deleted policy's gauges so stale values don't linger.
func ForgetPolicy(policy string) {
	NodesUnderRemediation.DeleteLabelValues(policy)
	BlastRadiusLimit.DeleteLabelValues(policy)
	UnhealthyNodes.DeleteLabelValues(policy)
}
