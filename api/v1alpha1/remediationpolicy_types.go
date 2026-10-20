/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// UnhealthyCondition says which node condition value counts as "unhealthy".
// Examples: {type: Ready, status: "False"}, {type: Ready, status: Unknown},
// {type: GPUHealthy, status: "False"}.
type UnhealthyCondition struct {
	// type is the node condition type, e.g. Ready, DiskPressure, GPUHealthy.
	// +required
	// +kubebuilder:validation:MinLength=1
	Type string `json:"type"`

	// status is the condition status that counts as unhealthy.
	// +required
	// +kubebuilder:validation:Enum=True;False;Unknown
	Status string `json:"status"`
}

// RemediationStrategy is the final, disruptive step after cordon and drain.
// +kubebuilder:validation:Enum=Reboot;Replace
type RemediationStrategy string

const (
	StrategyReboot  RemediationStrategy = "Reboot"
	StrategyReplace RemediationStrategy = "Replace"
)

// RetryPolicy controls exponential backoff between failed attempts.
type RetryPolicy struct {
	// maxAttempts is how many reboot/replace attempts to make before giving up
	// and marking the node Failed (it then stays cordoned for a human).
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	MaxAttempts int32 `json:"maxAttempts,omitempty"`

	// initialBackoff is the wait after the first failure. It doubles after
	// each further failure, up to maxBackoff.
	// +optional
	// +kubebuilder:default="30s"
	InitialBackoff metav1.Duration `json:"initialBackoff,omitempty"`

	// maxBackoff caps the wait between attempts.
	// +optional
	// +kubebuilder:default="10m"
	MaxBackoff metav1.Duration `json:"maxBackoff,omitempty"`
}

// RemediationPolicySpec defines the desired state of RemediationPolicy.
type RemediationPolicySpec struct {
	// nodeSelector picks the nodes this policy manages. An empty selector
	// matches every node.
	// +required
	NodeSelector metav1.LabelSelector `json:"nodeSelector"`

	// unhealthyConditions lists the condition values that make a node
	// unhealthy. Matching any one of them is enough.
	// +required
	// +kubebuilder:validation:MinItems=1
	UnhealthyConditions []UnhealthyCondition `json:"unhealthyConditions"`

	// unhealthyDuration is the hysteresis threshold: a condition must stay
	// unhealthy this long before the node is remediated. Short blips are ignored.
	// +optional
	// +kubebuilder:default="2m"
	UnhealthyDuration metav1.Duration `json:"unhealthyDuration,omitempty"`

	// maxConcurrent is the blast-radius cap: the most matching nodes that may
	// be under remediation (or Failed) at once. An integer ("2") or a percent
	// of matching nodes ("25%"). Percentages round down, but never below 1.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:XIntOrString
	MaxConcurrent intstr.IntOrString `json:"maxConcurrent,omitempty"`

	// strategy is the step after cordon and drain.
	// +optional
	// +kubebuilder:default=Reboot
	Strategy RemediationStrategy `json:"strategy,omitempty"`

	// drainTimeout is how long draining may take (e.g. while a
	// PodDisruptionBudget blocks evictions) before the attempt counts as failed.
	// +optional
	// +kubebuilder:default="5m"
	DrainTimeout metav1.Duration `json:"drainTimeout,omitempty"`

	// rebootTimeout is how long to wait for the node to come back healthy
	// after a reboot/replace request before the attempt counts as failed.
	// +optional
	// +kubebuilder:default="5m"
	RebootTimeout metav1.Duration `json:"rebootTimeout,omitempty"`

	// retry controls backoff between failed attempts.
	// +optional
	// +kubebuilder:default={}
	Retry RetryPolicy `json:"retry,omitempty"`

	// dryRun makes the controller decide and emit events/logs, but never
	// change nodes or evict pods.
	// +optional
	DryRun bool `json:"dryRun,omitempty"`
}

// NodePhase is where a node is in the remediation state machine.
type NodePhase string

const (
	// PhasePending: unhealthy, but not for long enough yet (hysteresis).
	PhasePending NodePhase = "Pending"
	// PhaseWaiting: past the threshold, but the blast-radius cap is full.
	PhaseWaiting NodePhase = "Waiting"
	// PhaseDryRun: dry-run mode; the controller would start remediating.
	PhaseDryRun NodePhase = "WouldRemediate"
	// PhaseDraining: cordoned, pods being evicted.
	PhaseDraining NodePhase = "Draining"
	// PhaseRebooting: reboot/replace requested, waiting for the node to recover.
	PhaseRebooting NodePhase = "Rebooting"
	// PhaseBackoff: the last attempt failed; waiting before retrying.
	PhaseBackoff NodePhase = "Backoff"
	// PhaseFailed: out of attempts. The node stays cordoned for a human.
	PhaseFailed NodePhase = "Failed"
)

// NodeStatus is one non-healthy node as seen by this policy.
type NodeStatus struct {
	// name of the node.
	Name string `json:"name"`
	// phase in the remediation state machine.
	Phase NodePhase `json:"phase"`
	// reason is the unhealthy condition, e.g. "Ready=Unknown".
	// +optional
	Reason string `json:"reason,omitempty"`
	// unhealthySince is when the unhealthy condition started.
	// +optional
	UnhealthySince *metav1.Time `json:"unhealthySince,omitempty"`
	// attempts is the number of reboot/replace attempts started.
	// +optional
	Attempts int32 `json:"attempts,omitempty"`
	// nextRetryTime is set while in Backoff.
	// +optional
	NextRetryTime *metav1.Time `json:"nextRetryTime,omitempty"`
	// message is a human-readable detail.
	// +optional
	Message string `json:"message,omitempty"`
}

// RemediationPolicyStatus defines the observed state of RemediationPolicy.
type RemediationPolicyStatus struct {
	// observedGeneration is the spec generation this status was computed from.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// matchingNodes is how many nodes the selector matches.
	// +optional
	MatchingNodes int32 `json:"matchingNodes"`
	// unhealthyNodes is how many matching nodes have an unhealthy condition.
	// +optional
	UnhealthyNodes int32 `json:"unhealthyNodes"`
	// nodesUnderRemediation counts nodes in Draining, Rebooting, Backoff or
	// Failed: the nodes that use up the blast-radius budget.
	// +optional
	NodesUnderRemediation int32 `json:"nodesUnderRemediation"`
	// maxConcurrent is the blast-radius cap resolved to a node count.
	// +optional
	MaxConcurrent int32 `json:"maxConcurrent"`
	// nodes lists every non-healthy node and where it is in remediation.
	// +optional
	// +listType=map
	// +listMapKey=name
	Nodes []NodeStatus `json:"nodes,omitempty"`
	// conditions: BlastRadiusLimited is True while unhealthy nodes are
	// waiting because the cap is full.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=rp
// +kubebuilder:printcolumn:name="Matching",type=integer,JSONPath=`.status.matchingNodes`
// +kubebuilder:printcolumn:name="Unhealthy",type=integer,JSONPath=`.status.unhealthyNodes`
// +kubebuilder:printcolumn:name="Remediating",type=integer,JSONPath=`.status.nodesUnderRemediation`
// +kubebuilder:printcolumn:name="Max",type=integer,JSONPath=`.status.maxConcurrent`
// +kubebuilder:printcolumn:name="DryRun",type=boolean,JSONPath=`.spec.dryRun`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// RemediationPolicy is the Schema for the remediationpolicies API
type RemediationPolicy struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of RemediationPolicy
	// +required
	Spec RemediationPolicySpec `json:"spec"`

	// status defines the observed state of RemediationPolicy
	// +optional
	Status RemediationPolicyStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// RemediationPolicyList contains a list of RemediationPolicy
type RemediationPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []RemediationPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &RemediationPolicy{}, &RemediationPolicyList{})
		return nil
	})
}
