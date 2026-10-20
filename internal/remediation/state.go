// Package remediation holds the operator's decision logic and node actions.
//
// Design note: the remediation state for a node lives in an annotation ON THE
// NODE, not in controller memory and not only in the policy status. That
// makes the controller crash-safe: after a restart (or a leader change) the
// new process reads the annotation and carries on from the recorded phase.
// Because the state sits on the same object we cordon, "claim the node + mark
// it unschedulable" is a single atomic API write.
package remediation

import (
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/jsuh20/node-health-operator/api/v1alpha1"
)

const (
	annotationPrefix = "remediation.nodehealth.dev/"

	// StateAnnotation holds the JSON-encoded State of a node under remediation.
	StateAnnotation = annotationPrefix + "state"

	// RequestAnnotation is how the controller asks the infrastructure layer
	// to reboot/replace a node. Value: the State.RequestID.
	RequestAnnotation = annotationPrefix + "request"

	// CompletedAnnotation is written by the infrastructure layer (in this
	// project, the fake-infra simulator) once it has carried out RequestID.
	CompletedAnnotation = annotationPrefix + "completed"
)

// State is the per-node record of an in-progress remediation.
type State struct {
	Policy         string             `json:"policy"`
	Phase          v1alpha1.NodePhase `json:"phase"`
	Reason         string             `json:"reason"`
	UnhealthySince time.Time          `json:"unhealthySince"`
	StartedAt      time.Time          `json:"startedAt"`
	PhaseStartedAt time.Time          `json:"phaseStartedAt"`
	Attempts       int32              `json:"attempts"`
	NextRetryAt    time.Time          `json:"nextRetryAt,omitzero"`
	RequestID      string             `json:"requestID,omitempty"`
	// CordonedByUs is false if an admin had already cordoned the node. We
	// only uncordon nodes we cordoned ourselves.
	CordonedByUs bool `json:"cordonedByUs"`
}

// GetState reads the state annotation. It returns (nil, nil) when the node
// is not under remediation.
func GetState(node *corev1.Node) (*State, error) {
	raw, ok := node.Annotations[StateAnnotation]
	if !ok {
		return nil, nil
	}
	var s State
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return nil, fmt.Errorf("decode %s on node %s: %w", StateAnnotation, node.Name, err)
	}
	return &s, nil
}

// SetState writes the state annotation onto the in-memory node object. The
// caller still has to send the change to the API server.
func SetState(node *corev1.Node, s *State) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	node.Annotations[StateAnnotation] = string(raw)
	return nil
}

// ClearState removes every annotation this operator owns.
func ClearState(node *corev1.Node) {
	delete(node.Annotations, StateAnnotation)
	delete(node.Annotations, RequestAnnotation)
	delete(node.Annotations, CompletedAnnotation)
}

// NewRequestID builds a unique ID for one reboot/replace request, e.g.
// "Reboot/2/1729440000". The infra layer echoes it back in
// CompletedAnnotation, so a stale completion from an earlier attempt can
// never be mistaken for the current one.
func NewRequestID(strategy v1alpha1.RemediationStrategy, attempt int32, now time.Time) string {
	return fmt.Sprintf("%s/%d/%d", strategy, attempt, now.Unix())
}
