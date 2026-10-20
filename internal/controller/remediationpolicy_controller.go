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

package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/recorder"

	"github.com/jsuh20/node-health-operator/api/v1alpha1"
	"github.com/jsuh20/node-health-operator/internal/metrics"
	"github.com/jsuh20/node-health-operator/internal/remediation"
)

const (
	finalizerName = "remediation.nodehealth.dev/cleanup"

	// drainPollInterval: we don't watch pods, so while draining we check
	// back on a timer.
	drainPollInterval = 5 * time.Second

	conditionBlastRadiusLimited = "BlastRadiusLimited"
)

// RemediationPolicyReconciler drives matching nodes toward healthy, one
// small, idempotent step per reconcile.
//
// The reconcile key is a POLICY, not a node. The blast-radius cap is a
// property of the whole set of nodes a policy matches, so the decision
// "may I start another remediation?" needs to see all of them at once.
type RemediationPolicyReconciler struct {
	client.Client
	// APIReader reads straight from the API server, skipping the cache. Used
	// for pod listing and for the cap re-check before starting work.
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Recorder  recorder.EventRecorder
	Clock     clock.PassiveClock
	Drainer   *remediation.Drainer

	// detected remembers which (policy, node, fault) we have already reported,
	// so the "Detected" event and the detection metric fire once per fault.
	// It is only for observability: losing it on restart just means one
	// duplicate event, never a wrong action.
	detectedMu sync.Mutex
	detected   map[string]string
}

// +kubebuilder:rbac:groups=remediation.nodehealth.dev,resources=remediationpolicies,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=remediation.nodehealth.dev,resources=remediationpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=remediation.nodehealth.dev,resources=remediationpolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile runs one observe -> decide -> act -> record pass for a policy.
func (r *RemediationPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("policy", req.Name)
	ctx = logf.IntoContext(ctx, log)

	var policy v1alpha1.RemediationPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		if apierrors.IsNotFound(err) {
			metrics.ForgetPolicy(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !policy.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, &policy)
	}
	if controllerutil.AddFinalizer(&policy, finalizerName) {
		// The finalizer makes Kubernetes wait for our cleanup (releasing the
		// nodes we own) before the policy is really deleted.
		if err := r.Update(ctx, &policy); err != nil {
			return ctrl.Result{}, err
		}
	}

	// ---- Observe ----------------------------------------------------------
	selector, err := metav1.LabelSelectorAsSelector(&policy.Spec.NodeSelector)
	if err != nil {
		r.Recorder.Eventf(&policy, nil, corev1.EventTypeWarning, "InvalidSelector", "Validate", "%v", err)
		return ctrl.Result{}, nil // retrying won't fix a bad spec; wait for an edit
	}
	var nodes corev1.NodeList
	if err := r.List(ctx, &nodes); err != nil {
		return ctrl.Result{}, err
	}

	now := r.Clock.Now()
	total := 0
	for i := range nodes.Items {
		if selector.Matches(labels.Set(nodes.Items[i].Labels)) {
			total++
		}
	}
	maxConcurrent, err := remediation.MaxConcurrent(policy.Spec.MaxConcurrent, total)
	if err != nil {
		r.Recorder.Eventf(&policy, nil, corev1.EventTypeWarning, "InvalidSpec", "Validate", "%v", err)
		return ctrl.Result{}, nil
	}

	// ---- Decide + act, node by node --------------------------------------
	var (
		statuses   []v1alpha1.NodeStatus
		candidates []remediation.Candidate
		unhealthy  int
		active     int
		requeue    = newRequeue()
	)
	for i := range nodes.Items {
		node := &nodes.Items[i]
		nlog := log.WithValues("node", node.Name)
		nctx := logf.IntoContext(ctx, nlog)

		state, err := remediation.GetState(node)
		if err != nil {
			nlog.Error(err, "Ignoring node with unreadable state annotation")
			continue
		}
		matches := selector.Matches(labels.Set(node.Labels))
		ours := state != nil && state.Policy == policy.Name
		if !matches && !ours {
			continue
		}
		if state != nil && !ours {
			continue // another policy owns this node; let it finish
		}

		health := remediation.EvaluateHealth(node, policy.Spec.UnhealthyConditions)
		if health.Unhealthy {
			unhealthy++
			r.noteDetected(nctx, &policy, node, health, now)
		} else {
			r.forgetDetected(policy.Name, node.Name)
		}

		if ours {
			st, after, stillActive, err := r.advance(nctx, &policy, node, state, health, now)
			if err != nil {
				return ctrl.Result{}, err
			}
			statuses = append(statuses, st)
			requeue.at(after)
			if stillActive {
				active++
			}
			continue
		}

		if !health.Unhealthy {
			continue
		}
		threshold := policy.Spec.UnhealthyDuration.Duration
		if !remediation.ThresholdReached(health, threshold, now) {
			statuses = append(statuses, nodeStatus(node.Name, v1alpha1.PhasePending, health,
				fmt.Sprintf("waiting for the %s threshold", threshold)))
			requeue.at(health.Since.Add(threshold).Sub(now))
			continue
		}
		candidates = append(candidates, remediation.Candidate{Name: node.Name, Health: health})
	}

	// ---- Blast-radius cap -------------------------------------------------
	if len(candidates) > 0 && !policy.Spec.DryRun {
		// The cache can lag behind our own writes by a moment. If we trusted it
		// right after starting a remediation, we could count one too few
		// active nodes and break the cap. So recount straight from the API
		// server before handing out any new slots.
		live, err := r.countActiveLive(ctx, policy.Name)
		if err != nil {
			return ctrl.Result{}, err
		}
		if live > active {
			active = live
		}
	}
	start, waiting := remediation.SelectToStart(candidates, active, maxConcurrent)

	for _, c := range start {
		node := nodeByName(nodes.Items, c.Name)
		if policy.Spec.DryRun {
			if prevStatus(&policy, c.Name).Phase != v1alpha1.PhaseDryRun {
				r.Recorder.Eventf(node, &policy, corev1.EventTypeNormal, "DryRun", "Cordon",
					"dry-run: would cordon, drain and %s node (%s for %s)",
					policy.Spec.Strategy, c.Health.Reason, now.Sub(c.Health.Since).Round(time.Second))
				log.Info("Dry-run: would start remediation", "node", c.Name, "reason", c.Health.Reason)
			}
			statuses = append(statuses, nodeStatus(c.Name, v1alpha1.PhaseDryRun, c.Health, "dry-run: no action taken"))
			continue
		}
		st, err := r.start(logf.IntoContext(ctx, log.WithValues("node", c.Name)), &policy, node, c.Health, now)
		if err != nil {
			return ctrl.Result{}, err
		}
		statuses = append(statuses, st)
		active++
		requeue.at(time.Second)
	}
	for _, c := range waiting {
		node := nodeByName(nodes.Items, c.Name)
		// Events only on state CHANGES: this loop runs every few seconds while
		// other nodes drain, and one event per pass would be spam.
		if prevStatus(&policy, c.Name).Phase != v1alpha1.PhaseWaiting {
			log.Info("Blast-radius cap reached; node must wait", "node", c.Name, "active", active, "max", maxConcurrent)
			r.Recorder.Eventf(node, &policy, corev1.EventTypeNormal, "BlastRadiusLimited", "Wait",
				"waiting: %d/%d nodes already under remediation", active, maxConcurrent)
		}
		statuses = append(statuses, nodeStatus(c.Name, v1alpha1.PhaseWaiting, c.Health,
			fmt.Sprintf("blast-radius cap reached (%d/%d)", active, maxConcurrent)))
	}

	// ---- Record -----------------------------------------------------------
	metrics.NodesUnderRemediation.WithLabelValues(policy.Name).Set(float64(active))
	metrics.BlastRadiusLimit.WithLabelValues(policy.Name).Set(float64(maxConcurrent))
	metrics.UnhealthyNodes.WithLabelValues(policy.Name).Set(float64(unhealthy))

	if err := r.updateStatus(ctx, &policy, total, unhealthy, active, maxConcurrent, statuses, len(waiting)); err != nil {
		return ctrl.Result{}, err
	}
	return requeue.result(), nil
}

// start claims a node and cordons it in ONE write: the state annotation and
// spec.unschedulable change together, so there is no moment where the node is
// cordoned but we have no record of why.
func (r *RemediationPolicyReconciler) start(ctx context.Context, policy *v1alpha1.RemediationPolicy,
	node *corev1.Node, health remediation.Health, now time.Time) (v1alpha1.NodeStatus, error) {
	state := &remediation.State{
		Policy:         policy.Name,
		Phase:          v1alpha1.PhaseDraining,
		Reason:         health.Reason,
		UnhealthySince: health.Since,
		StartedAt:      now,
		PhaseStartedAt: now,
		Attempts:       1,
		CordonedByUs:   !node.Spec.Unschedulable,
	}
	if err := r.patchNode(ctx, node, func(n *corev1.Node) error {
		n.Spec.Unschedulable = true
		return remediation.SetState(n, state)
	}); err != nil {
		return v1alpha1.NodeStatus{}, err
	}
	logf.FromContext(ctx).Info("Started remediation: cordoned node", "reason", health.Reason)
	r.Recorder.Eventf(node, policy, corev1.EventTypeNormal, "Cordoned", "Cordon",
		"%s for %s; cordoned, draining next", health.Reason, now.Sub(health.Since).Round(time.Second))
	metrics.RemediationsTotal.WithLabelValues(policy.Name, metrics.ResultStarted).Inc()
	return stateStatus(node.Name, state, "cordoned"), nil
}

// advance moves one owned node a single step through the state machine:
//
//	Draining -> Rebooting -> (healthy) done, uncordoned
//	    \           \
//	     +-----------+--> Backoff -> Draining (retry)   or   -> Failed
//
// Every step re-reads the world and writes the next phase to the node, so it
// is safe to run twice: after a crash it just repeats the current step.
//
// Returns the node's status, when to look again, and whether the node still
// counts against the blast-radius cap.
func (r *RemediationPolicyReconciler) advance(ctx context.Context, policy *v1alpha1.RemediationPolicy,
	node *corev1.Node, state *remediation.State, health remediation.Health, now time.Time,
) (v1alpha1.NodeStatus, time.Duration, bool, error) {
	log := logf.FromContext(ctx)
	spec := policy.Spec

	if spec.DryRun {
		// Dry-run never changes nodes, including ones that were mid-remediation
		// when dry-run was switched on. They stay paused where they are.
		return stateStatus(node.Name, state, "paused: policy is in dry-run"), 0, true, nil
	}

	switch state.Phase {
	case v1alpha1.PhaseDraining:
		res, err := r.Drainer.Drain(ctx, node)
		if err != nil {
			return v1alpha1.NodeStatus{}, 0, true, err
		}
		if res.Remaining == 0 {
			state.Phase = v1alpha1.PhaseRebooting
			state.PhaseStartedAt = now
			state.RequestID = remediation.NewRequestID(spec.Strategy, state.Attempts, now)
			if err := r.patchNode(ctx, node, func(n *corev1.Node) error {
				delete(n.Annotations, remediation.CompletedAnnotation)
				if err := remediation.SetState(n, state); err != nil {
					return err
				}
				n.Annotations[remediation.RequestAnnotation] = state.RequestID
				return nil
			}); err != nil {
				return v1alpha1.NodeStatus{}, 0, true, err
			}
			log.Info("Drained; requested "+string(spec.Strategy), "request", state.RequestID, "attempt", state.Attempts)
			r.Recorder.Eventf(node, policy, corev1.EventTypeNormal, string(spec.Strategy)+"Requested", string(spec.Strategy),
				"node drained; %s requested (attempt %d/%d)", spec.Strategy, state.Attempts, spec.Retry.MaxAttempts)
			return stateStatus(node.Name, state, "waiting for node to come back"), spec.RebootTimeout.Duration, true, nil
		}
		if now.Sub(state.PhaseStartedAt) > spec.DrainTimeout.Duration {
			return r.fail(ctx, policy, node, state, now, "drain_timeout",
				fmt.Sprintf("drain timed out with %d pods left (%d blocked by PDB)", res.Remaining, res.BlockedByPDB))
		}
		msg := fmt.Sprintf("%d pods left", res.Remaining)
		if res.BlockedByPDB > 0 {
			msg += fmt.Sprintf(", %d blocked by PodDisruptionBudget", res.BlockedByPDB)
			hint := ""
			if !remediation.NodeReady(node) {
				// The node is down, so its pods are already NotReady. A PDB with the
				// default unhealthyPodEvictionPolicy (IfHealthyBudget) then refuses to
				// evict even those dead pods. Say so; we still won't bypass the PDB.
				hint = "; node is not Ready, so these pods are likely already down. " +
					"Consider unhealthyPodEvictionPolicy: AlwaysAllow on the PDB"
				msg += " (node not Ready)"
			}
			if !strings.Contains(prevStatus(policy, node.Name).Message, "PodDisruptionBudget") {
				log.Info("Drain blocked by PodDisruptionBudget", "blocked", res.BlockedByPDB, "nodeReady", hint == "")
				r.Recorder.Eventf(node, policy, corev1.EventTypeNormal, "DrainBlockedByPDB", "Drain",
					"%d evictions refused by PodDisruptionBudgets; retrying%s", res.BlockedByPDB, hint)
			}
		}
		return stateStatus(node.Name, state, msg), drainPollInterval, true, nil

	case v1alpha1.PhaseRebooting:
		completed := node.Annotations[remediation.CompletedAnnotation] == state.RequestID
		if completed && !health.Unhealthy {
			return r.recover(ctx, policy, node, state, now)
		}
		deadline := state.PhaseStartedAt.Add(spec.RebootTimeout.Duration)
		if now.After(deadline) {
			reason := "node did not become healthy after " + string(spec.Strategy)
			if !completed {
				reason = string(spec.Strategy) + " was not carried out in time"
			}
			return r.fail(ctx, policy, node, state, now, "reboot_timeout", reason)
		}
		msg := "waiting for " + string(spec.Strategy)
		if completed {
			msg = string(spec.Strategy) + " done; waiting for node to report healthy"
		}
		// Node updates wake us up early; the timer is only for the timeout.
		return stateStatus(node.Name, state, msg), deadline.Sub(now), true, nil

	case v1alpha1.PhaseBackoff:
		if now.Before(state.NextRetryAt) {
			return stateStatus(node.Name, state, "backing off"), state.NextRetryAt.Sub(now), true, nil
		}
		state.Attempts++
		state.Phase = v1alpha1.PhaseDraining
		state.PhaseStartedAt = now
		state.NextRetryAt = time.Time{}
		if err := r.patchNode(ctx, node, func(n *corev1.Node) error {
			delete(n.Annotations, remediation.RequestAnnotation)
			return remediation.SetState(n, state)
		}); err != nil {
			return v1alpha1.NodeStatus{}, 0, true, err
		}
		log.Info("Backoff over; retrying", "attempt", state.Attempts)
		r.Recorder.Eventf(node, policy, corev1.EventTypeNormal, "Retrying", "Drain",
			"starting attempt %d/%d", state.Attempts, spec.Retry.MaxAttempts)
		return stateStatus(node.Name, state, "retrying"), time.Second, true, nil

	case v1alpha1.PhaseFailed:
		// Terminal. A human investigates, then removes the state annotation
		// (and uncordons). The node keeps using a cap slot until then: if
		// remediation keeps failing, that is a signal to stop, not speed up.
		return stateStatus(node.Name, state, "out of attempts; needs manual attention"), 0, true, nil

	default:
		log.Error(nil, "Unknown phase in state annotation; treating as Failed", "phase", state.Phase)
		return stateStatus(node.Name, state, "unknown phase "+string(state.Phase)), 0, true, nil
	}
}

// fail records a failed attempt and either schedules a retry with
// exponential backoff or gives up.
func (r *RemediationPolicyReconciler) fail(ctx context.Context, policy *v1alpha1.RemediationPolicy,
	node *corev1.Node, state *remediation.State, now time.Time, reason, msg string,
) (v1alpha1.NodeStatus, time.Duration, bool, error) {
	spec := policy.Spec
	metrics.AttemptFailuresTotal.WithLabelValues(policy.Name, reason).Inc()

	var after time.Duration
	if state.Attempts >= spec.Retry.MaxAttempts {
		state.Phase = v1alpha1.PhaseFailed
		metrics.RemediationsTotal.WithLabelValues(policy.Name, metrics.ResultFailed).Inc()
		r.Recorder.Eventf(node, policy, corev1.EventTypeWarning, "RemediationFailed", "GiveUp",
			"%s; giving up after %d attempts, node left cordoned", msg, state.Attempts)
	} else {
		wait := remediation.Backoff(state.Attempts, spec.Retry.InitialBackoff.Duration, spec.Retry.MaxBackoff.Duration)
		state.Phase = v1alpha1.PhaseBackoff
		state.NextRetryAt = now.Add(wait)
		after = wait
		r.Recorder.Eventf(node, policy, corev1.EventTypeWarning, "AttemptFailed", "Backoff",
			"%s (attempt %d/%d); retrying in %s", msg, state.Attempts, spec.Retry.MaxAttempts, wait)
	}
	state.PhaseStartedAt = now
	if err := r.patchNode(ctx, node, func(n *corev1.Node) error {
		return remediation.SetState(n, state)
	}); err != nil {
		return v1alpha1.NodeStatus{}, 0, true, err
	}
	logf.FromContext(ctx).Info("Remediation attempt failed", "reason", msg, "attempt", state.Attempts, "nextPhase", state.Phase)
	return stateStatus(node.Name, state, msg), after, true, nil
}

// recover finishes a successful remediation: uncordon (only if we cordoned)
// and drop our annotations, again in one write.
func (r *RemediationPolicyReconciler) recover(ctx context.Context, policy *v1alpha1.RemediationPolicy,
	node *corev1.Node, state *remediation.State, now time.Time,
) (v1alpha1.NodeStatus, time.Duration, bool, error) {
	if err := r.patchNode(ctx, node, func(n *corev1.Node) error {
		if state.CordonedByUs {
			n.Spec.Unschedulable = false
		}
		remediation.ClearState(n)
		return nil
	}); err != nil {
		return v1alpha1.NodeStatus{}, 0, true, err
	}
	took := now.Sub(state.UnhealthySince)
	metrics.TimeToRemediateSeconds.WithLabelValues(policy.Name).Observe(took.Seconds())
	metrics.RemediationsTotal.WithLabelValues(policy.Name, metrics.ResultSucceeded).Inc()
	logf.FromContext(ctx).Info("Remediation succeeded", "timeToRemediate", took.Round(time.Second), "attempts", state.Attempts)
	r.Recorder.Eventf(node, policy, corev1.EventTypeNormal, "Remediated", "Uncordon",
		"node healthy after %s (attempt %d); back in service, %s after the fault began",
		policy.Spec.Strategy, state.Attempts, took.Round(time.Second))
	return v1alpha1.NodeStatus{}, 0, false, nil // healthy nodes don't appear in status
}

// finalize runs when the policy is being deleted. It releases every node the
// policy owns. A node we cordoned is uncordoned only if it is healthy now;
// a still-broken node stays cordoned (safer than putting work on it).
func (r *RemediationPolicyReconciler) finalize(ctx context.Context, policy *v1alpha1.RemediationPolicy) error {
	if !controllerutil.ContainsFinalizer(policy, finalizerName) {
		return nil
	}
	var nodes corev1.NodeList
	if err := r.List(ctx, &nodes); err != nil {
		return err
	}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		state, err := remediation.GetState(node)
		if err != nil || state == nil || state.Policy != policy.Name {
			continue
		}
		healthy := !remediation.EvaluateHealth(node, policy.Spec.UnhealthyConditions).Unhealthy
		if err := r.patchNode(ctx, node, func(n *corev1.Node) error {
			if state.CordonedByUs && healthy {
				n.Spec.Unschedulable = false
			}
			remediation.ClearState(n)
			return nil
		}); err != nil {
			return err
		}
		if !healthy {
			r.Recorder.Eventf(node, policy, corev1.EventTypeWarning, "Released", "Release",
				"policy deleted mid-remediation; node is still unhealthy and stays cordoned")
		}
	}
	metrics.ForgetPolicy(policy.Name)
	controllerutil.RemoveFinalizer(policy, finalizerName)
	return r.Update(ctx, policy)
}

// patchNode applies `mutate` to a copy of the node and sends the difference
// as a merge patch WITH an optimistic lock. The patch carries the
// resourceVersion we read; if anyone changed the node since (another
// controller instance, a second policy, a stale cache) the API server
// rejects it with a Conflict and we simply reconcile again on fresh data.
func (r *RemediationPolicyReconciler) patchNode(ctx context.Context, node *corev1.Node, mutate func(*corev1.Node) error) error {
	before := node.DeepCopy()
	if err := mutate(node); err != nil {
		return err
	}
	return r.Patch(ctx, node, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

// countActiveLive recounts, from the API server, the nodes this policy is
// currently remediating.
func (r *RemediationPolicyReconciler) countActiveLive(ctx context.Context, policyName string) (int, error) {
	var nodes corev1.NodeList
	if err := r.APIReader.List(ctx, &nodes); err != nil {
		return 0, err
	}
	n := 0
	for i := range nodes.Items {
		if s, err := remediation.GetState(&nodes.Items[i]); err == nil && s != nil && s.Policy == policyName {
			n++
		}
	}
	return n, nil
}

func (r *RemediationPolicyReconciler) updateStatus(ctx context.Context, policy *v1alpha1.RemediationPolicy,
	total, unhealthy, active, maxConcurrent int, nodes []v1alpha1.NodeStatus, waiting int) error {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })

	updated := policy.DeepCopy()
	s := &updated.Status
	s.ObservedGeneration = policy.Generation
	s.MatchingNodes = int32(total)
	s.UnhealthyNodes = int32(unhealthy)
	s.NodesUnderRemediation = int32(active)
	s.MaxConcurrent = int32(maxConcurrent)
	s.Nodes = nodes

	cond := metav1.Condition{
		Type:               conditionBlastRadiusLimited,
		Status:             metav1.ConditionFalse,
		Reason:             "WithinCap",
		Message:            fmt.Sprintf("%d/%d nodes under remediation", active, maxConcurrent),
		ObservedGeneration: policy.Generation,
	}
	if waiting > 0 {
		cond.Status = metav1.ConditionTrue
		cond.Reason = "CapReached"
		cond.Message = fmt.Sprintf("%d nodes waiting; %d/%d under remediation", waiting, active, maxConcurrent)
	}
	meta.SetStatusCondition(&s.Conditions, cond)

	// Skip no-op writes. Every status write is an API call and an update
	// event; writing unconditionally would be a slow hot loop.
	if equality.Semantic.DeepEqual(policy.Status, updated.Status) {
		return nil
	}
	// A merge patch WITHOUT an optimistic lock: this controller is the only
	// writer of status and always sends the full picture, so a slightly stale
	// cached copy of the policy is no reason to fail with a Conflict.
	return r.Status().Patch(ctx, updated, client.MergeFrom(policy))
}

// noteDetected emits the Detected event and records detection latency the
// first time we see a given fault on a given node.
func (r *RemediationPolicyReconciler) noteDetected(ctx context.Context, policy *v1alpha1.RemediationPolicy,
	node *corev1.Node, h remediation.Health, now time.Time) {
	key := policy.Name + "/" + node.Name
	fault := fmt.Sprintf("%s@%d", h.Reason, h.Since.Unix())

	// A MUTEX makes this map safe if reconciles ever run concurrently.
	// `defer Unlock()` would hold the lock through the event call too, so we
	// unlock by hand as soon as the map work is done.
	r.detectedMu.Lock()
	if r.detected == nil {
		r.detected = map[string]string{}
	}
	seen := r.detected[key] == fault
	r.detected[key] = fault
	r.detectedMu.Unlock()
	if seen {
		return
	}

	latency := now.Sub(h.Since)
	if latency < 0 {
		latency = 0 // clock skew between nodes and controller
	}
	metrics.DetectionSeconds.WithLabelValues(policy.Name).Observe(latency.Seconds())
	logf.FromContext(ctx).Info("Detected unhealthy node", "reason", h.Reason, "since", h.Since, "detectionLatency", latency.Round(time.Millisecond))
	r.Recorder.Eventf(node, policy, corev1.EventTypeWarning, "Detected", "Observe",
		"%s (since %s); remediation after %s", h.Reason, h.Since.Format(time.RFC3339), policy.Spec.UnhealthyDuration.Duration)
}

func (r *RemediationPolicyReconciler) forgetDetected(policy, node string) {
	r.detectedMu.Lock()
	delete(r.detected, policy+"/"+node)
	r.detectedMu.Unlock()
}

// SetupWithManager wires up what triggers a reconcile:
//   - the policy itself, but only when its spec changes (generation), so our
//     own status writes don't re-trigger us;
//   - any node change that matters, mapped to the policies that care about it.
func (r *RemediationPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.RemediationPolicy{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.policiesForNode),
			builder.WithPredicates(nodeChangedPredicate())).
		Named("remediationpolicy").
		Complete(r)
}

// policiesForNode maps a node event to the policies that select or own it.
func (r *RemediationPolicyReconciler) policiesForNode(ctx context.Context, obj client.Object) []reconcile.Request {
	node, ok := obj.(*corev1.Node) // TYPE ASSERTION: get the concrete type back out of the interface
	if !ok {
		return nil
	}
	var policies v1alpha1.RemediationPolicyList
	if err := r.List(ctx, &policies); err != nil {
		logf.FromContext(ctx).Error(err, "Listing policies for node event")
		return nil
	}
	state, _ := remediation.GetState(node)
	var reqs []reconcile.Request
	for _, p := range policies.Items {
		sel, err := metav1.LabelSelectorAsSelector(&p.Spec.NodeSelector)
		owns := state != nil && state.Policy == p.Name
		if owns || (err == nil && sel.Matches(labels.Set(node.Labels))) {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Name: p.Name}})
		}
	}
	return reqs
}

// nodeChangedPredicate filters out the constant stream of node heartbeat
// updates. We only care about labels, annotations, cordon state, and
// condition type/status/transition-time.
func nodeChangedPredicate() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldN, ok1 := e.ObjectOld.(*corev1.Node)
			newN, ok2 := e.ObjectNew.(*corev1.Node)
			if !ok1 || !ok2 {
				return true
			}
			return !equality.Semantic.DeepEqual(oldN.Labels, newN.Labels) ||
				!equality.Semantic.DeepEqual(oldN.Annotations, newN.Annotations) ||
				oldN.Spec.Unschedulable != newN.Spec.Unschedulable ||
				conditionsChanged(oldN.Status.Conditions, newN.Status.Conditions)
		},
	}
}

func conditionsChanged(a, b []corev1.NodeCondition) bool {
	if len(a) != len(b) {
		return true
	}
	type key struct {
		status string
		ltt    int64
	}
	seen := make(map[corev1.NodeConditionType]key, len(a))
	for _, c := range a {
		seen[c.Type] = key{string(c.Status), c.LastTransitionTime.Unix()}
	}
	for _, c := range b {
		if seen[c.Type] != (key{string(c.Status), c.LastTransitionTime.Unix()}) {
			return true
		}
	}
	return false
}

// requeue tracks the soonest time any node needs another look.
type requeue struct{ d time.Duration }

func newRequeue() *requeue { return &requeue{} }

func (q *requeue) at(d time.Duration) {
	if d <= 0 {
		return
	}
	if q.d == 0 || d < q.d {
		q.d = d
	}
}

func (q *requeue) result() ctrl.Result {
	if q.d == 0 {
		return ctrl.Result{}
	}
	// Small floor so a near-zero timer can't turn into a hot loop.
	return ctrl.Result{RequeueAfter: max(q.d, 500*time.Millisecond)}
}

// prevStatus returns what the last recorded status said about a node.
func prevStatus(policy *v1alpha1.RemediationPolicy, name string) v1alpha1.NodeStatus {
	for _, n := range policy.Status.Nodes {
		if n.Name == name {
			return n
		}
	}
	return v1alpha1.NodeStatus{}
}

func nodeByName(nodes []corev1.Node, name string) *corev1.Node {
	for i := range nodes {
		if nodes[i].Name == name {
			return &nodes[i]
		}
	}
	return nil
}

func nodeStatus(name string, phase v1alpha1.NodePhase, h remediation.Health, msg string) v1alpha1.NodeStatus {
	since := metav1.NewTime(h.Since)
	return v1alpha1.NodeStatus{Name: name, Phase: phase, Reason: h.Reason, UnhealthySince: &since, Message: msg}
}

func stateStatus(name string, s *remediation.State, msg string) v1alpha1.NodeStatus {
	since := metav1.NewTime(s.UnhealthySince)
	ns := v1alpha1.NodeStatus{
		Name: name, Phase: s.Phase, Reason: s.Reason, UnhealthySince: &since,
		Attempts: s.Attempts, Message: msg,
	}
	if !s.NextRetryAt.IsZero() {
		t := metav1.NewTime(s.NextRetryAt)
		ns.NextRetryTime = &t
	}
	return ns
}
