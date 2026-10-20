package remediation

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const drainPassTimeout = 30 * time.Second

// Drainer evicts pods from a node using the Eviction API.
//
// Why evictions instead of deletes: the API server checks every eviction
// against the PodDisruptionBudgets that cover the pod. If evicting would
// break a PDB, the server refuses with HTTP 429 and the pod stays. So
// "respect PDBs" is enforced by Kubernetes itself, not by our own counting.
//
// Drain is non-blocking. Each call evicts what it can and reports what is
// left; the controller calls it again on the next reconcile. A reconcile
// never sits waiting for pods to terminate.
type Drainer struct {
	// Client creates evictions.
	Client client.Client
	// Reader lists pods. It should be an uncached reader
	// (mgr.GetAPIReader()) so the operator doesn't keep every pod in the
	// cluster in memory just to drain the occasional node.
	Reader client.Reader
}

// DrainResult summarizes one drain pass.
type DrainResult struct {
	Remaining    int // pods that still have to leave the node
	BlockedByPDB int // of those, how many a PDB refused to let go this pass
	Evicted      int // evictions accepted this pass
}

// Drain makes one eviction pass over the node's pods.
func (d *Drainer) Drain(ctx context.Context, node *corev1.Node) (DrainResult, error) {
	var res DrainResult

	// Bound the whole pass. One stuck API call must not stall the
	// controller's only worker (and with it every other policy).
	ctx, cancel := context.WithTimeout(ctx, drainPassTimeout)
	defer cancel()

	var pods corev1.PodList
	if err := d.Reader.List(ctx, &pods, client.MatchingFields{"spec.nodeName": node.Name}); err != nil {
		return res, fmt.Errorf("list pods on %s: %w", node.Name, err)
	}

	reachable := nodeReady(node)
	for i := range pods.Items {
		pod := &pods.Items[i]

		if skipForDrain(pod) {
			continue
		}
		if pod.DeletionTimestamp != nil {
			// Already terminating. If the kubelet is unreachable it can never
			// confirm the pod has stopped, so waiting would hang forever (the
			// same problem `kubectl drain` has). Treat it as gone: its
			// workload controller has already created a replacement elsewhere.
			if reachable {
				res.Remaining++
			}
			continue
		}

		eviction := &policyv1.Eviction{
			ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
		}
		err := d.Client.SubResource("eviction").Create(ctx, pod, eviction)
		switch {
		case err == nil:
			res.Evicted++
			res.Remaining++ // still on the node until it finishes terminating
		case apierrors.IsTooManyRequests(err):
			res.BlockedByPDB++
			res.Remaining++
		case apierrors.IsNotFound(err):
			// Gone already; nothing to do.
		default:
			return res, fmt.Errorf("evict %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return res, nil
}

// skipForDrain mirrors `kubectl drain`: leave DaemonSet pods (their
// controller would put them straight back), static/mirror pods (managed by
// the kubelet, not the API) and pods that have already finished.
func skipForDrain(pod *corev1.Pod) bool {
	if _, ok := pod.Annotations[corev1.MirrorPodAnnotationKey]; ok {
		return true
	}
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return true
	}
	for _, ref := range pod.OwnerReferences {
		if ref.Controller != nil && *ref.Controller && ref.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

func nodeReady(node *corev1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
