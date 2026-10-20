// fake-infra simulates the infrastructure around the operator on a kind
// cluster, so we can test without real GPUs or real machines. It plays two
// roles:
//
//  1. Health exporter. Keeps a GPUHealthy condition on every node and exposes
//     an HTTP API to inject faults:
//     - gpu:      GPUHealthy=False (like an Xid error reported by a GPU agent)
//     - notready: `docker stop` the kind node container, so the kubelet goes
//     silent and the node turns NotReady/Unknown for real
//
//  2. Reboot/replace provider (a stand-in for a cloud API or BMC). When the
//     operator writes remediation.nodehealth.dev/request on a node, this
//     `docker restart`s the node container, waits for the kubelet to report
//     Ready again, clears the GPU fault (a "sticky" fault survives a Reboot
//     but not a Replace), then writes .../completed with the request ID.
//
// It runs on your machine (not in the cluster) because it needs the docker
// CLI. `make run-fake-infra` starts it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/jsuh20/node-health-operator/internal/remediation"
)

const (
	gpuCondition    = corev1.NodeConditionType("GPUHealthy")
	stickyAnnotaton = "fake-infra.nodehealth.dev/sticky-fault"
)

type infra struct {
	c         client.Client
	useDocker bool

	mu         sync.Mutex
	inProgress map[string]bool // request IDs being handled right now
}

func main() {
	addr := flag.String("addr", ":8090", "HTTP listen address")
	useDocker := flag.Bool("docker", true, "really stop/restart kind node containers (false = only simulate)")
	flag.Parse()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		log.Fatal(err)
	}
	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		log.Fatal(err)
	}
	f := &infra{c: c, useDocker: *useDocker, inProgress: map[string]bool{}}

	ctx := ctrl.SetupSignalHandler()
	go f.loop(ctx)

	mux := http.NewServeMux()
	// Go 1.22+ patterns: "METHOD /path/{wildcard}".
	mux.HandleFunc("POST /api/nodes/{name}/fault", f.handleInject)
	mux.HandleFunc("DELETE /api/nodes/{name}/fault", f.handleClear)
	registerDashboard(mux, c) // dashboard.go
	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	log.Printf("fake-infra listening on %s (docker=%v)", *addr, *useDocker)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// loop runs every 2s: make sure every node has a GPUHealthy condition, and
// pick up new reboot/replace requests.
func (f *infra) loop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		var nodes corev1.NodeList
		if err := f.c.List(ctx, &nodes); err != nil {
			log.Printf("list nodes: %v", err)
		}
		for i := range nodes.Items {
			n := &nodes.Items[i]
			if findCondition(n, gpuCondition) == nil {
				if err := f.setGPU(ctx, n.Name, true); err != nil {
					log.Printf("init GPUHealthy on %s: %v", n.Name, err)
				}
			}
			req := n.Annotations[remediation.RequestAnnotation]
			if req != "" && n.Annotations[remediation.CompletedAnnotation] != req && f.claim(req) {
				go f.carryOut(ctx, n.Name, req) // one goroutine per request
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (f *infra) claim(req string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.inProgress[req] {
		return false
	}
	f.inProgress[req] = true
	return true
}

// carryOut performs one reboot/replace request. Request IDs look like
// "Reboot/1/1729440000".
func (f *infra) carryOut(ctx context.Context, node, req string) {
	defer func() {
		f.mu.Lock()
		delete(f.inProgress, req)
		f.mu.Unlock()
	}()
	replace := strings.HasPrefix(req, "Replace/")
	log.Printf("%s: carrying out %s", node, req)
	started := time.Now()

	if f.useDocker {
		if out, err := exec.CommandContext(ctx, "docker", "restart", node).CombinedOutput(); err != nil {
			log.Printf("%s: docker restart failed: %v: %s", node, err, out)
			return // the operator's rebootTimeout will turn this into a failed attempt
		}
		// Only a heartbeat sent AFTER the restart proves the kubelet came back.
		// (Heartbeats have 1s resolution, hence the extra second.)
		if err := f.waitForFreshReady(ctx, node, time.Now().Add(time.Second)); err != nil {
			log.Printf("%s: did not come back: %v", node, err)
			return
		}
	} else {
		time.Sleep(3 * time.Second)
	}

	var n corev1.Node
	if err := f.c.Get(ctx, client.ObjectKey{Name: node}, &n); err != nil {
		log.Printf("%s: get: %v", node, err)
		return
	}
	sticky := n.Annotations[stickyAnnotaton] == "true"
	if !sticky || replace {
		if err := f.setGPU(ctx, node, true); err != nil {
			log.Printf("%s: clear gpu fault: %v", node, err)
			return
		}
		f.unmarkSticky(ctx, node)
	}

	// Report completion: remove the request, echo its ID back.
	before := n.DeepCopy()
	delete(n.Annotations, remediation.RequestAnnotation)
	n.Annotations[remediation.CompletedAnnotation] = req
	if err := f.c.Patch(ctx, &n, client.MergeFrom(before)); err != nil {
		log.Printf("%s: write completion: %v", node, err)
		return
	}
	log.Printf("%s: %s done in %s (sticky fault kept: %v)", node, req, time.Since(started).Round(time.Second), sticky && !replace)
}

// waitForFreshReady waits until the kubelet has posted Ready=True after
// `since`, i.e. the node really came back from the restart.
func (f *infra) waitForFreshReady(ctx context.Context, node string, since time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	for {
		var n corev1.Node
		if err := f.c.Get(ctx, client.ObjectKey{Name: node}, &n); err == nil {
			if c := findCondition(&n, corev1.NodeReady); c != nil &&
				c.Status == corev1.ConditionTrue && c.LastHeartbeatTime.After(since) {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// ---- fault injection API -----------------------------------------------------

func (f *infra) handleInject(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("name")
	kind := r.URL.Query().Get("type")
	sticky := r.URL.Query().Get("sticky") == "true"
	var err error
	switch kind {
	case "gpu", "":
		kind = "gpu"
		err = f.setGPU(r.Context(), node, false)
		if err == nil && sticky {
			err = f.annotate(r.Context(), node, stickyAnnotaton, "true")
		}
	case "notready":
		err = f.docker(r.Context(), "stop", node)
	default:
		http.Error(w, "type must be gpu or notready", http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("%s: injected %s fault (sticky=%v)", node, kind, sticky)
	writeJSON(w, map[string]any{"node": node, "fault": kind, "sticky": sticky, "at": time.Now()})
}

func (f *infra) handleClear(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("name")
	if err := f.setGPU(r.Context(), node, true); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	f.unmarkSticky(r.Context(), node)
	if err := f.docker(r.Context(), "start", node); err != nil {
		log.Printf("%s: docker start: %v", node, err)
	}
	log.Printf("%s: faults cleared", node)
	writeJSON(w, map[string]any{"node": node, "cleared": true})
}

// setGPU writes the GPUHealthy condition with a STRATEGIC merge patch. Node
// conditions are a list keyed by `type`; a strategic patch updates just our
// entry. A plain JSON merge patch would replace the whole list and wipe out
// the kubelet's conditions.
func (f *infra) setGPU(ctx context.Context, node string, healthy bool) error {
	var n corev1.Node
	if err := f.c.Get(ctx, client.ObjectKey{Name: node}, &n); err != nil {
		return err
	}
	cond := corev1.NodeCondition{
		Type: gpuCondition, Status: corev1.ConditionTrue, Reason: "GPUOK", Message: "all GPUs healthy",
		LastHeartbeatTime: metav1.Now(),
	}
	if !healthy {
		cond.Status, cond.Reason = corev1.ConditionFalse, "XidError"
		cond.Message = "simulated GPU fault (Xid 79: GPU has fallen off the bus)"
	}
	if old := findCondition(&n, gpuCondition); old != nil && old.Status == cond.Status {
		cond.LastTransitionTime = old.LastTransitionTime // no change, keep the clock
	} else {
		cond.LastTransitionTime = metav1.Now()
	}
	before := n.DeepCopy()
	replaced := false
	for i := range n.Status.Conditions {
		if n.Status.Conditions[i].Type == gpuCondition {
			n.Status.Conditions[i] = cond
			replaced = true
		}
	}
	if !replaced {
		n.Status.Conditions = append(n.Status.Conditions, cond)
	}
	return f.c.Status().Patch(ctx, &n, client.StrategicMergeFrom(before))
}

func (f *infra) annotate(ctx context.Context, node, key, value string) error {
	var n corev1.Node
	if err := f.c.Get(ctx, client.ObjectKey{Name: node}, &n); err != nil {
		return err
	}
	before := n.DeepCopy()
	if n.Annotations == nil {
		n.Annotations = map[string]string{}
	}
	n.Annotations[key] = value
	return f.c.Patch(ctx, &n, client.MergeFrom(before))
}

func (f *infra) unmarkSticky(ctx context.Context, node string) {
	var n corev1.Node
	if err := f.c.Get(ctx, client.ObjectKey{Name: node}, &n); err != nil {
		return
	}
	if _, ok := n.Annotations[stickyAnnotaton]; !ok {
		return
	}
	before := n.DeepCopy()
	delete(n.Annotations, stickyAnnotaton)
	_ = f.c.Patch(ctx, &n, client.MergeFrom(before))
}

func (f *infra) docker(ctx context.Context, verb, node string) error {
	if !f.useDocker {
		return fmt.Errorf("docker disabled (-docker=false)")
	}
	if out, err := exec.CommandContext(ctx, "docker", verb, node).CombinedOutput(); err != nil {
		return fmt.Errorf("docker %s %s: %v: %s", verb, node, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func findCondition(n *corev1.Node, t corev1.NodeConditionType) *corev1.NodeCondition {
	for i := range n.Status.Conditions {
		if n.Status.Conditions[i].Type == t {
			return &n.Status.Conditions[i]
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
