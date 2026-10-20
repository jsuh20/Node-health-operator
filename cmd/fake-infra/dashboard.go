package main

import (
	"embed"
	"net/http"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/jsuh20/node-health-operator/api/v1alpha1"
	"github.com/jsuh20/node-health-operator/internal/remediation"
)

// go:embed bakes web/index.html into the binary at compile time, so the
// dashboard needs no separate files at runtime.
//
//go:embed web/index.html
var webFS embed.FS

// State is what GET /api/state returns. It's read-only: a snapshot of nodes,
// policies and recent operator events.
type State struct {
	Nodes    []NodeView   `json:"nodes"`
	Policies []PolicyView `json:"policies"`
	Events   []EventView  `json:"events"`
}

type NodeView struct {
	Name      string `json:"name"`
	Worker    bool   `json:"worker"`
	Ready     string `json:"ready"`
	GPU       string `json:"gpu"`
	Cordoned  bool   `json:"cordoned"`
	Phase     string `json:"phase"` // Healthy, or a v1alpha1.NodePhase
	Reason    string `json:"reason,omitempty"`
	Attempts  int32  `json:"attempts,omitempty"`
	Message   string `json:"message,omitempty"`
	Sticky    bool   `json:"sticky,omitempty"`
	Rebooting bool   `json:"rebooting,omitempty"`
}

type PolicyView struct {
	Name        string `json:"name"`
	Matching    int32  `json:"matching"`
	Unhealthy   int32  `json:"unhealthy"`
	Remediating int32  `json:"remediating"`
	Max         int32  `json:"max"`
	DryRun      bool   `json:"dryRun"`
	Threshold   string `json:"threshold"`
}

type EventView struct {
	Time   time.Time `json:"time"`
	Node   string    `json:"node"`
	Type   string    `json:"type"`
	Reason string    `json:"reason"`
	Note   string    `json:"note"`
}

func registerDashboard(mux *http.ServeMux, c client.Client) {
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, webFS, "web/index.html")
	})
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		s, err := snapshot(r, c)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, s)
	})
}

func snapshot(r *http.Request, c client.Client) (*State, error) {
	ctx := r.Context()
	var nodes corev1.NodeList
	var policies v1alpha1.RemediationPolicyList
	var events eventsv1.EventList
	if err := c.List(ctx, &nodes); err != nil {
		return nil, err
	}
	if err := c.List(ctx, &policies); err != nil {
		return nil, err
	}
	if err := c.List(ctx, &events); err != nil {
		return nil, err
	}

	// The policy status knows about phases that live nowhere else
	// (Pending, Waiting, WouldRemediate).
	statusByNode := map[string]v1alpha1.NodeStatus{}
	s := &State{}
	for _, p := range policies.Items {
		for _, n := range p.Status.Nodes {
			statusByNode[n.Name] = n
		}
		s.Policies = append(s.Policies, PolicyView{
			Name: p.Name, Matching: p.Status.MatchingNodes, Unhealthy: p.Status.UnhealthyNodes,
			Remediating: p.Status.NodesUnderRemediation, Max: p.Status.MaxConcurrent,
			DryRun: p.Spec.DryRun, Threshold: p.Spec.UnhealthyDuration.Duration.String(),
		})
	}

	for i := range nodes.Items {
		n := &nodes.Items[i]
		_, isCP := n.Labels["node-role.kubernetes.io/control-plane"]
		v := NodeView{
			Name: n.Name, Worker: !isCP, Phase: "Healthy", Cordoned: n.Spec.Unschedulable,
			Ready:     condStatus(n, corev1.NodeReady),
			GPU:       condStatus(n, gpuCondition),
			Sticky:    n.Annotations[stickyAnnotaton] == "true",
			Rebooting: n.Annotations[remediation.RequestAnnotation] != "",
		}
		if st, ok := statusByNode[n.Name]; ok {
			v.Phase, v.Reason, v.Attempts, v.Message = string(st.Phase), st.Reason, st.Attempts, st.Message
		} else if st, _ := remediation.GetState(n); st != nil {
			v.Phase, v.Reason, v.Attempts = string(st.Phase), st.Reason, st.Attempts
		}
		s.Nodes = append(s.Nodes, v)
	}
	sort.Slice(s.Nodes, func(i, j int) bool { return s.Nodes[i].Name < s.Nodes[j].Name })

	for _, e := range events.Items {
		if e.ReportingController != "node-health-operator" {
			continue
		}
		t := e.EventTime.Time
		if e.Series != nil {
			t = e.Series.LastObservedTime.Time
		}
		s.Events = append(s.Events, EventView{Time: t, Node: e.Regarding.Name, Type: e.Type, Reason: e.Reason, Note: e.Note})
	}
	sort.Slice(s.Events, func(i, j int) bool { return s.Events[i].Time.After(s.Events[j].Time) })
	if len(s.Events) > 40 {
		s.Events = s.Events[:40]
	}
	return s, nil
}

func condStatus(n *corev1.Node, t corev1.NodeConditionType) string {
	if c := findCondition(n, t); c != nil {
		return string(c.Status)
	}
	return ""
}
