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

// Integration tests: a real kube-apiserver + etcd (envtest) with the real
// controller running. There is no kubelet and no scheduler, so the tests play
// those parts by hand:
//   - "kubelet": finishPodDeletions() removes pods that were evicted;
//   - "infrastructure": completeReboot() answers a reboot request and clears
//     the fault, like the fake-infra simulator does on kind.

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/jsuh20/node-health-operator/api/v1alpha1"
	"github.com/jsuh20/node-health-operator/internal/remediation"
)

// ---- helpers ---------------------------------------------------------------

// pool returns a unique label value per test so tests don't see each
// other's nodes.
var poolSeq int

func newPool() string {
	poolSeq++
	return fmt.Sprintf("pool-%d", poolSeq)
}

func makeNode(name, pool string) *corev1.Node {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"pool": pool}}}
	Expect(k8sClient.Create(ctx, n)).To(Succeed())
	setConditions(name,
		corev1.NodeCondition{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
		corev1.NodeCondition{Type: "GPUHealthy", Status: corev1.ConditionTrue})
	DeferCleanup(func() {
		_ = k8sClient.Delete(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
	})
	return n
}

// setConditions replaces the node's conditions. LastTransitionTime defaults
// to "now" unless given.
func setConditions(name string, conds ...corev1.NodeCondition) {
	Eventually(func() error {
		n := getNode(name)
		for i := range conds {
			if conds[i].LastTransitionTime.IsZero() {
				conds[i].LastTransitionTime = metav1.Now()
			}
		}
		n.Status.Conditions = conds
		return k8sClient.Status().Update(ctx, n)
	}).Should(Succeed())
}

// breakGPU marks the node's GPU unhealthy, starting `ago` in the past.
func breakGPU(name string, ago time.Duration) {
	setConditions(name,
		corev1.NodeCondition{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
		corev1.NodeCondition{Type: "GPUHealthy", Status: corev1.ConditionFalse,
			LastTransitionTime: metav1.NewTime(time.Now().Add(-ago))})
}

func getNode(name string) *corev1.Node {
	n := &corev1.Node{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name}, n)).To(Succeed())
	return n
}

func phaseOf(name string) func() v1alpha1.NodePhase {
	return func() v1alpha1.NodePhase {
		s, err := remediation.GetState(getNode(name))
		Expect(err).NotTo(HaveOccurred())
		if s == nil {
			return ""
		}
		return s.Phase
	}
}

func makePolicy(name, pool string, mutate func(*v1alpha1.RemediationPolicySpec)) *v1alpha1.RemediationPolicy {
	p := &v1alpha1.RemediationPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.RemediationPolicySpec{
			NodeSelector: metav1.LabelSelector{MatchLabels: map[string]string{"pool": pool}},
			UnhealthyConditions: []v1alpha1.UnhealthyCondition{
				{Type: "Ready", Status: "False"},
				{Type: "Ready", Status: "Unknown"},
				{Type: "GPUHealthy", Status: "False"},
			},
			UnhealthyDuration: metav1.Duration{Duration: time.Minute},
			MaxConcurrent:     intstr.FromInt32(1),
			DrainTimeout:      metav1.Duration{Duration: 30 * time.Second},
			RebootTimeout:     metav1.Duration{Duration: 30 * time.Second},
			Retry: v1alpha1.RetryPolicy{
				MaxAttempts:    3,
				InitialBackoff: metav1.Duration{Duration: time.Second},
				MaxBackoff:     metav1.Duration{Duration: 4 * time.Second},
			},
		},
	}
	if mutate != nil {
		mutate(&p.Spec)
	}
	Expect(k8sClient.Create(ctx, p)).To(Succeed())
	DeferCleanup(func() {
		_ = k8sClient.Delete(ctx, p)
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(p), &v1alpha1.RemediationPolicy{}))
		}).Should(BeTrue())
	})
	return p
}

func getPolicy(name string) *v1alpha1.RemediationPolicy {
	p := &v1alpha1.RemediationPolicy{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name}, p)).To(Succeed())
	return p
}

func makePod(name, node string, podLabels map[string]string) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: podLabels},
		Spec: corev1.PodSpec{
			NodeName:   node,
			Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
		},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
	DeferCleanup(func() {
		_ = k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0))
	})
	// Mark it Running and Ready, like a kubelet would. (The Eviction API
	// skips PDB checks for Pending pods, so this matters for the PDB test.)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
}

// finishPodDeletions plays the kubelet: pods that have been evicted are
// terminating; remove them for good.
func finishPodDeletions(node string) {
	var pods corev1.PodList
	Expect(k8sClient.List(ctx, &pods, client.InNamespace("default"))).To(Succeed())
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Spec.NodeName == node && p.DeletionTimestamp != nil {
			_ = k8sClient.Delete(ctx, p, client.GracePeriodSeconds(0))
		}
	}
}

// completeReboot plays the infrastructure: answer the request and (if
// `fixes`) clear the fault, like a successful reboot would.
func completeReboot(name string, fixes bool) {
	Eventually(func() error {
		n := getNode(name)
		req := n.Annotations[remediation.RequestAnnotation]
		if req == "" {
			return fmt.Errorf("no reboot request yet")
		}
		delete(n.Annotations, remediation.RequestAnnotation)
		n.Annotations[remediation.CompletedAnnotation] = req
		return k8sClient.Update(ctx, n)
	}).Should(Succeed())
	if fixes {
		setConditions(name,
			corev1.NodeCondition{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			corev1.NodeCondition{Type: "GPUHealthy", Status: corev1.ConditionTrue})
	}
}

// ---- specs -----------------------------------------------------------------

var _ = Describe("RemediationPolicy controller", func() {

	It("ignores a fault until it outlasts the threshold (hysteresis)", func() {
		pool := newPool()
		makeNode("hyst-1", pool)
		makePolicy("hyst", pool, nil) // threshold 1m

		breakGPU("hyst-1", 10*time.Second) // only 10s old
		Eventually(func() []v1alpha1.NodeStatus { return getPolicy("hyst").Status.Nodes }).Should(
			ContainElement(HaveField("Phase", v1alpha1.PhasePending)))
		Consistently(func() bool { return getNode("hyst-1").Spec.Unschedulable }).Should(BeFalse())

		By("the blip clears before the threshold: nothing ever happens")
		setConditions("hyst-1", corev1.NodeCondition{Type: "GPUHealthy", Status: corev1.ConditionTrue})
		Eventually(func() []v1alpha1.NodeStatus { return getPolicy("hyst").Status.Nodes }).Should(BeEmpty())
		Expect(getNode("hyst-1").Spec.Unschedulable).To(BeFalse())
	})

	It("cordons, drains, reboots and uncordons a broken node", func() {
		pool := newPool()
		makeNode("happy-1", pool)
		makePod("happy-workload", "happy-1", nil)
		makePolicy("happy", pool, nil)

		breakGPU("happy-1", 5*time.Minute)

		By("cordoning and claiming the node in one write")
		Eventually(phaseOf("happy-1")).Should(Equal(v1alpha1.PhaseDraining))
		Expect(getNode("happy-1").Spec.Unschedulable).To(BeTrue())

		By("evicting the pod; the drain waits until it is really gone")
		Eventually(func() bool {
			p := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: "default", Name: "happy-workload"}, p)).To(Succeed())
			return p.DeletionTimestamp != nil
		}).Should(BeTrue())
		Consistently(phaseOf("happy-1"), time.Second).Should(Equal(v1alpha1.PhaseDraining))
		finishPodDeletions("happy-1")

		By("requesting a reboot once drained")
		Eventually(phaseOf("happy-1")).Should(Equal(v1alpha1.PhaseRebooting))
		completeReboot("happy-1", true)

		By("uncordoning and forgetting the node once it is healthy")
		Eventually(phaseOf("happy-1")).Should(BeEmpty())
		n := getNode("happy-1")
		Expect(n.Spec.Unschedulable).To(BeFalse())
		Expect(n.Annotations).NotTo(HaveKey(remediation.CompletedAnnotation))
	})

	It("never remediates more nodes at once than the blast-radius cap", func() {
		pool := newPool()
		names := []string{"cap-1", "cap-2", "cap-3", "cap-4"}
		for _, n := range names {
			makeNode(n, pool)
		}
		makePolicy("cap", pool, func(s *v1alpha1.RemediationPolicySpec) {
			s.MaxConcurrent = intstr.FromString("50%") // 2 of 4
		})
		for _, n := range names {
			breakGPU(n, 5*time.Minute)
		}

		active := func() int {
			count := 0
			for _, n := range names {
				if s, _ := remediation.GetState(getNode(n)); s != nil {
					count++
				}
			}
			return count
		}
		Eventually(active).Should(Equal(2))
		Consistently(active).Should(Equal(2)) // and never 3

		Eventually(func() metav1.ConditionStatus {
			c := meta.FindStatusCondition(getPolicy("cap").Status.Conditions, "BlastRadiusLimited")
			if c == nil {
				return ""
			}
			return c.Status
		}).Should(Equal(metav1.ConditionTrue))
		Expect(getPolicy("cap").Status.MaxConcurrent).To(Equal(int32(2)))

		By("finishing one remediation frees a slot for a waiting node")
		var first string
		for _, n := range names {
			if s, _ := remediation.GetState(getNode(n)); s != nil {
				first = n
				break
			}
		}
		Eventually(phaseOf(first)).Should(Equal(v1alpha1.PhaseRebooting))
		completeReboot(first, true)
		Eventually(phaseOf(first)).Should(BeEmpty())
		Eventually(active).Should(Equal(2))
		Consistently(active).Should(Equal(2))
	})

	It("respects PodDisruptionBudgets and backs off when the drain times out", func() {
		pool := newPool()
		makeNode("pdb-1", pool)
		makePod("pdb-protected", "pdb-1", map[string]string{"app": "critical"})
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Name: "critical", Namespace: "default"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: ptr.To(intstr.FromInt32(1)),
				Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "critical"}},
			},
		}
		Expect(k8sClient.Create(ctx, pdb)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pdb) })
		// envtest has no disruption controller, so play it: 1 healthy pod,
		// 1 required, so 0 disruptions allowed. Every eviction gets HTTP 429.
		pdb.Status = policyv1.PodDisruptionBudgetStatus{
			ObservedGeneration: pdb.Generation,
			CurrentHealthy:     1, DesiredHealthy: 1, ExpectedPods: 1, DisruptionsAllowed: 0,
		}
		Expect(k8sClient.Status().Update(ctx, pdb)).To(Succeed())

		makePolicy("pdb", pool, func(s *v1alpha1.RemediationPolicySpec) {
			s.DrainTimeout = metav1.Duration{Duration: 3 * time.Second}
			s.Retry.InitialBackoff = metav1.Duration{Duration: time.Hour} // stay in Backoff
			s.Retry.MaxBackoff = metav1.Duration{Duration: time.Hour}
		})
		breakGPU("pdb-1", 5*time.Minute)

		Eventually(func() string {
			for _, n := range getPolicy("pdb").Status.Nodes {
				return n.Message
			}
			return ""
		}).Should(ContainSubstring("blocked by PodDisruptionBudget"))

		By("the pod is never evicted")
		p := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: "default", Name: "pdb-protected"}, p)).To(Succeed())
		Expect(p.DeletionTimestamp).To(BeNil())

		By("the drain times out into Backoff instead of forcing it")
		Eventually(phaseOf("pdb-1")).Should(Equal(v1alpha1.PhaseBackoff))
		Expect(getNode("pdb-1").Annotations).NotTo(HaveKey(remediation.RequestAnnotation))
	})

	It("retries with backoff and finally gives up, leaving the node cordoned", func() {
		pool := newPool()
		makeNode("fail-1", pool)
		makePolicy("fail", pool, func(s *v1alpha1.RemediationPolicySpec) {
			s.RebootTimeout = metav1.Duration{Duration: 2 * time.Second}
			s.Retry.MaxAttempts = 2
		})
		breakGPU("fail-1", 5*time.Minute)
		// Nobody answers the reboot requests, so every attempt times out.

		Eventually(phaseOf("fail-1")).Should(Equal(v1alpha1.PhaseBackoff))
		s, _ := remediation.GetState(getNode("fail-1"))
		Expect(s.Attempts).To(Equal(int32(1)))
		Expect(s.NextRetryAt).NotTo(BeZero())

		Eventually(phaseOf("fail-1")).Should(Equal(v1alpha1.PhaseFailed))
		s, _ = remediation.GetState(getNode("fail-1"))
		Expect(s.Attempts).To(Equal(int32(2)))
		Expect(getNode("fail-1").Spec.Unschedulable).To(BeTrue())
		Expect(getPolicy("fail").Status.NodesUnderRemediation).To(Equal(int32(1)))
	})

	It("resumes from the recorded state after a controller crash", func() {
		pool := newPool()
		makeNode("resume-1", pool)
		breakGPU("resume-1", 5*time.Minute)

		By("faking a previous controller that died mid-drain")
		Eventually(func() error {
			n := getNode("resume-1")
			n.Spec.Unschedulable = true
			Expect(remediation.SetState(n, &remediation.State{
				Policy: "resume", Phase: v1alpha1.PhaseDraining, Reason: "GPUHealthy=False",
				UnhealthySince: time.Now().Add(-5 * time.Minute), StartedAt: time.Now().Add(-time.Minute),
				PhaseStartedAt: time.Now(), Attempts: 1, CordonedByUs: true,
			})).To(Succeed())
			return k8sClient.Update(ctx, n)
		}).Should(Succeed())

		makePolicy("resume", pool, nil)

		By("carrying on from Draining instead of starting over")
		Eventually(phaseOf("resume-1")).Should(Equal(v1alpha1.PhaseRebooting))
		s, _ := remediation.GetState(getNode("resume-1"))
		Expect(s.Attempts).To(Equal(int32(1)))
		completeReboot("resume-1", true)
		Eventually(phaseOf("resume-1")).Should(BeEmpty())
		Expect(getNode("resume-1").Spec.Unschedulable).To(BeFalse())
	})

	It("leaves an admin's cordon in place after remediating", func() {
		pool := newPool()
		makeNode("admin-1", pool)
		Eventually(func() error {
			n := getNode("admin-1")
			n.Spec.Unschedulable = true
			return k8sClient.Update(ctx, n)
		}).Should(Succeed())
		makePolicy("admin", pool, nil)
		breakGPU("admin-1", 5*time.Minute)

		Eventually(phaseOf("admin-1")).Should(Equal(v1alpha1.PhaseRebooting))
		completeReboot("admin-1", true)
		Eventually(phaseOf("admin-1")).Should(BeEmpty())
		Expect(getNode("admin-1").Spec.Unschedulable).To(BeTrue())
	})

	It("only reports in dry-run mode", func() {
		pool := newPool()
		makeNode("dry-1", pool)
		makePod("dry-workload", "dry-1", nil)
		makePolicy("dry", pool, func(s *v1alpha1.RemediationPolicySpec) { s.DryRun = true })
		breakGPU("dry-1", 5*time.Minute)

		Eventually(func() []v1alpha1.NodeStatus { return getPolicy("dry").Status.Nodes }).Should(
			ContainElement(HaveField("Phase", v1alpha1.PhaseDryRun)))
		Consistently(func() bool {
			n := getNode("dry-1")
			_, hasState := n.Annotations[remediation.StateAnnotation]
			return n.Spec.Unschedulable || hasState
		}).Should(BeFalse())
		p := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: "default", Name: "dry-workload"}, p)).To(Succeed())
		Expect(p.DeletionTimestamp).To(BeNil())
	})

	It("releases its nodes when the policy is deleted", func() {
		pool := newPool()
		makeNode("release-1", pool)
		p := makePolicy("release", pool, nil)
		breakGPU("release-1", 5*time.Minute)
		Eventually(phaseOf("release-1")).Should(Equal(v1alpha1.PhaseRebooting))

		Expect(k8sClient.Delete(ctx, p)).To(Succeed())
		Eventually(phaseOf("release-1")).Should(BeEmpty())
		n := getNode("release-1")
		Expect(n.Annotations).NotTo(HaveKey(remediation.RequestAnnotation))
		Expect(n.Spec.Unschedulable).To(BeTrue(), "still broken, so it stays cordoned")
	})
})
