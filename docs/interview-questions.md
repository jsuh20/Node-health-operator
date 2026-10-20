# Likely interview questions (with answers)

Short answers first, then the detail you can add if they dig in. File references
point at the code that backs each answer.

---

### 1. What happens if the controller dies mid-drain?

**Short:** Nothing is lost. The standby replica takes over and continues the drain
from where it was.

**Detail:** All remediation state lives in an annotation on the Node
(`remediation.nodehealth.dev/state`), not in controller memory. When the leader
dies, the other replica gets the Lease after it expires (about 15s for a crash; about 1s for a
clean shutdown, because we set `LeaderElectionReleaseOnCancel`). Its first
reconcile reads the annotation, sees `phase: Draining`, and runs another drain
pass. Draining is idempotent: pods already evicted are gone or terminating, and
an eviction for a pod that no longer exists returns NotFound, which we ignore.
Chaos scenario 5 tests this with a real `kill -9`: one reboot request, still
"attempt 1", node back in service.

### 2. What if it crashes between cordoning and writing down that it cordoned?

**Short:** That gap doesn't exist. Cordoning and recording the state are one API write.

**Detail:** The state annotation and `spec.unschedulable` are on the same object,
so `start()` sends them in a single patch. Either both land or neither does.
That was a deliberate reason to store state on the Node rather than only in
the policy status. Same for the end: uncordon and clearing the annotation are
one patch (`recover()`).

### 3. Why is state stored on the Node and not in the policy status?

- Atomic with the action (see above).
- It survives anything that happens to the policy object, and it acts as a lock:
  a node already claimed by policy A is skipped by policy B.
- Kubernetes convention: status should be reconstructable from observation. Our
  policy status is recomputed every reconcile from the nodes, so it's a view, not
  the source of truth.

Tradeoff: we write to Node objects (more RBAC, and the annotations are visible to
anyone who inspects the node, which arguably helps).

### 4. Why reconcile per policy instead of per node?

The blast-radius cap is a property of the whole set of matching nodes. The
question "may I start one more?" needs to see all of them together. Per-node
reconciles would have to coordinate through some shared counter. Per-policy
makes that one function call (`SelectToStart`). Tradeoff: one reconcile does
work for many nodes, so it must stay non-blocking (drains are incremental,
each pass has a timeout).

### 5. How do you guarantee the cap is never exceeded?

Three layers:
1. **Single writer:** leader election, so only one process decides at a time, and
   one reconcile worker per controller.
2. **Fresh count:** the informer cache can lag our own writes, so before handing
   out a slot we recount active nodes straight from the API server
   (`countActiveLive`). Without that, two reconciles back to back could each see
   "0 active" and start two nodes.
3. **Optimistic locking:** node patches carry the `resourceVersion` we read.
   Anyone else's change in between makes our write fail with Conflict, and we
   re-reconcile on fresh data.

Measured in chaos scenario 4: 3 of 4 nodes broken at once, max 1 under
remediation, sampled every second.

### 6. Why do Failed nodes count against the cap?

If remediation keeps failing, something bigger may be wrong (bad image, bad
firmware rollout, network). Freeing the slot would let the operator burn through
the fleet faster. Holding it means a run of failures stops the automation and
pulls in a human. Tradeoff: one permanently broken node blocks automatic repair
in a small pool until someone clears it. That is visible in status, events and
the dashboard.

### 7. What is hysteresis here and how is it implemented?

The rule is: only act if the bad condition has lasted longer than `unhealthyDuration`.
"How long" is measured from the condition's `lastTransitionTime`, which the
kubelet/node controller sets and which resets if the condition flips back.
So a blip restarts the clock. Using the API's timestamp (not "when I first noticed")
also makes it survive controller restarts. Detail: those timestamps have 1-second
resolution, so the threshold is accurate to ±1s (we hit this in chaos testing).

### 8. How do you respect PodDisruptionBudgets?

We don't count PDBs ourselves. We use the **Eviction API**, and the API server
checks every eviction against matching PDBs. If it would violate one, it returns
HTTP 429 and the pod stays. We report "blocked by PodDisruptionBudget", try
again on the next pass, and if `drainTimeout` passes, the attempt fails into
backoff. We never fall back to deleting the pod.
Gotcha we hit: the Eviction API doesn't apply PDBs to pods that aren't
running yet (Pending), which is correct but surprised our first test.

### 9. What about pods on a node that's NotReady? They can never finish terminating.

Right: `kubectl drain` hangs there. Once evicted, a pod on an unreachable node
stays Terminating forever because no kubelet confirms it stopped. We treat
terminating pods as gone **only if the node's Ready condition isn't True**. The
eviction still went through PDB checks, and the ReplicaSet already created a
replacement elsewhere. Tradeoff: if the node is actually still running the pod
(network partition), there's briefly a second copy. That is the same tradeoff
Kubernetes makes for unreachable nodes.

### 9b. What surprised you in chaos testing?

A dead node can deadlock its own drain. Its pods go NotReady, the PDB counts
them as already disrupted, and with the default `unhealthyPodEvictionPolicy:
IfHealthyBudget` the API server refuses to evict even those dead pods. The
operator correctly refused to bypass the PDB, timed out and backed off. The
node was fixed only after Kubernetes' taint-based eviction (about 5 min) removed the
pods. Fix: `unhealthyPodEvictionPolicy: AlwaysAllow` on the workload's PDB.
The operator's event now suggests it when it sees PDB blocks on a NotReady node.
I chose not to make the operator force-delete pods there: "respect PDBs" should
mean always, and the workload owner is the right person to decide.

### 10. How does backoff work and how do you avoid hot loops?

After a failed attempt, the wait is `initialBackoff * 2^(failures-1)`, capped
at `maxBackoff`. After `maxAttempts`, the node goes Failed. The retry time is
stored in the annotation (`nextRetryAt`), so a restart doesn't reset it.
Hot-loop protections:
- Every reconcile returns `RequeueAfter` = the soonest moment something can change.
- Status writes are skipped when nothing changed.
- Policy watch uses `GenerationChangedPredicate`, so our own status writes don't
  trigger us.
- The node watch ignores heartbeat-only updates.
- Errors go through controller-runtime's rate-limited queue.

### 11. What does leader election actually protect, and what doesn't it protect?

It makes one replica the active writer, using a Lease object with a time-based expiry.
It is not a perfect mutex: a paused leader could resume briefly after losing
the lease. Two things cover that: controller-runtime exits the process when it
loses the lease, and every node write uses optimistic locking, so a stale
leader's write conflicts instead of overwriting.

### 12. What does dry-run do?

It evaluates everything (health, threshold, cap) and emits `DryRun` events and
logs saying what it would do, and status shows `WouldRemediate`. It never patches
nodes or evicts pods. If dry-run is switched on mid-remediation, in-flight nodes
are paused where they are, not abandoned. Useful for rolling out a new policy
safely.

### 13. What happens if you delete a policy mid-remediation?

A finalizer makes deletion wait for cleanup. We remove our annotations from
every node the policy owns. We uncordon only nodes that we cordoned **and** that
are healthy now. A still-broken node stays cordoned, with a Warning event.

### 14. An admin had already cordoned a node before it failed. What then?

We record `cordonedByUs: false` and remediate it normally, but we leave it
cordoned at the end. We never undo a human's decision.

### 15. How do you know the reboot actually happened?

The controller writes a unique request ID (`Reboot/<attempt>/<unix>`). The
infra layer echoes it back in `.../completed` once done. We need both the
matching ID **and** a healthy node. The ID means a completion from an older
attempt can't be mistaken for this one. In production you'd also check that
`status.nodeInfo.bootID` changed. In kind all nodes share the host kernel, so
bootID doesn't change. That's why the simulator uses the echo protocol.

### 16. Why does the operator run on the control-plane node?

So it never drains itself. If its own pod were on a node being remediated, the
eviction would kill the leader mid-remediation (survivable, but pointless churn).
In a real cluster it would run in a system node pool that no policy selects.

### 17. What would you change for a 5,000-node GPU cluster?

- **Global circuit breaker:** if, say, more than 20% of nodes go unhealthy at once, it's
  probably the network or control plane, not the nodes. Stop remediating
  entirely. (The cap limits concurrency, not the total number of nodes eventually touched.)
- **Escalation:** Reboot → Reimage → Replace, instead of one strategy.
- **Real infra integration:** a cloud API or BMC call, a bootID check, handling Node
  objects that are deleted and re-created on replace.
- **Scale:** the operator already lists pods uncached (no cluster-wide pod
  informer). The next steps would be metadata-only node informers and sharding policies.
- **Validation webhook:** reject overlapping policies and nonsensical timeouts at
  admission time.

### 18. How did you test it?

- **Unit tests** (`internal/remediation/decision_test.go`) for the pure decision
  functions: health evaluation, threshold edges, cap rounding, backoff growth and
  cap, selection order.
- **envtest** (`internal/controller/*_test.go`): a real API server and etcd with the
  real controller. The test plays kubelet and infrastructure. It covers the happy path, cap,
  PDB blocking, backoff→Failed, crash resume, admin cordon, dry-run, policy deletion.
- **Chaos on kind** (`hack/chaos/`): real `docker stop` NotReady nodes, real
  `kill -9` of the leader mid-drain, 3-of-4 nodes broken at once, a sticky fault that
  reboots can't fix. Timings come from the operator's own events (µs precision).
  Results: [chaos-results.md](chaos-results.md).
