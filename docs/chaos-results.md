# Chaos test results

Recorded 2026-10-20 14:43 PDT by `make chaos` on a local kind cluster
(1 control plane + 4 workers, Kubernetes v1.37.0).
Policy: `gpu-pool` sample (threshold 60s, maxConcurrent 25% = 1 node).
All times are seconds after the fault was injected, taken from the operator's
own Kubernetes events (microsecond timestamps).

### 1. Single GPU fault (3 trials)

| Trial | Node | Detected | Cordoned | Reboot requested | Back in service |
|---|---|---|---|---|---|
| 1 | nodehealth-worker | 0.0s | 59.8s | 84.9s | 88.8s |
| 2 | nodehealth-worker2 | 0.0s | 59.4s | 84.5s | 87.4s |
| 3 | nodehealth-worker3 | 0.0s | 60.0s | 85.1s | 87.9s |

### 2. Transient 20s blip

Detected after 0.0s; never cordoned or drained. **Pass.**

### 3. NotReady node (kubelet stopped with docker stop)

| Ready=Unknown detected | Cordoned | Drained + reboot requested | Back in service | Pods moved off |
|---|---|---|---|---|
| 47.8s | 107.6s | 112.7s | 116.3s | 2 |

Detection here includes Kubernetes' own node-monitor grace period (the node controller marks the node Unknown only after it misses heartbeats); the operator reacts within a second of that.

### 4. Blast radius: 3 of 4 nodes broken at once (cap 25% = 1)

| Node | Cordoned | Back in service |
|---|---|---|
| nodehealth-worker | 59.6s | 88.5s |
| nodehealth-worker2 | 88.5s | 116.5s |
| nodehealth-worker3 | 116.5s | 144.5s |

Max nodes under remediation at once: **1** (cap 1), over 103 one-second samples. BlastRadiusLimited condition seen: yes.

### 5. Leader SIGKILLed mid-drain

Killed `node-health-operator-controller-manager-5f6fdcbcb7-mhf7s`; new leader `node-health-operator-controller-manager-5f6fdcbcb7-mhf7s` (a new process; the killed container restarted and won the lease).

| Leader failover | Back in service (from fault) | Reboot requests | PDB blocked an eviction |
|---|---|---|---|
| 17.0s | 101.8s | 1 | yes, at 59.9s |

Final event: `node healthy after Reboot (attempt 1); back in service, 1m42s after the fault began`

### 6. Sticky fault: backoff, give up, hold the slot

| Attempt 1 failed | Retry (after 10s backoff) | Gave up | Node left cordoned | Second broken node |
|---|---|---|---|---|
| 114.4s | 124.4s | 154.4s | yes | Waiting (not started) |

