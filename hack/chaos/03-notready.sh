#!/usr/bin/env bash
# Scenario 3: a real NotReady node. `docker stop` kills the node's kubelet;
# Kubernetes marks it Ready=Unknown after its own grace period; the operator
# remediates and fake-infra "reboots" it with `docker restart`.
source "$(dirname "$0")/lib.sh"

log "Scenario 3: NotReady node (kubelet stopped)"
reset_cluster
node=nodehealth-worker4
pods_before=$(kubectl get pods -l app=training-job --field-selector spec.nodeName=$node --no-headers | wc -l | tr -d ' ')
t0=$(now); inject "$node" notready
wait_for 480 "$node remediated" bash -c "[ -n \"\$(kubectl get events.events.k8s.io -A -o json | python3 '$HERE/event_offset.py' $node Remediated $t0)\" ]" || true
det=$(event_offset "$node" Detected "$t0"); cor=$(event_offset "$node" Cordoned "$t0")
reb=$(event_offset "$node" RebootRequested "$t0"); rem=$(event_offset "$node" Remediated "$t0")
record "### 3. NotReady node (kubelet stopped with docker stop)"
record ""
record "| Ready=Unknown detected | Cordoned | Drained + reboot requested | Back in service | Pods moved off |"
record "|---|---|---|---|---|"
record "| ${det}s | ${cor}s | ${reb}s | ${rem}s | $pods_before |"
record ""
record "Detection here includes Kubernetes' own node-monitor grace period (the node controller marks the node Unknown only after it misses heartbeats); the operator reacts within a second of that."
record ""
[ -n "$rem" ] && pass "NotReady node back in service after ${rem}s" || fail "NotReady node not remediated"
exit $FAILED
