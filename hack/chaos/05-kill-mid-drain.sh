#!/usr/bin/env bash
# Scenario 5: SIGKILL the leader while it is draining a node (a real crash:
# no graceful shutdown, no lease release). The standby must take over after
# the lease expires and finish the job from the state on the node, without
# starting over or issuing a second reboot.
source "$(dirname "$0")/lib.sh"

log "Scenario 5: kill the leader mid-drain"
reset_cluster
# Pick the worker running the most workload pods, so the drain takes a while.
node=$(kubectl get pods -l app=training-job -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort | uniq -c | sort -rn | awk 'NR==1{print $2}')
t0=$(now); inject "$node" gpu
log "GPU fault on $node; waiting for Draining"
wait_for 120 "$node draining" bash -c "[ \"\$(source '$HERE/lib.sh'; phase $node)\" = Draining ]"

old=$(leader)
old_pod=${old%%_*}
cid=$(kubectl -n "$NS" get pod "$old_pod" -o jsonpath='{.status.containerStatuses[0].containerID}' | sed 's|containerd://||')
pid=$(docker exec nodehealth-control-plane crictl inspect --output go-template --template '{{.info.pid}}' "$cid")
t_kill=$(now)
docker exec nodehealth-control-plane kill -9 "$pid"
log "SIGKILLed leader $old_pod (pid $pid) while $node was Draining"

wait_for 120 "new leader" bash -c "h=\$(kubectl -n $NS get lease $LEASE -o jsonpath='{.spec.holderIdentity}'); [ -n \"\$h\" ] && [ \"\$h\" != '$old' ]"
t_new=$(now); new=$(leader)
failover=$(python3 -c "print(f'{$t_new - $t_kill:.1f}')")
log "new leader ${new%%_*} after ${failover}s"

wait_for 300 "$node remediated" bash -c "[ -n \"\$(kubectl get events.events.k8s.io -A -o json | python3 '$HERE/event_offset.py' $node Remediated $t0)\" ]" || true
rem=$(event_offset "$node" Remediated "$t0")
reboots=$(kubectl get events.events.k8s.io -A -o json | python3 -c "
import json,sys
from datetime import datetime
n=0
for e in json.load(sys.stdin)['items']:
    if e.get('reportingController')=='node-health-operator' and e['regarding']['name']=='$node' and e['reason']=='RebootRequested':
        t=datetime.fromisoformat(e['eventTime'].replace('Z','+00:00')).timestamp()
        n += (t >= $t0) * (1 + (e.get('series') or {}).get('count', 1) - 1)
print(n)")
attempt=$(kubectl get events.events.k8s.io -A -o json | python3 -c "
import json,sys
for e in json.load(sys.stdin)['items']:
    if e.get('reportingController')=='node-health-operator' and e['regarding']['name']=='$node' and e['reason']=='Remediated':
        print(e['note'])" | tail -1)
pdb_blocks=$(event_offset "$node" DrainBlockedByPDB "$t0")

record "### 5. Leader SIGKILLed mid-drain"
record ""
record "Killed \`${old_pod}\`; new leader \`${new%%_*}\`."
record ""
record "| Leader failover | Back in service (from fault) | Reboot requests | PDB blocked an eviction |"
record "|---|---|---|---|"
record "| ${failover}s | ${rem}s | $reboots | $([ -n "$pdb_blocks" ] && echo "yes, at ${pdb_blocks}s" || echo no) |"
record ""
record "Final event: \`$attempt\`"
record ""
[ "$new" != "$old" ] && pass "leadership moved to ${new%%_*} in ${failover}s" || fail "no failover"
[ -n "$rem" ] && pass "node finished remediation after the crash (${rem}s)" || fail "node not remediated after crash"
[ "$reboots" = 1 ] && pass "exactly one reboot request" || fail "$reboots reboot requests"
case "$attempt" in *"attempt 1"*) pass "resumed attempt 1, did not start over";; *) fail "unexpected: $attempt";; esac
exit $FAILED
