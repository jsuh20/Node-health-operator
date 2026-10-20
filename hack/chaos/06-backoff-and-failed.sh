#!/usr/bin/env bash
# Scenario 6: a "sticky" GPU fault that a reboot does not fix. The operator
# must back off between attempts, give up after maxAttempts, leave the node
# cordoned, and keep counting it against the cap (so a second broken node
# waits instead of being remediated).
source "$(dirname "$0")/lib.sh"

log "Scenario 6: sticky fault -> backoff -> Failed"
reset_cluster
kubectl patch rp "$POLICY" --type merge -p '{"spec":{"rebootTimeout":"30s","retry":{"maxAttempts":2,"initialBackoff":"10s"}}}' >/dev/null
trap 'kubectl apply -f "$HERE/../../config/samples/remediation_v1alpha1_remediationpolicy.yaml" >/dev/null' EXIT

node=nodehealth-worker2
t0=$(now); inject "$node" gpu true
wait_for 360 "$node Failed" bash -c "[ \"\$(source '$HERE/lib.sh'; phase $node)\" = Failed ]" || true
fail1=$(event_offset "$node" AttemptFailed "$t0"); retry=$(event_offset "$node" Retrying "$t0")
gaveup=$(event_offset "$node" RemediationFailed "$t0")

log "second node breaks while $node holds the only slot"
other=nodehealth-worker3
t1=$(now); inject "$other" gpu
sleep 75
other_phase=$(phase "$other")
other_status=$(kubectl get rp "$POLICY" -o jsonpath="{.status.nodes[?(@.name=='$other')].phase}")

record "### 6. Sticky fault: backoff, give up, hold the slot"
record ""
record "| Attempt 1 failed | Retry (after 10s backoff) | Gave up | Node left cordoned | Second broken node |"
record "|---|---|---|---|---|"
record "| ${fail1}s | ${retry}s | ${gaveup}s | $(cordoned $node && echo yes || echo no) | $other_status (not started) |"
record ""
[ "$(phase "$node")" = Failed ] && pass "gave up after 2 attempts" || fail "node not Failed"
python3 -c "import sys; sys.exit(0 if ('$retry' and '$fail1' and float('$retry')-float('$fail1') >= 9.5) else 1)" \
  && pass "backoff respected ($(python3 -c "print(f'{float(\"$retry\")-float(\"$fail1\"):.1f}')")s between failure and retry)" || fail "backoff not respected"
cordoned "$node" && pass "Failed node stays cordoned" || fail "Failed node was uncordoned"
[ "$other_phase" = "-" ] && [ "$other_status" = Waiting ] && pass "Failed node holds the cap slot; $other is Waiting" \
  || fail "$other was $other_phase/$other_status, expected Waiting"
exit $FAILED
