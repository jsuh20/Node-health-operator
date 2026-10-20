#!/usr/bin/env bash
# Scenario 4: break 3 of 4 nodes at once. With maxConcurrent 25% (= 1 node),
# the operator must never work on more than one at a time, and must still
# fix all three in turn.
source "$(dirname "$0")/lib.sh"

log "Scenario 4: 3 nodes break at once (cap = 1)"
reset_cluster
victims=(nodehealth-worker nodehealth-worker2 nodehealth-worker3)
t0=$(now)
for n in "${victims[@]}"; do inject "$n" gpu; done
max_active=0; samples=0; saw_limited=0
start=$SECONDS
while (( SECONDS - start < 600 )); do
  a=$(active_count); samples=$((samples + 1))
  (( a > max_active )) && max_active=$a
  lim=$(kubectl get rp "$POLICY" -o jsonpath='{.status.conditions[?(@.type=="BlastRadiusLimited")].status}')
  [ "$lim" = "True" ] && saw_limited=1
  done_count=0
  for n in "${victims[@]}"; do [ -n "$(event_offset "$n" Remediated "$t0")" ] && done_count=$((done_count + 1)); done
  (( done_count == ${#victims[@]} )) && break
  sleep 1
done
record "### 4. Blast radius: 3 of 4 nodes broken at once (cap 25% = 1)"
record ""
record "| Node | Cordoned | Back in service |"
record "|---|---|---|"
for n in "${victims[@]}"; do
  record "| $n | $(event_offset "$n" Cordoned "$t0")s | $(event_offset "$n" Remediated "$t0")s |"
done
record ""
record "Max nodes under remediation at once: **$max_active** (cap 1), over $samples one-second samples. BlastRadiusLimited condition seen: $([ $saw_limited = 1 ] && echo yes || echo no)."
record ""
(( max_active <= 1 )) && pass "never more than $max_active node under remediation" || fail "cap broken: $max_active at once"
(( done_count == 3 )) && pass "all 3 nodes fixed one after another" || fail "only $done_count/3 fixed"
(( saw_limited == 1 )) && pass "BlastRadiusLimited condition reported" || fail "BlastRadiusLimited never reported"
exit $FAILED
