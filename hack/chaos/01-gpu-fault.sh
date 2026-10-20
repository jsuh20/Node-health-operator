#!/usr/bin/env bash
# Scenario 1: a single GPU fault, end to end. Repeated TRIALS times.
# Checks: hysteresis respected (no cordon before the 60s threshold) and the
# node returns to service. Measures each step from the moment of injection.
source "$(dirname "$0")/lib.sh"
TRIALS=${TRIALS:-3}

log "Scenario 1: single GPU fault ($TRIALS trials)"
record "### 1. Single GPU fault ($TRIALS trials)"
record ""
record "| Trial | Node | Detected | Cordoned | Reboot requested | Back in service |"
record "|---|---|---|---|---|---|"
reset_cluster
for i in $(seq 1 "$TRIALS"); do
  node=${WORKERS[$(( (i - 1) % ${#WORKERS[@]} ))]}
  t0=$(now); inject "$node" gpu
  log "trial $i: GPU fault on $node"
  wait_for 300 "$node remediated" bash -c "[ -n \"\$(kubectl get events.events.k8s.io -A -o json | python3 '$HERE/event_offset.py' $node Remediated $t0)\" ]" || continue
  det=$(event_offset "$node" Detected "$t0"); cor=$(event_offset "$node" Cordoned "$t0")
  reb=$(event_offset "$node" RebootRequested "$t0"); rem=$(event_offset "$node" Remediated "$t0")
  record "| $i | $node | ${det}s | ${cor}s | ${reb}s | ${rem}s |"
  # Condition timestamps have 1-second resolution (the fault start is rounded
  # down), so allow 1s of slack when checking the 60s threshold.
  python3 -c "import sys; sys.exit(0 if float('$cor') >= 59 else 1)" \
    && pass "trial $i: cordoned at ${cor}s (threshold 60s respected), back in service at ${rem}s" \
    || fail "trial $i: cordoned at ${cor}s, before the 60s threshold"
  cordoned "$node" && fail "trial $i: $node still cordoned" || true
  sleep 5
done
record ""
exit $FAILED
