#!/usr/bin/env bash
# Scenario 2: hysteresis. A 20s GPU blip must never trigger remediation.
source "$(dirname "$0")/lib.sh"

log "Scenario 2: transient 20s blip"
reset_cluster
node=nodehealth-worker3
t0=$(now); inject "$node" gpu; sleep 20; heal "$node"
log "blip over; watching for 70s more"
touched=0
for _ in $(seq 70); do
  if cordoned "$node" || [ "$(phase "$node")" != "-" ]; then touched=1; fi
  sleep 1
done
det=$(event_offset "$node" Detected "$t0"); cor=$(event_offset "$node" Cordoned "$t0")
record "### 2. Transient 20s blip"
record ""
if [ "$touched" = 0 ] && [ -z "$cor" ]; then
  pass "blip detected at ${det}s but never acted on"
  record "Detected after ${det}s; never cordoned or drained. **Pass.**"
else
  fail "node was touched by a 20s blip"
  record "Node was cordoned by a 20s blip. **Fail.**"
fi
record ""
exit $FAILED
