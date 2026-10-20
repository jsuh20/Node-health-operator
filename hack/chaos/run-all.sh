#!/usr/bin/env bash
# Run every chaos scenario and write docs/chaos-results.md.
# Needs: the kind cluster (make kind-up kind-deploy sample) and fake-infra
# running (make run-fake-infra).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/lib.sh"
set +e

curl -s -o /dev/null "$FAKE_INFRA/" || {
  echo "fake-infra is not reachable at $FAKE_INFRA (run: make run-fake-infra)"; exit 1; }

mkdir -p "$(dirname "$RESULTS")"
cat > "$RESULTS" <<HDR
# Chaos test results

Recorded $(date '+%Y-%m-%d %H:%M %Z') by \`make chaos\` on a local kind cluster
(1 control plane + 4 workers, Kubernetes $(kubectl version -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])')).
Policy: \`gpu-pool\` sample (threshold 60s, maxConcurrent 25% = 1 node).
All times are seconds after the fault was injected, taken from the operator's
own Kubernetes events (microsecond timestamps).

HDR

overall=0
for s in "$HERE"/0*.sh; do
  bash "$s"; rc=$?
  (( rc != 0 )) && overall=1
  echo
done
reset_cluster
echo "Results written to $RESULTS"
exit $overall
