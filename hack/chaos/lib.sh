#!/usr/bin/env bash
# Shared helpers for the chaos scenarios. Source this file; don't run it.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAKE_INFRA="${FAKE_INFRA:-http://localhost:8090}"
POLICY="${POLICY:-gpu-pool}"
NS=node-health-operator-system
LEASE=e74fdca7.nodehealth.dev
RESULTS="${RESULTS:-$HERE/../../docs/chaos-results.md}"
WORKERS=(nodehealth-worker nodehealth-worker2 nodehealth-worker3 nodehealth-worker4)

log()  { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
pass() { printf '  \033[32mPASS\033[0m %s\n' "$*"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAILED=1; }
FAILED=0

now()  { python3 -c 'import time; print(f"{time.time():.3f}")'; }

inject() { curl -fsS -X POST "$FAKE_INFRA/api/nodes/$1/fault?type=${2:-gpu}&sticky=${3:-false}" >/dev/null; }
heal()   { curl -fsS -X DELETE "$FAKE_INFRA/api/nodes/$1/fault" >/dev/null || true; }

# phase NODE -> remediation phase from the node annotation, or "-" if none.
phase() {
  kubectl get node "$1" -o jsonpath='{.metadata.annotations.remediation\.nodehealth\.dev/state}' |
    python3 -c 'import sys,json; d=sys.stdin.read().strip(); print(json.loads(d)["phase"] if d else "-")'
}
cordoned() { [ "$(kubectl get node "$1" -o jsonpath='{.spec.unschedulable}')" = "true" ]; }

# active_count -> nodes currently carrying a remediation state annotation.
active_count() {
  kubectl get nodes -o json | python3 -c '
import json,sys
print(sum(1 for n in json.load(sys.stdin)["items"]
          if "remediation.nodehealth.dev/state" in n["metadata"].get("annotations",{})))'
}

# leader -> full lease holder identity "<pod>_<uuid>". The uuid changes when
# a process restarts, even if the pod name stays the same.
leader() {
  kubectl -n "$NS" get lease "$LEASE" -o jsonpath='{.spec.holderIdentity}'
}

# wait_for TIMEOUT_S DESCRIPTION CMD... : poll CMD every second until it succeeds.
wait_for() {
  local timeout=$1 desc=$2; shift 2
  local start=$SECONDS
  until "$@"; do
    if (( SECONDS - start > timeout )); then fail "timed out after ${timeout}s waiting for: $desc"; return 1; fi
    sleep 1
  done
}

# event_offset NODE REASON T0 -> seconds from T0 to the first operator event
# with REASON on NODE at or after T0 (empty if none). Uses the events.k8s.io
# eventTime, which has microsecond precision.
event_offset() {
  kubectl get events.events.k8s.io -A -o json | python3 "$HERE/event_offset.py" "$1" "$2" "$3"
}

# reset_cluster: heal every worker and wait until none is under remediation.
reset_cluster() {
  for n in "${WORKERS[@]}"; do heal "$n"; done
  wait_for 300 "all workers Ready" all_ready
  # Clean up anything a scenario left behind (e.g. a Failed node).
  for n in "${WORKERS[@]}"; do
    if [ "$(phase "$n")" != "-" ]; then
      kubectl annotate node "$n" remediation.nodehealth.dev/state- remediation.nodehealth.dev/request- \
        remediation.nodehealth.dev/completed- >/dev/null
    fi
    kubectl uncordon "$n" >/dev/null
  done
  kubectl rollout status deploy/training-job --timeout=180s >/dev/null
}
all_ready() {
  kubectl get nodes --no-headers | awk '$2 != "Ready" && $2 != "Ready,SchedulingDisabled" {bad=1} END {exit bad}'
}

record() { echo "$*" >> "$RESULTS"; }
