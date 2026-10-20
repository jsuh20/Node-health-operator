#!/usr/bin/env bash
# Scripted demo: inject a GPU fault, watch the operator detect and fix it.
#
# Needs the cluster and fake-infra running (see README "Quick start").
# Record it:
#   brew install asciinema agg        # once
#   asciinema rec demo.cast -c hack/demo.sh
#   agg demo.cast docs/demo.gif
#
# With tmux installed, kubectl and the operator logs run in side-by-side
# panes. Without tmux, both streams are interleaved with colored prefixes.
set -euo pipefail
NODE=${NODE:-nodehealth-worker2}
FAKE_INFRA=${FAKE_INFRA:-http://localhost:8090}
NS=node-health-operator-system
HERE="$(cd "$(dirname "$0")" && pwd)"

leader_pod() {
  kubectl -n "$NS" get lease e74fdca7.nodehealth.dev -o jsonpath='{.spec.holderIdentity}' | cut -d_ -f1
}

if [ "${1:-}" != "--inner" ] && command -v tmux >/dev/null; then
  tmux new-session -d -s nhdemo -x 220 -y 50 "$HERE/demo.sh --inner; read"
  tmux split-window -h -t nhdemo "kubectl -n $NS logs -f $(leader_pod) --since=1s | grep --line-buffered -E 'Detected|remediation|Remediation|Drained|cap'"
  tmux select-pane -t nhdemo:0.0
  exec tmux attach -t nhdemo
fi

say() { printf '\n\033[1;36m# %s\033[0m\n' "$*"; sleep 1.5; }
run() { printf '\033[1m$ %s\033[0m\n' "$*"; eval "$@"; }

cleanup() { kill $(jobs -p) 2>/dev/null || true; }
trap cleanup EXIT

say "A 4-node GPU pool, all healthy. Policy: act after 60s, at most 25% (1 node) at a time."
run kubectl get nodes -L nodehealth.dev/pool
run kubectl get remediationpolicy gpu-pool

if ! command -v tmux >/dev/null || [ "${1:-}" != "--inner" ]; then
  say "Streaming operator logs (magenta) alongside kubectl (white)"
  kubectl -n "$NS" logs -f "$(leader_pod)" --since=1s 2>/dev/null |
    grep --line-buffered -E 'Detected|remediation|Remediation|Drained|cap' |
    sed -u $'s/^/\033[35m[operator]\033[0m /' &
fi

say "Inject a GPU fault on $NODE (fake-infra sets GPUHealthy=False, like an Xid error)"
run curl -fsS -X POST "'$FAKE_INFRA/api/nodes/$NODE/fault?type=gpu'"
echo

say "Detected at once, but hysteresis holds off for 60s: a blip would be ignored"
start=$SECONDS
last=""
while :; do
  st=$(kubectl get node "$NODE" -o jsonpath='{.metadata.annotations.remediation\.nodehealth\.dev/state}' |
    python3 -c 'import sys,json; d=sys.stdin.read().strip(); print(json.loads(d)["phase"] if d else "Healthy")')
  sched=$(kubectl get node "$NODE" -o jsonpath='{.spec.unschedulable}')
  gpu=$(kubectl get node "$NODE" -o jsonpath='{.status.conditions[?(@.type=="GPUHealthy")].status}')
  cur="phase=$st  cordoned=${sched:-false}  GPUHealthy=$gpu"
  if [ "$cur" != "$last" ]; then
    printf '\033[1m[kubectl]\033[0m t+%3ss  %s  %s\n' $((SECONDS - start)) "$NODE" "$cur"
    last=$cur
  fi
  if [ "$st" = Healthy ] && [ "$gpu" = True ] && [ $((SECONDS - start)) -gt 10 ]; then break; fi
  sleep 1
done

say "Back in service. The full story, from the operator's events:"
run "kubectl get events --field-selector involvedObject.name=$NODE --sort-by=.lastTimestamp \
  -o custom-columns=REASON:.reason,MESSAGE:.message | grep -vE 'NodeHas|NodeReady|NodeNotReady|Starting|Registered|Allocatable|NodeNotSchedulable|NodeSchedulable|Rebooted' | tail -5"
say "Done."
