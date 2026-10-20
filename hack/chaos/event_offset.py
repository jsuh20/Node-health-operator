"""Print seconds from T0 to the first operator event REASON on NODE.

Usage: kubectl get events.events.k8s.io -A -o json | python3 event_offset.py NODE REASON T0
"""
import json
import sys
from datetime import datetime

node, reason, t0 = sys.argv[1], sys.argv[2], float(sys.argv[3])
times = []
for e in json.load(sys.stdin)["items"]:
    if e.get("reportingController") != "node-health-operator":
        continue
    if e["regarding"]["name"] != node or e["reason"] != reason:
        continue
    ts = e.get("eventTime") or e["metadata"]["creationTimestamp"]
    t = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    if t >= t0 - 0.5:
        times.append(t)
if times:
    print(f"{min(times) - t0:.1f}")
