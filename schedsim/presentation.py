"""Render progress events from apiclient.run() and format final results.

All of this used to live server-side as direct print()s; now the server only
emits structured {"phase": ..., ...} dicts and everything about *how* they
look on a terminal lives here.
"""

import csv
import sys
from collections import Counter


def print_progress(event):
    """Print one non-terminal progress event. ("done"/"error" are handled by the caller.)"""
    phase = event["phase"]
    if phase == "kept":
        namespace = event["namespace"]
        print(f"kept objects in namespace {namespace} "
              f"(inspect: podman exec schedsim kubectl -n {namespace} get pods -o wide)")
    elif phase == "warning":
        print(event["message"], file=sys.stderr)
    elif phase == "scheduling timed out":
        print(f"\n{event['message']} ({event['done']}/{event['total']} settled)",
              file=sys.stderr)
    elif "left" in event:  # waiting for pods to actually terminate
        print(f"\r{phase}: {event['left']} left ({event['elapsed']}s)",
              end="", file=sys.stderr, flush=True)
    elif phase == "pods terminated":
        print(f"\r{phase} ({event['elapsed']}s)", file=sys.stderr, flush=True)
    elif "total" in event:  # counted progress: creating/deleting nodes or pods, scheduling
        done, total = event["done"], event["total"]
        suffix = f" ({event['elapsed']}s)" if "elapsed" in event else ""
        end = "\n" if done >= total else ""
        print(f"\r{phase}: {done}/{total}{suffix}", end=end, file=sys.stderr, flush=True)
    elif "elapsed" in event:  # heartbeat over a single blocking call (e.g. deleting namespace)
        if event.get("done"):
            print(f"\r{phase}: done ({event['elapsed']}s)", file=sys.stderr, flush=True)
        else:
            print(f"\r{phase}... ({event['elapsed']:.0f}s)",
                  end="", file=sys.stderr, flush=True)
    # unrecognised event shapes are ignored rather than crashing the CLI


def write_csv(rows, path):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["workload", "pod", "node", "status"])
        writer.writeheader()
        writer.writerows(rows)


def print_summary(rows):
    total = len(rows)
    scheduled = [r for r in rows if r["node"]]
    unscheduled = [r for r in rows if not r["node"]]
    print(f"{len(scheduled)}/{total} pods scheduled")
    if unscheduled:
        for status, count in Counter(r["status"] for r in unscheduled).most_common():
            print(f"  {count}  {status}")
        by_workload = Counter(r["workload"] for r in unscheduled)
        top = ", ".join(f"{w} ({c})" for w, c in by_workload.most_common(5))
        more = f", +{len(by_workload) - 5} more" if len(by_workload) > 5 else ""
        print(f"  affected workloads: {top}{more}")
    node_counts = Counter(r["node"] for r in scheduled)
    if node_counts:
        busiest, count = node_counts.most_common(1)[0]
        print(f"{len(node_counts)} nodes used (busiest: {busiest} with {count} pods)")
