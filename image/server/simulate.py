"""Turn a simplified nodes/workloads spec into k8s objects, schedule, report.

Runs inside the control-plane container, in the same process as the fake
object store (store.py), and calls it directly -- plain Python function
calls for every write and poll this module does. The external
kube-scheduler binary, a separate OS process, reaches that same store
over real HTTP instead, through fakeapi.py.

Every long-running step is a generator that `yield`s progress dicts instead
of printing — the API layer (app.py) forwards those as NDJSON to the CLI,
which is what actually renders them.
"""

import time
import uuid

from . import store

# Nodes are tainted so only pods that explicitly tolerate "the scheduler's
# playground isn't a real, workload-bearing node" land on them -- a real
# taint would come from a real kubelet; here it's just bookkeeping.
FAKE_NODE_TAINT = {"key": "schedsim.local/fake-node", "value": "true", "effect": "NoSchedule"}


class SimError(Exception):
    """A run-ending condition the caller should report, not a bug."""


def _apply_all(create, items, label, progress=True):
    """Run `create` over items, yielding progress dicts every 100 items.

    `label` is the full progress phrase, e.g. "creating nodes" or "deleting
    pods". Each `create` is a plain in-memory dict write, so a simple loop
    keeps up fine -- no concurrency needed.
    """
    if not items:
        return
    started = time.time()
    total = len(items)
    for done, item in enumerate(items, 1):
        create(item)
        if progress and (done % 100 == 0 or done == total):
            event = {"phase": label, "done": done, "total": total}
            if done == total:
                event["elapsed"] = round(time.time() - started, 1)
            yield event


def node_manifest(spec):
    resources = {"cpu": str(spec.get("cpu", "4")),
                 "memory": str(spec.get("memory", "8Gi")),
                 "pods": str(spec.get("pods", "110"))}
    labels = {"type": "fake", "kubernetes.io/role": "agent",
              "kubernetes.io/hostname": spec["name"]}
    labels.update(spec.get("labels", {}))
    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": spec["name"],
            "labels": labels,
        },
        "spec": {"taints": [FAKE_NODE_TAINT]},
        "status": {"capacity": resources, "allocatable": dict(resources)},
    }


def pod_manifests(spec):
    replicas = int(spec.get("replicas", 1))
    requests = {}
    if "cpu" in spec:
        requests["cpu"] = str(spec["cpu"])
    if "memory" in spec:
        requests["memory"] = str(spec["memory"])
    pod_spec = {
        "containers": [{
            "name": "app",
            "image": spec.get("image", "registry.k8s.io/pause:3.9"),
            "resources": {"requests": requests, "limits": dict(spec.get("limits", {}))},
        }],
        "tolerations": [dict(FAKE_NODE_TAINT, operator="Equal")],
    }
    for key in ("nodeSelector", "affinity", "topologySpreadConstraints",
                "priorityClassName", "nodeName"):
        if key in spec:
            pod_spec[key] = spec[key]
    pods = []
    for i in range(replicas):
        pods.append({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": f"{spec['name']}-{i}",
                "labels": dict(spec.get("labels", {}), workload=spec["name"]),
            },
            "spec": pod_spec,
        })
    return pods


def run(config, timeout=30, keep=False):
    """Apply nodes + workloads, wait for scheduling, yield progress and a final result.

    The last event is always either {"phase": "done", "rows": [...], "kept": bool}
    or {"phase": "error", "message": ...}.
    """
    namespace = "sim-" + uuid.uuid4().hex[:8]
    store.create_namespace(namespace)
    created_nodes = []
    expected = []
    rows = []
    try:
        node_specs = [n if n.get("kind") == "Node"  # raw manifest pass-through
                      else node_manifest(n) for n in config.get("nodes", [])]
        created_nodes = [n["metadata"]["name"] for n in node_specs]
        try:
            yield from _apply_all(store.create_node, node_specs, "creating nodes")
        except store.AlreadyExists as e:
            created_nodes = []  # don't delete nodes this run didn't create
            raise SimError(
                f"{e}\nnodes from an earlier run are still in the cluster; "
                f"reset with: python3 -m schedsim down && python3 -m schedsim up")
        _wait_for_nodes_ready(created_nodes, timeout)

        pods = []
        for workload in config.get("workloads", []):
            if workload.get("kind") == "Pod":  # raw manifest pass-through
                pods.append(workload)
            else:
                pods.extend(pod_manifests(workload))
        expected = [p["metadata"]["name"] for p in pods]
        yield from _apply_all(lambda pod: store.create_pod(namespace, pod), pods, "creating pods")

        rows = yield from _wait_for_scheduling(namespace, expected, timeout)
    finally:
        if keep:
            yield {"phase": "kept", "namespace": namespace}
        else:
            # The fake apiserver deletes synchronously (no finalizers, no
            # real kubelet to wait on), so cleanup is just three plain loops.
            yield from _apply_all(lambda name: store.delete_pod(namespace, name),
                                  expected, "deleting pods")
            store.delete_namespace(namespace)
            yield from _apply_all(store.delete_node, created_nodes, "deleting nodes")
    yield {"phase": "done", "rows": rows}


def _wait_for_nodes_ready(names, timeout):
    # The fake apiserver marks every node Ready as part of handling the
    # create (server/store.py), so in practice this returns on the first
    # check; it stays a poll rather than a flat assertion so a future,
    # slower write path would degrade gracefully instead of racing it.
    deadline = time.time() + timeout
    names = set(names)
    while time.time() < deadline:
        ready = {n["metadata"]["name"] for n in store.list_nodes()
                 if any(c["type"] == "Ready" and c["status"] == "True"
                        for c in n.get("status", {}).get("conditions", []))}
        if names <= ready:
            return
        time.sleep(0.5)
    raise SimError(f"nodes never became Ready: {sorted(names - ready)} "
                   "(check: podman logs schedsim)")


def _wait_for_scheduling(namespace, expected, timeout):
    """Poll until every pod is bound or unschedulable.

    `timeout` is the budget for making *no* progress: a large scenario can
    take far longer than that overall, so the deadline is extended each time
    another pod settles. kube-scheduler is a separate process, so this is
    genuinely watching for external changes -- unlike the create/delete
    calls above, which happen synchronously in this same process.
    """
    deadline = time.time() + timeout
    best = 0
    rows = {}
    while time.time() < deadline:
        rows = {}
        settled = True
        for pod in store.list_pods(namespace):
            name = pod["metadata"]["name"]
            node = pod["spec"].get("nodeName")
            if node:
                rows[name] = {"pod": name, "node": node, "status": "Scheduled",
                              "workload": pod["metadata"]["labels"].get("workload", name)}
                continue
            reason = _unschedulable_reason(pod)
            if reason:
                rows[name] = {"pod": name, "node": None,
                              "status": f"Unschedulable: {reason}",
                              "workload": pod["metadata"]["labels"].get("workload", name)}
            else:
                settled = False
        if settled and len(rows) == len(expected):
            if best:
                yield {"phase": "scheduling", "done": len(expected), "total": len(expected)}
            break
        if len(rows) > best:
            best = len(rows)
            deadline = time.time() + timeout
            yield {"phase": "scheduling", "done": best, "total": len(expected)}
        time.sleep(0.5)
    else:
        yield {"phase": "scheduling timed out", "done": len(rows), "total": len(expected),
               "message": f"gave up after {timeout}s with no further progress"}
    for name in expected:
        rows.setdefault(name, {"pod": name, "node": None, "status": "Pending (timed out)",
                               "workload": name})
    return [rows[name] for name in expected]


def _unschedulable_reason(pod):
    for cond in pod.get("status", {}).get("conditions", []):
        if cond["type"] == "PodScheduled" and cond["status"] == "False":
            return cond.get("message") or cond.get("reason", "unknown")
    return None
