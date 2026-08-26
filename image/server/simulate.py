"""Turn a simplified nodes/workloads spec into k8s objects, schedule, report.

Runs inside the control-plane container, talking to the local fake
apiserver (fakeapi.py). Every long-running step is a generator that
`yield`s progress dicts instead of printing — the API layer (app.py)
forwards those as NDJSON to the CLI, which is what actually renders them.
"""

import concurrent.futures
import time
import uuid

from .kube import KubeError

# Nodes are tainted so only pods that explicitly tolerate "the scheduler's
# playground isn't a real, workload-bearing node" land on them -- a real
# taint would come from a real kubelet; here it's just bookkeeping.
FAKE_NODE_TAINT = {"key": "schedsim.local/fake-node", "value": "true", "effect": "NoSchedule"}

# Each object is a separate apiserver round trip (~25ms), so a few thousand
# pods take a minute serially. The apiserver copes fine with a handful of
# concurrent writers.
WRITE_CONCURRENCY = 16


class SimError(Exception):
    """A run-ending condition the caller should report, not a bug."""


def _call_with_heartbeat(fn, label):
    """Run a single blocking call on a thread, yielding elapsed time while it's in flight."""
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        future = pool.submit(fn)
        started = time.time()
        while True:
            try:
                result = future.result(timeout=1)
                break
            except concurrent.futures.TimeoutError:
                yield {"phase": label, "elapsed": round(time.time() - started, 1)}
        yield {"phase": label, "elapsed": round(time.time() - started, 1), "done": True}
        return result


def _create_all(create, items, label, progress=True):
    """Run `create` over items concurrently, yielding progress dicts.

    `label` is the full progress phrase, e.g. "creating nodes" or "deleting pods".
    """
    if not items:
        return
    done = 0
    started = time.time()
    with concurrent.futures.ThreadPoolExecutor(WRITE_CONCURRENCY) as pool:
        for _ in pool.map(create, items):
            done += 1
            if progress and (done % 100 == 0 or done == len(items)):
                event = {"phase": label, "done": done, "total": len(items)}
                if done == len(items):
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


def run(client, config, timeout=30, keep=False):
    """Apply nodes + workloads, wait for scheduling, yield progress and a final result.

    The last event is always either {"phase": "done", "rows": [...], "kept": bool}
    or {"phase": "error", "message": ...}.
    """
    namespace = "sim-" + uuid.uuid4().hex[:8]
    client.create_namespace(namespace)
    created_nodes = []
    expected = []
    rows = []
    try:
        node_manifests = [n if n.get("kind") == "Node"  # raw manifest pass-through
                          else node_manifest(n) for n in config.get("nodes", [])]
        created_nodes = [n["metadata"]["name"] for n in node_manifests]
        try:
            yield from _create_all(client.create_node, node_manifests, "creating nodes")
        except KubeError as e:
            if "already exists" not in str(e):
                raise
            created_nodes = []  # don't delete nodes this run didn't create
            raise SimError(
                f"{e}\nnodes from an earlier run are still in the cluster; "
                f"reset with: python3 -m schedsim down && python3 -m schedsim up")
        _wait_for_nodes_ready(client, created_nodes, timeout)

        pods = []
        for workload in config.get("workloads", []):
            if workload.get("kind") == "Pod":  # raw manifest pass-through
                pods.append(workload)
            else:
                pods.extend(pod_manifests(workload))
        expected = [p["metadata"]["name"] for p in pods]
        yield from _create_all(lambda pod: client.create_pod(namespace, pod),
                               pods, "creating pods")

        rows = yield from _wait_for_scheduling(client, namespace, expected, timeout)
    finally:
        if keep:
            yield {"phase": "kept", "namespace": namespace}
        else:
            try:
                # Force-delete pods before removing their nodes. The fake
                # apiserver deletes immediately (no finalizers, no real
                # kubelet to wait on), so this settles fast; still confirmed
                # via _wait_for_pods_gone rather than assumed.
                yield from _create_all(lambda name: client.delete_pod(namespace, name),
                                       expected, "deleting pods")
                yield from _wait_for_pods_gone(client, namespace, timeout)
                yield from _call_with_heartbeat(lambda: client.delete_namespace(namespace),
                                                "deleting namespace")
                yield from _create_all(client.delete_node, created_nodes, "deleting nodes")
            except KubeError as e:
                # The fake apiserver's store is in-memory for the life of the
                # container, so only a rebuild (`down` then `up`) is a
                # guaranteed reset -- a plain restart keeps the leftovers.
                yield {"phase": "warning",
                       "message": f"cleanup incomplete ({e}); reset with: "
                                  f"python3 -m schedsim down && python3 -m schedsim up"}
    yield {"phase": "done", "rows": rows}


def _wait_for_nodes_ready(client, names, timeout):
    # The fake apiserver marks every node Ready the instant it's created
    # (server/fakeapi.py), so in practice this returns on the first poll;
    # it stays a poll rather than a flat assertion so a slow apiserver
    # write path degrades gracefully instead of racing it.
    deadline = time.time() + timeout
    names = set(names)
    while time.time() < deadline:
        ready = set()
        for node in client.list_nodes():
            for cond in node.get("status", {}).get("conditions", []):
                if cond["type"] == "Ready" and cond["status"] == "True":
                    ready.add(node["metadata"]["name"])
        if names <= ready:
            return
        time.sleep(0.5)
    raise SimError(f"nodes never became Ready: {sorted(names - ready)} "
                   "(check: podman logs schedsim)")


def _wait_for_pods_gone(client, namespace, timeout):
    deadline = time.time() + timeout
    remaining = None
    started = time.time()
    last_print = 0
    while time.time() < deadline:
        left = len(client.list_pods(namespace))
        if not left:
            if remaining is not None:
                yield {"phase": "pods terminated", "elapsed": round(time.time() - started, 1)}
            return
        if remaining is None or left < remaining:  # still draining, keep waiting
            remaining, deadline = left, time.time() + timeout
        now = time.time()
        if now - last_print >= 1:  # tick even when the count hasn't moved
            yield {"phase": "waiting for pods to terminate", "left": left,
                   "elapsed": round(now - started)}
            last_print = now
        time.sleep(0.5)
    raise KubeError(f"{remaining} pods in {namespace} did not terminate "
                    f"within {timeout}s")


def _wait_for_scheduling(client, namespace, expected, timeout):
    """Poll until every pod is bound or unschedulable.

    `timeout` is the budget for making *no* progress: a large scenario can
    take far longer than that overall, so the deadline is extended each time
    another pod settles.
    """
    deadline = time.time() + timeout
    best = 0
    rows = {}
    while time.time() < deadline:
        rows = {}
        settled = True
        for pod in client.list_pods(namespace):
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
