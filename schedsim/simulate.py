"""Turn a simplified nodes/workloads spec into k8s objects, schedule, report."""

import concurrent.futures
import sys
import time
import uuid

from .kube import KubeError

KWOK_TAINT = {"key": "kwok.x-k8s.io/node", "value": "fake", "effect": "NoSchedule"}

# Each object is a separate apiserver round trip (~25ms), so a few thousand
# pods take a minute serially. The apiserver copes fine with a handful of
# concurrent writers.
WRITE_CONCURRENCY = 16


def _call_with_heartbeat(fn, label):
    """Run a single blocking call on a thread, printing elapsed time while it's in flight."""
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        future = pool.submit(fn)
        started = time.time()
        while True:
            try:
                result = future.result(timeout=1)
                break
            except concurrent.futures.TimeoutError:
                print(f"\r{label}... ({time.time() - started:.0f}s)",
                      end="", file=sys.stderr, flush=True)
        print(f"\r{label}: done ({time.time() - started:.1f}s)",
              file=sys.stderr, flush=True)
        return result


def _create_all(create, items, label, progress=True):
    """Run `create` over items concurrently, reporting progress on stderr.

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
                print(f"\r{label}: {done}/{len(items)}",
                      end="" if done < len(items) else
                      f" ({time.time() - started:.1f}s)\n", file=sys.stderr, flush=True)


def node_manifest(spec):
    resources = {"cpu": str(spec.get("cpu", "4")),
                 "memory": str(spec.get("memory", "8Gi")),
                 "pods": str(spec.get("pods", "110"))}
    labels = {"type": "kwok", "kubernetes.io/role": "agent",
              "kubernetes.io/hostname": spec["name"]}
    labels.update(spec.get("labels", {}))
    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": spec["name"],
            "labels": labels,
            "annotations": {"kwok.x-k8s.io/node": "fake"},
        },
        "spec": {"taints": [KWOK_TAINT]},
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
        "tolerations": [dict(KWOK_TAINT, operator="Equal")],
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
    """Apply nodes + workloads, wait for scheduling, return allocation rows."""
    namespace = "sim-" + uuid.uuid4().hex[:8]
    client.create_namespace(namespace)
    created_nodes = []
    expected = []
    try:
        node_manifests = [n if n.get("kind") == "Node"  # raw manifest pass-through
                          else node_manifest(n) for n in config.get("nodes", [])]
        created_nodes = [n["metadata"]["name"] for n in node_manifests]
        try:
            _create_all(client.create_node, node_manifests, "creating nodes")
        except KubeError as e:
            if "already exists" not in str(e):
                raise
            created_nodes = []  # don't delete nodes this run didn't create
            raise SystemExit(
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
        _create_all(lambda pod: client.create_pod(namespace, pod), pods, "creating pods")

        rows = _wait_for_scheduling(client, namespace, expected, timeout)
    finally:
        if keep:
            print(f"kept objects in namespace {namespace} "
                  f"(inspect: podman exec schedsim kubectl -n {namespace} get pods -o wide)")
        else:
            try:
                # Force-delete pods and wait for them to actually go before
                # removing their nodes, otherwise stragglers hang in
                # Terminating (kwok only finalizes pods on live nodes).
                _create_all(lambda name: client.delete_pod(namespace, name),
                            expected, "deleting pods")
                _wait_for_pods_gone(client, namespace, timeout)
                _call_with_heartbeat(lambda: client.delete_namespace(namespace),
                                    "deleting namespace")
                _create_all(client.delete_node, created_nodes, "deleting nodes")
            except KubeError as e:
                # etcd lives in the container, so only a rebuild is a
                # guaranteed reset -- a restart keeps the leftovers.
                print(f"cleanup incomplete ({e}); reset with: "
                      f"python3 -m schedsim down && python3 -m schedsim up",
                      file=sys.stderr)
    return rows


def _wait_for_nodes_ready(client, names, timeout):
    deadline = time.time() + timeout
    names = set(names)
    while time.time() < deadline:
        ready = set()
        for node in client.list_nodes():
            for cond in node.get("status", {}).get("conditions", []):
                if cond["type"] == "Ready" and cond["status"] == "True":
                    ready.add(node["metadata"]["name"])
        if names <= ready:
            # The TaintNodesByCondition admission plugin taints new nodes
            # not-ready; with no kube-controller-manager running, nothing
            # removes it, so do the node lifecycle controller's job here.
            for name in sorted(names):
                client.patch(f"/api/v1/nodes/{name}",
                             {"spec": {"taints": [KWOK_TAINT]}})
            return
        time.sleep(0.5)
    raise SystemExit(f"nodes never became Ready: {sorted(names - ready)} "
                     "(is the kwok controller running? podman logs schedsim)")


def _wait_for_pods_gone(client, namespace, timeout):
    deadline = time.time() + timeout
    remaining = None
    started = time.time()
    last_print = 0
    while time.time() < deadline:
        left = len(client.list_pods(namespace))
        if not left:
            if remaining is not None:
                print(f"\rpods terminated ({time.time() - started:.1f}s)",
                      file=sys.stderr, flush=True)
            return
        if remaining is None or left < remaining:  # still draining, keep waiting
            remaining, deadline = left, time.time() + timeout
        now = time.time()
        if now - last_print >= 1:  # tick even when the count hasn't moved
            print(f"\rwaiting for pods to terminate: {left} left "
                  f"({now - started:.0f}s)", end="", file=sys.stderr, flush=True)
            last_print = now
        time.sleep(0.5)
    print(file=sys.stderr)
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
                print(f"\rscheduling: {len(expected)}/{len(expected)}",
                      file=sys.stderr)
            break
        if len(rows) > best:
            best = len(rows)
            deadline = time.time() + timeout
            print(f"\rscheduling: {best}/{len(expected)}", end="",
                  file=sys.stderr, flush=True)
        time.sleep(0.5)
    else:
        print(f"\rgave up after {timeout}s with no further progress "
              f"({len(rows)}/{len(expected)} settled)", file=sys.stderr)
    for name in expected:
        rows.setdefault(name, {"pod": name, "node": None, "status": "Pending (timed out)",
                               "workload": name})
    return [rows[name] for name in expected]


def _unschedulable_reason(pod):
    for cond in pod.get("status", {}).get("conditions", []):
        if cond["type"] == "PodScheduled" and cond["status"] == "False":
            return cond.get("message") or cond.get("reason", "unknown")
    return None
