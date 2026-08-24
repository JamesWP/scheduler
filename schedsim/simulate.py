"""Turn a simplified nodes/workloads spec into k8s objects, schedule, report."""

import time
import uuid

from .kube import KubeError

KWOK_TAINT = {"key": "kwok.x-k8s.io/node", "value": "fake", "effect": "NoSchedule"}


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
    try:
        for node in config.get("nodes", []):
            if node.get("kind") == "Node":  # raw manifest pass-through
                manifest = node
            else:
                manifest = node_manifest(node)
            client.create_node(manifest)
            created_nodes.append(manifest["metadata"]["name"])
        _wait_for_nodes_ready(client, created_nodes, timeout)

        expected = []
        for workload in config.get("workloads", []):
            if workload.get("kind") == "Pod":  # raw manifest pass-through
                pods = [workload]
            else:
                pods = pod_manifests(workload)
            for pod in pods:
                client.create_pod(namespace, pod)
                expected.append(pod["metadata"]["name"])

        rows = _wait_for_scheduling(client, namespace, expected, timeout)
    finally:
        if keep:
            print(f"kept objects in namespace {namespace} "
                  f"(inspect: podman exec schedsim kubectl -n {namespace} get pods -o wide)")
        else:
            try:
                # Force-delete pods before their nodes go away, otherwise they
                # hang in Terminating (kwok only finalizes pods on live nodes).
                for pod in client.list_pods(namespace):
                    client.delete(
                        f"/api/v1/namespaces/{namespace}/pods/{pod['metadata']['name']}",
                        {"gracePeriodSeconds": 0})
                client.delete_namespace(namespace)
                for name in created_nodes:
                    client.delete_node(name)
            except KubeError:
                pass
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


def _wait_for_scheduling(client, namespace, expected, timeout):
    deadline = time.time() + timeout
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
            break
        time.sleep(0.5)
    for name in expected:
        rows.setdefault(name, {"pod": name, "node": None, "status": "Pending (timed out)",
                               "workload": name})
    return [rows[name] for name in expected]


def _unschedulable_reason(pod):
    for cond in pod.get("status", {}).get("conditions", []):
        if cond["type"] == "PodScheduled" and cond["status"] == "False":
            return cond.get("message") or cond.get("reason", "unknown")
    return None
