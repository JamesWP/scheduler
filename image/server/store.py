"""Pure in-memory Kubernetes-object store.

No framework, no I/O, no threading/asyncio awareness -- just dicts, a few
stdlib modules, and a synchronous callback-based watch mechanism. Nothing
here knows this object store is served over HTTP, or that kube-scheduler
is the one calling it; it's importable and unit-testable on its own.

Two things sit on top of this module: fakeapi.py (the HTTP layer, speaking
the REST/watch surface kube-scheduler and kubectl actually expect) and
simulate.py (schedsim's own driver, calling the functions below directly
and in-process -- no HTTP hop for the data schedsim itself writes).

Resources split into two kinds:

- LIVE: nodes, namespaces, pods (+ the pods/binding and */status
  subresources). These are the ones schedsim actually creates, deletes and
  reads, and the ones kube-scheduler binds pods onto and reports failures
  against, so they get real CRUD, patch, and watch.
- STUB: everything else the scheduler's informers watch on startup (PVs,
  PVCs, StorageClasses, CSI objects, controllers, Services, PDBs, Events) so
  they can sync -- but that schedsim never populates. fakeapi.py serves
  these as permanently-empty, watchable collections; nothing here ever
  stores an object under one.
"""

import itertools
import time
import uuid


class AlreadyExists(Exception):
    pass


def now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class Store:
    """LIVE objects only, keyed by (group, resource) then by name
    (cluster-scoped) or "namespace/name" (namespaced). One global
    resourceVersion counter across every resource type, same as real etcd.

    Watching is a plain synchronous callback list: publish() calls each
    registered callback directly, on whatever thread called publish(). It
    has no opinion about asyncio, threads, or event loops -- a caller that
    needs this handed to an asyncio event loop safely (fakeapi.py's watch
    streams, since publish() can come from a worker thread) wraps its own
    callback accordingly; the store doesn't need to know that's happening.
    """

    def __init__(self):
        self._objects = {}
        self._rv = itertools.count(1)
        self._last_rv = 0
        self._watchers = {}

    def next_rv(self):
        self._last_rv = next(self._rv)
        return str(self._last_rv)

    def current_rv(self):
        return str(self._last_rv)

    def _bucket(self, group, resource):
        return self._objects.setdefault((group, resource), {})

    @staticmethod
    def _key(namespaced, namespace, name):
        return f"{namespace}/{name}" if namespaced else name

    def list(self, group, resource):
        return list(self._bucket(group, resource).values())

    def get(self, group, resource, namespaced, namespace, name):
        return self._bucket(group, resource).get(self._key(namespaced, namespace, name))

    def put(self, group, resource, namespaced, namespace, name, obj):
        self._bucket(group, resource)[self._key(namespaced, namespace, name)] = obj

    def delete(self, group, resource, namespaced, namespace, name):
        return self._bucket(group, resource).pop(self._key(namespaced, namespace, name), None)

    def watch(self, group, resource, callback):
        """Register callback(event) for every future event on this
        resource type. Returns a zero-arg function that unsubscribes."""
        watchers = self._watchers.setdefault((group, resource), [])
        watchers.append(callback)

        def unsubscribe():
            watchers.remove(callback)
        return unsubscribe

    def publish(self, group, resource, event_type, obj):
        event = {"type": event_type, "object": obj}
        for callback in list(self._watchers.get((group, resource), ())):
            callback(event)


STORE = Store()

# (group, resource) -> (version, Kind, namespaced)
LIVE = {
    ("", "nodes"): ("v1", "Node", False),
    ("", "namespaces"): ("v1", "Namespace", False),
    ("", "pods"): ("v1", "Pod", True),
}

# Watched by a stock scheduler's informers but never written by schedsim.
STUB = {
    ("", "persistentvolumes"): ("v1", "PersistentVolume", False),
    ("", "services"): ("v1", "Service", True),
    ("", "replicationcontrollers"): ("v1", "ReplicationController", True),
    ("", "persistentvolumeclaims"): ("v1", "PersistentVolumeClaim", True),
    ("", "events"): ("v1", "Event", True),
    ("apps", "replicasets"): ("v1", "ReplicaSet", True),
    ("apps", "statefulsets"): ("v1", "StatefulSet", True),
    ("policy", "poddisruptionbudgets"): ("v1", "PodDisruptionBudget", True),
    ("storage.k8s.io", "storageclasses"): ("v1", "StorageClass", False),
    ("storage.k8s.io", "csinodes"): ("v1", "CSINode", False),
    ("storage.k8s.io", "csidrivers"): ("v1", "CSIDriver", False),
    ("storage.k8s.io", "csistoragecapacities"): ("v1", "CSIStorageCapacity", True),
    ("events.k8s.io", "events"): ("v1", "Event", True),
}
# The scheduler tries to record FailedScheduling events; accept (and drop)
# them rather than making it log write errors. Nothing else is ever written
# to a stub collection.
STUB_WRITABLE = {"events"}


# --- object creation, shared by the direct driver API and fakeapi.py's POST

def stamp_new(obj, kind, api_version, namespace, name):
    meta = obj.setdefault("metadata", {})
    meta["name"] = name
    if namespace is not None:
        meta["namespace"] = namespace
    meta["uid"] = meta.get("uid") or str(uuid.uuid4())
    meta["creationTimestamp"] = meta.get("creationTimestamp") or now()
    meta.setdefault("labels", {})
    meta.setdefault("annotations", {})
    obj["kind"] = kind
    obj["apiVersion"] = api_version
    return obj


def _hook_node(obj):
    """A real cluster needs a kubelet to report Ready; kwok faked that.
    Here every node is just marked Ready the instant it's created."""
    status = obj.setdefault("status", {})
    conditions = [c for c in status.get("conditions", []) if c.get("type") != "Ready"]
    conditions.append({"type": "Ready", "status": "True", "reason": "FakeKubelet",
                        "message": "fake node, always ready", "lastHeartbeatTime": now(),
                        "lastTransitionTime": now()})
    status["conditions"] = conditions
    status.setdefault("phase", "Running")


def _hook_namespace(obj):
    obj.setdefault("status", {})["phase"] = "Active"


def _hook_pod(obj):
    obj.setdefault("status", {}).setdefault("phase", "Pending")
    # The real apiserver defaults an empty schedulerName to
    # "default-scheduler" at create time (k8s.io/api/core/v1/defaults.go).
    # kube-scheduler's own informer event handlers silently ignore any pod
    # whose schedulerName doesn't match theirs -- no error, no log line,
    # it just never gets scheduled -- so this default isn't optional.
    obj.setdefault("spec", {}).setdefault("schedulerName", "default-scheduler")


POST_CREATE_HOOKS = {
    ("", "nodes"): _hook_node,
    ("", "namespaces"): _hook_namespace,
    ("", "pods"): _hook_pod,
}


def create_object(group, resource, version, kind, namespaced, namespace, body):
    """The one place any LIVE object gets created. Applies defaults, the
    post-create hooks above, and publishes an ADDED watch event."""
    meta = body.setdefault("metadata", {})
    ns = namespace if namespaced else None
    if namespaced and ns is None:
        ns = meta.get("namespace") or "default"
    name = meta.get("name")
    if not name:
        raise ValueError("metadata.name is required")
    if STORE.get(group, resource, namespaced, ns, name) is not None:
        raise AlreadyExists(f'{kind} "{name}" already exists')
    api_version = version if not group else f"{group}/{version}"
    stamp_new(body, kind, api_version, ns, name)
    body["metadata"]["resourceVersion"] = STORE.next_rv()
    hook = POST_CREATE_HOOKS.get((group, resource))
    if hook:
        hook(body)
    STORE.put(group, resource, namespaced, ns, name, body)
    STORE.publish(group, resource, "ADDED", body)
    return body


def _cascade_delete_namespace(namespace):
    """Stand-in for the namespace controller's garbage collection."""
    for group, resource in LIVE:
        _, _kind, namespaced = LIVE[(group, resource)]
        if not namespaced:
            continue
        for obj in STORE.list(group, resource):
            if obj.get("metadata", {}).get("namespace") == namespace:
                STORE.delete(group, resource, True, namespace, obj["metadata"]["name"])
                STORE.publish(group, resource, "DELETED", obj)


def delete_object(group, resource, namespaced, namespace, name):
    """Delete + publish (+ cascade, for a namespace). Returns the deleted
    object, or None if it didn't exist -- deleting nothing is not an
    error here, callers decide what that means for them."""
    obj = STORE.get(group, resource, namespaced, namespace, name)
    if obj is None:
        return None
    STORE.delete(group, resource, namespaced, namespace, name)
    STORE.publish(group, resource, "DELETED", obj)
    if group == "" and resource == "namespaces":
        _cascade_delete_namespace(name)
    return obj


def merge_patch(dst, patch):
    """RFC 7386 JSON merge patch: recurse into shared dict keys, replace
    everything else (including lists) wholesale, drop keys set to null.
    Also good enough for the strategic-merge-patch and server-side-apply
    bodies the scheduler sends -- nothing here needs list-merge-by-key."""
    if not isinstance(patch, dict):
        return patch
    for k, v in patch.items():
        if v is None:
            dst.pop(k, None)
        elif isinstance(v, dict) and isinstance(dst.get(k), dict):
            merge_patch(dst[k], v)
        else:
            dst[k] = v
    return dst


def update_object(group, resource, namespaced, namespace, name, body, subresource=None, replace=False):
    """Apply a patch (merge_patch) or a full replace to a stored object,
    bump its resourceVersion, and publish a MODIFIED event. Returns the
    updated object, or None if it doesn't exist."""
    obj = STORE.get(group, resource, namespaced, namespace, name)
    if obj is None:
        return None
    if replace:
        if subresource:
            obj[subresource] = body.get(subresource, body)
        else:
            body["metadata"] = obj["metadata"]  # identity survives a full replace
            obj.clear()
            obj.update(body)
    elif subresource:
        # Body shape from client-go's typed Patch(..., subresource) call is
        # a full object with only that subresource populated, e.g.
        # {"status": {...}}; tolerate a bare subresource body too.
        merge_patch(obj.setdefault(subresource, {}), body.get(subresource, body))
    else:
        merge_patch(obj, body)
    obj["metadata"]["resourceVersion"] = STORE.next_rv()
    STORE.put(group, resource, namespaced, namespace, name, obj)
    STORE.publish(group, resource, "MODIFIED", obj)
    return obj


def bind_pod(namespace, name, node_name):
    """The pods/binding subresource: set spec.nodeName directly (there's
    no real kubelet to schedule around). Returns the updated pod, or None
    if it doesn't exist."""
    pod = STORE.get("", "pods", True, namespace, name)
    if pod is None:
        return None
    pod.setdefault("spec", {})["nodeName"] = node_name
    pod["metadata"]["resourceVersion"] = STORE.next_rv()
    STORE.put("", "pods", True, namespace, name, pod)
    STORE.publish("", "pods", "MODIFIED", pod)
    return pod


def matches_labels(labels, selector):
    for clause in filter(None, (c.strip() for c in selector.split(","))):
        if "!=" in clause:
            k, v = clause.split("!=", 1)
            if labels.get(k.strip()) == v.strip():
                return False
        elif "=" in clause:
            k, v = clause.split("=", 1)
            if labels.get(k.strip()) != v.strip():
                return False
        elif clause.startswith("!"):
            if clause[1:].strip() in labels:
                return False
        elif clause not in labels:
            return False
    return True


def list_objects(group, resource, namespace=None, label_selector=None):
    items = STORE.list(group, resource)
    if namespace is not None:
        items = [o for o in items if o.get("metadata", {}).get("namespace") == namespace]
    if label_selector:
        items = [o for o in items if matches_labels(o.get("metadata", {}).get("labels", {}), label_selector)]
    return items


# --- direct driver API -- what simulate.py calls, in-process, no HTTP -------

def create_node(node):
    return create_object("", "nodes", "v1", "Node", False, None, node)


def create_namespace(name):
    return create_object("", "namespaces", "v1", "Namespace", False, None, {"metadata": {"name": name}})


def create_pod(namespace, pod):
    return create_object("", "pods", "v1", "Pod", True, namespace, pod)


def list_nodes():
    return STORE.list("", "nodes")


def list_pods(namespace):
    return list_objects("", "pods", namespace)


def delete_node(name):
    return delete_object("", "nodes", False, None, name)


def delete_pod(namespace, name):
    return delete_object("", "pods", True, namespace, name)


def delete_namespace(name):
    return delete_object("", "namespaces", False, None, name)
