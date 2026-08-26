"""In-memory fake Kubernetes API server.

Stands in for etcd + kube-apiserver + kube-controller-manager + the KWOK
node/pod-lifecycle controller, so the only real Kubernetes binary left in
the image is the unmodified upstream kube-scheduler.

Two kinds of resource live here:

- LIVE: nodes, namespaces, pods (+ the pods/binding and */status
  subresources). These are the ones schedsim actually creates, deletes and
  reads, and the ones kube-scheduler binds pods onto and reports failures
  against, so they get real CRUD, patch, and watch.
- STUB: everything else the scheduler's informers watch on startup (PVs,
  PVCs, StorageClasses, CSI objects, controllers, Services, PDBs, Events) so
  they can sync -- but that schedsim never populates. These are served as
  permanently-empty, watchable collections: List always returns `[]`, Watch
  just idles. Events are additionally accepted (and discarded) on POST so
  the scheduler's FailedScheduling events don't error.

simulate.py, in the same process, calls the plain functions below (
create_node, create_pod, list_pods, ...) directly -- no HTTP hop for the
data schedsim itself writes. The FastAPI router further down is what the
*external* kube-scheduler and kubectl processes actually talk to over real
HTTP, and it's built on the exact same in-memory Store.
"""

import asyncio
import itertools
import json
import time
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

router = APIRouter()


class AlreadyExists(Exception):
    pass


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class Store:
    """LIVE objects only, keyed by (group, resource) then by name
    (cluster-scoped) or "namespace/name" (namespaced). One global
    resourceVersion counter across every resource type, same as real etcd."""

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

    def watchers(self, group, resource):
        return self._watchers.setdefault((group, resource), [])

    def publish(self, group, resource, event_type, obj):
        for q in list(self.watchers(group, resource)):
            q.put_nowait({"type": event_type, "object": obj})


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


# --- object creation, shared by simulate.py's direct calls and the HTTP layer

def _stamp_new(obj, kind, api_version, namespace, name):
    meta = obj.setdefault("metadata", {})
    meta["name"] = name
    if namespace is not None:
        meta["namespace"] = namespace
    meta["uid"] = meta.get("uid") or str(uuid.uuid4())
    meta["creationTimestamp"] = meta.get("creationTimestamp") or _now()
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
                        "message": "fake node, always ready", "lastHeartbeatTime": _now(),
                        "lastTransitionTime": _now()})
    status["conditions"] = conditions
    status.setdefault("phase", "Running")


def _hook_namespace(obj):
    obj.setdefault("status", {})["phase"] = "Active"


def _hook_pod(obj):
    obj.setdefault("status", {}).setdefault("phase", "Pending")


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
    _stamp_new(body, kind, api_version, ns, name)
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
    return [o for o in STORE.list("", "pods") if o.get("metadata", {}).get("namespace") == namespace]


def delete_node(name):
    obj = STORE.delete("", "nodes", False, None, name)
    if obj is not None:
        STORE.publish("", "nodes", "DELETED", obj)


def delete_pod(namespace, name):
    obj = STORE.delete("", "pods", True, namespace, name)
    if obj is not None:
        STORE.publish("", "pods", "DELETED", obj)


def delete_namespace(name):
    obj = STORE.delete("", "namespaces", False, None, name)
    if obj is not None:
        STORE.publish("", "namespaces", "DELETED", obj)
    _cascade_delete_namespace(name)


# --- HTTP helpers ------------------------------------------------------------

def _status(code, reason, message):
    return JSONResponse({"kind": "Status", "apiVersion": "v1", "status": "Failure",
                          "message": message, "reason": reason, "code": code},
                        status_code=code)


def _merge_patch(dst, patch):
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
            _merge_patch(dst[k], v)
        else:
            dst[k] = v
    return dst


def _truthy(v):
    return v in ("1", "true", "True")


def _label_match(labels, selector):
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


# --- discovery -----------------------------------------------------------------

def _resource_entries(group):
    entries = []
    for (g, r), (_v, kind, namespaced) in {**LIVE, **STUB}.items():
        if g != group:
            continue
        verbs = ["get", "list", "watch"]
        if (g, r) in LIVE:
            verbs += ["create", "delete", "patch", "update"]
        elif r in STUB_WRITABLE:
            verbs.append("create")
        entries.append({"name": r, "namespaced": namespaced, "kind": kind, "verbs": verbs})
    if group == "":
        entries += [
            {"name": "pods/binding", "namespaced": True, "kind": "Binding", "verbs": ["create"]},
            {"name": "pods/status", "namespaced": True, "kind": "Pod", "verbs": ["get", "patch", "update"]},
            {"name": "nodes/status", "namespaced": False, "kind": "Node", "verbs": ["get", "patch", "update"]},
        ]
    return entries


@router.get("/api")
def api_versions():
    return {"kind": "APIVersions", "versions": ["v1"],
            "serverAddressByClientCIDRs": [{"clientCIDR": "0.0.0.0/0", "serverAddress": ""}]}


@router.get("/apis")
def apis_root():
    groups = sorted({g for (g, _r) in STUB if g != ""})
    return {"kind": "APIGroupList", "apiVersion": "v1", "groups": [
        {"name": g, "versions": [{"groupVersion": f"{g}/v1", "version": "v1"}],
         "preferredVersion": {"groupVersion": f"{g}/v1", "version": "v1"}}
        for g in groups
    ]}


@router.get("/version")
def version_info():
    return {"major": "1", "minor": "31", "gitVersion": "v1.31.4", "gitCommit": "fake",
            "gitTreeState": "clean", "platform": "linux/amd64"}


@router.get("/readyz")
@router.get("/livez")
def readyz():
    return PlainTextResponse("ok")


# --- watch -----------------------------------------------------------------

async def _watch_stream(request, group, resource, namespace):
    queue = asyncio.Queue()
    STORE.watchers(group, resource).append(queue)
    try:
        while True:
            if await request.is_disconnected():
                return
            try:
                event = await asyncio.wait_for(queue.get(), timeout=15)
            except asyncio.TimeoutError:
                yield "\n"  # keepalive; the streaming JSON decoder skips whitespace
                continue
            obj = event["object"]
            if namespace is not None and obj.get("metadata", {}).get("namespace") != namespace:
                continue
            yield json.dumps(event) + "\n"
    finally:
        STORE.watchers(group, resource).remove(queue)


async def _idle_watch(request):
    """A watch on a STUB collection: nothing is ever published to it, so
    just hold the connection open, sending keepalives, until the client
    goes away."""
    while not await request.is_disconnected():
        await asyncio.sleep(15)
        yield "\n"


# --- LIVE CRUD ---------------------------------------------------------------

async def _list(request, group, resource, version, kind, namespaced, namespace):
    if _truthy(request.query_params.get("watch")):
        return StreamingResponse(_watch_stream(request, group, resource, namespace),
                                 media_type="application/json")
    items = STORE.list(group, resource)
    if namespace is not None:
        items = [o for o in items if o.get("metadata", {}).get("namespace") == namespace]
    label_selector = request.query_params.get("labelSelector")
    if label_selector:
        items = [o for o in items if _label_match(o.get("metadata", {}).get("labels", {}), label_selector)]
    api_version = version if not group else f"{group}/{version}"
    return JSONResponse({"kind": kind + "List", "apiVersion": api_version,
                          "metadata": {"resourceVersion": STORE.current_rv()}, "items": items})


async def _create(request, group, resource, version, kind, namespaced, namespace):
    try:
        body = await request.json()
    except ValueError:
        return _status(400, "BadRequest", "invalid JSON body")
    try:
        obj = create_object(group, resource, version, kind, namespaced, namespace, body)
    except AlreadyExists as e:
        return _status(409, "AlreadyExists", str(e))
    except ValueError as e:
        return _status(422, "Invalid", str(e))
    return JSONResponse(obj, status_code=201)


async def _bind_pod(request, namespace, name):
    try:
        body = await request.json()
    except ValueError:
        body = {}
    pod = STORE.get("", "pods", True, namespace, name)
    if pod is None:
        return _status(404, "NotFound", f'Pod "{name}" not found')
    pod.setdefault("spec", {})["nodeName"] = body.get("target", {}).get("name")
    pod["metadata"]["resourceVersion"] = STORE.next_rv()
    STORE.put("", "pods", True, namespace, name, pod)
    STORE.publish("", "pods", "MODIFIED", pod)
    body.setdefault("metadata", {"name": name, "namespace": namespace})
    body["kind"], body["apiVersion"] = "Binding", "v1"
    return JSONResponse(body, status_code=201)


async def _update(request, group, resource, namespaced, namespace, name, kind, subresource, replace):
    obj = STORE.get(group, resource, namespaced, namespace, name)
    if obj is None:
        return _status(404, "NotFound", f'{kind} "{name}" not found')
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/json-patch+json"):
        return _status(415, "UnsupportedMediaType",
                       "this fake apiserver only understands merge-patch/"
                       "strategic-merge-patch/apply-patch, not JSON Patch")
    try:
        body = await request.json()
    except ValueError:
        return _status(400, "BadRequest", "invalid patch/update body")
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
        _merge_patch(obj.setdefault(subresource, {}), body.get(subresource, body))
    else:
        _merge_patch(obj, body)
    obj["metadata"]["resourceVersion"] = STORE.next_rv()
    STORE.put(group, resource, namespaced, namespace, name, obj)
    STORE.publish(group, resource, "MODIFIED", obj)
    return JSONResponse(obj)


async def _delete(group, resource, namespaced, namespace, name, kind):
    obj = STORE.get(group, resource, namespaced, namespace, name)
    if obj is None:
        return _status(404, "NotFound", f'{kind} "{name}" not found')
    STORE.delete(group, resource, namespaced, namespace, name)
    STORE.publish(group, resource, "DELETED", obj)
    if group == "" and resource == "namespaces":
        _cascade_delete_namespace(name)
    return JSONResponse(obj)


async def _dispatch_live(request, group, version, resource, namespace, tail):
    _, kind, namespaced = LIVE[(group, resource)]
    if namespace is not None and not namespaced:
        return _status(404, "NotFound", f"{resource} is not a namespaced resource")
    name = tail[0] if tail else None
    subresource = tail[1] if len(tail) >= 2 else None
    method = request.method

    if name is None:
        if method == "GET":
            return await _list(request, group, resource, version, kind, namespaced, namespace)
        if method == "POST":
            return await _create(request, group, resource, version, kind, namespaced, namespace)
        return _status(405, "MethodNotAllowed", f"{method} not supported on a collection")

    if resource == "pods" and subresource == "binding" and method == "POST":
        return await _bind_pod(request, namespace, name)

    if method == "GET":
        obj = STORE.get(group, resource, namespaced, namespace, name)
        return JSONResponse(obj) if obj is not None else _status(404, "NotFound", f'{kind} "{name}" not found')
    if method == "DELETE":
        return await _delete(group, resource, namespaced, namespace, name, kind)
    if method in ("PATCH", "PUT"):
        return await _update(request, group, resource, namespaced, namespace, name, kind,
                             subresource, replace=(method == "PUT"))
    return _status(405, "MethodNotAllowed", f"{method} not supported")


# --- STUB collections: always empty, watchable, read-only (except events) --

async def _dispatch_stub(request, group, version, resource, namespace, tail):
    _, kind, namespaced = STUB[(group, resource)]
    if namespace is not None and not namespaced:
        return _status(404, "NotFound", f"{resource} is not a namespaced resource")
    method = request.method

    if tail:  # nothing is ever stored, so any single-object path is 404/405
        return (_status(404, "NotFound", f'{kind} "{tail[0]}" not found') if method == "GET"
                else _status(405, "MethodNotAllowed", f"{method} not supported"))

    if method == "GET":
        if _truthy(request.query_params.get("watch")):
            return StreamingResponse(_idle_watch(request), media_type="application/json")
        api_version = version if not group else f"{group}/{version}"
        return JSONResponse({"kind": kind + "List", "apiVersion": api_version,
                              "metadata": {"resourceVersion": STORE.current_rv()}, "items": []})

    if method == "POST" and resource in STUB_WRITABLE:
        try:
            body = await request.json()
        except ValueError:
            return _status(400, "BadRequest", "invalid JSON body")
        meta = body.setdefault("metadata", {})
        name = meta.get("name") or meta.get("generateName", "event") + uuid.uuid4().hex[:8]
        api_version = version if not group else f"{group}/{version}"
        _stamp_new(body, kind, api_version, namespace if namespaced else None, name)
        return JSONResponse(body, status_code=201)  # accepted, not retained

    return _status(405, "MethodNotAllowed", f"{method} not supported on a collection")


# --- routing -----------------------------------------------------------------

async def _dispatch(request, group, version, rest):
    if rest[0] == "namespaces" and len(rest) >= 3:
        namespace, resource, *tail = rest[1:]
    else:
        namespace, resource, tail = None, rest[0], rest[1:]

    if (group, resource) in LIVE and LIVE[(group, resource)][0] == version:
        return await _dispatch_live(request, group, version, resource, namespace, tail)
    if (group, resource) in STUB and STUB[(group, resource)][0] == version:
        return await _dispatch_stub(request, group, version, resource, namespace, tail)
    return _status(404, "NotFound", f"the server could not find the requested resource ({resource})")


@router.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def core_api(path: str, request: Request):
    parts = [p for p in path.split("/") if p]
    if not parts or parts[0] != "v1":
        return _status(404, "NotFound", "unsupported API path")
    rest = parts[1:]
    if not rest:
        return {"kind": "APIResourceList", "apiVersion": "v1", "groupVersion": "v1",
                "resources": _resource_entries("")}
    return await _dispatch(request, "", "v1", rest)


@router.api_route("/apis/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def grouped_api(path: str, request: Request):
    parts = [p for p in path.split("/") if p]
    if not parts:
        return apis_root()
    if len(parts) == 1:
        group = parts[0]
        if not _resource_entries(group):
            return _status(404, "NotFound", "unknown API group")
        return {"kind": "APIGroup", "apiVersion": "v1", "name": group,
                "versions": [{"groupVersion": f"{group}/v1", "version": "v1"}],
                "preferredVersion": {"groupVersion": f"{group}/v1", "version": "v1"}}
    group, version, *rest = parts
    entries = _resource_entries(group)
    if version != "v1" or not entries:
        return _status(404, "NotFound", "unknown API group/version")
    if not rest:
        return {"kind": "APIResourceList", "apiVersion": "v1",
                "groupVersion": f"{group}/{version}", "resources": entries}
    return await _dispatch(request, group, version, rest)
