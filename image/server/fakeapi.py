"""In-memory fake Kubernetes API server.

Stands in for etcd + kube-apiserver + kube-controller-manager + the KWOK
node/pod-lifecycle controller, so the only real Kubernetes binary left in
the image is the unmodified upstream kube-scheduler. It speaks just enough
of the real REST surface -- object CRUD, JSON [strategic-]merge patch,
list+watch, and the pods/binding subresource -- for a stock scheduler to
run its informers, bind pods, and report scheduling failures against it.

Resource types the scheduler's informers watch but that schedsim never
creates (PVs, PVCs, StorageClasses, CSI objects, controllers, PDBs) are
served as ordinary, permanently-empty collections: List returns `[]` and
Watch just idles, which is all a reflector needs to consider itself synced.

Mounted as a router into the same FastAPI app as /run (server/app.py), so
one process is both "the cluster" and the thing driving simulations
against it -- no separate apiserver process, no network hop.
"""

import asyncio
import itertools
import json
import time
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

router = APIRouter()

# (group, resource) -> (version, Kind, namespaced). group "" is the core API
# (served under /api/v1/...); every other group is served under
# /apis/{group}/{version}/....
REGISTRY = {
    ("", "nodes"): ("v1", "Node", False),
    ("", "namespaces"): ("v1", "Namespace", False),
    ("", "persistentvolumes"): ("v1", "PersistentVolume", False),
    ("", "pods"): ("v1", "Pod", True),
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


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class Store:
    """All objects, keyed by (group, resource) then by name (cluster-scoped)
    or "namespace/name" (namespaced). One global resourceVersion counter
    across every resource type, same as real etcd."""

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


# --- node/namespace/pod defaults, replacing kwok + the real controllers ----

def _hook_node(obj):
    """A real cluster needs a kubelet to report Ready; kwok faked that.
    Here we just mark every node Ready the instant it's created."""
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


# --- helpers -----------------------------------------------------------------

def _status(code, reason, message):
    return JSONResponse({"kind": "Status", "apiVersion": "v1", "status": "Failure",
                          "message": message, "reason": reason, "code": code},
                        status_code=code)


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


def _pointer_parts(path):
    return [p.replace("~1", "/").replace("~0", "~") for p in path.strip("/").split("/") if p != ""]


def _apply_json_patch(obj, ops):
    """Minimal RFC 6902 subset: add/replace/remove. No copy/move/test."""
    for op in ops:
        parts = _pointer_parts(op.get("path", ""))
        if not parts:
            continue
        *head, last = parts
        cur = obj
        for p in head:
            cur = cur[int(p)] if isinstance(cur, list) else cur.setdefault(p, {})
        if op["op"] in ("add", "replace"):
            if isinstance(cur, list):
                if last == "-":
                    cur.append(op["value"])
                else:
                    cur[int(last)] = op["value"]
            else:
                cur[last] = op["value"]
        elif op["op"] == "remove":
            if isinstance(cur, list):
                cur.pop(int(last))
            else:
                cur.pop(last, None)


def _truthy(v):
    return v in ("1", "true", "True")


def _dotted(obj, path):
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


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


def _field_match(obj, selector):
    for clause in filter(None, (c.strip() for c in selector.split(","))):
        neg = "!=" in clause
        sep = "!=" if neg else "="
        if sep not in clause:
            continue
        k, v = clause.split(sep, 1)
        if (str(_dotted(obj, k.strip())) == v.strip()) == neg:
            return False
    return True


def _apply_selectors(items, label_selector, field_selector):
    if label_selector:
        items = [o for o in items if _label_match(o.get("metadata", {}).get("labels", {}), label_selector)]
    if field_selector:
        items = [o for o in items if _field_match(o, field_selector)]
    return items


def _cascade_delete_namespace(namespace):
    """Stand-in for the namespace controller's garbage collection."""
    for (g, r), (_v, _k, namespaced) in REGISTRY.items():
        if not namespaced:
            continue
        for obj in STORE.list(g, r):
            if obj.get("metadata", {}).get("namespace") == namespace:
                STORE.delete(g, r, True, namespace, obj["metadata"]["name"])
                STORE.publish(g, r, "DELETED", obj)


# --- discovery -----------------------------------------------------------------

def _resource_entries(group):
    entries = [{"name": r, "namespaced": namespaced, "kind": kind,
                "verbs": ["get", "list", "watch", "create", "delete", "patch", "update"]}
               for (g, r), (_v, kind, namespaced) in REGISTRY.items() if g == group]
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
    groups = sorted({g for (g, _r) in REGISTRY if g != ""})
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


# --- CRUD --------------------------------------------------------------------

async def _list(request, group, resource, version, kind, namespaced, namespace):
    if _truthy(request.query_params.get("watch")):
        return StreamingResponse(_watch_stream(request, group, resource, namespace),
                                 media_type="application/json")
    items = STORE.list(group, resource)
    if namespace is not None:
        items = [o for o in items if o.get("metadata", {}).get("namespace") == namespace]
    items = _apply_selectors(items, request.query_params.get("labelSelector"),
                             request.query_params.get("fieldSelector"))
    api_version = version if not group else f"{group}/{version}"
    return JSONResponse({"kind": kind + "List", "apiVersion": api_version,
                          "metadata": {"resourceVersion": STORE.current_rv()}, "items": items})


async def _create(request, group, resource, version, kind, namespaced, namespace):
    try:
        body = await request.json()
    except ValueError:
        return _status(400, "BadRequest", "invalid JSON body")
    meta = body.setdefault("metadata", {})
    ns = namespace or meta.get("namespace") or ("default" if namespaced else None)
    name = meta.get("name")
    if not name:
        return _status(422, "Invalid", "metadata.name is required")
    if STORE.get(group, resource, namespaced, ns, name) is not None:
        return _status(409, "AlreadyExists", f'{kind} "{name}" already exists')
    api_version = version if not group else f"{group}/{version}"
    _stamp_new(body, kind, api_version, ns if namespaced else None, name)
    body["metadata"]["resourceVersion"] = STORE.next_rv()
    hook = POST_CREATE_HOOKS.get((group, resource))
    if hook:
        hook(body)
    STORE.put(group, resource, namespaced, ns if namespaced else None, name, body)
    STORE.publish(group, resource, "ADDED", body)
    return JSONResponse(body, status_code=201)


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
    try:
        body = await request.json()
    except ValueError:
        return _status(400, "BadRequest", "invalid patch/update body")
    content_type = request.headers.get("content-type", "")
    if replace:
        if subresource:
            obj[subresource] = body.get(subresource, body)
        else:
            body["metadata"] = obj["metadata"]  # identity survives a full replace
            obj.clear()
            obj.update(body)
    elif content_type.startswith("application/json-patch+json"):
        _apply_json_patch(obj, body)
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
    obj = STORE.delete(group, resource, namespaced, namespace, name)
    if obj is None:
        return _status(404, "NotFound", f'{kind} "{name}" not found')
    STORE.publish(group, resource, "DELETED", obj)
    if group == "" and resource == "namespaces":
        _cascade_delete_namespace(name)
    return JSONResponse(obj)


# --- routing -----------------------------------------------------------------

async def _dispatch(request, group, version, rest):
    if rest[0] == "namespaces" and len(rest) >= 3:
        namespace, resource, *tail = rest[1:]
    else:
        namespace, resource, tail = None, rest[0], rest[1:]

    spec = REGISTRY.get((group, resource))
    if not spec or spec[0] != version:
        return _status(404, "NotFound", f"the server could not find the requested resource ({resource})")
    _, kind, namespaced = spec
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
