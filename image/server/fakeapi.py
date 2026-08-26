"""HTTP layer for the fake Kubernetes API server.

Speaks just enough of the real REST/watch surface -- object CRUD, JSON
[strategic-]merge/apply patch, list+watch, discovery, the pods/binding
subresource -- for an unmodified kube-scheduler binary and kubectl to
treat this like a real apiserver. Everything about *what* a Node/Pod/
Namespace is, how creating or patching one behaves, and what's watchable
lives in store.py, which has no idea this HTTP layer exists; this module
only parses requests, calls into store.py, and shapes HTTP responses.

Mounted as a router into the same FastAPI app as /run (server/app.py), so
one process is both "the cluster" that the external kube-scheduler and
kubectl processes talk to over real HTTP, and the thing driving
simulations against it (simulate.py calls store.py directly, no HTTP hop).
"""

import asyncio
import json
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

from . import store

router = APIRouter()


# --- HTTP helpers ------------------------------------------------------------

def _status(code, reason, message):
    return JSONResponse({"kind": "Status", "apiVersion": "v1", "status": "Failure",
                          "message": message, "reason": reason, "code": code},
                        status_code=code)


def _truthy(v):
    return v in ("1", "true", "True")


# --- discovery -----------------------------------------------------------------

def _resource_entries(group):
    entries = []
    for (g, r), (_v, kind, namespaced) in {**store.LIVE, **store.STUB}.items():
        if g != group:
            continue
        verbs = ["get", "list", "watch"]
        if (g, r) in store.LIVE:
            verbs += ["create", "delete", "patch", "update"]
        elif r in store.STUB_WRITABLE:
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
    groups = sorted({g for (g, _r) in store.STUB if g != ""})
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
    """Works identically for a LIVE resource (real events flow through) and
    a STUB one (store.py never publishes to those keys, so this just idles
    on keepalives) -- store.py doesn't distinguish the two, only this
    module's dispatch does.
    """
    queue = asyncio.Queue()
    loop = asyncio.get_running_loop()
    # store.publish() can be called from this loop's own coroutines (the
    # handlers below) or from a worker thread (simulate.py's writes, since
    # its sync /run handler -- and the generator it returns -- run in
    # FastAPI's threadpool, not on the loop). asyncio.Queue isn't
    # thread-safe: a bare put_nowait() from the wrong thread can sit for
    # seconds before this coroutine notices. call_soon_threadsafe is the
    # documented safe way to hand it to the loop from either kind of caller.
    unsubscribe = store.STORE.watch(group, resource, lambda event: loop.call_soon_threadsafe(queue.put_nowait, event))
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
        unsubscribe()


# --- LIVE CRUD ---------------------------------------------------------------

async def _list(request, group, resource, version, kind, namespaced, namespace):
    if _truthy(request.query_params.get("watch")):
        return StreamingResponse(_watch_stream(request, group, resource, namespace),
                                 media_type="application/json")
    items = store.list_objects(group, resource, namespace, request.query_params.get("labelSelector"))
    api_version = version if not group else f"{group}/{version}"
    return JSONResponse({"kind": kind + "List", "apiVersion": api_version,
                          "metadata": {"resourceVersion": store.STORE.current_rv()}, "items": items})


async def _create(request, group, resource, version, kind, namespaced, namespace):
    try:
        body = await request.json()
    except ValueError:
        return _status(400, "BadRequest", "invalid JSON body")
    try:
        obj = store.create_object(group, resource, version, kind, namespaced, namespace, body)
    except store.AlreadyExists as e:
        return _status(409, "AlreadyExists", str(e))
    except ValueError as e:
        return _status(422, "Invalid", str(e))
    return JSONResponse(obj, status_code=201)


async def _bind_pod(request, namespace, name):
    try:
        body = await request.json()
    except ValueError:
        body = {}
    pod = store.bind_pod(namespace, name, body.get("target", {}).get("name"))
    if pod is None:
        return _status(404, "NotFound", f'Pod "{name}" not found')
    body.setdefault("metadata", {"name": name, "namespace": namespace})
    body["kind"], body["apiVersion"] = "Binding", "v1"
    return JSONResponse(body, status_code=201)


async def _update(request, group, resource, namespaced, namespace, name, kind, subresource, replace):
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/json-patch+json"):
        return _status(415, "UnsupportedMediaType",
                       "this fake apiserver only understands merge-patch/"
                       "strategic-merge-patch/apply-patch, not JSON Patch")
    try:
        body = await request.json()
    except ValueError:
        return _status(400, "BadRequest", "invalid patch/update body")
    obj = store.update_object(group, resource, namespaced, namespace, name, body, subresource, replace)
    if obj is None:
        return _status(404, "NotFound", f'{kind} "{name}" not found')
    return JSONResponse(obj)


async def _delete(group, resource, namespaced, namespace, name, kind):
    obj = store.delete_object(group, resource, namespaced, namespace, name)
    if obj is None:
        return _status(404, "NotFound", f'{kind} "{name}" not found')
    return JSONResponse(obj)


async def _dispatch_live(request, group, version, resource, namespace, tail):
    _, kind, namespaced = store.LIVE[(group, resource)]
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
        obj = store.STORE.get(group, resource, namespaced, namespace, name)
        return JSONResponse(obj) if obj is not None else _status(404, "NotFound", f'{kind} "{name}" not found')
    if method == "DELETE":
        return await _delete(group, resource, namespaced, namespace, name, kind)
    if method in ("PATCH", "PUT"):
        return await _update(request, group, resource, namespaced, namespace, name, kind,
                             subresource, replace=(method == "PUT"))
    return _status(405, "MethodNotAllowed", f"{method} not supported")


# --- STUB collections: always empty, watchable, read-only (except events) --

async def _dispatch_stub(request, group, version, resource, namespace, tail):
    _, kind, namespaced = store.STUB[(group, resource)]
    if namespace is not None and not namespaced:
        return _status(404, "NotFound", f"{resource} is not a namespaced resource")
    method = request.method

    if tail:  # nothing is ever stored, so any single-object path is 404/405
        return (_status(404, "NotFound", f'{kind} "{tail[0]}" not found') if method == "GET"
                else _status(405, "MethodNotAllowed", f"{method} not supported"))

    if method == "GET":
        if _truthy(request.query_params.get("watch")):
            return StreamingResponse(_watch_stream(request, group, resource, namespace),
                                     media_type="application/json")
        api_version = version if not group else f"{group}/{version}"
        return JSONResponse({"kind": kind + "List", "apiVersion": api_version,
                              "metadata": {"resourceVersion": store.STORE.current_rv()}, "items": []})

    if method == "POST" and resource in store.STUB_WRITABLE:
        try:
            body = await request.json()
        except ValueError:
            return _status(400, "BadRequest", "invalid JSON body")
        meta = body.setdefault("metadata", {})
        name = meta.get("name") or meta.get("generateName", "event") + uuid.uuid4().hex[:8]
        api_version = version if not group else f"{group}/{version}"
        store.stamp_new(body, kind, api_version, namespace if namespaced else None, name)
        return JSONResponse(body, status_code=201)  # accepted, not retained

    return _status(405, "MethodNotAllowed", f"{method} not supported on a collection")


# --- routing -----------------------------------------------------------------

async def _dispatch(request, group, version, rest):
    if rest[0] == "namespaces" and len(rest) >= 3:
        namespace, resource, *tail = rest[1:]
    else:
        namespace, resource, tail = None, rest[0], rest[1:]

    if (group, resource) in store.LIVE and store.LIVE[(group, resource)][0] == version:
        return await _dispatch_live(request, group, version, resource, namespace, tail)
    if (group, resource) in store.STUB and store.STUB[(group, resource)][0] == version:
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
