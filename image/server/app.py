"""HTTP API wrapping simulate.run() for the schedsim CLI to drive.

A single POST /run streams newline-delimited JSON: one progress object per
line while the run is in flight, followed by a final {"phase": "done", ...}
or {"phase": "error", ...} line. The whole request stays open for the run's
full duration; there is no separate job id to reconnect to, so a client
disconnect aborts the run (the in-progress `finally` cleanup in simulate.run
still executes locally even though the write back to the client will fail).

This same app also serves the fake Kubernetes API: fakeapi.py is the HTTP
layer, store.py underneath it is the actual in-memory object store, and
together they stand in for etcd + kube-apiserver + kube-controller-manager
+ kwok. One process is both "the cluster" that the unmodified kube-scheduler
binary talks to and the thing driving simulations against it: simulate.py
calls store.py directly, since they're the same process; the external
kube-scheduler and kubectl processes go through fakeapi.py's router instead.
"""

import json

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from . import fakeapi, simulate

app = FastAPI()
app.include_router(fakeapi.router)


class RunRequest(BaseModel):
    nodes: list = []
    workloads: list = []
    timeout: int = 30
    keep: bool = False
    progress: bool = True


@app.get("/healthz")
def healthz():
    # The fake apiserver runs in this same process, so answering this
    # request at all means the whole control plane is up.
    return {"status": "ok"}


@app.post("/run")
def run(req: RunRequest):
    config = {"nodes": req.nodes, "workloads": req.workloads}

    def stream():
        try:
            for event in simulate.run(config, timeout=req.timeout, keep=req.keep):
                if req.progress or event["phase"] in ("done", "error"):
                    yield json.dumps(event) + "\n"
        except Exception as e:
            # Any failure (expected SimError, or a bug) becomes a terminal
            # line rather than a silently truncated stream.
            yield json.dumps({"phase": "error", "message": str(e)}) + "\n"

    return StreamingResponse(stream(), media_type="application/x-ndjson")
