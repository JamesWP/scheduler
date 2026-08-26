"""HTTP API wrapping simulate.run() for the schedsim CLI to drive.

A single POST /run streams newline-delimited JSON: one progress object per
line while the run is in flight, followed by a final {"phase": "done", ...}
or {"phase": "error", ...} line. The whole request stays open for the run's
full duration; there is no separate job id to reconnect to, so a client
disconnect aborts the run (the in-progress `finally` cleanup in simulate.run
still executes locally even though the write back to the client will fail).

This same app also serves the fake Kubernetes API (fakeapi.py) that stands
in for etcd + kube-apiserver + kube-controller-manager + kwok -- one process
is both "the cluster" that the unmodified kube-scheduler binary talks to and
the thing driving simulations against it. simulate.py calls fakeapi.py
directly (no HTTP, they're the same process); only the external
kube-scheduler and kubectl processes go through the router below.
"""

import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from . import fakeapi, simulate


@asynccontextmanager
async def lifespan(app: FastAPI):
    # simulate.py writes to fakeapi.STORE from a worker thread (see /run
    # below), but its watch queues belong to this event loop; publish()
    # needs the loop handle to hand cross-thread notifications to it safely.
    fakeapi.STORE.set_loop(asyncio.get_running_loop())
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(fakeapi.router)


class RunRequest(BaseModel):
    nodes: list = []
    workloads: list = []
    timeout: int = 30
    keep: bool = False
    progress: bool = True


@app.get("/healthz")
def healthz():
    # No separate apiserver process to check anymore -- being able to
    # answer this request at all means the whole control plane is up.
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
