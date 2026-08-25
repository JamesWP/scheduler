"""HTTP client for the in-container run API (image/server/app.py)."""

import json
import urllib.error
import urllib.request


class RunError(Exception):
    pass


def run(server, config, timeout=30, keep=False, progress=True):
    """POST /run and yield each NDJSON event as it streams in.

    The last event is always {"phase": "done", ...} or {"phase": "error", ...}.
    """
    body = json.dumps({
        "nodes": config.get("nodes", []),
        "workloads": config.get("workloads", []),
        "timeout": timeout,
        "keep": keep,
        "progress": progress,
    }).encode()
    req = urllib.request.Request(
        f"{server}/run", data=body, method="POST",
        headers={"Content-Type": "application/json"})
    try:
        # No read timeout: a large scenario can legitimately hold this
        # connection open for minutes, and the server-side phases already
        # enforce their own progress deadlines.
        with urllib.request.urlopen(req, timeout=None) as resp:
            for line in resp:
                line = line.strip()
                if line:
                    yield json.loads(line)
    except urllib.error.URLError as e:
        raise RunError(f"could not reach schedsim API at {server}: {e}") from e
