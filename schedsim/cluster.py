"""Manage the schedsim control-plane container via podman."""

import subprocess
import sys
import time
import urllib.error
import urllib.request

IMAGE = "schedsim-image"
CONTAINER = "schedsim"
API = "http://127.0.0.1:8080"


def _podman(*args, check=True, capture=True):
    return subprocess.run(["podman", *args], check=check,
                          capture_output=capture, text=True)


def is_running():
    r = _podman("ps", "--filter", f"name=^{CONTAINER}$", "--format", "{{.Names}}",
                check=False)
    return CONTAINER in r.stdout.split()


def ready():
    try:
        urllib.request.urlopen(f"{API}/healthz", timeout=2)
        return True
    except (urllib.error.URLError, OSError):
        return False


def up(wait_seconds=90):
    """Start the container if needed and wait for its API to be ready.

    Returns the API's base URL for apiclient.run() to POST to.
    """
    if is_running():
        if ready():
            return API
    else:
        _podman("rm", "-f", CONTAINER, check=False)
        _podman("run", "-d", "--name", CONTAINER,
               "-p", "8080:8080", IMAGE)
    print("waiting for control plane...", file=sys.stderr)
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        if ready():
            return API
        if not is_running():
            logs = _podman("logs", CONTAINER, check=False)
            raise SystemExit(f"container exited during startup:\n{logs.stdout}{logs.stderr}")
        time.sleep(1)
    raise SystemExit(f"control plane not ready after {wait_seconds}s "
                     f"(check: podman logs {CONTAINER})")


def down():
    _podman("rm", "-f", CONTAINER, check=False)
