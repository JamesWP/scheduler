"""Manage the schedsim control-plane container via podman."""

import subprocess
import sys
import time

from .kube import KubeClient

IMAGE = "schedsim-image"
CONTAINER = "schedsim"


def _podman(*args, check=True, capture=True):
    return subprocess.run(["podman", *args], check=check,
                          capture_output=capture, text=True)


def is_running():
    r = _podman("ps", "--filter", f"name=^{CONTAINER}$", "--format", "{{.Names}}",
                check=False)
    return CONTAINER in r.stdout.split()


def up(wait_seconds=90):
    if is_running():
        client = KubeClient()
        if client.ready():
            return client
    else:
        _podman("rm", "-f", CONTAINER, check=False)
        _podman("run", "-d", "--name", CONTAINER, "-p", "6443:6443", IMAGE)
    client = KubeClient()
    print("waiting for control plane...", file=sys.stderr)
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        if client.ready():
            return client
        if not is_running():
            logs = _podman("logs", CONTAINER, check=False)
            raise SystemExit(f"container exited during startup:\n{logs.stdout}{logs.stderr}")
        time.sleep(1)
    raise SystemExit(f"control plane not ready after {wait_seconds}s "
                     f"(check: podman logs {CONTAINER})")


def down():
    _podman("rm", "-f", CONTAINER, check=False)
