"""Minimal Kubernetes apiserver REST client (stdlib only)."""

import json
import ssl
import urllib.error
import urllib.request

TOKEN = "schedsim-token"


class KubeError(Exception):
    pass


class KubeClient:
    def __init__(self, server="https://127.0.0.1:6443", token=TOKEN):
        self.server = server.rstrip("/")
        self.token = token
        self.ctx = ssl.create_default_context()
        self.ctx.check_hostname = False
        self.ctx.verify_mode = ssl.CERT_NONE

    def request(self, method, path, body=None, timeout=10):
        req = urllib.request.Request(self.server + path, method=method)
        req.add_header("Authorization", f"Bearer {self.token}")
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            req.add_header("Content-Type",
                           "application/merge-patch+json" if method == "PATCH"
                           else "application/json")
        try:
            with urllib.request.urlopen(req, data=data, context=self.ctx,
                                        timeout=timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")
            try:
                detail = json.loads(detail).get("message", detail)
            except (ValueError, AttributeError):
                pass
            raise KubeError(f"{method} {path}: HTTP {e.code}: {detail}") from e
        except (urllib.error.URLError, OSError) as e:
            raise KubeError(f"{method} {path}: {e}") from e
        if not raw:
            return None
        try:
            return json.loads(raw)
        except ValueError:  # e.g. /readyz returns plain "ok"
            return raw.decode(errors="replace")

    def get(self, path):
        return self.request("GET", path)

    def post(self, path, body):
        return self.request("POST", path, body)

    def delete(self, path, body=None, timeout=10):
        return self.request("DELETE", path, body, timeout=timeout)

    def patch(self, path, body):
        return self.request("PATCH", path, body)

    def ready(self):
        try:
            self.request("GET", "/readyz")
            return True
        except KubeError:
            return False

    # --- convenience -------------------------------------------------------
    def create_node(self, node):
        return self.post("/api/v1/nodes", node)

    def create_namespace(self, name):
        return self.post("/api/v1/namespaces",
                         {"apiVersion": "v1", "kind": "Namespace",
                          "metadata": {"name": name}})

    def delete_namespace(self, name):
        return self.delete(f"/api/v1/namespaces/{name}")

    def create_pod(self, namespace, pod):
        return self.post(f"/api/v1/namespaces/{namespace}/pods", pod)

    def list_pods(self, namespace):
        return self.get(f"/api/v1/namespaces/{namespace}/pods")["items"]

    def delete_pods(self, namespace):
        """Delete every pod in the namespace in one call, no grace period."""
        # Deleting thousands of pods in one call takes a while server-side.
        return self.delete(f"/api/v1/namespaces/{namespace}/pods",
                           {"gracePeriodSeconds": 0}, timeout=300)

    def delete_node(self, name):
        return self.delete(f"/api/v1/nodes/{name}")

    def list_nodes(self):
        return self.get("/api/v1/nodes")["items"]
