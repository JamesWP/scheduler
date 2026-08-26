"""Minimal Kubernetes apiserver REST client (stdlib only).

Talks to the fake apiserver (fakeapi.py) mounted in this same process --
plain HTTP on localhost, no TLS to set up and nothing enforcing the bearer
token, but it's still sent so a `podman exec ... kubectl` using the same
kubeconfig looks like a normal authenticated client.
"""

import json
import urllib.error
import urllib.request

TOKEN = "schedsim-token"


class KubeError(Exception):
    pass


class KubeClient:
    def __init__(self, server="http://127.0.0.1:8080", token=TOKEN):
        self.server = server.rstrip("/")
        self.token = token

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
            with urllib.request.urlopen(req, data=data, timeout=timeout) as resp:
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

    def delete_pod(self, namespace, name):
        """Force-delete one pod, no grace period."""
        return self.delete(f"/api/v1/namespaces/{namespace}/pods/{name}",
                           {"gracePeriodSeconds": 0})

    def delete_node(self, name):
        return self.delete(f"/api/v1/nodes/{name}")

    def list_nodes(self):
        return self.get("/api/v1/nodes")["items"]
