#!/bin/bash
# Starts the fake control plane (server/app.py: the run API plus the fake
# apiserver that stands in for etcd + kube-apiserver + kube-controller-manager
# + kwok) and the unmodified upstream kube-scheduler in one container. Exits
# if either dies.
set -euo pipefail

KUBECONFIG_PATH=/etc/kubernetes/admin.kubeconfig
mkdir -p /etc/kubernetes

# --- kubeconfig ----------------------------------------------------------
# Plain HTTP, no cert to generate: the fake apiserver doesn't enforce auth
# (same AlwaysAllow spirit the real one used to run with here), it just
# accepts a bearer token for realism. Points at the same process/port the
# run API listens on -- there's no separate apiserver to dial.
kubectl config set-cluster schedsim --server=http://127.0.0.1:8080 \
  --kubeconfig="$KUBECONFIG_PATH" >/dev/null
kubectl config set-credentials admin --token=schedsim-token --kubeconfig="$KUBECONFIG_PATH" >/dev/null
kubectl config set-context default --cluster=schedsim --user=admin --kubeconfig="$KUBECONFIG_PATH" >/dev/null
kubectl config use-context default --kubeconfig="$KUBECONFIG_PATH" >/dev/null
mkdir -p /root/.kube && cp "$KUBECONFIG_PATH" /root/.kube/config
export KUBECONFIG="$KUBECONFIG_PATH"

# --- fake control plane + run API (server/app.py, mounts fakeapi.py) -----
(cd /opt/schedsim && uvicorn server.app:app --host 0.0.0.0 --port 8080) \
  >/var/log/schedsim-api.log 2>&1 &

echo "waiting for fake apiserver..."
for i in $(seq 1 30); do
  kubectl get --raw /readyz >/dev/null 2>&1 && break
  sleep 1
  [ "$i" = 30 ] && { echo "fake apiserver never became ready"; cat /var/log/schedsim-api.log; exit 1; }
done
echo "fake apiserver ready"

# --- kube-scheduler (unmodified upstream binary) --------------------------
kube-scheduler \
  --kubeconfig="$KUBECONFIG_PATH" \
  --leader-elect=false \
  >/var/log/kube-scheduler.log 2>&1 &

echo "schedsim control plane up (fake apiserver, run API, kube-scheduler)"

# Exit when any component exits, so the container fails loudly.
wait -n
echo "a component exited; shutting down" >&2
exit 1
