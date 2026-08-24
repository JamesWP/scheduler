#!/bin/bash
# Starts etcd, kube-apiserver, kube-scheduler and the kwok controller in one
# container. Exits if any component dies.
set -euo pipefail

PKI=/etc/kubernetes/pki
KUBECONFIG_PATH=/etc/kubernetes/admin.kubeconfig
TOKEN=schedsim-token
mkdir -p "$PKI" /var/lib/etcd

# --- certs -------------------------------------------------------------------
if [ ! -f "$PKI/apiserver.crt" ]; then
  openssl genrsa -out "$PKI/ca.key" 2048 >/dev/null 2>&1
  openssl req -x509 -new -nodes -key "$PKI/ca.key" -subj "/CN=schedsim-ca" \
    -days 3650 -out "$PKI/ca.crt" >/dev/null 2>&1

  openssl genrsa -out "$PKI/apiserver.key" 2048 >/dev/null 2>&1
  cat > "$PKI/apiserver.cnf" <<'EOF'
[req]
distinguished_name = dn
req_extensions = ext
[dn]
[ext]
subjectAltName = DNS:localhost,DNS:kubernetes,DNS:kubernetes.default,IP:127.0.0.1,IP:10.96.0.1
EOF
  openssl req -new -key "$PKI/apiserver.key" -subj "/CN=kube-apiserver" \
    -config "$PKI/apiserver.cnf" -out "$PKI/apiserver.csr" >/dev/null 2>&1
  openssl x509 -req -in "$PKI/apiserver.csr" -CA "$PKI/ca.crt" -CAkey "$PKI/ca.key" \
    -CAcreateserial -days 3650 -extensions ext -extfile "$PKI/apiserver.cnf" \
    -out "$PKI/apiserver.crt" >/dev/null 2>&1

  # Service-account signing key (required by the apiserver even if unused).
  openssl genrsa -out "$PKI/sa.key" 2048 >/dev/null 2>&1
  openssl rsa -in "$PKI/sa.key" -pubout -out "$PKI/sa.pub" >/dev/null 2>&1

  echo "$TOKEN,admin,admin,system:masters" > /etc/kubernetes/tokens.csv
fi

# --- kubeconfig --------------------------------------------------------------
kubectl config set-cluster schedsim --server=https://127.0.0.1:6443 \
  --certificate-authority="$PKI/ca.crt" --embed-certs=true \
  --kubeconfig="$KUBECONFIG_PATH" >/dev/null
kubectl config set-credentials admin --token="$TOKEN" --kubeconfig="$KUBECONFIG_PATH" >/dev/null
kubectl config set-context default --cluster=schedsim --user=admin --kubeconfig="$KUBECONFIG_PATH" >/dev/null
kubectl config use-context default --kubeconfig="$KUBECONFIG_PATH" >/dev/null
mkdir -p /root/.kube && cp "$KUBECONFIG_PATH" /root/.kube/config
export KUBECONFIG="$KUBECONFIG_PATH"

# --- etcd --------------------------------------------------------------------
etcd --data-dir=/var/lib/etcd \
  --listen-client-urls=http://127.0.0.1:2379 \
  --advertise-client-urls=http://127.0.0.1:2379 \
  --listen-peer-urls=http://127.0.0.1:2380 \
  >/var/log/etcd.log 2>&1 &

# --- kube-apiserver ----------------------------------------------------------
kube-apiserver \
  --etcd-servers=http://127.0.0.1:2379 \
  --secure-port=6443 \
  --bind-address=0.0.0.0 \
  --tls-cert-file="$PKI/apiserver.crt" \
  --tls-private-key-file="$PKI/apiserver.key" \
  --client-ca-file="$PKI/ca.crt" \
  --token-auth-file=/etc/kubernetes/tokens.csv \
  --authorization-mode=AlwaysAllow \
  --service-account-key-file="$PKI/sa.pub" \
  --service-account-signing-key-file="$PKI/sa.key" \
  --service-account-issuer=https://kubernetes.default.svc \
  --service-cluster-ip-range=10.96.0.0/16 \
  --disable-admission-plugins=ServiceAccount \
  >/var/log/kube-apiserver.log 2>&1 &

echo "waiting for apiserver..."
for i in $(seq 1 60); do
  kubectl get --raw /readyz >/dev/null 2>&1 && break
  sleep 1
  [ "$i" = 60 ] && { echo "apiserver never became ready"; cat /var/log/kube-apiserver.log; exit 1; }
done
echo "apiserver ready"

# --- kube-scheduler ----------------------------------------------------------
kube-scheduler \
  --kubeconfig="$KUBECONFIG_PATH" \
  --leader-elect=false \
  >/var/log/kube-scheduler.log 2>&1 &

# --- kube-controller-manager (namespace + GC only, so deletions complete) ---
kube-controller-manager \
  --kubeconfig="$KUBECONFIG_PATH" \
  --controllers=namespace-controller,garbage-collector-controller \
  --leader-elect=false \
  >/var/log/kube-controller-manager.log 2>&1 &

# --- kwok controller ---------------------------------------------------------
KWOK_STAGE_ARGS=()
for f in /etc/kwok/stages/*.yaml; do
  KWOK_STAGE_ARGS+=(--config="$f")
done
kwok \
  --kubeconfig="$KUBECONFIG_PATH" \
  --manage-all-nodes=true \
  --node-ip=10.0.0.1 \
  --cidr=10.0.0.0/24 \
  "${KWOK_STAGE_ARGS[@]}" \
  >/var/log/kwok.log 2>&1 &

echo "schedsim control plane up (etcd, kube-apiserver, kube-scheduler, kwok)"

# Exit when any component exits, so the container fails loudly.
wait -n
echo "a component exited; shutting down" >&2
exit 1
