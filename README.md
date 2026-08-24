# schedsim — kube-scheduler placement simulator

Answers "given these nodes and these workloads, where would the **real**
Kubernetes scheduler place things?" without a real cluster.

A single container (run via podman) bundles etcd, kube-apiserver,
kube-scheduler, kube-controller-manager (namespace + GC controllers only) and
the [KWOK](https://kwok.sigs.k8s.io/) controller, which fakes kubelets so
nodes go Ready and pods run without any real workloads. The `schedsim` Python
CLI feeds it node/workload definitions and reports the allocation.

## Quick start

```bash
./run.sh          # builds the image, starts the container, runs examples/demo.yaml
```

Requires: podman, python3, PyYAML (`pip install pyyaml`; JSON input works without it).

## Usage

```bash
python3 -m schedsim up                # start the control plane (localhost:6443)
python3 -m schedsim run input.yaml    # schedule and print the allocation
python3 -m schedsim down              # tear down
```

`run` flags: `--json` (machine-readable), `--keep` (leave objects in the
cluster for inspection), `--timeout N`. Exit code 2 if anything was
unschedulable.

## Input format

```yaml
nodes:
  - name: node-a
    cpu: "4"
    memory: 8Gi
    labels: {zone: eu-west-1a}
workloads:
  - name: web
    replicas: 2
    cpu: 500m
    memory: 512Mi
    nodeSelector: {zone: eu-west-1b}          # optional
    # also supported: affinity, topologySpreadConstraints,
    # priorityClassName, labels, image, limits
```

Raw Kubernetes manifests also work: any entry with `kind: Node` /
`kind: Pod` is passed through untouched (pods must tolerate the
`kwok.x-k8s.io/node=fake:NoSchedule` taint).

Example output:

```
WORKLOAD  POD        NODE    STATUS
web       web-0      node-c  Scheduled
db        db-0       node-b  Scheduled
too-big   too-big-0  —       Unschedulable: 0/3 nodes are available: 3 Insufficient cpu. ...
```

Unschedulable messages come verbatim from the real scheduler.

## Poking at the cluster directly

kubectl ships inside the image; the host needs nothing:

```bash
podman exec -it schedsim kubectl get nodes,pods -A -o wide
```

Or hit the API from the host: `https://127.0.0.1:6443` with bearer token
`schedsim-token` (self-signed cert — this is a local sim, auth is a formality).

## How it works

1. `schedsim run` creates a throwaway namespace, then Node objects annotated
   `kwok.x-k8s.io/node: fake` — the KWOK controller adopts them and marks
   them Ready (the CLI then strips the `not-ready` admission taint, standing
   in for the node lifecycle controller, which isn't running).
2. Workloads become Pods (replicas expanded to `name-0..n`) with resource
   requests; the untouched upstream kube-scheduler binds them.
3. The CLI polls until every pod is bound or unschedulable, prints the table,
   and deletes everything it created (unless `--keep`).

Component versions are pinned in `image/Dockerfile` (k8s v1.31.4,
etcd v3.5.17, kwok v0.6.1).

## License

[MIT](LICENSE)
