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
python3 -m schedsim gen -o input.yaml # synthesise a scenario (see below)
python3 -m schedsim run input.yaml    # schedule and print the allocation
python3 -m schedsim down              # tear down
```

`run` flags: `--json` (machine-readable), `--keep` (leave objects in the
cluster for inspection), `--timeout N`. Exit code 2 if anything was
unschedulable.

`--timeout` (default 30s) is a *no-progress* budget, not a total runtime cap:
it resets every time another pod is placed, so a scenario with thousands of
pods just keeps going while progress is being made. Creation and scheduling
progress is reported on stderr.

If a run is interrupted, its nodes are left behind and the next run fails
with `already exists`. etcd lives inside the container, so a restart keeps
them — reset with `python3 -m schedsim down && python3 -m schedsim up`.

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

## Generating scenarios

`schedsim gen` writes an input file (YAML, or `--json`) instead of you
hand-writing one:

```bash
python3 -m schedsim gen --nodes 100 --node-cpu 16 --workloads 500 --seed 1 -o big.yaml
python3 -m schedsim run big.yaml
```

Cluster shape: `--nodes N`, `--node-cpu` (processors per host),
`--node-memory` (defaults to `--memory-per-cpu`, 4Gi, times the CPU count),
`--zones N` to label nodes `zone-0..N-1`.

Workload shape: `--workloads N`, `--replicas SPEC` (instances per workload),
`--cpu SPEC` (demand per instance), `--memory-per-pod-cpu` (defaults to 2Gi
per CPU of demand), `--name-prefix`, `--seed` for reproducibility.

`SPEC` is a distribution; numbers accept k8s units where they're CPU values:

| spec | meaning |
| --- | --- |
| `fixed:V` | always V |
| `uniform:LO,HI` | uniform in [LO, HI] |
| `exp:MEAN` | exponential with that mean |
| `pareto:MIN,ALPHA` | power law, x ≥ MIN (ALPHA ≈ 1.2 is very heavy-tailed) |
| `mixture:BASE,P,MAX` | BASE with probability 1−P, else log-uniform in [BASE, MAX] |

`--replicas` defaults to `mixture:4,0.05`: most workloads are 4 pods, 5% run
away up to MAX, which defaults to 80% of the node count — the rare whale that
wants an instance on nearly every host. Replica counts are capped there and
per-instance CPU is capped at one node's CPU, so a generated pod is always
placeable in principle. Writing to `-o` prints a capacity-vs-demand summary
(also kept as a comment at the top of the file):

```
wrote big.yaml: 100 nodes (1600 cpu, 6400Gi), 500 workloads / 2453 pods
requesting 1226500m cpu (77%), 2453Gi memory (38%)
```

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
