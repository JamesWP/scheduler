# schedsim — kube-scheduler placement simulator

Answers "given these nodes and these workloads, where would the **real**
Kubernetes scheduler place things?" without a real cluster.

A single container (run via podman) bundles the unmodified upstream
kube-scheduler binary with a small FastAPI service that is *both* the thing
driving the simulation *and* a fake, in-memory Kubernetes API server
standing in for etcd, kube-apiserver, kube-controller-manager, and the
[KWOK](https://kwok.sigs.k8s.io/) controller (which those used to need to
fake kubelets so nodes went Ready and pods "ran" without any real
workloads) — the fake apiserver just marks nodes Ready and binds pods
directly, no separate controller needed. The `schedsim` Python CLI is
presentation only: it parses the input file, POSTs it to that in-container
API, and renders the streamed progress and final allocation.

## Quick start

```bash
./run.sh          # builds the image, starts the container, runs examples/demo.yaml
```

Requires: podman, python3, PyYAML (`pip install pyyaml`; JSON input works without it).

## Usage

```bash
python3 -m schedsim up                # start the control plane (fake apiserver + run API on :8080)
python3 -m schedsim gen -o input.yaml # synthesise a scenario (see below)
python3 -m schedsim run input.yaml    # schedule and print a summary
python3 -m schedsim down              # tear down
```

`run` flags: `--json` (full per-pod results as JSON) or `--csv FILE` (full
per-pod results as CSV, plus the summary on stdout), `--keep` (leave objects
in the cluster for inspection), `--timeout N`. Exit code 2 if anything was
unschedulable. With neither `--json` nor `--csv`, only the summary prints,
with a hint to rerun with one of those flags for the full data.

`--timeout` (default 30s) is a *no-progress* budget, not a total runtime cap:
it resets every time another pod is placed, so a scenario with thousands of
pods just keeps going while progress is being made. Every phase — creating
nodes/pods, scheduling, and the deletion/cleanup that follows — streams live
progress to stderr for the whole run, not just the first half.

If a run is interrupted, its nodes are left behind and the next run fails
with `already exists`. The fake apiserver's object store is in-memory and
only lives as long as the container, but a container *restart* (as opposed
to `down`/`up`, which replaces it) keeps them — reset with
`python3 -m schedsim down && python3 -m schedsim up`.

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
`schedsim.local/fake-node=true:NoSchedule` taint).

Example output:

```
2/3 pods scheduled
  1  Unschedulable: 0/3 nodes are available: 3 Insufficient cpu. ...
  affected workloads: too-big (1)
2 nodes used (busiest: node-c with 1 pods)

for full per-pod results, rerun with --csv FILE or --json
```

Unschedulable messages come verbatim from the real scheduler; they're
preserved in full in the `--csv`/`--json` output even though the summary
only counts them.

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

Or hit the k8s API from the host directly: `http://127.0.0.1:8080` — plain
HTTP, and unauthenticated in practice (the fake apiserver accepts any bearer
token, same `AlwaysAllow` spirit a real apiserver would run this sim under).
It's the same port and the same process as the run API — same trust level
as the podman socket that already controls the container.

## How it works

The core logic (`image/server/`) runs *inside* the container as a single
FastAPI service that is both "the cluster" and the thing driving
simulations against it:

- `fakeapi.py` is a fake, in-memory Kubernetes API server: an in-memory
  store plus a FastAPI router mounted into the same app as `POST /run`.
  It only gives real CRUD/patch/watch to the three resource types anything
  actually writes to — Nodes, Namespaces, Pods (the *only* other thing left
  in the image is the unmodified upstream kube-scheduler binary, talking to
  these over plain HTTP exactly as it would to a real apiserver). There's
  no etcd, no real kube-apiserver, no kube-controller-manager, and no KWOK
  controller: `fakeapi.py` marks nodes Ready and cascades a namespace
  delete to everything in it itself, synchronously, as part of handling the
  write (standing in for the node-lifecycle and namespace controllers).
  Everything else the scheduler's informers watch on startup but this
  simulator never populates (PVs, PVCs, StorageClasses, CSI objects,
  Services, controllers, PDBs) is just served as a permanently-empty,
  watchable collection — enough for an informer to sync against, nothing more.
- `simulate.py` calls `fakeapi.py`'s store directly — plain Python function
  calls, not HTTP — since they run in the same process; only the external
  kube-scheduler and kubectl processes go through the HTTP router.
  `app.py` wires `POST /run` to `simulate.run()`.

Three things that only showed up testing against the real kube-scheduler
binary, not visible from the API shape alone: it defaults to sending
POST/PUT bodies (including the bind call) as protobuf, not JSON, so
`entrypoint.sh` passes `--kube-api-content-type=application/json` --
a standard client-go flag, not a binary patch; the real apiserver
defaults an unset `pod.spec.schedulerName` to `"default-scheduler"` on
create, which `fakeapi.py`'s pod-creation hook now does too, since a
scheduler silently ignores any pod addressed to a different name; and
its default client-side rate limit (`--kube-api-qps=50
--kube-api-burst=100`) exists to protect a *real* apiserver, which this
one doesn't need protecting from -- at the default rate a several-
thousand-pod scenario spends most of its wall time just waiting on its
own throttling, so `entrypoint.sh` raises both well past anything this
simulator would ever need.

The `POST /run` flow itself:

1. The CLI POSTs the parsed input to `POST /run`; the server creates a
   throwaway namespace, then Node objects tainted
   `schedsim.local/fake-node=true:NoSchedule` — the fake apiserver marks them
   Ready as part of handling the create, no separate controller involved.
2. Workloads become Pods (replicas expanded to `name-0..n`) with resource
   requests; the untouched upstream kube-scheduler binds them.
3. The server polls until every pod is bound or unschedulable, streaming
   newline-delimited JSON progress events back over the same connection, then
   deletes everything it created (unless `--keep`) and streams a final
   `{"phase": "done", "rows": [...]}` event.
4. The CLI renders each event as it arrives and, once `rows` shows up, prints
   the summary (or writes `--csv`/`--json`).

The kube-scheduler version is pinned in `image/Dockerfile` (v1.31.4).

## License

[MIT](LICENSE)
