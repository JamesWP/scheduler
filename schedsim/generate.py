"""Generate synthetic nodes/workloads specs for `schedsim run`.

Sizes (replica counts, per-instance CPU) come from small distribution
specs so a scenario can be anything from uniform to heavy-tailed: mostly
tiny workloads with a rare one that spans most of the cluster.
"""

import random

# --- units ------------------------------------------------------------------

_MEM_UNITS = {"": 1, "k": 10**3, "M": 10**6, "G": 10**9, "T": 10**12,
              "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40}


def parse_cpu(value):
    """'500m' | '2' | 1500 -> millicores (int)."""
    text = str(value).strip()
    if text.endswith("m"):
        return int(float(text[:-1]))
    return int(round(float(text) * 1000))


def format_cpu(millis):
    millis = max(1, int(round(millis)))
    return str(millis // 1000) if millis % 1000 == 0 else f"{millis}m"


def parse_memory(value):
    """'8Gi' | '512Mi' | 1024 -> bytes (int)."""
    text = str(value).strip()
    for suffix in sorted(_MEM_UNITS, key=len, reverse=True):
        if suffix and text.endswith(suffix):
            return int(float(text[:-len(suffix)]) * _MEM_UNITS[suffix])
    return int(float(text))


def format_memory(nbytes):
    nbytes = max(1, int(nbytes))
    for suffix in ("Gi", "Mi", "Ki"):
        unit = _MEM_UNITS[suffix]
        if nbytes >= unit and nbytes % unit == 0:
            return f"{nbytes // unit}{suffix}"
    # round up to the nearest Mi rather than emit a raw byte count
    return f"{-(-nbytes // _MEM_UNITS['Mi'])}Mi"


# --- distributions ----------------------------------------------------------

def make_distribution(spec, *, cap=None, parse=float):
    """Build rng -> number from a spec string.

    fixed:V                  always V
    uniform:LO,HI            uniform in [LO, HI]
    exp:MEAN                 exponential with the given mean
    pareto:MIN,ALPHA         power law, x >= MIN (alpha ~1.2 is very heavy)
    mixture:BASE,P,MAX       BASE with probability 1-P, otherwise
                             log-uniform in [BASE, MAX] (the default shape:
                             most workloads small, a few enormous)

    `cap` clamps every draw; `parse` converts the spec's numbers (e.g. to
    millicores for CPU values).
    """
    kind, _, rest = str(spec).partition(":")
    kind = kind.strip().lower()
    args = [a.strip() for a in rest.split(",") if a.strip()]

    def num(i, default=None):
        if i < len(args):
            return parse(args[i])
        if default is None:
            raise SystemExit(f"distribution {kind!r} is missing an argument: {spec!r}")
        return default

    if kind == "fixed":
        v = num(0)
        draw = lambda rng: v                                    # noqa: E731
    elif kind == "uniform":
        lo, hi = num(0), num(1)
        draw = lambda rng: rng.uniform(lo, hi)                  # noqa: E731
    elif kind == "exp":
        mean = num(0)
        draw = lambda rng: rng.expovariate(1.0 / mean)          # noqa: E731
    elif kind == "pareto":
        lo, alpha = num(0), float(args[1]) if len(args) > 1 else 1.2
        draw = lambda rng: lo * rng.paretovariate(alpha)        # noqa: E731
    elif kind == "mixture":
        base = num(0)
        tail_p = float(args[1]) if len(args) > 1 else 0.05
        top = num(2, cap if cap is not None else base * 20)
        top = max(top, base)

        def draw(rng, base=base, tail_p=tail_p, top=top):
            if rng.random() >= tail_p:
                return base
            return base * (top / base) ** rng.random() if base > 0 else rng.uniform(0, top)
    else:
        raise SystemExit(f"unknown distribution {kind!r} in {spec!r} "
                         "(fixed, uniform, exp, pareto, mixture)")

    def sample(rng):
        v = draw(rng)
        return min(v, cap) if cap is not None else v

    return sample


# --- generation -------------------------------------------------------------

def generate(nodes=10, node_cpu="8", node_memory=None, memory_per_cpu="4Gi",
             zones=0, workloads=10, replicas="mixture:4,0.05",
             cpu="fixed:500m", memory_per_pod_cpu="2Gi", seed=None,
             name_prefix="app"):
    """Return a config dict ready for `schedsim run` (or YAML/JSON dump)."""
    rng = random.Random(seed)

    node_cpu_m = parse_cpu(node_cpu)
    if node_memory is None:
        node_mem = int(node_cpu_m / 1000 * parse_memory(memory_per_cpu))
    else:
        node_mem = parse_memory(node_memory)

    node_list = []
    for i in range(nodes):
        node = {"name": f"node-{i:0{len(str(max(nodes - 1, 1)))}d}",
                "cpu": format_cpu(node_cpu_m),
                "memory": format_memory(node_mem)}
        if zones:
            node["labels"] = {"zone": f"zone-{i % zones}"}
        node_list.append(node)

    # A "whale" workload is one instance per node on 80% of the cluster.
    replicas_dist = make_distribution(replicas, cap=max(1, int(nodes * 0.8)),
                                      parse=float)
    cpu_dist = make_distribution(cpu, cap=node_cpu_m, parse=parse_cpu)
    mem_per_cpu = parse_memory(memory_per_pod_cpu)

    workload_list = []
    for i in range(workloads):
        count = max(1, int(round(replicas_dist(rng))))
        pod_cpu = max(1, int(round(cpu_dist(rng))))
        workload_list.append({
            "name": f"{name_prefix}-{i:0{len(str(max(workloads - 1, 1)))}d}",
            "replicas": count,
            "cpu": format_cpu(pod_cpu),
            "memory": format_memory(pod_cpu / 1000 * mem_per_cpu),
        })

    return {"nodes": node_list, "workloads": workload_list}


def summarize(config):
    """One-line capacity vs demand summary, useful as a file header."""
    cap_cpu = sum(parse_cpu(n["cpu"]) for n in config["nodes"])
    cap_mem = sum(parse_memory(n["memory"]) for n in config["nodes"])
    pods = sum(w["replicas"] for w in config["workloads"])
    req_cpu = sum(parse_cpu(w["cpu"]) * w["replicas"] for w in config["workloads"])
    req_mem = sum(parse_memory(w["memory"]) * w["replicas"] for w in config["workloads"])
    return (f"{len(config['nodes'])} nodes ({format_cpu(cap_cpu)} cpu, "
            f"{format_memory(cap_mem)}), "
            f"{len(config['workloads'])} workloads / {pods} pods requesting "
            f"{format_cpu(req_cpu)} cpu ({100 * req_cpu / cap_cpu:.0f}%), "
            f"{format_memory(req_mem)} memory ({100 * req_mem / cap_mem:.0f}%)")
