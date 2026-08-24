"""schedsim: simulate kube-scheduler placement decisions.

Usage:
  python -m schedsim up             # build/start the control-plane container
  python -m schedsim run input.yaml [--json] [--keep] [--timeout N]
  python -m schedsim down           # remove the container
"""

import argparse
import json
import sys

from . import cluster, simulate


def load_config(path):
    with open(path) as f:
        text = f.read()
    try:
        import yaml
        return yaml.safe_load(text)
    except ImportError:
        try:
            return json.loads(text)
        except ValueError:
            raise SystemExit(
                "PyYAML is not installed and the input is not JSON. "
                "Install it with: pip install pyyaml")


def print_table(rows):
    headers = ["WORKLOAD", "POD", "NODE", "STATUS"]
    table = [[r["workload"], r["pod"], r["node"] or "—", r["status"]] for r in rows]
    widths = [max(len(str(row[i])) for row in [headers] + table) for i in range(4)]
    for row in [headers] + table:
        print("  ".join(str(cell).ljust(w) for cell, w in zip(row, widths)).rstrip())


def main():
    parser = argparse.ArgumentParser(prog="schedsim", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("up", help="start the control-plane container")
    sub.add_parser("down", help="remove the control-plane container")
    run_p = sub.add_parser("run", help="schedule workloads onto nodes and print the allocation")
    run_p.add_argument("input", help="YAML/JSON file with nodes: and workloads:")
    run_p.add_argument("--json", action="store_true", help="machine-readable output")
    run_p.add_argument("--keep", action="store_true",
                       help="leave nodes/pods in the cluster for inspection")
    run_p.add_argument("--timeout", type=int, default=30,
                       help="seconds to wait for scheduling decisions")
    args = parser.parse_args()

    if args.command == "up":
        cluster.up()
        print("control plane ready at https://127.0.0.1:6443")
    elif args.command == "down":
        cluster.down()
        print("removed")
    elif args.command == "run":
        config = load_config(args.input)
        client = cluster.up()
        rows = simulate.run(client, config, timeout=args.timeout, keep=args.keep)
        if args.json:
            print(json.dumps(rows, indent=2))
        else:
            print_table(rows)
        if any(r["node"] is None for r in rows):
            sys.exit(2)


if __name__ == "__main__":
    main()
