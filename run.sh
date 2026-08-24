#!/bin/bash
# Build the control-plane image, start it, and run the demo simulation.
set -euo pipefail
cd "$(dirname "$0")"

echo "==> building image"
podman build -t schedsim-image image/

echo "==> starting control plane"
python3 -m schedsim up

echo "==> running demo simulation"
python3 -m schedsim run examples/demo.yaml || true  # exits 2 if anything unschedulable (expected for the demo)

cat <<'EOF'

Done. Useful commands:
  python3 -m schedsim run <input.yaml> [--json] [--keep]
  python3 -m schedsim down
  podman exec -it schedsim kubectl get nodes,pods -A -o wide
EOF
