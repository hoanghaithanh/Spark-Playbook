#!/usr/bin/env bash
# Build the Spark Playbook cluster image (PLAN.md D2/D3).
#
# One image is reused for master/worker/driver roles; only the compose `command`
# differs per service. Run this once, and again whenever Dockerfile.spark changes.
#
# Usage:
#   ./compose/build.sh
#
# Works from WSL2/Linux/macOS bash, and from Git Bash on Windows (the intended
# environment per PLAN.md D1 — Docker Desktop + WSL2).
set -euo pipefail

IMAGE_NAME="sparkpb/spark:4.0.3"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Building ${IMAGE_NAME} from ${SCRIPT_DIR}/Dockerfile.spark ..."
docker build \
    -f "${SCRIPT_DIR}/Dockerfile.spark" \
    -t "${IMAGE_NAME}" \
    "${SCRIPT_DIR}"

echo "Built ${IMAGE_NAME}"
