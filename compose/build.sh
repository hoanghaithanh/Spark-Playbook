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
KAFKA_IMAGE_NAME="sparkpb/kafka:3.9.0"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Building ${IMAGE_NAME} from ${SCRIPT_DIR}/Dockerfile.spark ..."
docker build \
    -f "${SCRIPT_DIR}/Dockerfile.spark" \
    -t "${IMAGE_NAME}" \
    "${SCRIPT_DIR}"

echo "Built ${IMAGE_NAME}"

# JMX-exporter-instrumented Kafka broker image (docs/architecture/
# multi-broker-kafka-cluster.md D-MBK6, US-MBK3). Only the compose template's
# Kafka service uses this; the Spark master/worker/driver services above are
# unaffected.
echo "Building ${KAFKA_IMAGE_NAME} from ${SCRIPT_DIR}/Dockerfile.kafka ..."
docker build \
    -f "${SCRIPT_DIR}/Dockerfile.kafka" \
    -t "${KAFKA_IMAGE_NAME}" \
    "${SCRIPT_DIR}"

echo "Built ${KAFKA_IMAGE_NAME}"
