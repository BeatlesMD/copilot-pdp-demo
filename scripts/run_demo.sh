#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:-dev}"
JOB_KEY="${2:-profile-memory-demo}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

if ! command -v databricks >/dev/null 2>&1; then
  echo "Databricks CLI is required. Install: https://docs.databricks.com/dev-tools/cli/install.html"
  exit 1
fi

if [[ -z "${DATABRICKS_HOST:-}" ]]; then
  echo "DATABRICKS_HOST is required. Set it in your shell or .env."
  exit 1
fi

if [[ -z "${DATABRICKS_TOKEN:-}" && -z "${DATABRICKS_CONFIG_PROFILE:-}" ]]; then
  echo "Set DATABRICKS_TOKEN or DATABRICKS_CONFIG_PROFILE before running."
  exit 1
fi

echo "==> Validating bundle (target=${TARGET})"
databricks bundle validate -t "${TARGET}"

echo "==> Deploying bundle (target=${TARGET})"
databricks bundle deploy -t "${TARGET}"

echo "==> Running job (${JOB_KEY})"
databricks bundle run "${JOB_KEY}" -t "${TARGET}"

echo "Done."
