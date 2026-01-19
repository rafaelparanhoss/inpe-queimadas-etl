#!/usr/bin/env bash
set -euo pipefail

uv_cmd() {
  if command -v uv >/dev/null 2>&1; then
    uv "$@"
  else
    python -m uv "$@"
  fi
}
