#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/_uv.sh"

export PYTHONPATH=src
uv_cmd run python -m etl.app enrich "$@"
