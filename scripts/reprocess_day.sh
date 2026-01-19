#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH=src
python -m uv run python -m etl.app reprocess "$@"
