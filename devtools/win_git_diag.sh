#!/usr/bin/env bash
set -euo pipefail

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[win_git_diag] not inside a git repository" >&2
  exit 1
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

target="${1:-sqlm/marts/aux/031_uf_poly_day_full.sql}"
log_path="${2:-logs/win_git_diag.log}"
mkdir -p "$(dirname "$log_path")"

{
  echo "== env =="
  date
  echo "pwd: $(pwd)"
  echo "repo: $(git rev-parse --show-toplevel)"
  echo "git: $(git --version)"
  echo "uname: $(uname -a || true)"
  echo "msystem: ${MSYSTEM:-}"

  echo
  echo "== git core/feature config =="
  git config --show-origin --list | grep -Ei "(^|\\.)core\\.|(^|\\.)feature\\." || true

  echo
  echo "== target fs checks =="
  echo "target: $target"
  ls -la "$(dirname "$target")" || true
  ls -la "$target" || true
  stat "$target" || true
  if [[ -f "$target" ]]; then
    echo "test -f: OK_EXISTS"
  else
    echo "test -f: MISSING"
  fi

  echo
  echo "== python open/read test =="
  python - "$target" <<'PY'
import os
import sys

p = sys.argv[1]
print("path", p)
print("exists", os.path.exists(p), "isfile", os.path.isfile(p))
try:
    with open(p, "rb") as f:
        data = f.read()
    print("read_bytes", len(data))
except Exception as exc:
    print("open_error", repr(exc))
PY

  echo
  echo "== git add verbose target =="
  git add --verbose -- "$target" || true

  echo
  echo "== git add trace target =="
  GIT_TRACE=1 GIT_TRACE_PERFORMANCE=1 git add -- "$target" || true
} 2>&1 | tee "$log_path"

echo "[win_git_diag] log saved: $log_path"
