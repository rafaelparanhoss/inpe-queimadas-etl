#!/usr/bin/env bash
set -euo pipefail

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[win_git_repair] not inside a git repository" >&2
  exit 1
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

echo "[win_git_repair] repo: $repo_root"
echo "[win_git_repair] checking running processes"
if command -v tasklist.exe >/dev/null 2>&1; then
  tasklist.exe | grep -Ei "git\.exe|code\.exe" >/dev/null 2>&1 && {
    echo "[win_git_repair] warning: git/code process detected. close active git operations first."
  } || true
fi

if [[ -f ".git/index.lock" ]]; then
  echo "[win_git_repair] removing stale .git/index.lock"
  rm -f ".git/index.lock"
else
  echo "[win_git_repair] .git/index.lock not present"
fi

echo "[win_git_repair] rebuilding index"
rm -f ".git/index"
git reset

echo "[win_git_repair] validating add dry-run"
git add -A --dry-run
echo "[win_git_repair] ok"
