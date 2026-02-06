#!/usr/bin/env bash
set -euo pipefail

force_unlock=0
if [[ "${1:-}" == "--force-unlock" ]]; then
  force_unlock=1
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[win_git_repair] not inside a git repository" >&2
  exit 1
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

echo "[win_git_repair] repo: $repo_root"

has_git_proc=0
if command -v tasklist.exe >/dev/null 2>&1; then
  if tasklist.exe | grep -Ei "git\.exe|git-lfs\.exe|git-remote-https\.exe" >/dev/null 2>&1; then
    has_git_proc=1
    echo "[win_git_repair] git process detected"
  else
    echo "[win_git_repair] no git process detected"
  fi
fi

if [[ -f ".git/index.lock" ]]; then
  echo "[win_git_repair] .git/index.lock present"
  if [[ "$has_git_proc" -eq 1 ]]; then
    echo "[win_git_repair] refusing to remove lock while git process is running" >&2
    exit 2
  fi
  if [[ "$force_unlock" -ne 1 ]]; then
    echo "[win_git_repair] re-run with --force-unlock to remove stale lock" >&2
    exit 3
  fi
  rm -f ".git/index.lock"
  echo "[win_git_repair] stale lock removed"
else
  echo "[win_git_repair] .git/index.lock not present"
fi

echo "[win_git_repair] rebuilding index"
rm -f ".git/index"
git reset

echo "[win_git_repair] validating add dry-run"
git add -A --dry-run
echo "[win_git_repair] ok"
