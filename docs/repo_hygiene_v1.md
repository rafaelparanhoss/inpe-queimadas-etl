# Repo Hygiene v1

Scope: inventory and conservative cleanup for release v1.

## Candidate inventory

Checked candidate groups:
- `archive/` (legacy superset sandbox)
- `devtools/*` (windows git recovery scripts)
- `docs/*.md` (runtime/support docs)
- `logs/` and `*.log`

Reference check command used:

```powershell
rg -n "archive/|devtools/|docs/archive|superset" README.md docs scripts src .github -S
```

## Decision by group

1. `archive/`
- Result: no runtime references found in README/docs/scripts/src.
- Action: removed.
- Rationale: legacy files, not part of ETL/API/WEB runtime.

2. `devtools/*`
- Result: referenced by `README.md` and `src/etl/validate_repo.py`.
- Action: kept.
- Rationale: active recovery tooling for git issues on Windows.

3. `docs/*.md`
- Result: core docs referenced by README and validation flow.
- Action: kept core docs; no aggressive doc deletion in this pass.
- Rationale: avoid release risk by preserving operational docs.

4. `logs/` and `*.log`
- Result: only `logs/.gitkeep` tracked; runtime creates transient logs.
- Action: kept `.gitkeep`, expanded `.gitignore` for caches/build artifacts.
- Rationale: prevent accidental noise in git while keeping logs dir.
