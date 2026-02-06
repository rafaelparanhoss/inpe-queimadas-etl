from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path


def _write_report(payload: dict, out_path: Path) -> None:
    lines = [
        "# validation last run",
        "",
        f"timestamp_utc: {payload.get('timestamp_utc', datetime.utcnow().isoformat() + 'Z')}",
        "",
        "marts:",
    ]
    marts = payload.get("marts", {})
    lines += [
        f"- applied: {marts.get('applied')}",
        f"- skipped_date: {marts.get('skipped_date')}",
        f"- skipped_stub: {marts.get('skipped_stub')}",
        f"- failed: {marts.get('failed')}",
        "",
        "checks:",
    ]
    checks = payload.get("checks", {})
    lines += [
        f"- applied: {checks.get('applied')}",
        f"- skipped_date: {checks.get('skipped_date')}",
        f"- skipped_stub: {checks.get('skipped_stub')}",
        f"- failed: {checks.get('failed')}",
        "",
        "check_results:",
    ]
    for item in payload.get("check_results", []):
        status = "ok" if item.get("ok") else "fail"
        detail = f" | {item.get('error')}" if item.get("error") else ""
        lines.append(f"- {item.get('name')}: {status}{detail}")

    counts = payload.get("counts", {})
    lines += [
        "",
        "last_day_counts:",
        f"- uf_day: {counts.get('uf_day')}",
        f"- uf_rows: {counts.get('uf_rows')}",
        f"- mun_day: {counts.get('mun_day')}",
        f"- mun_features: {counts.get('mun_features')}",
        f"- scatter_day: {counts.get('scatter_day')}",
        f"- scatter_rows: {counts.get('scatter_rows')}",
        "",
    ]
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="generate validation report from json")
    parser.add_argument("--input", default="logs/last_run.json", help="input json path")
    parser.add_argument("--output", default="docs/validation_last_run.md", help="output md path")
    args = parser.parse_args(argv)

    payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
    _write_report(payload, Path(args.output))


if __name__ == "__main__":
    main()
