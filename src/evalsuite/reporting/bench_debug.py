from __future__ import annotations

from pathlib import Path

from evalsuite.core.storage import load_results


def write_bench_debug(run_dir: Path) -> None:
    results = load_results(run_dir)
    by_bench: dict[str, list[dict]] = {}
    for r in results:
        by_bench.setdefault(r["bench"], []).append(r)

    for bench, items in by_bench.items():
        mismatches = [r for r in items if not r.get("match")]
        lines = [f"# {bench} debug", ""]
        lines.append("## Top mismatches")
        for r in mismatches[:5]:
            lines.append("- question: " + str(r.get("extra", {}).get("question", "")))
            lines.append(f"  gold_sql: `{r.get('gold_sql')}`")
            lines.append(f"  pred_sql: `{r.get('pred_sql')}`")
            lines.append(f"  reason: {r.get('error_type')}")
            if r.get("gold", {}).get("rows") is not None:
                lines.append(f"  gold_rows: {r.get('gold').get('rows')}")
            if r.get("pred", {}).get("rows") is not None:
                lines.append(f"  pred_rows: {r.get('pred').get('rows')}")
        lines.append("")
        lines.append("## Error types")
        err_counts: dict[str, int] = {}
        for r in mismatches:
            err = r.get("error_type") or "unknown"
            err_counts[err] = err_counts.get(err, 0) + 1
        for k, v in sorted(err_counts.items(), key=lambda kv: kv[1], reverse=True):
            lines.append(f"- {k}: {v}")

        (run_dir / f"{bench}_debug.md").write_text("\n".join(lines))
