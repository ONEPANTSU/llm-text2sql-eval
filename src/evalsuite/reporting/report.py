from __future__ import annotations

import json
import statistics
from pathlib import Path
from typing import Any

from evalsuite.core.storage import load_results, write_report
from evalsuite.core.types import BenchSummary
from evalsuite.reporting.bench_debug import write_bench_debug

ERROR_HEAD_LEN = 120
TOP_PATTERNS = 10
EXAMPLES_PER_PATTERN = 3


def _compute_latency_stats(values: list[float]) -> dict[str, Any]:
    """Compute mean, median, p90, p99 from a list of latency values (ms)."""
    if not values:
        return {}
    values_sorted = sorted(values)
    n = len(values_sorted)
    return {
        "mean_ms": round(statistics.mean(values_sorted), 1),
        "median_ms": round(statistics.median(values_sorted), 1),
        "p90_ms": round(values_sorted[int(n * 0.9)] if n >= 10 else values_sorted[-1], 1),
        "p99_ms": round(values_sorted[int(n * 0.99)] if n >= 100 else values_sorted[-1], 1),
        "total_tasks_with_latency": n,
    }


def _compute_candidates_stats(values: list[int]) -> dict[str, Any]:
    """Compute mean, median from a list of candidates_count values."""
    if not values:
        return {}
    return {
        "mean": round(statistics.mean(values), 2),
        "median": round(statistics.median(values), 1),
    }


def _top_error_patterns(results: list[dict]) -> list[dict[str, Any]]:
    """Aggregate by error_type and error_message head (~120 chars). Top N patterns with 1-3 examples each."""
    # Group by (error_type, error_head)
    groups: dict[tuple[str, str], list[dict]] = {}
    for r in results:
        if r.get("status") == "ok":
            continue
        err_type = r.get("error_type") or r.get("status") or "unknown"
        err_msg = (r.get("error_message") or "")[:ERROR_HEAD_LEN]
        key = (err_type, err_msg)
        if key not in groups:
            groups[key] = []
        groups[key].append(r)
    # Sort by count desc, take top N
    sorted_keys = sorted(groups.keys(), key=lambda k: -len(groups[k]))[:TOP_PATTERNS]
    out: list[dict[str, Any]] = []
    for key in sorted_keys:
        items = groups[key]
        examples = [
            {
                "task_id": r.get("task_id"),
                "bench": r.get("bench"),
                "error_type": key[0],
                "error_message": (r.get("error_message") or "")[:200],
                "pred_sql_snippet": (
                    (r.get("pred_sql") or "")[:300] + ("..." if len(r.get("pred_sql") or "") > 300 else "")
                ),
            }
            for r in items[:EXAMPLES_PER_PATTERN]
        ]
        out.append(
            {
                "error_type": key[0],
                "error_head": key[1],
                "count": len(items),
                "examples": examples,
            }
        )
    return out


def _load_bench_summaries(run_dir: Path) -> tuple[list[BenchSummary], dict]:
    path = run_dir / "summary.json"
    if not path.exists():
        return [], {}
    payload = json.loads(path.read_text())
    benches_raw = payload["benches"] if isinstance(payload, dict) else payload
    out = [BenchSummary(**entry) for entry in benches_raw]
    overall = payload.get("overall", {}) if isinstance(payload, dict) else {}
    return out, overall


def generate_report(output_root: Path, run_id: str) -> None:
    run_dir = output_root / run_id
    summaries, overall = _load_bench_summaries(run_dir)
    total_executed = sum(s.executed for s in summaries)
    total_compared = sum(s.compared for s in summaries)
    aggregate = sum(s.ex_correct for s in summaries) / total_executed if total_executed else 0.0
    results = load_results(run_dir)

    # Toolchain diagnostics for tpcds (not part of score)
    toolchain_results = [r for r in results if r.get("bench") == "tpcds" and r.get("extra")]
    toolchain_diagnostics: dict = {}
    if toolchain_results:
        tool_calls_counts = [
            r["extra"].get("tool_calls_count", 0) for r in toolchain_results if "tool_calls_count" in r.get("extra", {})
        ]
        inspected_list = [
            r["extra"].get("inspected_tables", [])
            for r in toolchain_results
            if "inspected_tables" in r.get("extra", {})
        ]
        tpcds_fact = {"store_sales", "store_returns", "web_sales", "web_returns", "catalog_sales", "catalog_returns"}
        no_fact = sum(1 for tbls in inspected_list if not (set(tbls) & tpcds_fact))
        table_counts: dict[str, int] = {}
        for tbls in inspected_list:
            for t in tbls:
                table_counts[t] = table_counts.get(t, 0) + 1
        toolchain_diagnostics = {
            "avg_tool_calls": sum(tool_calls_counts) / len(tool_calls_counts) if tool_calls_counts else 0,
            "pct_no_fact_table_inspected": (no_fact / len(inspected_list) * 100) if inspected_list else 0,
            "most_inspected_tables": sorted(table_counts.items(), key=lambda x: -x[1])[:10],
        }

    # Error breakdown (pred_parse_fail, pred_bind_fail, pred_schema_fail, pred_runtime_fail, etc.)
    error_breakdown: dict[str, int] = {}
    for r in results:
        if r.get("status") != "ok":
            key = r.get("error_type") or r.get("status")
            error_breakdown[key] = error_breakdown.get(key, 0) + 1

    # v1.2 autofix and schema-warn counters (from extra)
    schema_warn_count = sum(1 for r in results if r.get("extra", {}).get("schema_warn"))
    pred_bind_fail_autofix_success = sum(1 for r in results if r.get("extra", {}).get("autofix_success") is True)
    pred_bind_fail_autofix_failed = sum(1 for r in results if r.get("extra", {}).get("autofix_failed") is True)

    # Bind errors with candidate bindings (fixable pattern group)
    bind_error_candidate_fixable: list[dict[str, Any]] = []
    for r in results:
        if r.get("error_type") != "pred_bind_fail":
            continue
        err_msg = r.get("error_message") or ""
        extra = r.get("extra") or {}
        if "Candidate binding" not in err_msg and "candidate binding" not in err_msg.lower():
            if extra.get("auto_patch_type") not in ("candidate_binding", "prefix"):
                continue
        bind_error_candidate_fixable.append(
            {
                "task_id": r.get("task_id"),
                "bench": r.get("bench"),
                "error_snippet": err_msg[:200],
                "auto_patch_from": extra.get("auto_patch_from"),
                "auto_patch_to": extra.get("auto_patch_to"),
                "auto_patch_type": extra.get("auto_patch_type"),
                "autofix_success": extra.get("autofix_success"),
                "autofix_failed": extra.get("autofix_failed"),
            }
        )
    bind_error_candidate_fixable = bind_error_candidate_fixable[:10]  # cap examples

    top_patterns = _top_error_patterns(results)

    # Self-consistency diagnostics (when architecture is self_consistency)
    sc_results = [r for r in results if (r.get("extra") or {}).get("candidates")]
    self_consistency_diagnostics: dict[str, Any] = {}
    if sc_results:
        preflight_ok_total = exec_ok_total = total_cands = 0
        majority_strengths: list[float] = []
        for r in sc_results:
            cands = (r.get("extra") or {}).get("candidates") or []
            total_cands += len(cands)
            for c in cands:
                if c.get("preflight_ok"):
                    preflight_ok_total += 1
                if c.get("exec_ok"):
                    exec_ok_total += 1
            agg = (r.get("extra") or {}).get("aggregation") or {}
            votes = agg.get("votes") or {}
            by_sig = votes.get("by_signature") or {}
            by_sql = votes.get("by_sql") or {}
            if by_sig:
                vals = list(by_sig.values())
                if vals:
                    majority_strengths.append(max(vals) / len(cands) if cands else 0)
            elif by_sql:
                vals = list(by_sql.values())
                if vals:
                    majority_strengths.append(max(vals) / len(cands) if cands else 0)
        self_consistency_diagnostics = {
            "tasks_with_candidates": len(sc_results),
            "total_candidates": total_cands,
            "candidate_preflight_pass_rate": (preflight_ok_total / total_cands) if total_cands else 0,
            "candidate_exec_pass_rate": (exec_ok_total / total_cands) if total_cands else 0,
            "avg_exec_ok_per_example": (exec_ok_total / len(sc_results)) if sc_results else 0,
            "avg_majority_strength": (sum(majority_strengths) / len(majority_strengths)) if majority_strengths else 0,
        }

    # Hybrid diagnostics (from raw/hybrid_*.json artifacts)
    hybrid_diagnostics: dict[str, Any] = {}
    raw_dir = run_dir / "raw"
    hybrid_artifacts: list[dict[str, Any]] = []
    if raw_dir.is_dir():
        for p in sorted(raw_dir.glob("hybrid_*.json")):
            try:
                hybrid_artifacts.append(json.loads(p.read_text()))
            except Exception:
                pass
    if hybrid_artifacts:
        # Variant distribution
        variant_a = sum(1 for a in hybrid_artifacts if a.get("variant") == "A")
        variant_b = sum(1 for a in hybrid_artifacts if a.get("variant") == "B")

        # Initial candidates stats
        initial_counts = [len(a.get("initial_candidates", [])) for a in hybrid_artifacts]
        avg_initial = statistics.mean(initial_counts) if initial_counts else 0

        # Expansion stats (Variant B only)
        expansion_accepted = [
            a["expansion"]["variations_accepted"]
            for a in hybrid_artifacts
            if a.get("expansion") and isinstance(a["expansion"].get("variations_accepted"), (int, float))
        ]
        avg_expansion_accepted = statistics.mean(expansion_accepted) if expansion_accepted else 0

        # Aggregation groups: count distinct result_signatures among exec_ok candidates
        agg_groups_counts: list[int] = []
        for a in hybrid_artifacts:
            all_cands = list(a.get("initial_candidates", []))
            exp = a.get("expansion", {})
            if isinstance(exp, dict):
                all_cands.extend(exp.get("candidates", []))
            sigs = {c["result_signature"] for c in all_cands if c.get("exec_ok") and c.get("result_signature")}
            agg_groups_counts.append(len(sigs))
        avg_agg_groups = statistics.mean(agg_groups_counts) if agg_groups_counts else 0

        # Pool size stats
        pool_sizes = [a.get("aggregation", {}).get("total_pool_size", 0) for a in hybrid_artifacts]
        avg_pool_size = statistics.mean(pool_sizes) if pool_sizes else 0

        hybrid_diagnostics = {
            "tasks_with_hybrid": len(hybrid_artifacts),
            "variant_distribution": {"A": variant_a, "B": variant_b},
            "avg_initial_candidates": round(avg_initial, 2),
            "avg_expansion_accepted": round(avg_expansion_accepted, 2),
            "avg_aggregation_groups": round(avg_agg_groups, 2),
            "avg_pool_size": round(avg_pool_size, 2),
        }

    # SQL-Factory diagnostics (when architecture is sql_factory)
    sf_results = [r for r in results if (r.get("extra") or {}).get("sql_factory")]
    sql_factory_diagnostics: dict[str, Any] = {}
    if sf_results:
        summaries_sf = [(r.get("extra") or {}).get("sql_factory", {}).get("summary", {}) for r in sf_results]
        n = len(sf_results)
        sql_factory_diagnostics = {
            "tasks_with_sql_factory": len(sf_results),
            "avg_rounds": sum(s.get("avg_rounds", 0) for s in summaries_sf) / n if n else 0,
            "avg_pool_size": sum(s.get("avg_pool_size", 0) for s in summaries_sf) / n if n else 0,
            "reject_exec_fail_rate": sum(s.get("reject_exec_fail_rate", 0) for s in summaries_sf) / n if n else 0,
            "reject_similarity_rate": sum(s.get("reject_similarity_rate", 0) for s in summaries_sf) / n if n else 0,
            "exploration_vs_exploitation_ratio": sum(
                s.get("exploration_vs_exploitation_ratio", 0) for s in summaries_sf
            )
            / n
            if n
            else 0,
        }

    # Latency stats (from latency_ms field; skip tasks with latency_ms=0 or None)
    all_latencies = [r["latency_ms"] for r in results if r.get("latency_ms") and r["latency_ms"] > 0]
    latency_stats = _compute_latency_stats(all_latencies)

    # Per-bench latency breakdown
    bench_names = {r.get("bench") for r in results if r.get("bench")}
    per_bench_latency: dict[str, dict[str, Any]] = {}
    for bench in sorted(bench_names):
        bench_latencies = [
            r["latency_ms"] for r in results if r.get("bench") == bench and r.get("latency_ms") and r["latency_ms"] > 0
        ]
        if bench_latencies:
            per_bench_latency[bench] = _compute_latency_stats(bench_latencies)
    if per_bench_latency:
        latency_stats["per_bench"] = per_bench_latency

    # Candidates count stats (from extra.candidates_count)
    all_candidates_counts = [
        r["extra"]["candidates_count"]
        for r in results
        if r.get("extra") and isinstance(r["extra"].get("candidates_count"), (int, float))
    ]
    candidates_stats = _compute_candidates_stats(all_candidates_counts)

    report: dict = {
        "run_id": run_id,
        "aggregate_execution_accuracy": aggregate,
        "overall": overall
        or {
            "total": sum(s.total for s in summaries),
            "executed": sum(s.executed for s in summaries),
            "skipped": sum(s.skipped for s in summaries),
            "gold_failed": sum(s.gold_failed for s in summaries),
            "pred_failed": sum(s.pred_failed for s in summaries),
            "ex_correct": sum(s.ex_correct for s in summaries),
            "compared": total_compared,
        },
        "error_breakdown": error_breakdown,
        "schema_warn_count": schema_warn_count,
        "pred_bind_fail_autofix_success": pred_bind_fail_autofix_success,
        "pred_bind_fail_autofix_failed": pred_bind_fail_autofix_failed,
        "bind_error_candidate_fixable": bind_error_candidate_fixable,
        "top_error_patterns": top_patterns,
        "benches": [
            {
                "bench": s.bench,
                "coverage": s.executed / s.total if s.total else 0.0,
                # Use executed as denominator so gold/pred failures impact accuracy.
                "execution_accuracy": (s.ex_correct / s.executed) if s.executed else 0.0,
                "ex_correct": s.ex_correct,
                "compared": s.compared,
                "gold_failed": s.gold_failed,
                "pred_failed": s.pred_failed,
                "skipped": s.skipped,
            }
            for s in summaries
        ],
        "latency": latency_stats if latency_stats else None,
        "candidates_count": candidates_stats if candidates_stats else None,
        "toolchain_diagnostics": toolchain_diagnostics if toolchain_diagnostics else None,
        "self_consistency_diagnostics": self_consistency_diagnostics if self_consistency_diagnostics else None,
        "sql_factory_diagnostics": sql_factory_diagnostics if sql_factory_diagnostics else None,
        "hybrid_diagnostics": hybrid_diagnostics if hybrid_diagnostics else None,
    }

    lines = [f"# EvalSuite report for {run_id}", ""]
    lines.append(f"- Aggregate execution accuracy: {aggregate:.3f}")
    lines.append("")
    if overall:
        lines.append("## Overall")
        lines.append(f"- total: {overall.get('total')}")
        lines.append(f"- executed: {overall.get('executed')} | skipped: {overall.get('skipped')}")
        lines.append(f"- gold_failed: {overall.get('gold_failed')} | pred_failed: {overall.get('pred_failed')}")
        lines.append(f"- ex_correct: {overall.get('ex_correct')} / compared: {overall.get('compared')}")
        lines.append("")
    if error_breakdown:
        lines.append("## Error breakdown")
        for k, v in sorted(error_breakdown.items(), key=lambda kv: kv[1], reverse=True):
            lines.append(f"- {k}: {v}")
        lines.append("")
    lines.append("## Autofix & schema (v1.2)")
    lines.append(f"- schema_warn_count: {schema_warn_count}")
    lines.append(f"- pred_bind_fail_autofix_success: {pred_bind_fail_autofix_success}")
    lines.append(f"- pred_bind_fail_autofix_failed: {pred_bind_fail_autofix_failed}")
    lines.append("")
    if latency_stats:
        lines.append("## Latency")
        lines.append(f"- mean: {latency_stats.get('mean_ms', 0):.1f} ms")
        lines.append(f"- median: {latency_stats.get('median_ms', 0):.1f} ms")
        lines.append(f"- p90: {latency_stats.get('p90_ms', 0):.1f} ms")
        lines.append(f"- p99: {latency_stats.get('p99_ms', 0):.1f} ms")
        lines.append(f"- tasks_with_latency: {latency_stats.get('total_tasks_with_latency', 0)}")
        if per_bench_latency:
            for bench, bl in per_bench_latency.items():
                lines.append(
                    f"- {bench}: mean={bl['mean_ms']:.1f} ms, median={bl['median_ms']:.1f} ms, p90={bl['p90_ms']:.1f} ms"
                )
        lines.append("")
    if candidates_stats:
        lines.append("## Candidates count")
        lines.append(f"- mean: {candidates_stats.get('mean', 0):.2f}")
        lines.append(f"- median: {candidates_stats.get('median', 0):.1f}")
        lines.append("")
    if bind_error_candidate_fixable:
        lines.append("## Bind error (candidate fixable)")
        for ex in bind_error_candidate_fixable[:5]:
            lines.append(
                f"- **{ex.get('task_id')}** | type={ex.get('auto_patch_type')} | from={ex.get('auto_patch_from')} -> to={ex.get('auto_patch_to')} | success={ex.get('autofix_success')} failed={ex.get('autofix_failed')}"
            )
        lines.append("")
    if top_patterns:
        lines.append("## Top error patterns")
        for i, p in enumerate(top_patterns, 1):
            lines.append(f"### {i}. {p['error_type']} (count={p['count']})")
            lines.append(
                f"Error head: `{p['error_head'][:80]}...`"
                if len(p["error_head"]) > 80
                else f"Error head: `{p['error_head']}`"
            )
            for ex in p.get("examples", [])[:EXAMPLES_PER_PATTERN]:
                lines.append(f"- **{ex.get('task_id')}** ({ex.get('bench')}): {ex.get('error_message', '')[:100]}...")
            lines.append("")
    if toolchain_diagnostics:
        lines.append("## Toolchain diagnostics (tpcds)")
        lines.append(f"- avg_tool_calls: {toolchain_diagnostics.get('avg_tool_calls', 0):.2f}")
        lines.append(
            f"- pct_no_fact_table_inspected: {toolchain_diagnostics.get('pct_no_fact_table_inspected', 0):.1f}%"
        )
        lines.append("- most_inspected_tables: " + str(toolchain_diagnostics.get("most_inspected_tables", [])))
        lines.append("")
    if self_consistency_diagnostics:
        lines.append("## Self-consistency diagnostics")
        for k, v in self_consistency_diagnostics.items():
            if isinstance(v, float):
                lines.append(f"- {k}: {v:.3f}")
            else:
                lines.append(f"- {k}: {v}")
        lines.append("")
    if sql_factory_diagnostics:
        lines.append("## SQL-Factory diagnostics")
        for k, v in sql_factory_diagnostics.items():
            if isinstance(v, float):
                lines.append(f"- {k}: {v:.3f}")
            else:
                lines.append(f"- {k}: {v}")
        lines.append("")
    if hybrid_diagnostics:
        lines.append("## Hybrid diagnostics")
        lines.append(f"- tasks_with_hybrid: {hybrid_diagnostics.get('tasks_with_hybrid', 0)}")
        vd = hybrid_diagnostics.get("variant_distribution", {})
        lines.append(f"- variant_distribution: A={vd.get('A', 0)}, B={vd.get('B', 0)}")
        lines.append(f"- avg_initial_candidates: {hybrid_diagnostics.get('avg_initial_candidates', 0):.2f}")
        lines.append(f"- avg_expansion_accepted: {hybrid_diagnostics.get('avg_expansion_accepted', 0):.2f}")
        lines.append(f"- avg_aggregation_groups: {hybrid_diagnostics.get('avg_aggregation_groups', 0):.2f}")
        lines.append(f"- avg_pool_size: {hybrid_diagnostics.get('avg_pool_size', 0):.2f}")
        lines.append("")
    for s in summaries:
        lines.append(f"## {s.bench}")
        lines.append(f"- coverage: {s.executed}/{s.total} ({(s.executed / s.total * 100 if s.total else 0):.1f}%)")
        lines.append(f"- execution_accuracy: {(s.ex_correct / s.executed if s.executed else 0):.3f}")
        lines.append(f"- ex_correct: {s.ex_correct}")
        lines.append(f"- compared: {s.compared}")
        lines.append(f"- gold_failed: {s.gold_failed}")
        lines.append(f"- pred_failed: {s.pred_failed}")
        lines.append(f"- skipped: {s.skipped}")
        bl = per_bench_latency.get(s.bench)
        if bl:
            lines.append(
                f"- latency: mean={bl['mean_ms']:.1f} ms, median={bl['median_ms']:.1f} ms, p90={bl['p90_ms']:.1f} ms"
            )
        lines.append("")

    write_report(run_dir, report, "\n".join(lines))
    _write_llm_report(run_dir, report)
    write_bench_debug(run_dir)


def _write_llm_report(run_dir: Path, summary_report: dict) -> None:
    """Create a compact JSON for LLM consumption with top errors and failing tasks."""
    results = load_results(run_dir)
    top_errors = {}
    examples = []
    for r in results:
        if r.get("status") != "ok":
            key = r.get("error_type") or r.get("status")
            top_errors[key] = top_errors.get(key, 0) + 1
            if len(examples) < 10:
                examples.append(
                    {
                        "bench": r.get("bench"),
                        "task_id": r.get("task_id"),
                        "status": r.get("status"),
                        "error_type": r.get("error_type"),
                        "error_message": r.get("error_message"),
                        "question": r.get("extra", {}).get("question"),
                    }
                )
    llm_payload = {
        "summary": summary_report,
        "top_errors": top_errors,
        "examples": examples,
    }
    (run_dir / "llm_report.json").write_text(json.dumps(llm_payload, indent=2))
