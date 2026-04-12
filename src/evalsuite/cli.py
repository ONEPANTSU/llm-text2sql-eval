"""Command line entrypoint for EvalSuite."""

from __future__ import annotations

import json
from pathlib import Path

from evalsuite.core.config import load_config
from evalsuite.core.storage import list_runs
from evalsuite.orchestrator import RunOrchestrator
from evalsuite.reporting.report import generate_report


def _bench_list(value: str) -> list[str]:
    if value == "all":
        return ["bird_sqlite", "spider2", "tpcds"]
    return [part.strip() for part in value.split(",") if part.strip()]


def _task_timeout_sec(cli_val: int | None, config) -> int | None:
    val = cli_val if cli_val is not None else getattr(config, "task_timeout_sec", 120)
    return None if val == 0 else val


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(prog="evalsuite", description="Text-to-SQL evaluation suite.")
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    common.add_argument("--out", default="runs", help="Output directory root")

    # --- run ---
    run_p = sub.add_parser("run", parents=[common], help="Run evaluation")
    run_p.add_argument("--model", default="default", help="Model name (from config.yaml models section, or 'default')")
    run_p.add_argument("--bench", default="all", help="Benchmarks: all, or comma-separated (bird_sqlite,spider2,tpcds)")
    run_p.add_argument("--limit", type=int, default=None, help="Limit tasks per benchmark")
    run_p.add_argument(
        "--architecture",
        choices=["plain", "self_consistency", "sgr", "sql_factory", "hybrid"],
        default=None,
        help="Override architecture from config",
    )
    run_p.add_argument(
        "--context-mode", choices=["none", "full_schema", "toolchain"], default=None, help="Schema context mode"
    )
    run_p.add_argument("--task-timeout-sec", type=int, default=None, help="Max seconds per task (0=no limit)")

    # --- run-all ---
    run_all_p = sub.add_parser("run-all", parents=[common], help="Run all benchmarks, all tasks")
    run_all_p.add_argument("--model", default="default", help="Model name")
    run_all_p.add_argument(
        "--architecture",
        choices=["plain", "self_consistency", "sgr", "sql_factory", "hybrid"],
        default=None,
        help="Override architecture",
    )

    # --- resume / report / list ---
    resume_p = sub.add_parser("resume", parents=[common], help="Resume a previous run")
    resume_p.add_argument("--run-id", required=True, help="Run id to resume")

    report_p = sub.add_parser("report", parents=[common], help="Generate report for a run")
    report_p.add_argument("--run-id", required=True, help="Run id to report")

    list_p = sub.add_parser("list", parents=[common], help="List completed runs")
    list_p.add_argument("--json", action="store_true", help="Output as JSON")

    args = parser.parse_args(argv)
    config = load_config(Path(args.config))

    if args.command == "run":
        benches = _bench_list(args.bench)
        orchestrator = RunOrchestrator(config=config, output_root=Path(args.out))
        orchestrator.run(
            model_name=args.model,
            benches=benches,
            limit=args.limit,
            shard=None,
            time_budget=None,
            run_all=args.limit is None,
            context_mode=args.context_mode if args.context_mode is not None else config.context_mode,
            schema_max_tables=config.schema_max_tables,
            schema_max_cols_per_table=config.schema_max_cols_per_table,
            schema_format=config.schema_format,
            toolchain_max_steps=config.toolchain_max_steps,
            toolchain_timeout_sec=config.toolchain_timeout_sec,
            toolchain_allow_sample_values=config.toolchain_allow_sample_values,
            task_timeout_sec=_task_timeout_sec(args.task_timeout_sec, config),
            architecture=args.architecture,
        )

    elif args.command == "run-all":
        orchestrator = RunOrchestrator(config=config, output_root=Path(args.out))
        orchestrator.run(
            model_name=args.model,
            benches=["bird_sqlite", "spider2", "tpcds"],
            limit=None,
            shard=None,
            time_budget=None,
            run_all=True,
            context_mode=config.context_mode,
            schema_max_tables=config.schema_max_tables,
            schema_max_cols_per_table=config.schema_max_cols_per_table,
            schema_format=config.schema_format,
            toolchain_max_steps=config.toolchain_max_steps,
            toolchain_timeout_sec=config.toolchain_timeout_sec,
            toolchain_allow_sample_values=config.toolchain_allow_sample_values,
            task_timeout_sec=_task_timeout_sec(None, config),
            architecture=args.architecture,
        )

    elif args.command == "resume":
        orchestrator = RunOrchestrator(config=config, output_root=Path(args.out))
        orchestrator.resume(args.run_id)

    elif args.command == "report":
        generate_report(Path(args.out), args.run_id)

    elif args.command == "list":
        runs = list_runs(Path(args.out))
        if args.json:
            print(json.dumps(runs, indent=2))
        else:
            if not runs:
                print("No runs found.")
            for r in runs:
                print(f"  {r['run_id']}  {r.get('status', '?')}  {r.get('benches', '?')}")
