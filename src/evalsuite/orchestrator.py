from __future__ import annotations

import json
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from pathlib import Path

from rich.console import Console
from rich.progress import BarColumn, Progress, TaskID, TextColumn, TimeElapsedColumn

from evalsuite.adapters.models.base import build_model_adapter
from evalsuite.architectures.plain import get_architecture_config
from evalsuite.benchmarks.bird import BirdSQLiteBenchmark
from evalsuite.benchmarks.spider2 import Spider2Benchmark
from evalsuite.benchmarks.tpcds import TPCDSNLBenchmark
from evalsuite.core.config import Config, load_config_json, resolve_generation_config
from evalsuite.core.storage import (
    ensure_run_dir,
    generate_run_id,
    load_completed_task_ids,
    load_results,
    save_config,
    save_run_config,
    write_bench_summaries,
    write_event,
    write_result,
)
from evalsuite.core.types import BenchSummary, ExecResult, GenerationRunConfig, TaskResult, TaskSpec
from evalsuite.reporting.report import generate_report

# tpcds = NL→SQL benchmark (data/tpcds/tasks.jsonl)
BENCH_REGISTRY = {
    "bird_sqlite": BirdSQLiteBenchmark,
    "spider2": Spider2Benchmark,
    "tpcds": TPCDSNLBenchmark,
}


def _slice_tasks(tasks: list[TaskSpec], limit: int | None, shard: str | None) -> list[TaskSpec]:
    sliced = tasks
    if shard:
        try:
            idx, total = shard.split("/")
            i = int(idx)
            k = int(total)
            sliced = [t for j, t in enumerate(sliced) if j % k == (i - 1)]
        except Exception:
            pass
    if limit:
        sliced = sliced[:limit]
    return sliced


def _dict_to_task_result(data: dict) -> TaskResult:
    return TaskResult(
        task_id=data["task_id"],
        bench=data["bench"],
        gold_sql=data["gold_sql"],
        pred_sql=data["pred_sql"],
        prompt=data.get("prompt", ""),
        gold=ExecResult(**data["gold"]),
        pred=ExecResult(**data["pred"]),
        match=data["match"],
        status=data.get("status", "ok"),
        error_message=data.get("error_message"),
        error_type=data.get("error_type"),
        latency_ms=data.get("latency_ms"),
        timestamp=data.get("timestamp"),
        extra=data.get("extra", {}),
    )


class RunOrchestrator:
    def __init__(self, config: Config, output_root: Path):
        self.config = config
        self.output_root = output_root
        self.console = Console()

    def _bench_instance(self, name: str, model_adapter):
        cls = BENCH_REGISTRY.get(name)
        if cls is None:
            raise ValueError(f"Unknown benchmark: {name}")
        return cls(
            config=self.config,
            model=model_adapter,
            float_tol=self.config.comparator.float_tol,
            column_order_insensitive=self.config.comparator.column_order_insensitive,
            string_normalize=self.config.comparator.string_normalize,
        )

    def _run_bench(
        self,
        bench_name: str,
        bench,
        tasks: list[TaskSpec],
        progress: Progress,
        task_handle: TaskID,
        run_dir: Path,
        completed_ids: set[str],
        results_sink: dict[str, list[TaskResult]],
        time_budget: int | None,
        start_time: float,
        task_timeout_sec: int | None = None,
    ) -> None:
        existing = results_sink.get(bench_name, [])
        matches = sum(1 for r in existing if r.match and r.status == "ok")
        pred_fail = sum(1 for r in existing if r.status == "pred_fail")
        gold_fail = sum(1 for r in existing if r.status == "gold_fail")
        skipped = sum(1 for r in existing if r.status == "skip")
        executed = len(existing) - skipped
        progress.update(
            task_handle,
            description=f"{bench_name} ok:{matches}/{executed} pf:{pred_fail} gf:{gold_fail} skip:{skipped}",
        )
        bench.run_dir = run_dir
        for t in tasks:
            if time_budget and (time.time() - start_time) > time_budget:
                break
            if t.task_id in completed_ids:
                progress.update(task_handle, advance=1)
                continue
            if task_timeout_sec:
                executor = ThreadPoolExecutor(max_workers=1)
                future = executor.submit(bench.run_task, t)
                try:
                    res = future.result(timeout=task_timeout_sec)
                except FuturesTimeoutError:
                    res = TaskResult(
                        task_id=t.task_id,
                        bench=bench_name,
                        gold_sql=t.gold_sql,
                        pred_sql="",
                        prompt="",
                        gold=ExecResult(ok=False, rows=None, error="task_timeout"),
                        pred=ExecResult(ok=False, rows=None, error="task timeout"),
                        match=False,
                        status="pred_fail",
                        error_message="task timeout",
                        error_type="task_timeout",
                        latency_ms=0,
                        extra={"task_timeout_sec": task_timeout_sec, "candidates_count": 0},
                    )
                finally:
                    executor.shutdown(wait=False)
            else:
                res = bench.run_task(t)
            results_sink[bench_name].append(res)
            write_result(run_dir, res)
            if res.status == "skip":
                skipped += 1
            elif res.status == "gold_fail":
                gold_fail += 1
                executed += 1
            elif res.status == "pred_fail":
                pred_fail += 1
                executed += 1
            else:
                executed += 1
                if res.match:
                    matches += 1
            progress.update(
                task_handle,
                advance=1,
                description=f"{bench_name} ok:{matches}/{executed} pf:{pred_fail} gf:{gold_fail} skip:{skipped}",
            )

    def run(
        self,
        *,
        model_name: str,
        benches: Iterable[str],
        limit: int | None,
        shard: str | None,
        time_budget: int | None,
        run_all: bool,
        bench_limits: dict[str, int] | None = None,
        context_mode: str = "none",
        schema_max_tables: int = 50,
        schema_max_cols_per_table: int = 30,
        schema_format: str = "compact",
        toolchain_max_steps: int = 10,
        toolchain_timeout_sec: int = 30,
        toolchain_allow_sample_values: int = 0,
        task_timeout_sec: int | None = None,
        architecture: str | None = None,
        reasoning: str | None = None,
        sampling: str | None = None,
        sc_samples: int | None = None,
        sc_aggregation: str | None = None,
    ) -> str:
        run_id = generate_run_id(model_name)
        run_dir = ensure_run_dir(self.output_root, run_id)
        save_config(self.config, run_dir, effective_model_name=model_name)
        gen_cfg = resolve_generation_config(
            architecture=architecture,
            reasoning=reasoning,
            sampling=sampling,
            sc_samples=sc_samples,
            sc_aggregation=sc_aggregation,
            raw_config=getattr(self.config, "raw", None),
        )
        save_run_config(
            {
                "model_name": model_name,
                "benches": list(benches),
                "limit": limit,
                "bench_limits": bench_limits,
                "shard": shard,
                "time_budget": time_budget,
                "run_all": run_all,
                "context_mode": context_mode,
                "schema_max_tables": schema_max_tables,
                "schema_max_cols_per_table": schema_max_cols_per_table,
                "schema_format": schema_format,
                "toolchain_max_steps": toolchain_max_steps,
                "toolchain_max_describe": self.config.toolchain_max_describe,
                "toolchain_timeout_sec": toolchain_timeout_sec,
                "toolchain_allow_sample_values": toolchain_allow_sample_values,
                "sql_execution_timeout_sec": self.config.sql_execution_timeout_sec,
                "task_timeout_sec": task_timeout_sec,
                "architecture": gen_cfg.architecture,
                "generation": {
                    "architecture": gen_cfg.architecture,
                    "reasoning": gen_cfg.reasoning,
                    "sampling": gen_cfg.sampling,
                    "sc_samples": gen_cfg.sc_samples,
                    "sc_aggregation": gen_cfg.sc_aggregation,
                },
            },
            run_dir,
        )
        write_event(run_dir, {"type": "start", "run_id": run_id})
        completed_ids = load_completed_task_ids(run_dir)
        self._execute(
            run_dir=run_dir,
            benches=benches,
            model_name=model_name,
            limit=limit,
            bench_limits=bench_limits,
            shard=shard,
            time_budget=time_budget,
            completed_ids=completed_ids,
            run_all=run_all,
            context_mode=context_mode,
            schema_max_tables=schema_max_tables,
            schema_max_cols_per_table=schema_max_cols_per_table,
            schema_format=schema_format,
            toolchain_max_steps=toolchain_max_steps,
            toolchain_max_describe=self.config.toolchain_max_describe,
            toolchain_timeout_sec=toolchain_timeout_sec,
            toolchain_allow_sample_values=toolchain_allow_sample_values,
            sql_execution_timeout_sec=self.config.sql_execution_timeout_sec,
            task_timeout_sec=task_timeout_sec,
            architecture=architecture,
            reasoning=reasoning,
            sampling=sampling,
            sc_samples=sc_samples,
            sc_aggregation=sc_aggregation,
            generation_config=gen_cfg,
        )
        return run_id

    def resume(self, *, run_id: str) -> None:
        run_dir = ensure_run_dir(self.output_root, run_id)
        saved_cfg = run_dir / "config.json"
        if saved_cfg.exists():
            self.config = load_config_json(saved_cfg)
        run_config_path = run_dir / "run_config.json"
        run_meta = {
            "model_name": None,
            "benches": ["bird_sqlite", "spider2", "tpcds"],
            "limit": None,
            "bench_limits": None,
            "shard": None,
            "time_budget": None,
            "run_all": True,
            "context_mode": self.config.context_mode,
            "schema_max_tables": self.config.schema_max_tables,
            "schema_max_cols_per_table": self.config.schema_max_cols_per_table,
            "schema_format": self.config.schema_format,
            "toolchain_max_steps": self.config.toolchain_max_steps,
            "toolchain_max_describe": self.config.toolchain_max_describe,
            "toolchain_timeout_sec": self.config.toolchain_timeout_sec,
            "toolchain_allow_sample_values": self.config.toolchain_allow_sample_values,
            "sql_execution_timeout_sec": self.config.sql_execution_timeout_sec,
            "task_timeout_sec": self.config.task_timeout_sec,
            "architecture": None,
        }
        if run_config_path.exists():
            run_meta.update(json.loads(run_config_path.read_text()))
        completed_ids = load_completed_task_ids(run_dir)
        gen_from_meta = run_meta.get("generation") or {}
        gen_cfg_resume = (
            GenerationRunConfig(
                architecture=gen_from_meta.get("architecture", "plain"),
                reasoning=gen_from_meta.get("reasoning", "none"),
                sampling=gen_from_meta.get("sampling", "single"),
                sc_samples=gen_from_meta.get("sc_samples"),
                sc_aggregation=gen_from_meta.get("sc_aggregation", "majority_result"),
            )
            if gen_from_meta
            else None
        )
        self._execute(
            run_dir=run_dir,
            benches=run_meta.get("benches") or ["bird_sqlite", "spider2", "tpcds"],
            model_name=run_meta.get("model_name") or self.config.model.model,
            limit=run_meta.get("limit"),
            bench_limits=run_meta.get("bench_limits"),
            shard=run_meta.get("shard"),
            time_budget=run_meta.get("time_budget"),
            completed_ids=completed_ids,
            run_all=run_meta.get("run_all", True),
            context_mode=run_meta.get("context_mode", self.config.context_mode),
            schema_max_tables=run_meta.get("schema_max_tables", self.config.schema_max_tables),
            schema_max_cols_per_table=run_meta.get("schema_max_cols_per_table", self.config.schema_max_cols_per_table),
            schema_format=run_meta.get("schema_format", self.config.schema_format),
            toolchain_max_steps=run_meta.get("toolchain_max_steps", self.config.toolchain_max_steps),
            toolchain_max_describe=run_meta.get("toolchain_max_describe", self.config.toolchain_max_describe),
            toolchain_timeout_sec=run_meta.get("toolchain_timeout_sec", self.config.toolchain_timeout_sec),
            toolchain_allow_sample_values=run_meta.get(
                "toolchain_allow_sample_values", self.config.toolchain_allow_sample_values
            ),
            sql_execution_timeout_sec=run_meta.get("sql_execution_timeout_sec", self.config.sql_execution_timeout_sec),
            task_timeout_sec=run_meta.get("task_timeout_sec", self.config.task_timeout_sec),
            architecture=run_meta.get("architecture"),
            generation_config=gen_cfg_resume,
        )

    def _execute(
        self,
        *,
        run_dir: Path,
        benches: Iterable[str],
        model_name: str,
        limit: int | None,
        bench_limits: dict[str, int] | None,
        shard: str | None,
        time_budget: int | None,
        completed_ids: set[str],
        run_all: bool = True,
        context_mode: str = "none",
        schema_max_tables: int = 50,
        schema_max_cols_per_table: int = 30,
        schema_format: str = "compact",
        toolchain_max_steps: int = 10,
        toolchain_max_describe: int = 6,
        toolchain_timeout_sec: int = 30,
        toolchain_allow_sample_values: int = 0,
        sql_execution_timeout_sec: int | None = None,
        task_timeout_sec: int | None = None,
        architecture: str | None = None,
        reasoning: str | None = None,
        sampling: str | None = None,
        sc_samples: int | None = None,
        sc_aggregation: str | None = None,
        generation_config: GenerationRunConfig | None = None,
    ) -> None:
        raw = getattr(self.config, "raw", {}) or {}
        gen_cfg = generation_config
        if gen_cfg is None:
            gen_cfg = resolve_generation_config(
                architecture=architecture,
                reasoning=reasoning,
                sampling=sampling,
                sc_samples=sc_samples,
                sc_aggregation=sc_aggregation,
                raw_config=raw,
            )
        # Effective architecture name for loading params: plain+self_consistency -> self_consistency
        effective_arch = gen_cfg.architecture
        if gen_cfg.architecture == "plain" and gen_cfg.sampling == "self_consistency":
            effective_arch = "self_consistency"
        # Build a separate dict for get_architecture_config — do NOT mutate self.config.raw
        arch_raw = {
            "architecture": {
                "name": effective_arch,
                "params": (raw.get("architecture") or {}).get("params", {}),
            },
        }
        arch_cfg = get_architecture_config(arch_raw)
        model_adapter = build_model_adapter(self.config, model_name)
        previous = [_dict_to_task_result(r) for r in load_results(run_dir)]
        results: dict[str, list[TaskResult]] = {b: [] for b in benches}
        for item in previous:
            if item.bench in results:
                results[item.bench].append(item)
        summaries: list[BenchSummary] = []

        progress = Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=self.console,
        )

        start_time = time.time()
        with progress:
            tasks_handles: dict[str, TaskID] = {}
            bench_tasks: dict[str, list[TaskSpec]] = {}
            for name in benches:
                bench = self._bench_instance(name, model_adapter)
                all_tasks = bench.discover_tasks()
                # Determine limit for this bench: explicit bench limit wins, else global limit (unless run_all).
                per_bench_limit = None
                if bench_limits and name in bench_limits:
                    per_bench_limit = bench_limits[name]
                elif not run_all:
                    per_bench_limit = limit
                tasks = _slice_tasks(all_tasks, limit=per_bench_limit, shard=shard)
                bench_tasks[name] = tasks
                done = len([t for t in tasks if t.task_id in completed_ids])
                tasks_handles[name] = progress.add_task(f"{name}", total=len(tasks), completed=done)

            for name in benches:
                bench = self._bench_instance(name, model_adapter)
                bench.float_tol = self.config.comparator.float_tol
                bench.column_order_insensitive = self.config.comparator.column_order_insensitive
                bench.string_normalize = self.config.comparator.string_normalize
                # TPC-DS baseline: use toolchain by default when context_mode is none
                effective_context = context_mode
                if name == "tpcds" and context_mode == "none":
                    effective_context = "toolchain"
                bench.context_mode = effective_context
                bench.schema_max_tables = schema_max_tables
                bench.schema_max_cols_per_table = schema_max_cols_per_table
                bench.schema_format = schema_format
                bench.toolchain_max_steps = toolchain_max_steps
                bench.toolchain_max_describe = toolchain_max_describe
                bench.toolchain_timeout_sec = toolchain_timeout_sec
                bench.toolchain_allow_sample_values = toolchain_allow_sample_values
                bench.sql_execution_timeout_sec = sql_execution_timeout_sec
                bench.architecture_config = arch_cfg
                bench.generation_config = gen_cfg
                # TPC-DS: copy DB once before first task (large DuckDB copy can take minutes; then 0/N is toolchain+API)
                if name == "tpcds" and bench_tasks[name] and hasattr(bench, "ensure_db_copy"):
                    first_task = bench_tasks[name][0]
                    if first_task.db_path:
                        bench.ensure_db_copy(Path(first_task.db_path))
                self._run_bench(
                    bench_name=name,
                    bench=bench,
                    tasks=bench_tasks[name],
                    progress=progress,
                    task_handle=tasks_handles[name],
                    run_dir=run_dir,
                    completed_ids=completed_ids,
                    results_sink=results,
                    time_budget=time_budget,
                    start_time=start_time,
                    task_timeout_sec=task_timeout_sec,
                )

        for name in benches:
            bench = self._bench_instance(name, model_adapter)
            summaries.append(bench.summarize(results[name]))

        write_bench_summaries(run_dir, summaries)
        generate_report(self.output_root, run_dir.name)
        write_event(run_dir, {"type": "complete", "run_id": run_dir.name})
