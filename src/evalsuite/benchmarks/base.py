from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeoutError
from pathlib import Path
from time import perf_counter
from typing import Any

from evalsuite.architectures import select as sampling_select
from evalsuite.architectures.hybrid import build_hybrid_params, run_hybrid
from evalsuite.architectures.plain import (
    aggregate_candidates_from_sql_list,
    build_self_consistency_params,
    run_self_consistency,
)
from evalsuite.architectures.sql_factory import build_sql_factory_params, run_sql_factory
from evalsuite.core.types import (
    BenchSummary,
    DialectConstraints,
    ExecResult,
    SchemaContext,
    TaskResult,
    TaskSpec,
)
from evalsuite.pipeline.schema import build_schema_prompt
from evalsuite.pipeline.sql_sanitize import strip_sql_fences
from evalsuite.pipeline.toolchain import SchemaToolsExecutor, run_toolchain

_log = logging.getLogger(__name__)


class Benchmark(ABC):
    """Contract for benchmark datasets.

    To add a new benchmark, subclass this and implement the abstract/hook methods.

    Required:
        discover_tasks() -> list[TaskSpec]     # Load tasks from dataset
        run_task(task) -> TaskResult           # Execute single task (or use _run_task_common)

    Hooks (override for _run_task_common):
        _get_dialect() -> str                  # "sqlite" or "duckdb"
        _get_constraints() -> DialectConstraints
        _get_schema_context(db_path) -> SchemaContext
        _get_tool_executor(db_path) -> SchemaToolsExecutor
        _execute_sql(db_path, sql) -> ExecResult

    Optional:
        _should_skip(task) -> TaskResult|None  # Skip logic
        _post_execute(...)                     # Post-processing (auto-patch, validation)
        summarize(results) -> BenchSummary     # Custom summary

    Example: see benchmarks/bird.py for a complete implementation.

    Register in orchestrator.py BENCH_REGISTRY:
        BENCH_REGISTRY = {"my_bench": MyBenchmark, ...}
    """

    name: str
    context_mode: str = "none"
    model: Any = None  # ModelAdapter, set by orchestrator
    config: Any = None  # Config, set by orchestrator
    architecture_config: Any | None = None  # ArchitectureConfig when set by orchestrator
    generation_config: Any | None = None  # GenerationRunConfig: architecture + reasoning + sampling
    run_dir: Path | None = None  # Set by orchestrator for artifact writes (e.g. sql_factory)
    schema_max_tables: int = 50
    schema_max_cols_per_table: int = 30
    schema_format: str = "compact"
    toolchain_max_steps: int = 10
    toolchain_max_describe: int = 6
    toolchain_max_list_tables: int = 1
    toolchain_max_describe_per_table: int = 1
    toolchain_max_tool_only_streak: int = 4
    toolchain_max_tool_calls: int = 10
    toolchain_timeout_sec: int = 30
    toolchain_allow_sample_values: int = 0
    sql_execution_timeout_sec: int | None = None  # DuckDB run timeout; None = run in main thread (no timeout)

    # Comparison settings (subclasses may override)
    float_tol: float = 1e-4
    column_order_insensitive: bool = True
    string_normalize: bool = True

    @abstractmethod
    def discover_tasks(self) -> list[TaskSpec]: ...

    @abstractmethod
    def run_task(self, task: TaskSpec) -> TaskResult: ...

    # ------------------------------------------------------------------
    # Hooks for _run_task_common — subclasses override to provide
    # benchmark-specific behaviour.  Default implementations raise
    # NotImplementedError so that existing subclasses that override
    # run_task() directly continue to work.
    # ------------------------------------------------------------------

    def _get_dialect(self) -> str:
        """Return SQL dialect string: 'sqlite' or 'duckdb'."""
        return "sqlite"

    def _get_constraints(self) -> DialectConstraints:
        """Return dialect-specific SQL constraints."""
        raise NotImplementedError

    def _get_schema_context(self, db_path: Path) -> SchemaContext:
        """Build SchemaContext via live introspection of *db_path*."""
        raise NotImplementedError

    def _get_tool_executor(self, db_path: Path) -> SchemaToolsExecutor:
        """Return a SchemaToolsExecutor wired to *db_path*."""
        raise NotImplementedError

    def _execute_sql(self, db_path: str, sql: str) -> ExecResult:
        """Execute *sql* against the DB at *db_path* and return an ExecResult."""
        raise NotImplementedError

    def _should_skip(self, task: TaskSpec) -> TaskResult | None:
        """Return a skip TaskResult if the task cannot be executed, else None.

        Default: skip when db_path is missing or the backing file doesn't exist
        (for tasks where meta['db_id'] is set).
        """
        prompt = task.question
        db_path_obj = Path(task.db_path) if task.db_path else None
        is_expected = bool(task.meta.get("db_id")) if task.meta else False
        if not task.db_path or (is_expected and db_path_obj is not None and not db_path_obj.exists()):
            skip_extra: dict[str, Any] = {"question": task.question, "db_path": task.db_path, "candidates_count": 0}
            if task.meta and task.meta.get("smoke"):
                skip_extra["smoke"] = True
            return TaskResult(
                task_id=task.task_id,
                bench=self.name,
                gold_sql=task.gold_sql,
                pred_sql="",
                prompt=prompt,
                gold=ExecResult(ok=False, rows=None, error="missing_db"),
                pred=ExecResult(ok=False, rows=None, error="missing_db"),
                match=False,
                status="skip",
                error_message="DB not found",
                error_type="missing_db",
                latency_ms=0,
                extra=skip_extra,
            )
        return None

    def _post_execute(
        self,
        task: TaskSpec,
        pred_sql: str,
        pred_exec: ExecResult,
        err_type: str | None,
        extra: dict[str, Any],
    ) -> tuple[str, ExecResult, str | None, dict[str, Any]]:
        """Hook called after pred SQL execution, before comparison.

        Subclasses (e.g. tpcds) can perform auto-patching, schema
        validation, or other post-processing here.

        Returns: (pred_sql, pred_exec, err_type, extra)  — possibly modified.
        """
        return pred_sql, pred_exec, err_type, extra

    def _build_extra_base(self, task: TaskSpec) -> dict[str, Any]:
        """Build the initial extra dict for a task result."""
        extra: dict[str, Any] = {"question": task.question, "db_path": task.db_path, "context_mode": self.context_mode}
        if task.meta and task.meta.get("smoke"):
            extra["smoke"] = True
        return extra

    # ------------------------------------------------------------------
    # Common run_task implementation.
    #
    # Subclasses that want to use the common dispatch can call
    # self._run_task_common(task) from their run_task().  The method
    # orchestrates: schema context → architecture dispatch → SQL
    # generation → execution → comparison → TaskResult.
    # ------------------------------------------------------------------

    def _run_task_common(self, task: TaskSpec) -> TaskResult:
        """Shared task runner with architecture/context-mode dispatch.

        Calls the hook methods defined above for benchmark-specific work.
        """
        # 0. Skip check
        skip = self._should_skip(task)
        if skip is not None:
            return skip

        db_path_obj = Path(task.db_path)
        db_path_str = self._resolve_db_path(task)
        dialect = self._get_dialect()
        prompt = task.question
        t0 = perf_counter()
        pred_sql = ""
        tool_calls_info: list[dict[str, Any]] = []
        extra_base = self._build_extra_base(task)

        try:
            constraints = self._get_constraints()
            arch = getattr(self, "architecture_config", None)
            arch_name = getattr(arch, "name", None) if arch else None
            arch_params = getattr(arch, "params", None) if arch else None

            use_sgr = arch_name == "sgr"
            use_sql_factory = arch_name == "sql_factory" and arch_params
            use_self_consistency = arch_name == "self_consistency" and arch_params
            use_hybrid = arch_name == "hybrid" and arch_params

            if use_hybrid:
                pred_sql, extra_base = self._dispatch_hybrid(
                    task, db_path_obj, db_path_str, dialect, constraints, extra_base
                )
            elif use_sgr:
                pred_sql, extra_base = self._dispatch_sgr(
                    task, db_path_obj, db_path_str, dialect, constraints, extra_base
                )
            elif use_sql_factory:
                pred_sql, extra_base = self._dispatch_sql_factory(
                    task, db_path_obj, db_path_str, dialect, constraints, extra_base
                )
            elif use_self_consistency:
                pred_sql, tool_calls_info, extra_base = self._dispatch_self_consistency(
                    task, db_path_obj, db_path_str, dialect, constraints, extra_base
                )
            elif self.context_mode == "full_schema":
                pred_sql, extra_base = self._dispatch_full_schema(
                    task, db_path_obj, db_path_str, dialect, constraints, extra_base
                )
            elif self.context_mode == "toolchain":
                result = self._dispatch_toolchain_plain(
                    task, db_path_obj, db_path_str, dialect, constraints, extra_base, t0
                )
                if isinstance(result, TaskResult):
                    return result  # early return on toolchain failure
                pred_sql, tool_calls_info, extra_base = result
            else:
                pred_sql = self.model.generate_sql(task.question)
                extra_base["candidates_count"] = 1

            extra_base["tool_calls"] = tool_calls_info
            latency_ms = (perf_counter() - t0) * 1000
        except Exception as exc:  # pragma: no cover - network failure path
            return TaskResult(
                task_id=task.task_id,
                bench=self.name,
                gold_sql=task.gold_sql,
                pred_sql="",
                prompt=prompt,
                gold=ExecResult(ok=False, rows=None, error="gold_not_executed"),
                pred=ExecResult(ok=False, rows=None, error=str(exc)),
                match=False,
                status="pred_fail",
                error_message=str(exc),
                error_type="pred_generation_fail",
                latency_ms=(perf_counter() - t0) * 1000,
                extra={
                    **extra_base,
                    "tool_calls": tool_calls_info,
                    "candidates_count": extra_base.get("candidates_count", 0),
                },
            )

        # --- Post-generation: sanitize, execute, compare ---
        pred_sql = strip_sql_fences(pred_sql)
        gold_exec: ExecResult = self._execute_sql(db_path_str, task.gold_sql)
        pred_exec: ExecResult = self._execute_sql(db_path_str, pred_sql)

        # Determine initial error type
        err_type: str | None = None
        # Check for hybrid all_candidates_failed status
        hybrid_art = extra_base.get("hybrid")
        if hybrid_art and hybrid_art.get("aggregation", {}).get("aggregation_reason") == "all_candidates_failed":
            err_type = "all_candidates_failed"
        elif not pred_exec.ok:
            err_type = "pred_exec_fail"

        # Post-execute hook (auto-patching etc.)
        pred_sql, pred_exec, err_type, extra_base = self._post_execute(task, pred_sql, pred_exec, err_type, extra_base)

        # Compare
        order_by = "order by" in task.gold_sql.lower()
        match = False
        status = "ok"
        err_msg: str | None = None
        if gold_exec.ok and pred_exec.ok:
            from evalsuite.compare.comparator import compare_results

            comp = compare_results(
                gold_exec.rows or [],
                pred_exec.rows or [],
                order_by=order_by,
                float_tol=self.float_tol,
                column_order_insensitive=self.column_order_insensitive,
                string_normalize=self.string_normalize,
            )
            match = comp.match
            err_type = comp.reason
        elif not gold_exec.ok:
            status = "gold_fail"
            err_type = "gold_exec_fail"
            err_msg = gold_exec.error
        else:
            status = "pred_fail"
            err_type = err_type or "pred_exec_fail"
            err_msg = pred_exec.error

        return TaskResult(
            task_id=task.task_id,
            bench=self.name,
            gold_sql=task.gold_sql,
            pred_sql=pred_sql,
            prompt=prompt,
            gold=gold_exec,
            pred=pred_exec,
            match=match,
            status=status,
            error_message=err_msg,
            error_type=err_type,
            latency_ms=latency_ms,
            extra=extra_base,
        )

    # ------------------------------------------------------------------
    # Shared dispatch helpers — each architecture path is factored out
    # so subclasses can override individual paths if needed.
    # ------------------------------------------------------------------

    def _resolve_db_path(self, task: TaskSpec) -> str:
        """Return the actual DB path string to open.  Subclasses (e.g. tpcds)
        may return a temp-copy path to avoid lock conflicts."""
        return task.db_path

    def _build_schema_prompt(self, schema_ctx: SchemaContext, constraints: DialectConstraints) -> str:
        return build_schema_prompt(
            schema_ctx,
            fmt=self.schema_format,
            max_tables=self.schema_max_tables,
            max_cols=self.schema_max_cols_per_table,
            constraints=constraints,
        )

    def _run_toolchain_once(
        self,
        task: TaskSpec,
        db_path: Path,
        constraints: DialectConstraints,
        *,
        max_steps: int | None = None,
        timeout_sec: int | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        seed: int | None = None,
    ) -> tuple[str, Any, Any, Any, Any, Any]:
        """Single toolchain invocation.  Returns the same tuple as run_toolchain."""
        return run_toolchain(
            model=self.model,
            question=task.question,
            tools=self._get_tool_executor(db_path),
            constraints=constraints,
            max_steps=max_steps if max_steps is not None else self.toolchain_max_steps,
            timeout_sec=timeout_sec if timeout_sec is not None else self.toolchain_timeout_sec,
            allow_sample_values=self.toolchain_allow_sample_values,
            max_describe=self.toolchain_max_describe,
            max_list_tables=self.toolchain_max_list_tables,
            max_describe_per_table=self.toolchain_max_describe_per_table,
            max_tool_only_streak=self.toolchain_max_tool_only_streak,
            max_tool_calls=self.toolchain_max_tool_calls,
            temperature=temperature,
            top_p=top_p,
            seed=seed,
        )

    def _maybe_sgr_enrich(
        self, schema_prompt: str | None, task: TaskSpec, extra: dict[str, Any]
    ) -> tuple[str | None, dict[str, Any]]:
        """If generation_config.reasoning == 'sgr', enrich schema_prompt with SGR grounding."""
        gen_cfg = getattr(self, "generation_config", None)
        if gen_cfg and getattr(gen_cfg, "reasoning", None) == "sgr":
            from evalsuite.architectures.sgr.layer import run_sgr_grounding_and_plan

            sgr_ctx = run_sgr_grounding_and_plan(task.question, schema_prompt, self.model)
            schema_prompt = (schema_prompt or "") + (sgr_ctx.prompt_addendum or "")
            extra["reasoning"] = {
                "sgr": {
                    "grounding": sgr_ctx.grounding.model_dump(),
                    "plan": sgr_ctx.plan.model_dump(),
                    "repair_attempts": [],
                }
            }
        return schema_prompt, extra

    # --- Architecture dispatchers ---

    def _dispatch_sgr(
        self,
        task: TaskSpec,
        db_path_obj: Path,
        db_path_str: str,
        dialect: str,
        constraints: DialectConstraints,
        extra: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        from evalsuite.architectures.sgr.standalone import run_sgr_standalone

        schema_ctx = self._get_schema_context(db_path_obj)
        _schema: str | None = None
        toolchain_timeout_sgr = min(25, self.toolchain_timeout_sec)
        toolchain_max_steps_sgr = min(3, self.toolchain_max_steps)
        if self.context_mode == "toolchain":
            sql_text_tc, tool_calls, _m, _fr, _ex, inspected = self._run_toolchain_once(
                task,
                db_path_obj,
                constraints,
                max_steps=toolchain_max_steps_sgr,
                timeout_sec=toolchain_timeout_sgr,
            )
            extra["tool_calls"] = [tc.__dict__ for tc in tool_calls]
            _schema = self._build_schema_prompt(schema_ctx, constraints)
        elif self.context_mode == "full_schema":
            _schema = self._build_schema_prompt(schema_ctx, constraints)

        def get_context_sgr() -> dict[str, Any]:
            return {"question": task.question, "schema": _schema}

        arch = self.architecture_config
        candidates, sgr_artifact, _ = run_sgr_standalone(
            task_id=task.task_id,
            get_context=get_context_sgr,
            model=self.model,
            db_path=db_path_str,
            dialect=dialect,
            params=getattr(arch, "params", {}) or {},
            sql_execution_timeout_sec=self.sql_execution_timeout_sec,
            num_candidates=6,
        )
        extra["reasoning"] = {"sgr": sgr_artifact.get("sgr", {})}
        gen_cfg = getattr(self, "generation_config", None)
        sampling_mode = gen_cfg.sampling if gen_cfg else "single"
        sampling_config = {"sc_aggregation": gen_cfg.sc_aggregation} if gen_cfg else {}
        selected, sampling_meta = sampling_select(
            candidates, mode=sampling_mode, context={"question": task.question}, config=sampling_config
        )
        extra["sampling"] = sampling_meta
        extra["candidates_count"] = len(candidates)
        return selected.sql, extra

    def _dispatch_sql_factory(
        self,
        task: TaskSpec,
        db_path_obj: Path,
        db_path_str: str,
        dialect: str,
        constraints: DialectConstraints,
        extra: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        arch = self.architecture_config
        sf_params = build_sql_factory_params(arch.params)
        _schema: str | None = None
        _table_names: list[str] = []
        if self.context_mode == "toolchain":
            sql_text, tool_calls, _m, _fr, _ex, inspected = self._run_toolchain_once(task, db_path_obj, constraints)
            extra["tool_calls"] = [tc.__dict__ for tc in tool_calls]
            _table_names = list(inspected) if inspected else []
            schema_ctx = self._get_schema_context(db_path_obj)
            _schema = self._build_schema_prompt(schema_ctx, constraints)
            if not _table_names:
                _table_names = [t.name for t in schema_ctx.tables]
        elif self.context_mode == "full_schema":
            schema_ctx = self._get_schema_context(db_path_obj)
            _schema = self._build_schema_prompt(schema_ctx, constraints)
            _table_names = [t.name for t in schema_ctx.tables]

        _schema, extra = self._maybe_sgr_enrich(_schema, task, extra)

        def get_context_sf() -> dict[str, Any]:
            return {"question": task.question, "schema": _schema}

        candidates, sql_factory_artifact, _ = run_sql_factory(
            task_id=task.task_id,
            get_context=get_context_sf,
            model=self.model,
            db_path=db_path_str,
            dialect=dialect,
            params=sf_params,
            sql_execution_timeout_sec=self.sql_execution_timeout_sec,
            all_table_names=_table_names or None,
            run_dir=self.run_dir,
        )
        extra["sql_factory"] = sql_factory_artifact
        gen_cfg = getattr(self, "generation_config", None)
        sampling_mode = gen_cfg.sampling if gen_cfg else "single"
        sampling_config = {"sc_aggregation": gen_cfg.sc_aggregation} if gen_cfg else {}
        selected, sampling_meta = sampling_select(
            candidates, mode=sampling_mode, context={"question": task.question}, config=sampling_config
        )
        extra["sampling"] = sampling_meta
        extra["candidates_count"] = len(candidates)
        return selected.sql, extra

    def _dispatch_hybrid(
        self,
        task: TaskSpec,
        db_path_obj: Path,
        db_path_str: str,
        dialect: str,
        constraints: DialectConstraints,
        extra: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        """Hybrid architecture dispatch: K independent candidates + aggregation."""
        arch = self.architecture_config
        hybrid_params = build_hybrid_params(arch.params)

        # Build schema context (same as sql_factory / self-consistency)
        _schema: str | None = None
        _schema_info: dict[str, list[str]] | None = None
        if self.context_mode == "toolchain":
            sql_text, tool_calls, _m, _fr, _ex, inspected = self._run_toolchain_once(task, db_path_obj, constraints)
            extra["tool_calls"] = [tc.__dict__ for tc in tool_calls]
            schema_ctx = self._get_schema_context(db_path_obj)
            _schema = self._build_schema_prompt(schema_ctx, constraints)
            _schema_info = {t.name: [c.name for c in t.columns] for t in schema_ctx.tables}
        elif self.context_mode == "full_schema":
            schema_ctx = self._get_schema_context(db_path_obj)
            _schema = self._build_schema_prompt(schema_ctx, constraints)
            _schema_info = {t.name: [c.name for c in t.columns] for t in schema_ctx.tables}

        # Phase 1: optional SGR grounding enrichment
        if hybrid_params.sgr_grounding:
            try:
                from evalsuite.architectures.sgr.layer import run_sgr_grounding_and_plan

                sgr_ctx = run_sgr_grounding_and_plan(task.question, _schema, self.model)
                _schema = (_schema or "") + (sgr_ctx.prompt_addendum or "")
                extra["reasoning"] = {
                    "sgr": {
                        "grounding": sgr_ctx.grounding.model_dump(),
                        "plan": sgr_ctx.plan.model_dump(),
                    }
                }
            except Exception as sgr_err:
                extra["reasoning"] = {"sgr": {"error": str(sgr_err)}}

        def get_context_hybrid() -> dict[str, Any]:
            if _schema is not None:
                return {"question": task.question, "schema": _schema}
            return {"question": task.question}

        sel_sql, candidates, artifact = run_hybrid(
            task_id=task.task_id,
            get_context=get_context_hybrid,
            model=self.model,
            db_path=db_path_str,
            dialect=dialect,
            params=hybrid_params,
            sql_execution_timeout_sec=self.sql_execution_timeout_sec,
            run_dir=self.run_dir,
            schema_info=_schema_info,
        )
        extra["hybrid"] = artifact
        extra["candidates_count"] = len(candidates)
        return sel_sql, extra

    def _dispatch_self_consistency(
        self,
        task: TaskSpec,
        db_path_obj: Path,
        db_path_str: str,
        dialect: str,
        constraints: DialectConstraints,
        extra: dict[str, Any],
    ) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
        """Returns (pred_sql, tool_calls_info, extra)."""
        arch = self.architecture_config
        sc_params = build_self_consistency_params(arch.params)
        tool_calls_info: list[dict[str, Any]] = []

        if self.context_mode == "toolchain":
            pred_sql, tool_calls_info, extra = self._sc_toolchain(
                task, db_path_obj, db_path_str, dialect, constraints, sc_params, extra
            )
        else:
            pred_sql, extra = self._sc_schema_or_none(
                task, db_path_obj, db_path_str, dialect, constraints, sc_params, extra
            )
        return pred_sql, tool_calls_info, extra

    def _sc_toolchain(
        self,
        task: TaskSpec,
        db_path_obj: Path,
        db_path_str: str,
        dialect: str,
        constraints: DialectConstraints,
        sc_params: Any,
        extra: dict[str, Any],
    ) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
        """Self-consistency with K independent toolchain runs."""
        tool_calls_info: list[dict[str, Any]] = []

        def _one_run(i: int) -> tuple[int, str, Any]:
            seed = (sc_params.base_seed + i) if sc_params.seed_strategy == "per_attempt" else sc_params.base_seed
            sql_text, tool_calls, _m, _fr, _ex, _in = self._run_toolchain_once(
                task,
                db_path_obj,
                constraints,
                temperature=sc_params.temperature,
                top_p=sc_params.top_p,
                seed=seed,
            )
            return (i, sql_text or "", tool_calls)

        sql_list: list[tuple[int, str]] = []
        worker_timeout_sec = sc_params.generation_timeout_per_attempt or 60
        if sc_params.parallelism == "parallel" and sc_params.num_samples > 1:
            workers = min(sc_params.max_workers, sc_params.num_samples)
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = [ex.submit(_one_run, i) for i in range(sc_params.num_samples)]
                future_to_i = {f: j for j, f in enumerate(futures)}
                try:
                    for f in as_completed(futures, timeout=worker_timeout_sec):
                        try:
                            i, sql_text, tool_calls = f.result()
                        except Exception:
                            i, sql_text, tool_calls = future_to_i[f], "", []
                        sql_list.append((i, sql_text))
                        if i == 0 and tool_calls:
                            tool_calls_info = [tc.__dict__ for tc in tool_calls]
                except (FuturesTimeoutError, TimeoutError):
                    pending = sum(1 for f in futures if not f.done())
                    print(
                        f"[evalsuite] worker batch timeout ({worker_timeout_sec}s), marking {pending} unfinished",
                        flush=True,
                    )
                    for j, f in enumerate(futures):
                        if not f.done():
                            sql_list.append((j, ""))
                sql_list.sort(key=lambda x: x[0])
        else:
            with ThreadPoolExecutor(max_workers=1) as ex:
                for i in range(sc_params.num_samples):
                    fut = ex.submit(_one_run, i)
                    try:
                        i, sql_text, tool_calls = fut.result(timeout=worker_timeout_sec)
                    except (FuturesTimeoutError, TimeoutError):
                        print(f"[evalsuite] worker timeout ({worker_timeout_sec}s) on attempt {i}", flush=True)
                        sql_text, tool_calls = "", []
                    except Exception:
                        sql_text, tool_calls = "", []
                    sql_list.append((i, sql_text))
                    if i == 0 and tool_calls:
                        tool_calls_info = [tc.__dict__ for tc in tool_calls]

        pred_sql, candidates_dicts, agg_dict = aggregate_candidates_from_sql_list(
            sql_list,
            db_path_str,
            dialect,
            sc_params,
            self.sql_execution_timeout_sec,
        )
        extra["candidates"] = candidates_dicts
        extra["aggregation"] = agg_dict
        extra["sampling"] = {
            "mode": "self_consistency",
            "selected_attempt_id": agg_dict.get("selected_attempt_id"),
            "aggregation_reason": agg_dict.get("aggregation_reason"),
            "votes": agg_dict.get("votes", {}),
        }
        extra["candidates_count"] = len(candidates_dicts)
        return pred_sql, tool_calls_info, extra

    def _sc_schema_or_none(
        self,
        task: TaskSpec,
        db_path_obj: Path,
        db_path_str: str,
        dialect: str,
        constraints: DialectConstraints,
        sc_params: Any,
        extra: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        """Self-consistency with full_schema or no context."""
        _sc_schema: str | None = None
        if self.context_mode == "full_schema":
            schema_ctx = self._get_schema_context(db_path_obj)
            _sc_schema = self._build_schema_prompt(schema_ctx, constraints)
            _sc_schema, extra = self._maybe_sgr_enrich(_sc_schema, task, extra)

        def get_context() -> dict[str, Any]:
            if _sc_schema is not None:
                return {"question": task.question, "schema": _sc_schema}
            return {"question": task.question}

        pred_sql, candidates_dicts, agg_dict, task_timeout_hit = run_self_consistency(
            task_id=task.task_id,
            get_context=get_context,
            model=self.model,
            db_path=db_path_str,
            dialect=dialect,
            params=sc_params,
            sql_execution_timeout_sec=self.sql_execution_timeout_sec,
        )
        extra["candidates"] = candidates_dicts
        extra["aggregation"] = agg_dict
        extra["sampling"] = {
            "mode": "self_consistency",
            "selected_attempt_id": agg_dict.get("selected_attempt_id"),
            "aggregation_reason": agg_dict.get("aggregation_reason"),
            "votes": agg_dict.get("votes", {}),
        }
        extra["candidates_count"] = len(candidates_dicts)
        if task_timeout_hit:
            extra["task_timeout"] = True
        return pred_sql, extra

    def _dispatch_full_schema(
        self,
        task: TaskSpec,
        db_path_obj: Path,
        db_path_str: str,
        dialect: str,
        constraints: DialectConstraints,
        extra: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        """Plain generation with full schema in prompt."""
        schema_ctx = self._get_schema_context(db_path_obj)
        schema_prompt = self._build_schema_prompt(schema_ctx, constraints)
        schema_prompt, extra = self._maybe_sgr_enrich(schema_prompt, task, extra)
        pred_sql = self.model.generate_sql(task.question, schema=schema_prompt)
        extra["candidates_count"] = 1
        return pred_sql, extra

    def _dispatch_toolchain_plain(
        self,
        task: TaskSpec,
        db_path_obj: Path,
        db_path_str: str,
        dialect: str,
        constraints: DialectConstraints,
        extra: dict[str, Any],
        t0: float,
    ) -> TaskResult | tuple[str, list[dict[str, Any]], dict[str, Any]]:
        """Plain generation via toolchain.  Returns TaskResult on failure or
        (pred_sql, tool_calls_info, extra) on success."""
        sql_text, tool_calls, _messages, _fail_reason, _exploration, _inspected = self._run_toolchain_once(
            task, db_path_obj, constraints
        )
        tool_calls_info = [tc.__dict__ for tc in tool_calls]
        if sql_text:
            extra["candidates_count"] = 1
            return sql_text, tool_calls_info, extra
        fail_extra: dict[str, Any] = {"question": task.question, "tool_calls": tool_calls_info, "candidates_count": 0}
        if extra.get("smoke"):
            fail_extra["smoke"] = True
        return TaskResult(
            task_id=task.task_id,
            bench=self.name,
            gold_sql=task.gold_sql,
            pred_sql="",
            prompt=task.question,
            gold=ExecResult(ok=False, rows=None, error="gold_not_executed"),
            pred=ExecResult(ok=False, rows=None, error="toolchain_no_sql"),
            match=False,
            status="pred_fail",
            error_message="toolchain_no_sql",
            error_type="pred_generation_fail",
            latency_ms=(perf_counter() - t0) * 1000,
            extra=fail_extra,
        )

    # ------------------------------------------------------------------
    # Default summarize — identical across bird, spider2, tpcds.
    # Subclasses can still override if they need custom logic.
    # ------------------------------------------------------------------

    def summarize(self, results: Iterable[TaskResult]) -> BenchSummary:
        res_list = list(results)
        smoke_count = sum(1 for r in res_list if r.extra.get("smoke"))
        if smoke_count:
            _log.warning(
                "%s: %d/%d results are smoke tasks — excluding from summary",
                self.name,
                smoke_count,
                len(res_list),
            )
            res_list = [r for r in res_list if not r.extra.get("smoke")]
        total = len(res_list)
        skipped = sum(1 for r in res_list if r.status == "skip")
        gold_failed = sum(1 for r in res_list if r.status == "gold_fail")
        pred_failed = sum(1 for r in res_list if r.status == "pred_fail")
        executed = total - skipped
        comparable = [r for r in res_list if r.status == "ok" and r.gold.ok and r.pred.ok]
        compared = len(comparable)
        ex_correct = sum(1 for r in comparable if r.match)

        return BenchSummary(
            bench=self.name,
            total=total,
            executed=executed,
            skipped=skipped,
            gold_failed=gold_failed,
            pred_failed=pred_failed,
            ex_correct=ex_correct,
            compared=compared,
        )
