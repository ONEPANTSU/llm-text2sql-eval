"""Integration tests for hybrid architecture through the full benchmark pipeline.

Tests Variant A (expansion_enabled=False) and Variant B (expansion_enabled=True)
end-to-end using BirdSQLiteBenchmark, Spider2Benchmark, and TPCDSNLBenchmark
with smoke/synthetic tasks and mock models.
This validates the complete path: run_task -> _run_task_common -> _dispatch_hybrid ->
run_hybrid -> aggregation -> compare -> TaskResult.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

duckdb = pytest.importorskip("duckdb")

from evalsuite.architectures.plain import ArchitectureConfig
from evalsuite.benchmarks.bird import BirdSQLiteBenchmark
from evalsuite.benchmarks.spider2 import Spider2Benchmark
from evalsuite.benchmarks.tpcds import TPCDSNLBenchmark
from evalsuite.core.config import ComparatorConfig, Config, DatasetPaths, ModelConfig, resolve_generation_config
from evalsuite.core.types import TaskSpec


class HybridFakeModel:
    """Model that accepts hybrid-specific kwargs (temperature, top_p, seed, messages)."""

    def generate_sql(
        self,
        question: str,
        schema: str | None = None,
        messages: Any = None,
        *,
        temperature: float | None = None,
        top_p: float | None = None,
        seed: int | None = None,
        **kwargs: Any,
    ) -> str:
        q = question.lower()
        if "version" in q:
            return "SELECT sqlite_version();"
        if "how many rows are in the sample" in q:
            return "SELECT COUNT(*) FROM sample;"
        if "values from sample ordered" in q:
            return "SELECT value FROM sample ORDER BY value ASC;"
        if "average v in metrics" in q:
            return "SELECT AVG(v) FROM metrics;"
        if "sum of v" in q:
            return "SELECT SUM(v) FROM metrics;"
        if "max v" in q:
            return "SELECT MAX(v) FROM metrics;"
        if "min v" in q:
            return "SELECT MIN(v) FROM metrics;"
        if "distinct values in sample" in q:
            return "SELECT COUNT(DISTINCT value) FROM sample;"
        if "contains 'alpha'" in q:
            return "SELECT COUNT(*) FROM sample WHERE value='alpha';"
        if "metrics greater than 10" in q:
            return "SELECT name, v FROM metrics WHERE v > 10 ORDER BY v DESC;"
        if "metrics less than 25" in q:
            return "SELECT name, v FROM metrics WHERE v < 25 ORDER BY v ASC;"
        if "average v by first letter" in q:
            return "SELECT substr(name,1,1) AS prefix, AVG(v) FROM metrics GROUP BY prefix;"
        if "how many metrics entries" in q:
            return "SELECT COUNT(*) FROM metrics;"
        if "top metric by v" in q:
            return "SELECT name, v FROM metrics ORDER BY v DESC LIMIT 1;"
        if "bottom metric by v" in q:
            return "SELECT name, v FROM metrics ORDER BY v ASC LIMIT 1;"
        if "with id > 1" in q:
            return "SELECT value FROM sample WHERE id > 1 ORDER BY id;"
        if "with id < 3" in q:
            return "SELECT value FROM sample WHERE id < 3 ORDER BY id;"
        if "ids reversed" in q:
            return "SELECT id FROM sample ORDER BY id DESC;"
        if "join sample to metrics" in q:
            return "SELECT s.id, s.value, m.name FROM sample s LEFT JOIN metrics m ON (s.id % 2) = (m.v % 2) ORDER BY s.id;"
        if "table name" in q:
            return "SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;"
        if "table" in q:
            return "SELECT COUNT(*) FROM sqlite_master WHERE type='table';"
        return "SELECT 1;"


def _config(tmp: Path) -> Config:
    return Config(
        model=ModelConfig(),
        comparator=ComparatorConfig(),
        datasets=DatasetPaths(
            bird_root=tmp / "bird",
            spider2_root=tmp / "spider2",
            tpcds_duckdb=tmp / "tpcds" / "duckdb.db",
        ),
        raw={},
    )


def _make_hybrid_bench(
    tmp_path: Path,
    expansion_enabled: bool,
    sgr_grounding: bool = False,
    initial_candidates: int = 3,
    run_dir: Path | None = None,
) -> BirdSQLiteBenchmark:
    """Create a BirdSQLiteBenchmark configured for hybrid architecture."""
    cfg = _config(tmp_path)
    model = HybridFakeModel()

    bench = BirdSQLiteBenchmark(config=cfg, model=model)
    bench.context_mode = "none"  # no schema discovery needed for smoke tasks

    # Set architecture config
    params = {
        "sgr_grounding": sgr_grounding,
        "initial_candidates": initial_candidates,
        "temperature": 0.7,
        "top_p": 0.9,
        "parallelism": "sequential",
        "max_workers": 1,
        "generation_timeout": 10,
        "expansion_enabled": expansion_enabled,
        "expansion_seeds": 2,
        "expansion_per_seed": 2,
        "expansion_sim_threshold": 0.85,
        "expansion_timeout": 10,
        "aggregation_mode": "hybrid",
        "execution_timeout": 10,
    }
    bench.architecture_config = ArchitectureConfig(name="hybrid", params=params)
    bench.generation_config = resolve_generation_config(architecture="hybrid")
    if run_dir:
        bench.run_dir = run_dir
    return bench


# ---------------------------------------------------------------------------
# Variant A: expansion_enabled=False (SC + optional SGR grounding)
# ---------------------------------------------------------------------------


class TestVariantA:
    """Variant A: expansion_enabled=False."""

    def test_variant_a_produces_results(self, tmp_path: Path):
        """Hybrid Variant A runs through full pipeline and produces TaskResults."""
        bench = _make_hybrid_bench(tmp_path, expansion_enabled=False)
        tasks = bench.discover_tasks()[:3]
        results = [bench.run_task(t) for t in tasks]
        assert len(results) == 3
        for r in results:
            assert r.pred_sql, f"task {r.task_id}: pred_sql should not be empty"
            assert r.latency_ms > 0, f"task {r.task_id}: latency_ms should be positive"
            assert "candidates_count" in r.extra

    def test_variant_a_matches_gold(self, tmp_path: Path):
        """Variant A with deterministic model produces correct matches."""
        bench = _make_hybrid_bench(tmp_path, expansion_enabled=False)
        tasks = bench.discover_tasks()[:5]
        results = [bench.run_task(t) for t in tasks]
        matching = [r for r in results if r.match]
        # All tasks should match (FakeModel returns gold SQL)
        assert len(matching) == len(results), (
            f"Expected all {len(results)} to match, got {len(matching)}. "
            f"Failed: {[(r.task_id, r.status, r.error_type) for r in results if not r.match]}"
        )

    def test_variant_a_extra_has_hybrid_artifact(self, tmp_path: Path):
        """Variant A results include hybrid artifact in extra."""
        bench = _make_hybrid_bench(tmp_path, expansion_enabled=False)
        tasks = bench.discover_tasks()[:1]
        result = bench.run_task(tasks[0])
        assert "hybrid" in result.extra
        artifact = result.extra["hybrid"]
        assert artifact["variant"] == "A"
        assert "initial_candidates" in artifact
        assert "aggregation" in artifact

    def test_variant_a_no_expansion_in_artifact(self, tmp_path: Path):
        """Variant A should not have expansion section (expansion_enabled=False)."""
        bench = _make_hybrid_bench(tmp_path, expansion_enabled=False)
        tasks = bench.discover_tasks()[:1]
        result = bench.run_task(tasks[0])
        artifact = result.extra["hybrid"]
        # Variant A: no expansion phase — expansion key absent from artifact
        assert "expansion" not in artifact

    def test_variant_a_candidates_count(self, tmp_path: Path):
        """Variant A candidates_count equals initial_candidates."""
        bench = _make_hybrid_bench(tmp_path, expansion_enabled=False, initial_candidates=3)
        tasks = bench.discover_tasks()[:1]
        result = bench.run_task(tasks[0])
        # All 3 candidates should succeed (no failures with FakeModel)
        assert result.extra["candidates_count"] == 3

    def test_variant_a_writes_artifact_file(self, tmp_path: Path):
        """Variant A writes hybrid artifact JSON to run_dir/raw/."""
        run_dir = tmp_path / "run_test"
        run_dir.mkdir()
        bench = _make_hybrid_bench(tmp_path, expansion_enabled=False, run_dir=run_dir)
        tasks = bench.discover_tasks()[:1]
        bench.run_task(tasks[0])
        raw_dir = run_dir / "raw"
        artifacts = list(raw_dir.glob("hybrid_*.json"))
        assert len(artifacts) == 1
        with artifacts[0].open() as f:
            data = json.load(f)
        assert data["variant"] == "A"
        assert "params" in data
        assert "latency_ms" in data


# ---------------------------------------------------------------------------
# Variant B: expansion_enabled=True (SC + expansion)
# ---------------------------------------------------------------------------


class TestVariantB:
    """Variant B: expansion_enabled=True."""

    def test_variant_b_produces_results(self, tmp_path: Path):
        """Hybrid Variant B runs through full pipeline and produces TaskResults."""
        bench = _make_hybrid_bench(tmp_path, expansion_enabled=True, initial_candidates=3)
        tasks = bench.discover_tasks()[:3]
        results = [bench.run_task(t) for t in tasks]
        assert len(results) == 3
        for r in results:
            assert r.pred_sql, f"task {r.task_id}: pred_sql should not be empty"
            assert r.latency_ms > 0

    def test_variant_b_matches_gold(self, tmp_path: Path):
        """Variant B with deterministic model produces correct matches."""
        bench = _make_hybrid_bench(tmp_path, expansion_enabled=True, initial_candidates=3)
        tasks = bench.discover_tasks()[:5]
        results = [bench.run_task(t) for t in tasks]
        matching = [r for r in results if r.match]
        assert len(matching) == len(results), (
            f"Expected all {len(results)} to match, got {len(matching)}. "
            f"Failed: {[(r.task_id, r.status, r.error_type) for r in results if not r.match]}"
        )

    def test_variant_b_extra_has_hybrid_artifact(self, tmp_path: Path):
        """Variant B results include hybrid artifact with expansion section."""
        bench = _make_hybrid_bench(tmp_path, expansion_enabled=True, initial_candidates=3)
        tasks = bench.discover_tasks()[:1]
        result = bench.run_task(tasks[0])
        assert "hybrid" in result.extra
        artifact = result.extra["hybrid"]
        assert artifact["variant"] == "B"
        assert "expansion" in artifact
        assert artifact["expansion"]["seeds_used"] >= 0

    def test_variant_b_candidates_count_includes_expansion(self, tmp_path: Path):
        """Variant B candidates_count >= initial (expansion adds more)."""
        bench = _make_hybrid_bench(tmp_path, expansion_enabled=True, initial_candidates=3)
        tasks = bench.discover_tasks()[:1]
        result = bench.run_task(tasks[0])
        # With expansion, pool may be larger than initial (or same if expansions filtered)
        assert result.extra["candidates_count"] >= 3

    def test_variant_b_writes_artifact_file(self, tmp_path: Path):
        """Variant B writes hybrid artifact JSON to run_dir/raw/."""
        run_dir = tmp_path / "run_test"
        run_dir.mkdir()
        bench = _make_hybrid_bench(tmp_path, expansion_enabled=True, initial_candidates=3, run_dir=run_dir)
        tasks = bench.discover_tasks()[:1]
        bench.run_task(tasks[0])
        raw_dir = run_dir / "raw"
        artifacts = list(raw_dir.glob("hybrid_*.json"))
        assert len(artifacts) == 1
        with artifacts[0].open() as f:
            data = json.load(f)
        assert data["variant"] == "B"
        assert "expansion" in data
        assert data["expansion"]["seeds_used"] >= 0

    def test_variant_b_multiple_tasks_all_succeed(self, tmp_path: Path):
        """Run 5 tasks with Variant B — all should complete without errors."""
        run_dir = tmp_path / "run_test"
        run_dir.mkdir()
        bench = _make_hybrid_bench(tmp_path, expansion_enabled=True, initial_candidates=3, run_dir=run_dir)
        tasks = bench.discover_tasks()[:5]
        results = [bench.run_task(t) for t in tasks]
        for r in results:
            assert r.status != "pred_fail" or r.error_type != "pred_generation_fail", (
                f"task {r.task_id}: unexpected generation failure"
            )
        # All artifacts written
        artifacts = list((run_dir / "raw").glob("hybrid_*.json"))
        assert len(artifacts) == 5


# ---------------------------------------------------------------------------
# Cross-variant tests
# ---------------------------------------------------------------------------


class TestCrossVariant:
    """Tests comparing Variant A and B behavior."""

    def test_summary_excludes_smoke(self, tmp_path: Path):
        """Smoke tasks with hybrid architecture are excluded from summary."""
        bench = _make_hybrid_bench(tmp_path, expansion_enabled=False)
        tasks = bench.discover_tasks()[:5]
        results = [bench.run_task(t) for t in tasks]
        summary = bench.summarize(results)
        # All tasks are smoke → total should be 0
        assert summary.total == 0

    def test_both_variants_produce_valid_results(self, tmp_path: Path):
        """Both variants produce valid TaskResults for the same tasks."""
        tasks_a = _make_hybrid_bench(tmp_path, expansion_enabled=False).discover_tasks()[:3]
        tasks_b = _make_hybrid_bench(tmp_path, expansion_enabled=True, initial_candidates=3).discover_tasks()[:3]

        bench_a = _make_hybrid_bench(tmp_path, expansion_enabled=False)
        bench_b = _make_hybrid_bench(tmp_path, expansion_enabled=True, initial_candidates=3)

        results_a = [bench_a.run_task(t) for t in tasks_a]
        results_b = [bench_b.run_task(t) for t in tasks_b]

        for ra, rb in zip(results_a, results_b):
            assert ra.task_id == rb.task_id
            assert ra.pred_sql  # both produce SQL
            assert rb.pred_sql
            assert ra.extra["hybrid"]["variant"] == "A"
            assert rb.extra["hybrid"]["variant"] == "B"


# ===========================================================================
# Spider2 hybrid E2E tests
# ===========================================================================


class Spider2FakeModel:
    """Model returning gold SQL for Spider2 smoke tasks (employees table)."""

    def generate_sql(
        self,
        question: str,
        schema: str | None = None,
        messages: Any = None,
        *,
        temperature: float | None = None,
        top_p: float | None = None,
        seed: int | None = None,
        **kwargs: Any,
    ) -> str:
        q = question.lower()
        if "count tables" in q:
            return "SELECT COUNT(*) FROM sqlite_master WHERE type='table';"
        if "any table name" in q:
            return "SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;"
        if "how many employees" in q:
            return "SELECT COUNT(*) FROM employees;"
        if "departments and headcount" in q:
            return "SELECT dept, COUNT(*) AS c FROM employees GROUP BY dept ORDER BY c DESC;"
        if "average salary per department" in q:
            return "SELECT dept, AVG(salary) FROM employees GROUP BY dept ORDER BY dept;"
        if "total salary payout" in q:
            return "SELECT SUM(salary) FROM employees;"
        if "max salary overall" in q:
            return "SELECT MAX(salary) FROM employees;"
        if "min salary overall" in q:
            return "SELECT MIN(salary) FROM employees;"
        if "employees in eng" in q:
            return "SELECT COUNT(*) FROM employees WHERE dept='eng';"
        if "average salary in eng" in q:
            return "SELECT AVG(salary) FROM employees WHERE dept='eng';"
        if "ordered by salary desc" in q:
            return "SELECT name, salary FROM employees ORDER BY salary DESC;"
        if "top 2 salaries" in q:
            return "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 2;"
        if "bottom 2 salaries" in q:
            return "SELECT name, salary FROM employees ORDER BY salary ASC LIMIT 2;"
        if "distinct departments" in q:
            return "SELECT DISTINCT dept FROM employees ORDER BY dept;"
        if "salary sum per department" in q:
            return "SELECT dept, SUM(salary) FROM employees GROUP BY dept ORDER BY dept;"
        if "eng vs non-eng" in q:
            return (
                "SELECT CASE WHEN dept='eng' THEN 'eng' ELSE 'other' END AS grp, COUNT(*) FROM employees GROUP BY grp;"
            )
        if "range" in q:
            return "SELECT MAX(salary) - MIN(salary) FROM employees;"
        if "salary above 100k" in q:
            return "SELECT name FROM employees WHERE salary > 100000 ORDER BY salary DESC;"
        if "salary below 95k" in q:
            return "SELECT name FROM employees WHERE salary < 95000 ORDER BY salary ASC;"
        if "highest average salary" in q:
            return "SELECT dept FROM employees GROUP BY dept ORDER BY AVG(salary) DESC LIMIT 1;"
        return "SELECT 1;"


def _make_spider2_hybrid_bench(
    tmp_path: Path,
    expansion_enabled: bool,
    initial_candidates: int = 3,
    run_dir: Path | None = None,
) -> Spider2Benchmark:
    """Create a Spider2Benchmark configured for hybrid architecture."""
    cfg = _config(tmp_path)
    model = Spider2FakeModel()

    bench = Spider2Benchmark(config=cfg, model=model)
    bench.context_mode = "none"

    params = {
        "sgr_grounding": False,
        "initial_candidates": initial_candidates,
        "temperature": 0.7,
        "top_p": 0.9,
        "parallelism": "sequential",
        "max_workers": 1,
        "generation_timeout": 10,
        "expansion_enabled": expansion_enabled,
        "expansion_seeds": 2,
        "expansion_per_seed": 2,
        "expansion_sim_threshold": 0.85,
        "expansion_timeout": 10,
        "aggregation_mode": "hybrid",
        "execution_timeout": 10,
    }
    bench.architecture_config = ArchitectureConfig(name="hybrid", params=params)
    bench.generation_config = resolve_generation_config(architecture="hybrid")
    if run_dir:
        bench.run_dir = run_dir
    return bench


class TestSpider2VariantA:
    """Spider2 + Variant A: expansion_enabled=False."""

    def test_produces_results(self, tmp_path: Path):
        bench = _make_spider2_hybrid_bench(tmp_path, expansion_enabled=False)
        tasks = bench.discover_tasks()[:3]
        results = [bench.run_task(t) for t in tasks]
        assert len(results) == 3
        for r in results:
            assert r.pred_sql, f"task {r.task_id}: pred_sql should not be empty"
            assert r.latency_ms > 0
            assert "candidates_count" in r.extra

    def test_matches_gold(self, tmp_path: Path):
        bench = _make_spider2_hybrid_bench(tmp_path, expansion_enabled=False)
        tasks = bench.discover_tasks()[:5]
        results = [bench.run_task(t) for t in tasks]
        matching = [r for r in results if r.match]
        assert len(matching) == len(results), (
            f"Expected all {len(results)} to match, got {len(matching)}. "
            f"Failed: {[(r.task_id, r.status, r.error_type) for r in results if not r.match]}"
        )

    def test_artifact_variant_a(self, tmp_path: Path):
        bench = _make_spider2_hybrid_bench(tmp_path, expansion_enabled=False)
        tasks = bench.discover_tasks()[:1]
        result = bench.run_task(tasks[0])
        assert "hybrid" in result.extra
        artifact = result.extra["hybrid"]
        assert artifact["variant"] == "A"
        assert "expansion" not in artifact

    def test_writes_artifact_file(self, tmp_path: Path):
        run_dir = tmp_path / "run_test"
        run_dir.mkdir()
        bench = _make_spider2_hybrid_bench(tmp_path, expansion_enabled=False, run_dir=run_dir)
        tasks = bench.discover_tasks()[:1]
        bench.run_task(tasks[0])
        artifacts = list((run_dir / "raw").glob("hybrid_*.json"))
        assert len(artifacts) == 1
        with artifacts[0].open() as f:
            data = json.load(f)
        assert data["variant"] == "A"


class TestSpider2VariantB:
    """Spider2 + Variant B: expansion_enabled=True."""

    def test_produces_results(self, tmp_path: Path):
        bench = _make_spider2_hybrid_bench(tmp_path, expansion_enabled=True)
        tasks = bench.discover_tasks()[:3]
        results = [bench.run_task(t) for t in tasks]
        assert len(results) == 3
        for r in results:
            assert r.pred_sql, f"task {r.task_id}: pred_sql should not be empty"

    def test_artifact_variant_b(self, tmp_path: Path):
        bench = _make_spider2_hybrid_bench(tmp_path, expansion_enabled=True)
        tasks = bench.discover_tasks()[:1]
        result = bench.run_task(tasks[0])
        assert "hybrid" in result.extra
        artifact = result.extra["hybrid"]
        assert artifact["variant"] == "B"
        assert "expansion" in artifact


# ===========================================================================
# TPC-DS NL hybrid E2E tests (DuckDB dialect)
# ===========================================================================


def _create_tpcds_duckdb(db_path: Path) -> None:
    """Create a minimal DuckDB with a demo_sales table for TPC-DS NL tests."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS demo_sales (
                id INTEGER,
                category VARCHAR,
                amount DOUBLE
            );
        """)
        con.execute("DELETE FROM demo_sales;")
        con.execute("INSERT INTO demo_sales VALUES (1, 'books', 10.0), (2, 'games', 20.0), (3, 'books', 30.0);")
    finally:
        con.close()


def _tpcds_synthetic_tasks(db_path: Path) -> list[TaskSpec]:
    """Create synthetic TPC-DS NL tasks with DuckDB-compatible SQL."""
    sqls = [
        ("Total sales amount in demo_sales?", "SELECT sum(amount) FROM demo_sales;"),
        ("How many demo_sales rows exist?", "SELECT count(*) FROM demo_sales;"),
        ("Max sale amount.", "SELECT max(amount) FROM demo_sales;"),
        ("Min sale amount.", "SELECT min(amount) FROM demo_sales;"),
        ("Count of unique amounts.", "SELECT COUNT(DISTINCT amount) FROM demo_sales;"),
    ]
    tasks = []
    for idx, (q, sql) in enumerate(sqls, start=1):
        tasks.append(
            TaskSpec(
                task_id=f"tpcds_test_{idx}",
                question=q,
                gold_sql=sql,
                db_path=str(db_path),
                bench="tpcds",
                meta={},
            )
        )
    return tasks


class TpcdsFakeModel:
    """Model returning gold SQL for DuckDB demo_sales tasks."""

    def generate_sql(
        self,
        question: str,
        schema: str | None = None,
        messages: Any = None,
        *,
        temperature: float | None = None,
        top_p: float | None = None,
        seed: int | None = None,
        **kwargs: Any,
    ) -> str:
        q = question.lower()
        if "total sales amount" in q:
            return "SELECT sum(amount) FROM demo_sales;"
        if "how many demo_sales rows" in q:
            return "SELECT count(*) FROM demo_sales;"
        if "max sale amount" in q:
            return "SELECT max(amount) FROM demo_sales;"
        if "min sale amount" in q:
            return "SELECT min(amount) FROM demo_sales;"
        if "unique amounts" in q:
            return "SELECT COUNT(DISTINCT amount) FROM demo_sales;"
        return "SELECT 1;"


def _make_tpcds_hybrid_bench(
    tmp_path: Path,
    expansion_enabled: bool,
    initial_candidates: int = 3,
    run_dir: Path | None = None,
) -> TPCDSNLBenchmark:
    """Create a TPCDSNLBenchmark configured for hybrid + full_schema context."""
    db_path = tmp_path / "tpcds" / "duckdb.db"
    _create_tpcds_duckdb(db_path)

    cfg = Config(
        model=ModelConfig(),
        comparator=ComparatorConfig(),
        datasets=DatasetPaths(
            bird_root=tmp_path / "bird",
            spider2_root=tmp_path / "spider2",
            tpcds_duckdb=db_path,
        ),
        raw={},
    )
    model = TpcdsFakeModel()

    bench = TPCDSNLBenchmark(config=cfg, model=model)
    bench.context_mode = "full_schema"  # TPC-DS requires context_mode != "none"

    params = {
        "sgr_grounding": False,
        "initial_candidates": initial_candidates,
        "temperature": 0.7,
        "top_p": 0.9,
        "parallelism": "sequential",
        "max_workers": 1,
        "generation_timeout": 10,
        "expansion_enabled": expansion_enabled,
        "expansion_seeds": 2,
        "expansion_per_seed": 2,
        "expansion_sim_threshold": 0.85,
        "expansion_timeout": 10,
        "aggregation_mode": "hybrid",
        "execution_timeout": 10,
    }
    bench.architecture_config = ArchitectureConfig(name="hybrid", params=params)
    bench.generation_config = resolve_generation_config(architecture="hybrid")
    if run_dir:
        bench.run_dir = run_dir
    return bench


class TestTpcdsVariantA:
    """TPC-DS NL (DuckDB) + Variant A: expansion_enabled=False."""

    def test_produces_results(self, tmp_path: Path):
        bench = _make_tpcds_hybrid_bench(tmp_path, expansion_enabled=False)
        tasks = _tpcds_synthetic_tasks(tmp_path / "tpcds" / "duckdb.db")[:3]
        results = [bench.run_task(t) for t in tasks]
        assert len(results) == 3
        for r in results:
            assert r.pred_sql, f"task {r.task_id}: pred_sql should not be empty"
            assert r.latency_ms > 0
            assert "candidates_count" in r.extra

    def test_matches_gold(self, tmp_path: Path):
        bench = _make_tpcds_hybrid_bench(tmp_path, expansion_enabled=False)
        tasks = _tpcds_synthetic_tasks(tmp_path / "tpcds" / "duckdb.db")[:5]
        results = [bench.run_task(t) for t in tasks]
        matching = [r for r in results if r.match]
        assert len(matching) == len(results), (
            f"Expected all {len(results)} to match, got {len(matching)}. "
            f"Failed: {[(r.task_id, r.status, r.error_type) for r in results if not r.match]}"
        )

    def test_artifact_variant_a(self, tmp_path: Path):
        bench = _make_tpcds_hybrid_bench(tmp_path, expansion_enabled=False)
        tasks = _tpcds_synthetic_tasks(tmp_path / "tpcds" / "duckdb.db")[:1]
        result = bench.run_task(tasks[0])
        assert "hybrid" in result.extra
        artifact = result.extra["hybrid"]
        assert artifact["variant"] == "A"
        assert "expansion" not in artifact
        assert "aggregation" in artifact

    def test_duckdb_dialect_in_pipeline(self, tmp_path: Path):
        """Verify the DuckDB dialect is properly handled through hybrid dispatch."""
        run_dir = tmp_path / "run_test"
        run_dir.mkdir()
        bench = _make_tpcds_hybrid_bench(tmp_path, expansion_enabled=False, run_dir=run_dir)
        tasks = _tpcds_synthetic_tasks(tmp_path / "tpcds" / "duckdb.db")[:1]
        result = bench.run_task(tasks[0])
        assert result.pred_sql
        assert result.match
        artifacts = list((run_dir / "raw").glob("hybrid_*.json"))
        assert len(artifacts) == 1


class TestTpcdsVariantB:
    """TPC-DS NL (DuckDB) + Variant B: expansion_enabled=True."""

    def test_produces_results(self, tmp_path: Path):
        bench = _make_tpcds_hybrid_bench(tmp_path, expansion_enabled=True)
        tasks = _tpcds_synthetic_tasks(tmp_path / "tpcds" / "duckdb.db")[:3]
        results = [bench.run_task(t) for t in tasks]
        assert len(results) == 3
        for r in results:
            assert r.pred_sql, f"task {r.task_id}: pred_sql should not be empty"

    def test_artifact_variant_b(self, tmp_path: Path):
        bench = _make_tpcds_hybrid_bench(tmp_path, expansion_enabled=True)
        tasks = _tpcds_synthetic_tasks(tmp_path / "tpcds" / "duckdb.db")[:1]
        result = bench.run_task(tasks[0])
        assert "hybrid" in result.extra
        artifact = result.extra["hybrid"]
        assert artifact["variant"] == "B"
        assert "expansion" in artifact
