# Extending EvalSuite

## Adding a New LLM Model

**Simplest (OpenAI-compatible API):** just add an entry to `config.yaml`:

```yaml
models:
  my-model:
    base_url: https://api.example.com/v1
    model: my-model-id
    api_key: ${LLM_API_KEY}
    temperature: 0.0
```

Then run: `uv run python -m evalsuite run --model my-model --bench bird_sqlite --limit 5`

**Custom API (non-OpenAI):** implement the `ModelAdapter` protocol.

1. Create `evalsuite/adapters/models/my_provider.py`:

```python
class MyProviderAdapter:
    def __init__(self, model: str, base_url: str, api_key: str, extra: dict):
        ...

    def generate_sql(self, question: str, schema: str | None = None,
                     messages: list[dict] | None = None) -> str:
        """Send question + schema to LLM, return SQL string."""
        ...

    def generate_structured(self, prompt: str, schema_model: type[T], *,
                           system_prompt: str | None = None, ...) -> T:
        """Send prompt, parse JSON response, validate with Pydantic model."""
        ...
```

2. Register in `adapters/models/base.py` `build_model_adapter()`:

```python
if provider == "my_provider":
    return MyProviderAdapter(model=model_id, base_url=base_url, ...)
```

See `adapters/models/openai.py` for reference.

---

## Adding a New Benchmark

1. Create `evalsuite/benchmarks/my_bench.py`:

```python
from evalsuite.benchmarks.base import Benchmark
from evalsuite.core.types import TaskSpec, TaskResult, ExecResult, SchemaContext, DialectConstraints
from evalsuite.pipeline.toolchain import SchemaToolsExecutor

@dataclass
class MyBenchmark(Benchmark):
    name: str = "my_bench"

    def discover_tasks(self) -> list[TaskSpec]:
        """Load tasks from data/my_bench/tasks.jsonl"""
        ...

    def _get_dialect(self) -> str:
        return "sqlite"  # or "duckdb"

    def _get_constraints(self) -> DialectConstraints:
        return DialectConstraints(
            dialect="sqlite",
            allowed_statements=["SELECT", "WITH"],
        )

    def _get_schema_context(self, db_path: Path) -> SchemaContext:
        """Extract schema from database for prompt building."""
        ...

    def _get_tool_executor(self, db_path: Path) -> SchemaToolsExecutor:
        """Return executor for toolchain mode (list_tables, describe_table)."""
        ...

    def _execute_sql(self, db_path: str, sql: str) -> ExecResult:
        """Execute SQL against database, return result."""
        ...
```

2. Register in `orchestrator.py`:

```python
BENCH_REGISTRY = {
    ...,
    "my_bench": MyBenchmark,
}
```

3. Place data in `data/my_bench/`

4. Run: `uv run python -m evalsuite run --bench my_bench --limit 5`

See `benchmarks/bird.py` (simplest) or `benchmarks/tpcds.py` (most complex).

---

## Adding a New Architecture

1. Create `evalsuite/architectures/my_arch.py`:

```python
def run_my_arch(
    *,
    task_id: str,
    question: str,
    schema: str,
    model: ModelAdapter,
    db_path: str,
    dialect: str,
    params: dict,
    **kwargs,
) -> list[CandidateResult]:
    """Generate SQL candidates using your strategy.

    Returns list of CandidateResult with sql, exec_ok, result_signature, etc.
    The framework will select the best candidate via the sampling layer.
    """
    ...
```

2. Register in `benchmarks/base.py` `_run_task_common()`:

```python
elif arch_name == "my_arch":
    candidates = run_my_arch(task_id=task.task_id, ...)
```

3. Add to CLI choices in `cli.py` and `core/config.py` `VALID_ARCHITECTURES`

See `architectures/plain.py` for the simplest, `architectures/hybrid.py` for the most complex.

---

## Adding a New Database Adapter

1. Create `evalsuite/adapters/db/my_db.py`:

```python
from evalsuite.core.types import ExecResult

def execute_sql(db_path: str, sql: str, timeout_sec: int | None = None) -> ExecResult:
    """Execute SQL and return ExecResult(ok, rows, error, exec_time_ms)."""
    ...
```

2. Use in your benchmark's `_execute_sql()` method

See `adapters/db/sqlite.py` (simple) or `adapters/db/duckdb.py` (with preflight/EXPLAIN).

---

## Key Data Types (core/types.py)

| Type | Purpose |
|------|---------|
| `TaskSpec` | Input: task_id, question, gold_sql, db_path |
| `TaskResult` | Output: pred_sql, match, status, error_type |
| `ExecResult` | SQL execution result: ok, rows, error, exec_time_ms |
| `CandidateResult` | Architecture output: sql, exec_ok, score, result_signature |
| `SchemaContext` | Schema data: tables with columns, FKs |
| `DialectConstraints` | SQL rules: dialect, allowed_statements, forbidden_tokens |
| `BenchSummary` | Aggregated metrics per benchmark |
