# EvalSuite: Text-to-SQL Benchmark Framework

A unified evaluation framework for benchmarking LLM-based SQL code generation across three datasets: **BIRD**, **Spider2-Lite**, and **TPC-DS NL**.

Built as part of a diploma research project investigating how different generation architectures (plain, self-consistency, SGR, SQL Factory, hybrid) affect text-to-SQL accuracy.

## Key Results

Best configuration: **qwen3-coder-next + Hybrid** = **33.7% overall accuracy** (592/1756 tasks).

| Architecture      | BIRD (1534) | Spider2 (123) | TPC-DS (99) | Total (1756) |
|-------------------|-------------|---------------|-------------|--------------|
| Plain             | 26.3%       | 12.2%         | 3.0%        | 24.0%        |
| Self-consistency  | 30.8%       | 65.0%         | 10.1%       | 32.0%        |
| SGR               | 30.1%       | 67.4%         | 4.0%        | 31.2%        |
| SQL Factory       | 29.8%       | 57.7%         | 8.1%        | 30.5%        |
| **Hybrid**        | **32.3%**   | **71.5%**     | 9.1%        | **33.7%**    |

Full results with error analysis: [EN](docs/en/RESULTS.md) | [RU](docs/ru/RESULTS.md)

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/your-username/sql-eval.git
cd sql-eval
uv sync
```

### 2. Configure API key

```bash
cp .env.example .env
# Edit .env: set LLM_API_KEY and LLM_BASE_URL for any OpenAI-compatible API
```

### 3. Configure settings

```bash
cp config.example.yaml config.yaml
# Edit config.yaml if needed (defaults work with OpenRouter)
```

All benchmark datasets are included in the repository under `data/`.

### 4. Run evaluation

```bash
# Quick test: 1 task on BIRD
uv run python -m evalsuite run --bench bird_sqlite --limit 1

# Full run on all benchmarks
uv run python -m evalsuite run-all

# With specific architecture
uv run python -m evalsuite run --bench bird_sqlite --architecture hybrid

# With named model from config
uv run python -m evalsuite run --model qwen3-coder-next --bench bird_sqlite
```

## CLI Reference

| Command | Description |
|---------|-------------|
| `run` | Run evaluation on selected benchmarks |
| `run-all` | Run all benchmarks, all tasks |
| `resume --run-id <id>` | Resume an interrupted run |
| `report --run-id <id>` | Generate report for a completed run |
| `list` | List completed runs |

### Flags (run)

| Flag | Default | Description |
|------|---------|-------------|
| `--model <name>` | `default` | Model from config.yaml (`default` = `model:` section) |
| `--bench <list>` | `all` | `all` or comma-separated: `bird_sqlite,spider2,tpcds` |
| `--limit N` | all tasks | Limit tasks per benchmark |
| `--architecture <name>` | from config | `plain`, `self_consistency`, `sgr`, `sql_factory`, `hybrid` |
| `--context-mode <mode>` | from config | `none`, `full_schema`, `toolchain` |
| `--task-timeout-sec N` | 120 | Max seconds per task (0 = no limit) |

All other settings (schema format, toolchain params, comparator, etc.) come from `config.yaml`.

## Configuration

| Source | Purpose |
|--------|---------|
| `.env` | Secrets: `LLM_API_KEY`, `LLM_BASE_URL` |
| `config.yaml` | Everything else: model, datasets, architecture, comparator |
| CLI flags | Override model, bench, architecture per run |

See [config.example.yaml](config.example.yaml) and [.env.example](.env.example).

## Run Output

Runs are organized as `runs/{benchmark}/{model}/{architecture}/`:

```
runs/bird/qwen3-coder-next/hybrid/
  raw/bird_sqlite.jsonl    # Per-task results (pred_sql, gold_sql, match, errors)
  summary.json             # Aggregated metrics
  report.md                # Human-readable report
  bird_sqlite_debug.md     # Error analysis
```

## Project Structure

```
sql-eval/
  evalsuite/                  # Main package (in src/)
    cli.py                    # CLI entry point
    orchestrator.py           # Run loop (discovery -> generation -> execution -> reporting)

    adapters/                 # External integrations
      db/                     # Database executors
        sqlite.py             #   SQLite (BIRD, Spider2)
        duckdb.py             #   DuckDB (TPC-DS)
      models/                 # LLM clients
        base.py               #   ModelAdapter protocol + factory
        openai.py             #   OpenAI-compatible HTTP client

    benchmarks/               # Dataset implementations
      base.py                 #   Abstract Benchmark class
      bird.py                 #   BIRD SQLite (1534 tasks)
      spider2.py              #   Spider2-Lite (123 tasks)
      tpcds.py                #   TPC-DS NL (99 tasks)

    architectures/            # SQL generation strategies
      plain.py                #   Single-shot baseline
      self_consistency.py     #   K-gen + majority vote
      sql_factory.py          #   Multi-round exploration/exploitation
      hybrid.py               #   Combined (SGR + SC + expansion)
      similarity.py           #   SQL similarity metrics (Jaccard, AST)
      single.py               #   Single candidate selector
      sgr/                    #   Semantic Graph Reasoning
        standalone.py         #     Full SGR pipeline
        layer.py              #     Grounding + plan extraction
        prompts.py            #     Prompt templates
        schema.py             #     Pydantic models

    core/                     # Shared infrastructure
      types.py                #   Data models (TaskSpec, ExecResult, CandidateResult, ...)
      config.py               #   YAML config loading + env var interpolation
      storage.py              #   Run directory IO (write results, summaries)
      generation_config.py    #   Architecture/sampling config resolution

    pipeline/                 # SQL execution pipeline
      toolchain.py            #   LLM tool-calling loop (list_tables -> describe -> SQL)
      schema.py               #   Schema prompt building (compact/DDL/JSON)
      schema_extract.py       #   Schema extraction from SQLite/DuckDB
      preflight.py            #   SQL validation, error classification, auto-patching
      aggregation.py          #   Candidate voting (majority, best_score, hybrid)
      result_signature.py     #   Result hashing for majority voting
      sql_sanitize.py         #   Strip markdown fences, detect placeholders

    compare/                  # Result comparison
      comparator.py           #   Float tolerance, column order, string normalization

    reporting/                # Report generation
      report.py               #   summary.json, report.md
      bench_debug.py          #   Error analysis per benchmark

  data/                       # Benchmark datasets (in repo)
    bird/                     #   dev.json + SQLite databases (1.4 GB)
    spider2/                  #   tasks.jsonl + SQLite databases
    tpcds/                    #   DuckDB + gold queries + NL tasks

  runs/                       # Evaluation results
    {bench}/{model}/{arch}/   #   Organized by benchmark/model/architecture

  docs/
    en/                       # English documentation
    ru/                       # Russian documentation

  config.yaml                 # Active configuration
  config.example.yaml         # Configuration template
  .env.example                # Environment variables template
  pyproject.toml              # Python package + ruff + pytest config
  .pre-commit-config.yaml     # Pre-commit hooks (ruff)
```

## Extending the Framework

### Adding a new LLM model

1. Add entry to `config.yaml` under `models:`:
```yaml
models:
  my-model:
    base_url: https://api.example.com/v1
    model: my-model-id
    api_key: ${LLM_API_KEY}
    temperature: 0.0
```
2. Run: `uv run python -m evalsuite run --model my-model --bench bird_sqlite --limit 5`

Any OpenAI-compatible API works out of the box. For non-OpenAI APIs, add a new adapter in `adapters/models/` implementing the `ModelAdapter` protocol (see `adapters/models/base.py`).

### Adding a new benchmark

1. Create `benchmarks/my_bench.py` extending `benchmarks.base.Benchmark`
2. Implement required methods:
   - `discover_tasks() -> list[TaskSpec]` — load tasks from dataset
   - `_get_dialect() -> str` — `"sqlite"` or `"duckdb"`
   - `_get_constraints() -> DialectConstraints` — allowed SQL statements
   - `_get_schema_context(db_path) -> SchemaContext` — extract schema
   - `_get_tool_executor(db_path) -> SchemaToolsExecutor` — for toolchain mode
   - `_execute_sql(db_path, sql) -> ExecResult` — run SQL
3. Register in `orchestrator.py` `BENCH_REGISTRY`
4. Add dataset to `data/my_bench/`

See `benchmarks/bird.py` for a complete example.

### Adding a new architecture

1. Create `architectures/my_arch.py` with a `run_my_arch()` function
2. It should accept task context (question, schema, model) and return `list[CandidateResult]`
3. Register in `benchmarks/base.py` `_run_task_common()` dispatch
4. Add to CLI choices in `cli.py`

See `architectures/plain.py` for the simplest example, `architectures/hybrid.py` for the most complex.

## Benchmarks

| Benchmark | Tasks | Database | Source | Notes |
|-----------|-------|----------|--------|-------|
| **BIRD** | 1534 | SQLite | [bird-bench.github.io](https://bird-bench.github.io) | Dev split from 12,751-task dataset |
| **Spider2-Lite** | 123 | SQLite | [spider2-sql.github.io](https://spider2-sql.github.io) | SQLite subset (Snowflake/BigQuery excluded) |
| **TPC-DS NL** | 99 | DuckDB | Generated | NL questions from 99 TPC-DS analytical queries |

## Architectures

| Architecture | Description | Docs |
|---|---|---|
| **Plain** | Single SQL generation per task | [EN](docs/en/architectures/PLAIN.md) |
| **Self-consistency** | K independent generations + majority vote | [EN](docs/en/architectures/SELF_CONSISTENCY.md) |
| **SGR** | Schema grounding + plan + synthesis + repair | [EN](docs/en/architectures/SGR.md) |
| **SQL Factory** | Multi-round pool with quality gates | [EN](docs/en/architectures/SQL_FACTORY.md) |
| **Hybrid** | All techniques combined (best results) | [EN](docs/en/architectures/HYBRID.md) |

## Models Tested

| Model | Provider | Notes |
|-------|----------|-------|
| qwen3-coder-next | OpenRouter | Best overall SQL generation quality |
| qwen3-32b | Self-hosted (vLLM) | Lower latency, fewer exec failures |
| gpt-oss-120b | Self-hosted | Poor SQL generation (63% generation failures) |

## Running Tests

```bash
uv run python -m pytest tests/ -v
```

## Development

```bash
uv sync --extra dev          # Install dev dependencies (ruff, pytest, pre-commit)
pre-commit install            # Enable pre-commit hooks
uv run ruff check src/ tests/ # Lint
uv run ruff format src/ tests/ # Format
```
