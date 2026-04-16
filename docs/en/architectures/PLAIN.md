# Plain (Baseline)

## Overview

Single SQL generation per task. The simplest approach — one LLM call produces one SQL query, which is then executed against the database.

## Algorithm

1. Build schema context (full_schema / toolchain / none)
2. Send question + schema to LLM
3. Extract SQL from response
4. Execute SQL against the database
5. Compare result with gold standard

## Parameters

No architecture-specific parameters. Uses global settings:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `context_mode` | `toolchain` | How schema is provided: none, full_schema, toolchain |
| `task_timeout_sec` | 120 | Max seconds per task |

## CLI

```bash
uv run python -m evalsuite run --model qwen3-coder-next --bench bird_sqlite --architecture plain
```

## Results

For qwen3-coder-next, `context_mode=toolchain`:

| Benchmark | Accuracy | Exec failures |
|-----------|----------|---------------|
| BIRD | 31.4% (481/1534) | 59 |
| Spider2 | 48.0% (59/123) | 26 |
| TPC-DS | 8.1% (8/99) | 24 |

Dominant error category: `schema_mismatch` (76-100% of exec failures) — model hallucinates column/table names.
