# Evaluation Results

## Datasets

- [BIRD](https://bird-bench.github.io) — 1534 tasks. Dev split from the full 12,751-task dataset.
- [Spider2-Lite](https://spider2-sql.github.io) — 123 tasks. SQLite subset only (Snowflake/BigQuery excluded).
- [TPC-DS NL](https://www.tpc.org/tpcds/) — 99 tasks. NL questions generated from 99 TPC-DS analytical SQL queries.

## Models

All models tested in **plain mode** with `context_mode=toolchain`.

| Model                 | BIRD                     | Spider2              | TPC-DS              | Total                  |
|-----------------------|--------------------------|----------------------|---------------------|------------------------|
| qwen3-coder-next      | **0.263** (403 / 1534)   | 0.122 (15 / 123)     | 0.030 (3 / 99)      | **0.240** (421 / 1756) |
| qwen3-32b | 0.229 (352 / 1534)       | **0.293** (36 / 123) | **0.101** (10 / 99)  | 0.227 (398 / 1756)     |
| gpt-oss-120b          | 0.080 (123 / 1534)       | 0.065 (8 / 123)      | 0.061 (6 / 99)       | 0.078 (137 / 1756)     |

## Architectures

Results for **qwen3-coder-next**. Architecture details: [docs/en/architectures/](architectures/).

| Architecture         | BIRD                     | Spider2                  | TPC-DS                 | Total                    |
|----------------------|--------------------------|--------------------------|------------------------|--------------------------|
| Plain                | 0.263 (403 / 1534)       | 0.122 (15 / 123)         | 0.030 (3 / 99)         | 0.240 (421 / 1756)       |
| Self-consistency     | 0.308 (472 / 1534)       | 0.650 (80 / 123)         | **0.101 (10 / 99)**     | 0.320 (562 / 1756)       |
| SGR                  | 0.301 (461 / 1534)       | 0.674 (83 / 123)         | 0.040 (4 / 99)         | 0.312 (548 / 1756)       |
| SQL Factory          | 0.298 (457 / 1534)       | 0.577 (71 / 123)         | 0.081 (8 / 99)         | 0.305 (536 / 1756)       |
| **Hybrid**           | **0.323 (495 / 1534)**   | **0.715 (88 / 123)**     | 0.091 (9 / 99)         | **0.337 (592 / 1756)**   |

## Latency (avg ms per task)

### By model (plain)

| Model                 | BIRD       | Spider2     | TPC-DS      |
|-----------------------|------------|-------------|-------------|
| qwen3-coder-next      | 8 267      | 12 801      | 20 536      |
| qwen3-32b | **5 149**  | **8 293**   | **12 638**  |
| gpt-oss-120b          | 21 250     | 16 947      | 19 747      |

### By architecture (qwen3-coder-next)

| Architecture         | BIRD       | Spider2     | TPC-DS      |
|----------------------|------------|-------------|-------------|
| Plain                | **8 267**  | **12 801**  | **20 536**  |
| Self-consistency     | 12 265     | 12 928      | 25 307      |
| SGR                  | 16 767     | 29 162      | 21 968      |
| SQL Factory          | 14 679     | 30 142      | 39 454      |
| Hybrid               | 16 164     | 30 893      | 36 758      |

## Error Analysis

### Error types

| Error type | Description |
|------------|-------------|
| `pred_exec_fail` | SQL generated but failed at execution (syntax error, missing column/table) |
| `pred_bind_fail` | DuckDB binder error (strict column/table resolution) |
| `pred_parse_fail` | SQL parsing failed (usually markdown artifacts) |
| `pred_invalid_sql` | Placeholders left in SQL (`{{year}}`, `<replace>`) |
| `pred_generation_fail` | Toolchain failed to extract SQL from LLM response |
| `task_timeout` | Task exceeded total timeout |
| `gold_exec_fail` | Gold SQL itself failed (test data issue) |

### BIRD errors by architecture (qwen3-coder-next)

| Error type             | Plain | Self-consistency | SGR  | SQL Factory | Hybrid      |
|------------------------|-------|------------------|------|-------------|-------------|
| pred_exec_fail         | 426   | 18               | 85   | 61          | **0**       |
| pred_generation_fail   | 26    | **0**            | **0**| **0**       | 4           |
| task_timeout           | 21    | 124              | 22   | 34          | **12**      |
| **Total failures**     | 473   | 142              | 108  | 101         | **16**      |

### Spider2 errors by architecture (qwen3-coder-next)

| Error type             | Plain | Self-consistency | SGR  | SQL Factory | Hybrid      |
|------------------------|-------|------------------|------|-------------|-------------|
| pred_exec_fail         | 90    | 10               | 13   | 25          | **0**       |
| pred_generation_fail   | 0     | **0**            | **0**| **0**       | 5           |
| task_timeout           | **0** | 9                | **0**| **0**       | 2           |
| gold_exec_fail         | 5     | 5                | 5    | 5           | 5           |
| **Total failures**     | 96    | 19               | 16   | 27          | **12**      |

### Root causes

1. **Schema mismatch** — model hallucinates column names (e.g. `CreationDate` vs `creation_date`)
2. **Markdown artifacts** — 32 parse failures on TPC-DS plain
3. **Timeouts** — K generations x toolchain calls exceed task_timeout
4. **Toolchain extraction failures** — gpt-oss-120b talks instead of generating SQL (966 cases)
5. **Test data issues** — 5 gold_exec_fail in Spider2 (missing fixture tables)
