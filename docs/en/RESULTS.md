# Evaluation Results

## Datasets

- [BIRD](https://bird-bench.github.io) — 1534 tasks. Dev split from the full 12,751-task dataset.
- [Spider2-Lite](https://spider2-sql.github.io) — 123 tasks. SQLite subset only (Snowflake/BigQuery excluded).
- [TPC-DS NL](https://www.tpc.org/tpcds/) — 99 tasks. NL questions generated from 99 TPC-DS analytical SQL queries.

## Models

All models tested in **plain mode** with `context_mode=toolchain`.

| Model                 | BIRD                     | Spider2              | TPC-DS              | Total                  |
|-----------------------|--------------------------|----------------------|---------------------|------------------------|
| qwen3-coder-next      | **0.314** (481 / 1534)   | **0.480** (59 / 123) | **0.081** (8 / 99)  | **0.312** (548 / 1756) |
| qwen3-32b             | 0.075 (115 / 1534)       | 0.065 (8 / 123)      | 0.051 (5 / 99)      | 0.073 (128 / 1756)     |
| gpt-oss-120b          | 0.060 (92 / 1534)        | 0.057 (7 / 123)      | 0.051 (5 / 99)      | 0.059 (104 / 1756)     |

## Architectures

Results for **qwen3-coder-next**, `context_mode=toolchain`. Architecture details: [docs/en/architectures/](architectures/).

| Architecture         | BIRD                     | Spider2                  | TPC-DS                 | Total                    |
|----------------------|--------------------------|--------------------------|------------------------|--------------------------|
| Plain                | 0.314 (481 / 1534)       | 0.480 (59 / 123)         | 0.081 (8 / 99)         | 0.312 (548 / 1756)       |
| Self-consistency     | 0.308 (472 / 1534)       | 0.650 (80 / 123)         | **0.101 (10 / 99)**     | 0.320 (562 / 1756)       |
| SGR                  | 0.301 (461 / 1534)       | 0.674 (83 / 123)         | 0.040 (4 / 99)         | 0.312 (548 / 1756)       |
| SQL Factory          | 0.298 (457 / 1534)       | 0.577 (71 / 123)         | 0.081 (8 / 99)         | 0.305 (536 / 1756)       |
| **Hybrid**           | **0.323 (495 / 1534)**   | **0.715 (88 / 123)**     | 0.091 (9 / 99)         | **0.337 (592 / 1756)**   |

**Note:** Plain results above are from the April 2026 run with corrected post-processing (strip_sql_fences). The earlier February run showed 26.3% on BIRD due to a markdown-fences bug that inflated pred_exec_fail counts. Self-consistency, SGR, SQL Factory, and Hybrid runs were unaffected as they already included proper post-processing.

## Token Usage (toolchain mode, BIRD, n=10)

### By model (plain)

| Model                 | Avg calls | Avg prompt | Avg compl | Avg total |
|-----------------------|-----------|------------|-----------|-----------|
| qwen3-coder-next      | 4.7       | 3 983      | 113       | 4 096     |
| qwen3-32b             | 2.3       | 939        | 1 245     | 2 184     |
| gpt-oss-120b          | 2.0       | 932        | 1 040     | 1 972     |

### By architecture (qwen3-coder-next)

| Architecture         | Avg calls | Avg prompt | Avg compl | Avg total | vs plain |
|----------------------|-----------|------------|-----------|-----------|----------|
| Plain                | 4.7       | 3 983      | 113       | 4 096     | 1.0x     |
| Self-consistency     | 21.3      | 16 648     | 531       | 17 179    | 4.2x     |
| SQL Factory          | 10.4      | 6 572      | 441       | 7 013     | 1.7x     |
| SGR                  | 12.0      | 8 541      | 725       | 9 266     | 2.3x     |
| Hybrid               | 11.4      | 7 559      | 650       | 8 210     | 2.0x     |

Hybrid is cheaper than SGR (2.0x vs 2.3x) due to smart early stop: expansion was skipped on 85.5% of BIRD tasks (1312/1534) because consensus was already reached among initial candidates. Average candidates per task: 5.3 (theoretical max: 11).

## Latency (avg ms per task, from full runs)

### By model (plain)

| Model                 | BIRD       | Spider2     | TPC-DS      |
|-----------------------|------------|-------------|-------------|
| qwen3-coder-next      | 8 267      | 12 801      | 20 536      |
| qwen3-32b             | **5 149**  | **8 293**   | **12 638**  |
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
| `pred_parse_fail` | SQL parsing failed |
| `pred_invalid_sql` | Placeholders left in SQL (`{{year}}`, `<replace>`) |
| `pred_generation_fail` | Toolchain failed to extract SQL from LLM response |
| `task_timeout` | Task exceeded total timeout |
| `gold_exec_fail` | Gold SQL itself failed (test data issue) |

### Exec failure classification (plain, qwen3-coder-next)

| Benchmark | Total exec_fail | Schema mismatch | Syntax | Other |
|-----------|-----------------|-----------------|--------|-------|
| BIRD      | 59              | 45 (76%)        | 12     | 2     |
| Spider2   | 26              | 24 (92%)        | 1      | 1     |
| TPC-DS    | 24              | 24 (100%)       | 0      | 0     |

Dominant error category: **schema mismatch** (column/table name hallucination). This is exactly what Hybrid's fuzzy schema validation (edit distance ≤ 2) and diagnostic retry address.

### Errors by model (plain, BIRD)

| Error type             | qwen3-coder-next | qwen3-32b | gpt-oss-120b |
|------------------------|------------------|-----------|--------------|
| pred_generation_fail   | 22               | **618**   | **914**      |
| pred_exec_fail         | 59               | 475       | 348          |
| task_timeout           | 8                | 38        | 52           |
| **Total failures**     | **89**           | 1131      | 1314         |

qwen3-32b and gpt-oss-120b both suffer from **toolchain_no_sql** — the model produces reasoning/thinking tokens instead of final SQL, so the toolchain cannot extract an executable query. qwen3-coder-next is significantly more reliable at producing structured SQL output.

### Errors by architecture (qwen3-coder-next, BIRD)

| Error type             | Plain | Self-consistency | SGR  | SQL Factory | Hybrid      |
|------------------------|-------|------------------|------|-------------|-------------|
| pred_exec_fail         | 59    | 18               | 85   | 61          | **0**       |
| pred_generation_fail   | 22    | **0**            | **0**| **0**       | 4           |
| task_timeout           | 8     | 124              | 22   | 34          | **12**      |
| **Total failures**     | 89    | 142              | 108  | 101         | **16**      |

Hybrid achieves pred_exec_fail = **0** on BIRD (vs 59 in plain). The remaining 16 failures are timeouts (12) and generation failures (4).

### Root causes

1. **Schema mismatch** — model hallucinates column names (e.g. `CreationDate` vs `creation_date`). Fixed by Hybrid's fuzzy schema validation (edit distance ≤ 2) and diagnostic retry
2. **Timeouts** — K generations × toolchain calls exceed task_timeout (124 on BIRD in self-consistency)
3. **Toolchain extraction failures** — thinking-heavy models (qwen3-32b, gpt-oss-120b) produce reasoning tokens instead of SQL; extraction yields `toolchain_no_sql` (618 and 914 cases respectively on BIRD)
4. **Test data issues** — 4-5 gold_exec_fail in Spider2 (missing fixture tables)

### TPC-DS anomaly

On TPC-DS, Self-consistency (10.1%) outperforms Hybrid (9.1%). Root cause: Hybrid's SGR grounding phase generates 16 `pred_invalid_sql` errors (placeholders like `{{year}}`, `<state>`) vs only 2 in Self-consistency. The SGR grounding prompt on TPC-DS's analytical domain causes the model to leave parameters unsubstituted.
