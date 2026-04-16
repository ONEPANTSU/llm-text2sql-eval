# EvalSuite report for 20260309_130752_qwen3-coder-next

- Aggregate execution accuracy: 0.091

## Overall
- total: 99
- executed: 99 | skipped: 0
- gold_failed: 0 | pred_failed: 22
- ex_correct: 9 / compared: 77

## Error breakdown
- pred_invalid_sql: 16
- task_timeout: 3
- pred_runtime_fail: 2
- pred_generation_fail: 1

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Latency
- mean: 36758.0 ms
- median: 33596.1 ms
- p90: 55498.2 ms
- p99: 108944.8 ms
- tasks_with_latency: 96
- tpcds: mean=36758.0 ms, median=33596.1 ms, p90=55498.2 ms

## Candidates count
- mean: 5.17
- median: 5.0

## Top error patterns
### 1. pred_invalid_sql (count=16)
Error head: `pred_invalid_sql:empty_sql`
- **tpcds_q05** (tpcds): pred_invalid_sql:empty_sql...
- **tpcds_q09** (tpcds): pred_invalid_sql:empty_sql...
- **tpcds_q16** (tpcds): pred_invalid_sql:empty_sql...

### 2. task_timeout (count=3)
Error head: `task timeout`
- **tpcds_q08** (tpcds): task timeout...
- **tpcds_q72** (tpcds): task timeout...
- **tpcds_q83** (tpcds): task timeout...

### 3. pred_runtime_fail (count=2)
Error head: `Invalid Input Error: Values were not provided for the following prepared stateme...`
- **tpcds_q22** (tpcds): Invalid Input Error: Values were not provided for the following prepared statement parameters: 1...
- **tpcds_q28** (tpcds): Invalid Input Error: Values were not provided for the following prepared statement parameters: 1...

### 4. pred_generation_fail (count=1)
Error head: `HTTPSConnectionPool(host='openrouter.ai', port=443): Read timed out. (read timeo...`
- **tpcds_q36** (tpcds): HTTPSConnectionPool(host='openrouter.ai', port=443): Read timed out. (read timeout=60)...

## Toolchain diagnostics (tpcds)
- avg_tool_calls: 0.00
- pct_no_fact_table_inspected: 0.0%
- most_inspected_tables: []

## Hybrid diagnostics
- tasks_with_hybrid: 97
- variant_distribution: A=0, B=97
- avg_initial_candidates: 5.00
- avg_expansion_accepted: 0.32
- avg_aggregation_groups: 1.63
- avg_pool_size: 5.38

## tpcds
- coverage: 99/99 (100.0%)
- execution_accuracy: 0.091
- ex_correct: 9
- compared: 77
- gold_failed: 0
- pred_failed: 22
- skipped: 0
- latency: mean=36758.0 ms, median=33596.1 ms, p90=55498.2 ms
