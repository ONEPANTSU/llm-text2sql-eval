# EvalSuite report for 20260416_040541_qwen3-32b

- Aggregate execution accuracy: 0.051

## Overall
- total: 99
- executed: 99 | skipped: 0
- gold_failed: 0 | pred_failed: 83
- ex_correct: 5 / compared: 16

## Error breakdown
- pred_generation_fail: 60
- pred_bind_fail: 15
- task_timeout: 3
- pred_runtime_fail: 3
- pred_parse_fail: 2

## Autofix & schema (v1.2)
- schema_warn_count: 17
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Latency
- mean: 51017.6 ms
- median: 41923.9 ms
- p90: 89517.7 ms
- p99: 114529.7 ms
- tasks_with_latency: 96
- tpcds: mean=51017.6 ms, median=41923.9 ms, p90=89517.7 ms

## Candidates count
- mean: 0.36
- median: 0.0

## Bind error (candidate fixable)
- **tpcds_q06** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q22** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q34** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q42** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q49** | type=None | from=None -> to=None | success=None failed=None

## Top error patterns
### 1. pred_generation_fail (count=59)
Error head: `toolchain_no_sql`
- **tpcds_q01** (tpcds): toolchain_no_sql...
- **tpcds_q05** (tpcds): toolchain_no_sql...
- **tpcds_q08** (tpcds): toolchain_no_sql...

### 2. task_timeout (count=3)
Error head: `task timeout`
- **tpcds_q25** (tpcds): task timeout...
- **tpcds_q32** (tpcds): task timeout...
- **tpcds_q40** (tpcds): task timeout...

### 3. pred_runtime_fail (count=2)
Error head: `Invalid Input Error: Values were not provided for the following prepared stateme...`
- **tpcds_q44** (tpcds): Invalid Input Error: Values were not provided for the following prepared statement parameters: 1...
- **tpcds_q83** (tpcds): Invalid Input Error: Values were not provided for the following prepared statement parameters: 1...

### 4. pred_bind_fail (count=1)
Error head: `Binder Error: Table "customer" does not have a column named "c_address_sk"

Cand...`
- **tpcds_q06** (tpcds): Binder Error: Table "customer" does not have a column named "c_address_sk"

Candidate bindings: : "c...

### 5. pred_parse_fail (count=1)
Error head: `Parser Error: syntax error at or near "schema"

LINE 1: EXPLAIN with the schema ...`
- **tpcds_q07** (tpcds): Parser Error: syntax error at or near "schema"

LINE 1: EXPLAIN with the schema and requirements. No...

### 6. pred_bind_fail (count=1)
Error head: `Binder Error: Referenced table "ws" not found!
Candidate tables: "c"

LINE 31:  ...`
- **tpcds_q10** (tpcds): Binder Error: Referenced table "ws" not found!
Candidate tables: "c"

LINE 31:           WHERE ws.ws...

### 7. pred_bind_fail (count=1)
Error head: `Binder Error: Referenced column "month_seq" not found in FROM clause!
Candidate ...`
- **tpcds_q22** (tpcds): Binder Error: Referenced column "month_seq" not found in FROM clause!
Candidate bindings: "d_month_s...

### 8. pred_bind_fail (count=1)
Error head: `Binder Error: Referenced column "ws_customer_sk" not found in FROM clause!
Candi...`
- **tpcds_q34** (tpcds): Binder Error: Referenced column "ws_customer_sk" not found in FROM clause!
Candidate bindings: "ws_b...

### 9. pred_bind_fail (count=1)
Error head: `Binder Error: Table "d" does not have a column named "d_month"

Candidate bindin...`
- **tpcds_q42** (tpcds): Binder Error: Table "d" does not have a column named "d_month"

Candidate bindings: : "d_month_seq",...

### 10. pred_bind_fail (count=1)
Error head: `Binder Error: Table "cs" does not have a column named "cs_sold_quantity"

Candid...`
- **tpcds_q49** (tpcds): Binder Error: Table "cs" does not have a column named "cs_sold_quantity"

Candidate bindings: : "cs_...

## Toolchain diagnostics (tpcds)
- avg_tool_calls: 0.00
- pct_no_fact_table_inspected: 0.0%
- most_inspected_tables: []

## tpcds
- coverage: 99/99 (100.0%)
- execution_accuracy: 0.051
- ex_correct: 5
- compared: 16
- gold_failed: 0
- pred_failed: 83
- skipped: 0
- latency: mean=51017.6 ms, median=41923.9 ms, p90=89517.7 ms
