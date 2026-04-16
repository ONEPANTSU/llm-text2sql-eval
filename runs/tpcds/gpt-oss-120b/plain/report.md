# EvalSuite report for 20260416_171316_gpt-oss-120b

- Aggregate execution accuracy: 0.051

## Overall
- total: 99
- executed: 99 | skipped: 0
- gold_failed: 0 | pred_failed: 77
- ex_correct: 5 / compared: 22

## Error breakdown
- pred_generation_fail: 57
- task_timeout: 9
- pred_bind_fail: 9
- pred_parse_fail: 2

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Latency
- mean: 31522.6 ms
- median: 28201.0 ms
- p90: 62747.0 ms
- p99: 116543.8 ms
- tasks_with_latency: 90
- tpcds: mean=31522.6 ms, median=28201.0 ms, p90=62747.0 ms

## Candidates count
- mean: 0.33
- median: 0.0

## Bind error (candidate fixable)
- **tpcds_q24** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q26** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q40** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q49** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q53** | type=None | from=None -> to=None | success=None failed=None

## Top error patterns
### 1. pred_generation_fail (count=36)
Error head: `'NoneType' object has no attribute 'strip'`
- **tpcds_q02** (tpcds): 'NoneType' object has no attribute 'strip'...
- **tpcds_q04** (tpcds): 'NoneType' object has no attribute 'strip'...
- **tpcds_q08** (tpcds): 'NoneType' object has no attribute 'strip'...

### 2. pred_generation_fail (count=21)
Error head: `toolchain_no_sql`
- **tpcds_q03** (tpcds): toolchain_no_sql...
- **tpcds_q09** (tpcds): toolchain_no_sql...
- **tpcds_q10** (tpcds): toolchain_no_sql...

### 3. task_timeout (count=9)
Error head: `task timeout`
- **tpcds_q05** (tpcds): task timeout...
- **tpcds_q07** (tpcds): task timeout...
- **tpcds_q31** (tpcds): task timeout...

### 4. pred_bind_fail (count=1)
Error head: `Binder Error: Referenced column "c_customer_sk" not found in FROM clause!
Candid...`
- **tpcds_q24** (tpcds): Binder Error: Referenced column "c_customer_sk" not found in FROM clause!
Candidate bindings: "cs_it...

### 5. pred_bind_fail (count=1)
Error head: `Binder Error: Table "cs" does not have a column named "cs_customer_sk"

Candidat...`
- **tpcds_q26** (tpcds): Binder Error: Table "cs" does not have a column named "cs_customer_sk"

Candidate bindings: : "cs_it...

### 6. pred_parse_fail (count=1)
Error head: `Parser Error: syntax error at or near "row"

LINE 1: EXPLAIN with rollup row g_s...`
- **tpcds_q27** (tpcds): Parser Error: syntax error at or near "row"

LINE 1: EXPLAIN with rollup row g_state. They want incl...

### 7. pred_bind_fail (count=1)
Error head: `Binder Error: Referenced column "ws_customer_sk" not found in FROM clause!
Candi...`
- **tpcds_q40** (tpcds): Binder Error: Referenced column "ws_customer_sk" not found in FROM clause!
Candidate bindings: "ws_b...

### 8. pred_bind_fail (count=1)
Error head: `Binder Error: Table "d" does not have a column named "d_month"

Candidate bindin...`
- **tpcds_q49** (tpcds): Binder Error: Table "d" does not have a column named "d_month"

Candidate bindings: : "d_month_seq",...

### 9. pred_bind_fail (count=1)
Error head: `Binder Error: Table "s" does not have a column named "s_store_type"

Candidate b...`
- **tpcds_q53** (tpcds): Binder Error: Table "s" does not have a column named "s_store_type"

Candidate bindings: : "s_street...

### 10. pred_bind_fail (count=1)
Error head: `Binder Error: Referenced table "d2" not found!
Candidate tables: "d"

LINE 24:  ...`
- **tpcds_q57** (tpcds): Binder Error: Referenced table "d2" not found!
Candidate tables: "d"

LINE 24:           AND d2.d_mo...

## Toolchain diagnostics (tpcds)
- avg_tool_calls: 0.00
- pct_no_fact_table_inspected: 0.0%
- most_inspected_tables: []

## tpcds
- coverage: 99/99 (100.0%)
- execution_accuracy: 0.051
- ex_correct: 5
- compared: 22
- gold_failed: 0
- pred_failed: 77
- skipped: 0
- latency: mean=31522.6 ms, median=28201.0 ms, p90=62747.0 ms
