# EvalSuite report for 20260415_004521_qwen3-coder-next

- Aggregate execution accuracy: 0.081

## Overall
- total: 99
- executed: 99 | skipped: 0
- gold_failed: 0 | pred_failed: 35
- ex_correct: 8 / compared: 64

## Error breakdown
- pred_bind_fail: 24
- pred_generation_fail: 7
- pred_invalid_sql: 2
- pred_parse_fail: 2

## Autofix & schema (v1.2)
- schema_warn_count: 5
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Latency
- mean: 11389.9 ms
- median: 10086.3 ms
- p90: 17476.0 ms
- p99: 31637.7 ms
- tasks_with_latency: 99
- tpcds: mean=11389.9 ms, median=10086.3 ms, p90=17476.0 ms

## Candidates count
- mean: 0.93
- median: 1.0

## Bind error (candidate fixable)
- **tpcds_q05** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q09** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q11** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q18** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q33** | type=None | from=None -> to=None | success=None failed=None

## Top error patterns
### 1. pred_generation_fail (count=4)
Error head: `toolchain_no_sql`
- **tpcds_q13** (tpcds): toolchain_no_sql...
- **tpcds_q34** (tpcds): toolchain_no_sql...
- **tpcds_q68** (tpcds): toolchain_no_sql...

### 2. pred_generation_fail (count=3)
Error head: `'NoneType' object has no attribute 'strip'`
- **tpcds_q02** (tpcds): 'NoneType' object has no attribute 'strip'...
- **tpcds_q16** (tpcds): 'NoneType' object has no attribute 'strip'...
- **tpcds_q45** (tpcds): 'NoneType' object has no attribute 'strip'...

### 3. pred_invalid_sql (count=2)
Error head: `pred_invalid_sql:placeholder:<[^>]+>`
- **tpcds_q21** (tpcds): pred_invalid_sql:placeholder:<[^>]+>...
- **tpcds_q38** (tpcds): pred_invalid_sql:placeholder:<[^>]+>...

### 4. pred_bind_fail (count=2)
Error head: `Binder Error: Referenced column "s_store_type" not found in FROM clause!
Candida...`
- **tpcds_q53** (tpcds): Binder Error: Referenced column "s_store_type" not found in FROM clause!
Candidate bindings: "s_stre...
- **tpcds_q86** (tpcds): Binder Error: Referenced column "s_store_type" not found in FROM clause!
Candidate bindings: "s_stre...

### 5. pred_bind_fail (count=1)
Error head: `Binder Error: Table "wr" does not have a column named "wr_web_site_sk"

Candidat...`
- **tpcds_q05** (tpcds): Binder Error: Table "wr" does not have a column named "wr_web_site_sk"

Candidate bindings: : "wr_it...

### 6. pred_bind_fail (count=1)
Error head: `Binder Error: Ambiguous reference to column name "i_category" (use: "i.i_categor...`
- **tpcds_q06** (tpcds): Binder Error: Ambiguous reference to column name "i_category" (use: "i.i_category" or "ic.i_category...

### 7. pred_bind_fail (count=1)
Error head: `Binder Error: Table "store_sales" does not have a column named "ss_reason_sk"

C...`
- **tpcds_q09** (tpcds): Binder Error: Table "store_sales" does not have a column named "ss_reason_sk"

Candidate bindings: :...

### 8. pred_bind_fail (count=1)
Error head: `Binder Error: Referenced column "ws_customer_sk" not found in FROM clause!
Candi...`
- **tpcds_q11** (tpcds): Binder Error: Referenced column "ws_customer_sk" not found in FROM clause!
Candidate bindings: "ws_b...

### 9. pred_bind_fail (count=1)
Error head: `Binder Error: Values list "ss" does not have a column named "ss_customer_sk"

LI...`
- **tpcds_q17** (tpcds): Binder Error: Values list "ss" does not have a column named "ss_customer_sk"

LINE 47:         AND s...

### 10. pred_bind_fail (count=1)
Error head: `Binder Error: Table "customer" does not have a column named "c_first_cdemo_sk"

...`
- **tpcds_q18** (tpcds): Binder Error: Table "customer" does not have a column named "c_first_cdemo_sk"

Candidate bindings: ...

## Toolchain diagnostics (tpcds)
- avg_tool_calls: 0.00
- pct_no_fact_table_inspected: 0.0%
- most_inspected_tables: []

## tpcds
- coverage: 99/99 (100.0%)
- execution_accuracy: 0.081
- ex_correct: 8
- compared: 64
- gold_failed: 0
- pred_failed: 35
- skipped: 0
- latency: mean=11389.9 ms, median=10086.3 ms, p90=17476.0 ms
