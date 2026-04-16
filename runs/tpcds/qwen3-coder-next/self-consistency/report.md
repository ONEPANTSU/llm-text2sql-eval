# EvalSuite report for 20260214_151349_openrouter:qwen_qwen3-coder-next

- Aggregate execution accuracy: 0.101

## Overall
- total: 99
- executed: 99 | skipped: 0
- gold_failed: 0 | pred_failed: 20
- ex_correct: 10 / compared: 79

## Error breakdown
- pred_bind_fail: 8
- task_timeout: 7
- pred_invalid_sql: 2
- pred_runtime_fail: 2
- pred_parse_fail: 1

## Autofix & schema (v1.2)
- schema_warn_count: 6
- pred_bind_fail_autofix_success: 1
- pred_bind_fail_autofix_failed: 1

## Bind error (candidate fixable)
- **tpcds_q05** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q74** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q78** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q83** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q93** | type=None | from=None -> to=None | success=None failed=None

## Top error patterns
### 1. task_timeout (count=7)
Error head: `task timeout`
- **tpcds_q04** (tpcds): task timeout...
- **tpcds_q15** (tpcds): task timeout...
- **tpcds_q46** (tpcds): task timeout...

### 2. pred_bind_fail (count=1)
Error head: `Binder Error: Referenced column "wr_web_site_sk" not found in FROM clause!
Candi...`
- **tpcds_q05** (tpcds): Binder Error: Referenced column "wr_web_site_sk" not found in FROM clause!
Candidate bindings: "wr_w...

### 3. pred_parse_fail (count=1)
Error head: `Parser Error: syntax error at or near "GROUP"

LINE 103: GROUP BY ROLLUP(channel...`
- **tpcds_q14** (tpcds): Parser Error: syntax error at or near "GROUP"

LINE 103: GROUP BY ROLLUP(channel, i_brand_id, i_clas...

### 4. pred_bind_fail (count=1)
Error head: `Binder Error: GROUP BY clause cannot contain aggregates!

LINE 2:     COUNT(DIST...`
- **tpcds_q16** (tpcds): Binder Error: GROUP BY clause cannot contain aggregates!

LINE 2:     COUNT(DISTINCT cs.cs_order_num...

### 5. pred_invalid_sql (count=1)
Error head: `pred_invalid_sql:placeholder:<[^>]+>`
- **tpcds_q21** (tpcds): pred_invalid_sql:placeholder:<[^>]+>...

### 6. pred_runtime_fail (count=1)
Error head: `Invalid Input Error: Values were not provided for the following prepared stateme...`
- **tpcds_q22** (tpcds): Invalid Input Error: Values were not provided for the following prepared statement parameters: 1, 2...

### 7. pred_invalid_sql (count=1)
Error head: `pred_invalid_sql:placeholder:Replace\s+with`
- **tpcds_q28** (tpcds): pred_invalid_sql:placeholder:Replace\s+with...

### 8. pred_runtime_fail (count=1)
Error head: `Invalid Input Error: More than one row returned by a subquery used as an express...`
- **tpcds_q39** (tpcds): Invalid Input Error: More than one row returned by a subquery used as an expression - scalar subquer...

### 9. pred_bind_fail (count=1)
Error head: `Binder Error: Referenced column "s_store_type" not found in FROM clause!
Candida...`
- **tpcds_q74** (tpcds): Binder Error: Referenced column "s_store_type" not found in FROM clause!
Candidate bindings: "s_stre...

### 10. pred_bind_fail (count=1)
Error head: `Binder Error: Referenced column "cd_occupation" not found in FROM clause!
Candid...`
- **tpcds_q78** (tpcds): Binder Error: Referenced column "cd_occupation" not found in FROM clause!
Candidate bindings: "cd_de...

## Toolchain diagnostics (tpcds)
- avg_tool_calls: 6.76
- pct_no_fact_table_inspected: 23.3%
- most_inspected_tables: [('store_sales', 46), ('date_dim', 28), ('store', 15), ('web_sales', 14), ('item', 14), ('store_returns', 12), ('catalog_sales', 12), ('customer', 12), ('customer_demographics', 8), ('reason', 2)]

## Self-consistency diagnostics
- tasks_with_candidates: 92
- total_candidates: 460
- candidate_preflight_pass_rate: 0.587
- candidate_exec_pass_rate: 0.576
- avg_exec_ok_per_example: 2.88
- avg_majority_strength: 0.432

## tpcds
- coverage: 99/99 (100.0%)
- execution_accuracy: 0.101
- ex_correct: 10
- compared: 79
- gold_failed: 0
- pred_failed: 20
- skipped: 0
