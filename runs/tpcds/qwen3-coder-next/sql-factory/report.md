# EvalSuite report for 20260216_081542_openrouter:qwen_qwen3-coder-next

- Aggregate execution accuracy: 0.081

## Overall
- total: 99
- executed: 99 | skipped: 0
- gold_failed: 0 | pred_failed: 25
- ex_correct: 8 / compared: 74

## Error breakdown
- task_timeout: 10
- pred_bind_fail: 9
- pred_generation_fail: 2
- pred_invalid_sql: 2
- pred_parse_fail: 2

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Bind error (candidate fixable)
- **tpcds_q05** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q39** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q53** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q74** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q78** | type=None | from=None -> to=None | success=None failed=None

## Top error patterns
### 1. task_timeout (count=10)
Error head: `task timeout`
- **tpcds_q04** (tpcds): task timeout...
- **tpcds_q15** (tpcds): task timeout...
- **tpcds_q16** (tpcds): task timeout...

### 2. pred_bind_fail (count=3)
Error head: `Binder Error: Referenced column "s_store_type" not found in FROM clause!
Candida...`
- **tpcds_q53** (tpcds): Binder Error: Referenced column "s_store_type" not found in FROM clause!
Candidate bindings: "s_stre...
- **tpcds_q74** (tpcds): Binder Error: Referenced column "s_store_type" not found in FROM clause!
Candidate bindings: "s_stre...
- **tpcds_q86** (tpcds): Binder Error: Referenced column "s_store_type" not found in FROM clause!
Candidate bindings: "s_stre...

### 3. pred_invalid_sql (count=2)
Error head: `pred_invalid_sql:placeholder:<[^>]+>`
- **tpcds_q21** (tpcds): pred_invalid_sql:placeholder:<[^>]+>...
- **tpcds_q62** (tpcds): pred_invalid_sql:placeholder:<[^>]+>...

### 4. pred_generation_fail (count=1)
Error head: `HTTPSConnectionPool(host='openrouter.ai', port=443): Read timed out.`
- **tpcds_q01** (tpcds): HTTPSConnectionPool(host='openrouter.ai', port=443): Read timed out....

### 5. pred_bind_fail (count=1)
Error head: `Binder Error: Table "cr" does not have a column named "cr_return_amt"

Candidate...`
- **tpcds_q05** (tpcds): Binder Error: Table "cr" does not have a column named "cr_return_amt"

Candidate bindings: : "cr_ret...

### 6. pred_bind_fail (count=1)
Error head: `Binder Error: Ambiguous reference to column name "i_category" (use: "i.i_categor...`
- **tpcds_q06** (tpcds): Binder Error: Ambiguous reference to column name "i_category" (use: "i.i_category" or "ca.i_category...

### 7. pred_bind_fail (count=1)
Error head: `Binder Error: column avg_sales must appear in the GROUP BY clause or be used in ...`
- **tpcds_q14** (tpcds): Binder Error: column avg_sales must appear in the GROUP BY clause or be used in an aggregate functio...

### 8. pred_parse_fail (count=1)
Error head: `Parser Error: syntax error at or near ":"

LINE 18:     AND d.d_month_seq = :mon...`
- **tpcds_q22** (tpcds): Parser Error: syntax error at or near ":"

LINE 18:     AND d.d_month_seq = :month_seq
             ...

### 9. pred_parse_fail (count=1)
Error head: `Parser Error: syntax error at or near "{"

LINE 9:     WHERE d.d_year = {{year}}...`
- **tpcds_q28** (tpcds): Parser Error: syntax error at or near "{"

LINE 9:     WHERE d.d_year = {{year}}
                   ...

### 10. pred_bind_fail (count=1)
Error head: `Binder Error: Referenced column "d_month" not found in FROM clause!
Candidate bi...`
- **tpcds_q39** (tpcds): Binder Error: Referenced column "d_month" not found in FROM clause!
Candidate bindings: "d_month_seq...

## Toolchain diagnostics (tpcds)
- avg_tool_calls: 0.00
- pct_no_fact_table_inspected: 0.0%
- most_inspected_tables: []

## SQL-Factory diagnostics
- tasks_with_sql_factory: 87
- avg_rounds: 2.805
- avg_pool_size: 2.437
- reject_exec_fail_rate: 0.247
- reject_similarity_rate: 0.266
- exploration_vs_exploitation_ratio: 0.770

## tpcds
- coverage: 99/99 (100.0%)
- execution_accuracy: 0.081
- ex_correct: 8
- compared: 74
- gold_failed: 0
- pred_failed: 25
- skipped: 0
