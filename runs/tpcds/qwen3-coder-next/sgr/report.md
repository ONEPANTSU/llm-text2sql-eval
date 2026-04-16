# EvalSuite report for 20260216_163841_openrouter:qwen_qwen3-coder-next

- Aggregate execution accuracy: 0.061

## Overall
- total: 99
- executed: 99 | skipped: 0
- gold_failed: 0 | pred_failed: 44
- ex_correct: 6 / compared: 55

## Error breakdown
- pred_generation_fail: 27
- pred_bind_fail: 13
- pred_parse_fail: 3
- task_timeout: 1

## Autofix & schema (v1.2)
- schema_warn_count: 3
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 1

## Bind error (candidate fixable)
- **tpcds_q01** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q05** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q09** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q53** | type=None | from=None -> to=None | success=None failed=None
- **tpcds_q73** | type=None | from=None -> to=None | success=None failed=None

## Top error patterns
### 1. pred_generation_fail (count=27)
Error head: `toolchain_no_sql`
- **tpcds_q06** (tpcds): toolchain_no_sql...
- **tpcds_q11** (tpcds): toolchain_no_sql...
- **tpcds_q12** (tpcds): toolchain_no_sql...

### 2. pred_bind_fail (count=2)
Error head: `Binder Error: Referenced column "s_store_type" not found in FROM clause!
Candida...`
- **tpcds_q53** (tpcds): Binder Error: Referenced column "s_store_type" not found in FROM clause!
Candidate bindings: "s_stre...
- **tpcds_q86** (tpcds): Binder Error: Referenced column "s_store_type" not found in FROM clause!
Candidate bindings: "s_stre...

### 3. pred_bind_fail (count=1)
Error head: `Binder Error: Table "sr" does not have a column named "c_customer_sk"

Candidate...`
- **tpcds_q01** (tpcds): Binder Error: Table "sr" does not have a column named "c_customer_sk"

Candidate bindings: : "sr_cus...

### 4. pred_bind_fail (count=1)
Error head: `Binder Error: Referenced column "cs_sold_date_sk" not found in FROM clause!
Cand...`
- **tpcds_q05** (tpcds): Binder Error: Referenced column "cs_sold_date_sk" not found in FROM clause!
Candidate bindings: "ws_...

### 5. pred_bind_fail (count=1)
Error head: `Binder Error: Table "store_sales" does not have a column named "ss_reason_sk"

C...`
- **tpcds_q09** (tpcds): Binder Error: Table "store_sales" does not have a column named "ss_reason_sk"

Candidate bindings: :...

### 6. pred_bind_fail (count=1)
Error head: `Binder Error: Ambiguous reference to column name "i_class_id" (use: "item.i_clas...`
- **tpcds_q14** (tpcds): Binder Error: Ambiguous reference to column name "i_class_id" (use: "item.i_class_id" or "cross_item...

### 7. task_timeout (count=1)
Error head: `task timeout`
- **tpcds_q15** (tpcds): task timeout...

### 8. pred_bind_fail (count=1)
Error head: `Binder Error: GROUP BY clause cannot contain aggregates!

LINE 2:     COUNT(DIST...`
- **tpcds_q16** (tpcds): Binder Error: GROUP BY clause cannot contain aggregates!

LINE 2:     COUNT(DISTINCT cs.cs_order_num...

### 9. pred_parse_fail (count=1)
Error head: `Parser Error: syntax error at or near "}"

LINE 18:     AND d.d_month_seq = {mon...`
- **tpcds_q22** (tpcds): Parser Error: syntax error at or near "}"

LINE 18:     AND d.d_month_seq = {month_seq}
            ...

### 10. pred_parse_fail (count=1)
Error head: `Parser Error: syntax error at or near "{"

LINE 9:     WHERE d.d_year = {{year}}...`
- **tpcds_q28** (tpcds): Parser Error: syntax error at or near "{"

LINE 9:     WHERE d.d_year = {{year}}
                   ...

## Toolchain diagnostics (tpcds)
- avg_tool_calls: 7.02
- pct_no_fact_table_inspected: 29.6%
- most_inspected_tables: [('store_sales', 53), ('date_dim', 24), ('catalog_sales', 18), ('item', 15), ('store', 14), ('web_sales', 14), ('store_returns', 12), ('customer', 10), ('customer_demographics', 6), ('inventory', 2)]

## tpcds
- coverage: 99/99 (100.0%)
- execution_accuracy: 0.061
- ex_correct: 6
- compared: 55
- gold_failed: 0
- pred_failed: 44
- skipped: 0
