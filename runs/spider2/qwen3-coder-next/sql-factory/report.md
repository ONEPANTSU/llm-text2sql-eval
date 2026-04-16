# EvalSuite report for 20260216_102718_openrouter:qwen_qwen3-coder-next

- Aggregate execution accuracy: 0.577

## Overall
- total: 123
- executed: 123 | skipped: 0
- gold_failed: 5 | pred_failed: 27
- ex_correct: 71 / compared: 91

## Error breakdown
- pred_exec_fail: 25
- gold_exec_fail: 5
- task_timeout: 2

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Top error patterns
### 1. pred_exec_fail (count=3)
Error head: `no such table: customers`
- **spider2_31** (spider2): no such table: customers...
- **spider2_32** (spider2): no such table: customers...
- **spider2_33** (spider2): no such table: customers...

### 2. pred_exec_fail (count=2)
Error head: `near "[1]": syntax error`
- **spider2_5** (spider2): near "[1]": syntax error...
- **spider2_6** (spider2): near "[1]": syntax error...

### 3. task_timeout (count=2)
Error head: `task timeout`
- **spider2_18** (spider2): task timeout...
- **spider2_89** (spider2): task timeout...

### 4. gold_exec_fail (count=1)
Error head: `no such table: Belts`
- **spider2_10** (spider2): no such table: Belts...

### 5. pred_exec_fail (count=1)
Error head: `no such column: bs.striker`
- **spider2_14** (spider2): no such column: bs.striker...

### 6. pred_exec_fail (count=1)
Error head: `no such column: ba.total_runs_scored`
- **spider2_15** (spider2): no such column: ba.total_runs_scored...

### 7. pred_exec_fail (count=1)
Error head: `no such column: o.customer_unique_id`
- **spider2_22** (spider2): no such column: o.customer_unique_id...

### 8. gold_exec_fail (count=1)
Error head: `no such table: hardware_fact_sales_monthly`
- **spider2_34** (spider2): no such table: hardware_fact_sales_monthly...

### 9. pred_exec_fail (count=1)
Error head: `no such column: t.calendar_month_desc`
- **spider2_40** (spider2): no such column: t.calendar_month_desc...

### 10. pred_exec_fail (count=1)
Error head: `no such column: date`
- **spider2_42** (spider2): no such column: date...

## SQL-Factory diagnostics
- tasks_with_sql_factory: 121
- avg_rounds: 2.727
- avg_pool_size: 3.421
- reject_exec_fail_rate: 0.350
- reject_similarity_rate: 0.060
- exploration_vs_exploitation_ratio: 0.798

## spider2
- coverage: 123/123 (100.0%)
- execution_accuracy: 0.577
- ex_correct: 71
- compared: 91
- gold_failed: 5
- pred_failed: 27
- skipped: 0
