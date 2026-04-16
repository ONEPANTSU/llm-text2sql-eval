# EvalSuite report for 20260214_172946_openrouter:qwen_qwen3-coder-next

- Aggregate execution accuracy: 0.650

## Overall
- total: 123
- executed: 123 | skipped: 0
- gold_failed: 5 | pred_failed: 19
- ex_correct: 80 / compared: 99

## Error breakdown
- pred_exec_fail: 10
- task_timeout: 9
- gold_exec_fail: 5

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Top error patterns
### 1. task_timeout (count=9)
Error head: `task timeout`
- **spider2_2** (spider2): task timeout...
- **spider2_8** (spider2): task timeout...
- **spider2_18** (spider2): task timeout...

### 2. gold_exec_fail (count=1)
Error head: `no such table: Belts`
- **spider2_10** (spider2): no such table: Belts...

### 3. pred_exec_fail (count=1)
Error head: `no such column: bs.batsman_id`
- **spider2_14** (spider2): no such column: bs.batsman_id...

### 4. pred_exec_fail (count=1)
Error head: `no such column: bs.runs`
- **spider2_15** (spider2): no such column: bs.runs...

### 5. pred_exec_fail (count=1)
Error head: `no such column: runs_scored`
- **spider2_16** (spider2): no such column: runs_scored...

### 6. pred_exec_fail (count=1)
Error head: `no such table: customers`
- **spider2_31** (spider2): no such table: customers...

### 7. pred_exec_fail (count=1)
Error head: `no such table: invoices`
- **spider2_32** (spider2): no such table: invoices...

### 8. gold_exec_fail (count=1)
Error head: `no such table: hardware_fact_sales_monthly`
- **spider2_34** (spider2): no such table: hardware_fact_sales_monthly...

### 9. pred_exec_fail (count=1)
Error head: `no such column: cco.id`
- **spider2_46** (spider2): no such column: cco.id...

### 10. gold_exec_fail (count=1)
Error head: `no such table: shopping_cart_page_hierarchy`
- **spider2_55** (spider2): no such table: shopping_cart_page_hierarchy...

## Self-consistency diagnostics
- tasks_with_candidates: 114
- total_candidates: 570
- candidate_preflight_pass_rate: 0.370
- candidate_exec_pass_rate: 0.370
- avg_exec_ok_per_example: 1.85
- avg_majority_strength: 0.662

## spider2
- coverage: 123/123 (100.0%)
- execution_accuracy: 0.650
- ex_correct: 80
- compared: 99
- gold_failed: 5
- pred_failed: 19
- skipped: 0
