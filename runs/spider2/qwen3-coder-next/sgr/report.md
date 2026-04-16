# EvalSuite report for 20260216_201700_openrouter:qwen_qwen3-coder-next

- Aggregate execution accuracy: 0.675

## Overall
- total: 123
- executed: 123 | skipped: 0
- gold_failed: 5 | pred_failed: 16
- ex_correct: 83 / compared: 102

## Error breakdown
- pred_exec_fail: 13
- gold_exec_fail: 5
- task_timeout: 2
- pred_generation_fail: 1

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Top error patterns
### 1. task_timeout (count=2)
Error head: `task timeout`
- **spider2_23** (spider2): task timeout...
- **spider2_61** (spider2): task timeout...

### 2. pred_exec_fail (count=1)
Error head: `no such function: LEAST`
- **spider2_6** (spider2): no such function: LEAST...

### 3. gold_exec_fail (count=1)
Error head: `no such table: Belts`
- **spider2_10** (spider2): no such table: Belts...

### 4. gold_exec_fail (count=1)
Error head: `no such table: hardware_fact_sales_monthly`
- **spider2_34** (spider2): no such table: hardware_fact_sales_monthly...

### 5. pred_exec_fail (count=1)
Error head: `no such function: LEFT`
- **spider2_42** (spider2): no such function: LEFT...

### 6. gold_exec_fail (count=1)
Error head: `no such table: shopping_cart_page_hierarchy`
- **spider2_55** (spider2): no such table: shopping_cart_page_hierarchy...

### 7. gold_exec_fail (count=1)
Error head: `no such table: interest_map`
- **spider2_57** (spider2): no such table: interest_map...

### 8. pred_exec_fail (count=1)
Error head: `no such table: Students`
- **spider2_62** (spider2): no such table: Students...

### 9. pred_exec_fail (count=1)
Error head: `no such table: directors`
- **spider2_67** (spider2): no such table: directors...

### 10. pred_exec_fail (count=1)
Error head: `no such table: ratings`
- **spider2_68** (spider2): no such table: ratings...

## spider2
- coverage: 123/123 (100.0%)
- execution_accuracy: 0.675
- ex_correct: 83
- compared: 102
- gold_failed: 5
- pred_failed: 16
- skipped: 0
