# EvalSuite report for 20260415_002736_qwen3-coder-next

- Aggregate execution accuracy: 0.480

## Overall
- total: 123
- executed: 123 | skipped: 0
- gold_failed: 4 | pred_failed: 33
- ex_correct: 59 / compared: 86

## Error breakdown
- pred_exec_fail: 26
- pred_generation_fail: 7
- gold_exec_fail: 4

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Latency
- mean: 8643.3 ms
- median: 7293.9 ms
- p90: 14210.6 ms
- p99: 22450.8 ms
- tasks_with_latency: 123
- spider2: mean=8643.3 ms, median=7293.9 ms, p90=14210.6 ms

## Candidates count
- mean: 0.94
- median: 1.0

## Top error patterns
### 1. pred_generation_fail (count=6)
Error head: `'NoneType' object has no attribute 'strip'`
- **spider2_24** (spider2): 'NoneType' object has no attribute 'strip'...
- **spider2_27** (spider2): 'NoneType' object has no attribute 'strip'...
- **spider2_39** (spider2): 'NoneType' object has no attribute 'strip'...

### 2. gold_exec_fail (count=1)
Error head: `no such table: Belts`
- **spider2_10** (spider2): no such table: Belts...

### 3. pred_exec_fail (count=1)
Error head: `no such column: runs`
- **spider2_11** (spider2): no such column: runs...

### 4. pred_exec_fail (count=1)
Error head: `no such column: bs.runs`
- **spider2_12** (spider2): no such column: bs.runs...

### 5. pred_exec_fail (count=1)
Error head: `no such column: bs.batsman_id`
- **spider2_15** (spider2): no such column: bs.batsman_id...

### 6. pred_exec_fail (count=1)
Error head: `no such column: c.company_id`
- **spider2_30** (spider2): no such column: c.company_id...

### 7. pred_exec_fail (count=1)
Error head: `no such table: customers`
- **spider2_31** (spider2): no such table: customers...

### 8. pred_exec_fail (count=1)
Error head: `no such table: invoice_items`
- **spider2_32** (spider2): no such table: invoice_items...

### 9. pred_exec_fail (count=1)
Error head: `near "FINAL": syntax error`
- **spider2_33** (spider2): near "FINAL": syntax error...

### 10. gold_exec_fail (count=1)
Error head: `no such table: hardware_fact_sales_monthly`
- **spider2_34** (spider2): no such table: hardware_fact_sales_monthly...

## spider2
- coverage: 123/123 (100.0%)
- execution_accuracy: 0.480
- ex_correct: 59
- compared: 86
- gold_failed: 4
- pred_failed: 33
- skipped: 0
- latency: mean=8643.3 ms, median=7293.9 ms, p90=14210.6 ms
