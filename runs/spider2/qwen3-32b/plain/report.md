# EvalSuite report for 20260416_020842_qwen3-32b

- Aggregate execution accuracy: 0.065

## Overall
- total: 123
- executed: 123 | skipped: 0
- gold_failed: 1 | pred_failed: 110
- ex_correct: 8 / compared: 12

## Error breakdown
- pred_generation_fail: 63
- pred_exec_fail: 37
- task_timeout: 10
- gold_exec_fail: 1

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Latency
- mean: 49696.3 ms
- median: 45875.9 ms
- p90: 77927.8 ms
- p99: 105274.5 ms
- tasks_with_latency: 113
- spider2: mean=49696.3 ms, median=45875.9 ms, p90=77927.8 ms

## Candidates count
- mean: 0.41
- median: 0.0

## Top error patterns
### 1. pred_generation_fail (count=62)
Error head: `toolchain_no_sql`
- **spider2_2** (spider2): toolchain_no_sql...
- **spider2_3** (spider2): toolchain_no_sql...
- **spider2_6** (spider2): toolchain_no_sql...

### 2. task_timeout (count=10)
Error head: `task timeout`
- **spider2_0** (spider2): task timeout...
- **spider2_14** (spider2): task timeout...
- **spider2_36** (spider2): task timeout...

### 3. pred_exec_fail (count=2)
Error head: `no such table: customer`
- **spider2_40** (spider2): no such table: customer...
- **spider2_63** (spider2): no such table: customer...

### 4. pred_exec_fail (count=1)
Error head: `no such column: f.distance`
- **spider2_5** (spider2): no such column: f.distance...

### 5. pred_exec_fail (count=1)
Error head: `no such column: b.runs`
- **spider2_12** (spider2): no such column: b.runs...

### 6. pred_exec_fail (count=1)
Error head: `no such column: player_id`
- **spider2_13** (spider2): no such column: player_id...

### 7. pred_exec_fail (count=1)
Error head: `no such column: bs.runs`
- **spider2_16** (spider2): no such column: bs.runs...

### 8. pred_exec_fail (count=1)
Error head: `no such column: over`
- **spider2_17** (spider2): no such column: over...

### 9. pred_exec_fail (count=1)
Error head: `ambiguous column name: order_id`
- **spider2_20** (spider2): ambiguous column name: order_id...

### 10. pred_exec_fail (count=1)
Error head: `no such column: c.city`
- **spider2_21** (spider2): no such column: c.city...

## spider2
- coverage: 123/123 (100.0%)
- execution_accuracy: 0.065
- ex_correct: 8
- compared: 12
- gold_failed: 1
- pred_failed: 110
- skipped: 0
- latency: mean=49696.3 ms, median=45875.9 ms, p90=77927.8 ms
