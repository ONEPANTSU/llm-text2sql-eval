# EvalSuite report for 20260416_160138_gpt-oss-120b

- Aggregate execution accuracy: 0.057

## Overall
- total: 123
- executed: 123 | skipped: 0
- gold_failed: 1 | pred_failed: 112
- ex_correct: 7 / compared: 10

## Error breakdown
- pred_generation_fail: 80
- pred_exec_fail: 27
- task_timeout: 5
- gold_exec_fail: 1

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Latency
- mean: 31322.0 ms
- median: 25229.5 ms
- p90: 76849.9 ms
- p99: 104862.5 ms
- tasks_with_latency: 118
- spider2: mean=31322.0 ms, median=25229.5 ms, p90=76849.9 ms

## Candidates count
- mean: 0.31
- median: 0.0

## Top error patterns
### 1. pred_generation_fail (count=43)
Error head: `'NoneType' object has no attribute 'strip'`
- **spider2_0** (spider2): 'NoneType' object has no attribute 'strip'...
- **spider2_2** (spider2): 'NoneType' object has no attribute 'strip'...
- **spider2_6** (spider2): 'NoneType' object has no attribute 'strip'...

### 2. pred_generation_fail (count=37)
Error head: `toolchain_no_sql`
- **spider2_8** (spider2): toolchain_no_sql...
- **spider2_14** (spider2): toolchain_no_sql...
- **spider2_15** (spider2): toolchain_no_sql...

### 3. task_timeout (count=5)
Error head: `task timeout`
- **spider2_55** (spider2): task timeout...
- **spider2_77** (spider2): task timeout...
- **spider2_93** (spider2): task timeout...

### 4. pred_exec_fail (count=3)
Error head: `near ")": syntax error`
- **spider2_82** (spider2): near ")": syntax error...
- **spider2_92** (spider2): near ")": syntax error...
- **spider2_115** (spider2): near ")": syntax error...

### 5. pred_exec_fail (count=1)
Error head: `no such column: o.order_timestamp`
- **spider2_1** (spider2): no such column: o.order_timestamp...

### 6. pred_exec_fail (count=1)
Error head: `no such column: b.playerID`
- **spider2_4** (spider2): no such column: b.playerID...

### 7. pred_exec_fail (count=1)
Error head: `incomplete input`
- **spider2_5** (spider2): incomplete input...

### 8. pred_exec_fail (count=1)
Error head: `no such column: runs`
- **spider2_12** (spider2): no such column: runs...

### 9. pred_exec_fail (count=1)
Error head: `no such column: pm.player_id`
- **spider2_13** (spider2): no such column: pm.player_id...

### 10. pred_exec_fail (count=1)
Error head: `no such column: o.order_delivered_timestamp`
- **spider2_18** (spider2): no such column: o.order_delivered_timestamp...

## spider2
- coverage: 123/123 (100.0%)
- execution_accuracy: 0.057
- ex_correct: 7
- compared: 10
- gold_failed: 1
- pred_failed: 112
- skipped: 0
- latency: mean=31322.0 ms, median=25229.5 ms, p90=76849.9 ms
