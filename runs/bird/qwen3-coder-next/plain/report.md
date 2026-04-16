# EvalSuite report for 20260414_210632_qwen3-coder-next

- Aggregate execution accuracy: 0.314

## Overall
- total: 1534
- executed: 1534 | skipped: 0
- gold_failed: 0 | pred_failed: 89
- ex_correct: 481 / compared: 1445

## Error breakdown
- pred_exec_fail: 59
- pred_generation_fail: 22
- task_timeout: 8

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Latency
- mean: 6870.7 ms
- median: 5380.0 ms
- p90: 11847.6 ms
- p99: 30663.6 ms
- tasks_with_latency: 1526
- bird_sqlite: mean=6870.7 ms, median=5380.0 ms, p90=11847.6 ms

## Candidates count
- mean: 0.98
- median: 1.0

## Top error patterns
### 1. pred_generation_fail (count=11)
Error head: `toolchain_no_sql`
- **bird_6** (bird_sqlite): toolchain_no_sql...
- **bird_16** (bird_sqlite): toolchain_no_sql...
- **bird_34** (bird_sqlite): toolchain_no_sql...

### 2. pred_generation_fail (count=10)
Error head: `'NoneType' object has no attribute 'strip'`
- **bird_465** (bird_sqlite): 'NoneType' object has no attribute 'strip'...
- **bird_730** (bird_sqlite): 'NoneType' object has no attribute 'strip'...
- **bird_761** (bird_sqlite): 'NoneType' object has no attribute 'strip'...

### 3. task_timeout (count=8)
Error head: `task timeout`
- **bird_350** (bird_sqlite): task timeout...
- **bird_701** (bird_sqlite): task timeout...
- **bird_733** (bird_sqlite): task timeout...

### 4. pred_exec_fail (count=3)
Error head: `no such column: a.client_id`
- **bird_93** (bird_sqlite): no such column: a.client_id...
- **bird_112** (bird_sqlite): no such column: a.client_id...
- **bird_125** (bird_sqlite): no such column: a.client_id...

### 5. pred_exec_fail (count=3)
Error head: `no such column: l.T`
- **bird_1227** (bird_sqlite): no such column: l.T...
- **bird_1232** (bird_sqlite): no such column: l.T...
- **bird_1298** (bird_sqlite): no such column: l.T...

### 6. pred_exec_fail (count=2)
Error head: `near "Type": syntax error`
- **bird_26** (bird_sqlite): near "Type": syntax error...
- **bird_76** (bird_sqlite): near "Type": syntax error...

### 7. pred_exec_fail (count=2)
Error head: `You can only execute one statement at a time.`
- **bird_259** (bird_sqlite): You can only execute one statement at a time....
- **bird_883** (bird_sqlite): You can only execute one statement at a time....

### 8. pred_exec_fail (count=2)
Error head: `no such column: ha.value`
- **bird_740** (bird_sqlite): no such column: ha.value...
- **bird_769** (bird_sqlite): no such column: ha.value...

### 9. pred_exec_fail (count=2)
Error head: `no such table: superheroes`
- **bird_760** (bird_sqlite): no such table: superheroes...
- **bird_775** (bird_sqlite): no such table: superheroes...

### 10. pred_exec_fail (count=2)
Error head: `no such column: t.player_api_id`
- **bird_1127** (bird_sqlite): no such column: t.player_api_id...
- **bird_1131** (bird_sqlite): no such column: t.player_api_id...

## bird_sqlite
- coverage: 1534/1534 (100.0%)
- execution_accuracy: 0.314
- ex_correct: 481
- compared: 1445
- gold_failed: 0
- pred_failed: 89
- skipped: 0
- latency: mean=6870.7 ms, median=5380.0 ms, p90=11847.6 ms
