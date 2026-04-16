# EvalSuite report for 20260416_053324_gpt-oss-120b

- Aggregate execution accuracy: 0.060

## Overall
- total: 1534
- executed: 1534 | skipped: 0
- gold_failed: 0 | pred_failed: 1314
- ex_correct: 92 / compared: 220

## Error breakdown
- pred_generation_fail: 914
- pred_exec_fail: 348
- task_timeout: 52

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Latency
- mean: 21145.6 ms
- median: 13064.3 ms
- p90: 46684.5 ms
- p99: 99237.5 ms
- tasks_with_latency: 1482
- bird_sqlite: mean=21145.6 ms, median=13064.3 ms, p90=46684.5 ms

## Candidates count
- mean: 0.37
- median: 0.0

## Top error patterns
### 1. pred_generation_fail (count=478)
Error head: `'NoneType' object has no attribute 'strip'`
- **bird_2** (bird_sqlite): 'NoneType' object has no attribute 'strip'...
- **bird_3** (bird_sqlite): 'NoneType' object has no attribute 'strip'...
- **bird_5** (bird_sqlite): 'NoneType' object has no attribute 'strip'...

### 2. pred_generation_fail (count=429)
Error head: `toolchain_no_sql`
- **bird_0** (bird_sqlite): toolchain_no_sql...
- **bird_1** (bird_sqlite): toolchain_no_sql...
- **bird_13** (bird_sqlite): toolchain_no_sql...

### 3. task_timeout (count=52)
Error head: `task timeout`
- **bird_21** (bird_sqlite): task timeout...
- **bird_39** (bird_sqlite): task timeout...
- **bird_100** (bird_sqlite): task timeout...

### 4. pred_exec_fail (count=10)
Error head: `no such column: p.patient_id`
- **bird_1159** (bird_sqlite): no such column: p.patient_id...
- **bird_1172** (bird_sqlite): no such column: p.patient_id...
- **bird_1212** (bird_sqlite): no such column: p.patient_id...

### 5. pred_exec_fail (count=8)
Error head: `no such column: s.name`
- **bird_27** (bird_sqlite): no such column: s.name...
- **bird_739** (bird_sqlite): no such column: s.name...
- **bird_741** (bird_sqlite): no such column: s.name...

### 6. pred_exec_fail (count=7)
Error head: `near ".": syntax error`
- **bird_177** (bird_sqlite): near ".": syntax error...
- **bird_366** (bird_sqlite): near ".": syntax error...
- **bird_717** (bird_sqlite): near ".": syntax error...

### 7. pred_exec_fail (count=7)
Error head: `no such table: molecules`
- **bird_205** (bird_sqlite): no such table: molecules...
- **bird_219** (bird_sqlite): no such table: molecules...
- **bird_233** (bird_sqlite): no such table: molecules...

### 8. pred_exec_fail (count=5)
Error head: `no such table: store_sales`
- **bird_129** (bird_sqlite): no such table: store_sales...
- **bird_144** (bird_sqlite): no such table: store_sales...
- **bird_1507** (bird_sqlite): no such table: store_sales...

### 9. pred_exec_fail (count=5)
Error head: `no such column: m.name`
- **bird_300** (bird_sqlite): no such column: m.name...
- **bird_305** (bird_sqlite): no such column: m.name...
- **bird_321** (bird_sqlite): no such column: m.name...

### 10. pred_exec_fail (count=5)
Error head: `no such column: c.colour_name`
- **bird_722** (bird_sqlite): no such column: c.colour_name...
- **bird_733** (bird_sqlite): no such column: c.colour_name...
- **bird_735** (bird_sqlite): no such column: c.colour_name...

## bird_sqlite
- coverage: 1534/1534 (100.0%)
- execution_accuracy: 0.060
- ex_correct: 92
- compared: 220
- gold_failed: 0
- pred_failed: 1314
- skipped: 0
- latency: mean=21145.6 ms, median=13064.3 ms, p90=46684.5 ms
