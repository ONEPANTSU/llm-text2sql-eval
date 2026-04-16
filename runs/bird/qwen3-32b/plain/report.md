# EvalSuite report for 20260415_085621_qwen3-32b

- Aggregate execution accuracy: 0.075

## Overall
- total: 1534
- executed: 1534 | skipped: 0
- gold_failed: 0 | pred_failed: 1131
- ex_correct: 115 / compared: 403

## Error breakdown
- pred_generation_fail: 618
- pred_exec_fail: 475
- task_timeout: 38

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Latency
- mean: 38251.0 ms
- median: 36105.8 ms
- p90: 59514.5 ms
- p99: 96026.8 ms
- tasks_with_latency: 1496
- bird_sqlite: mean=38251.0 ms, median=36105.8 ms, p90=59514.5 ms

## Candidates count
- mean: 0.57
- median: 1.0

## Top error patterns
### 1. pred_generation_fail (count=604)
Error head: `toolchain_no_sql`
- **bird_1** (bird_sqlite): toolchain_no_sql...
- **bird_8** (bird_sqlite): toolchain_no_sql...
- **bird_10** (bird_sqlite): toolchain_no_sql...

### 2. task_timeout (count=38)
Error head: `task timeout`
- **bird_23** (bird_sqlite): task timeout...
- **bird_28** (bird_sqlite): task timeout...
- **bird_29** (bird_sqlite): task timeout...

### 3. pred_exec_fail (count=17)
Error head: `no such table: customer`
- **bird_93** (bird_sqlite): no such table: customer...
- **bird_162** (bird_sqlite): no such table: customer...
- **bird_187** (bird_sqlite): no such table: customer...

### 4. pred_exec_fail (count=8)
Error head: `no such table: item`
- **bird_290** (bird_sqlite): no such table: item...
- **bird_339** (bird_sqlite): no such table: item...
- **bird_368** (bird_sqlite): no such table: item...

### 5. pred_exec_fail (count=7)
Error head: `no such column: d.name`
- **bird_126** (bird_sqlite): no such column: d.name...
- **bird_870** (bird_sqlite): no such column: d.name...
- **bird_874** (bird_sqlite): no such column: d.name...

### 6. pred_exec_fail (count=7)
Error head: `no such column: m.name`
- **bird_338** (bird_sqlite): no such column: m.name...
- **bird_1320** (bird_sqlite): no such column: m.name...
- **bird_1368** (bird_sqlite): no such column: m.name...

### 7. pred_exec_fail (count=7)
Error head: `no such table: superheroes`
- **bird_723** (bird_sqlite): no such table: superheroes...
- **bird_768** (bird_sqlite): no such table: superheroes...
- **bird_795** (bird_sqlite): no such table: superheroes...

### 8. pred_exec_fail (count=6)
Error head: `no such column: m.carcinogenic`
- **bird_212** (bird_sqlite): no such column: m.carcinogenic...
- **bird_255** (bird_sqlite): no such column: m.carcinogenic...
- **bird_261** (bird_sqlite): no such column: m.carcinogenic...

### 9. pred_exec_fail (count=6)
Error head: `no such table: bonds`
- **bird_224** (bird_sqlite): no such table: bonds...
- **bird_234** (bird_sqlite): no such table: bonds...
- **bird_254** (bird_sqlite): no such table: bonds...

### 10. pred_exec_fail (count=6)
Error head: `no such column: name`
- **bird_721** (bird_sqlite): no such column: name...
- **bird_753** (bird_sqlite): no such column: name...
- **bird_838** (bird_sqlite): no such column: name...

## bird_sqlite
- coverage: 1534/1534 (100.0%)
- execution_accuracy: 0.075
- ex_correct: 115
- compared: 403
- gold_failed: 0
- pred_failed: 1131
- skipped: 0
- latency: mean=38251.0 ms, median=36105.8 ms, p90=59514.5 ms
