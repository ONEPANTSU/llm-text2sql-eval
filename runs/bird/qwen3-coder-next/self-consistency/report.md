# EvalSuite report for 20260213_231212_openrouter:qwen_qwen3-coder-next

- Aggregate execution accuracy: 0.308

## Overall
- total: 1534
- executed: 1534 | skipped: 0
- gold_failed: 0 | pred_failed: 142
- ex_correct: 472 / compared: 1392

## Error breakdown
- task_timeout: 124
- pred_exec_fail: 18

## Autofix & schema (v1.2)
- schema_warn_count: 0
- pred_bind_fail_autofix_success: 0
- pred_bind_fail_autofix_failed: 0

## Top error patterns
### 1. task_timeout (count=124)
Error head: `task timeout`
- **bird_22** (bird_sqlite): task timeout...
- **bird_40** (bird_sqlite): task timeout...
- **bird_43** (bird_sqlite): task timeout...

### 2. pred_exec_fail (count=2)
Error head: `near "order": syntax error`
- **bird_142** (bird_sqlite): near "order": syntax error...
- **bird_164** (bird_sqlite): near "order": syntax error...

### 3. pred_exec_fail (count=2)
Error head: `near "format": syntax error`
- **bird_207** (bird_sqlite): near "format": syntax error...
- **bird_751** (bird_sqlite): near "format": syntax error...

### 4. pred_exec_fail (count=2)
Error head: `no such column: l.T`
- **bird_1227** (bird_sqlite): no such column: l.T...
- **bird_1232** (bird_sqlite): no such column: l.T...

### 5. pred_exec_fail (count=1)
Error head: `near "Count": syntax error`
- **bird_26** (bird_sqlite): near "Count": syntax error...

### 6. pred_exec_fail (count=1)
Error head: `no such table: branch`
- **bird_109** (bird_sqlite): no such table: branch...

### 7. pred_exec_fail (count=1)
Error head: `no such column: dist.district_name`
- **bird_185** (bird_sqlite): no such column: dist.district_name...

### 8. pred_exec_fail (count=1)
Error head: `no such column: is_carci`
- **bird_334** (bird_sqlite): no such column: is_carci...

### 9. pred_exec_fail (count=1)
Error head: `You can only execute one statement at a time.`
- **bird_883** (bird_sqlite): You can only execute one statement at a time....

### 10. pred_exec_fail (count=1)
Error head: `no such column: ta.chance_creation_passing`
- **bird_1098** (bird_sqlite): no such column: ta.chance_creation_passing...

## Self-consistency diagnostics
- tasks_with_candidates: 1410
- total_candidates: 7048
- candidate_preflight_pass_rate: 0.896
- candidate_exec_pass_rate: 0.896
- avg_exec_ok_per_example: 4.48
- avg_majority_strength: 0.778

## bird_sqlite
- coverage: 1534/1534 (100.0%)
- execution_accuracy: 0.308
- ex_correct: 472
- compared: 1392
- gold_failed: 0
- pred_failed: 142
- skipped: 0
