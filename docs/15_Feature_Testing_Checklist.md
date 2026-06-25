# Feature Testing Checklist

**Version:** Use this checklist for every Belle release. Mark each item PASS/FAIL before deploying.

---

## Pre-Release Validation Checklist

### 1. Namespace & API Surface

- [ ] All `belle.X` attributes exist (run `tests/test_backwards_compatibility` Test 1)
- [ ] `belle.VERSION` is updated to new version string
- [ ] No new module-level names pollute consumer namespace (check with `dir()` diff)
- [ ] `BellerophonTracer` class accessible and functional
- [ ] `BelleNamespace` facade maps all new public API

### 2. Configuration

- [ ] All existing config defaults unchanged (Test 2)
- [ ] `temp_config()` context manager applies and reverts (Test 7)
- [ ] `from_env()` loads BELLE_* environment variables
- [ ] `reset_defaults()` restores all defaults
- [ ] New config attributes have sensible defaults
- [ ] `VALID_LOAD_MODES` contains all 7 modes

### 3. Write Modes (Standard Orchestrator)

- [ ] `full` — Drops and recreates table
- [ ] `insert` — Appends rows (schema evolution via mergeSchema)
- [ ] `merge` — Upserts (match on merge_keys, update + insert)
- [ ] `update` — Updates matched rows only (no new inserts)
- [ ] `delete` — Removes matched rows from target
- [ ] `refresh_n_days-N` — Replaces last N days of data
- [ ] `full_if_not_exists` — Writes once, skips on subsequent runs

### 4. DAG Execution

- [ ] Topological sort produces correct stage ordering
- [ ] Cycle detection raises clear error (Test 4)
- [ ] Parallel execution within stages (verify with 4+ tables in same stage)
- [ ] Failed table skips dependents (not the whole run)
- [ ] `tables_to_run` subset filtering works
- [ ] DAG visualisation displays correctly (`show_dag=True`)

### 5. OutputRegistry

- [ ] `set_output()` / `get_output()` / `clear_outputs()` work (Test 5)
- [ ] Overwrite existing key replaces value
- [ ] Missing key returns None (not error)
- [ ] Registry health check detects cleared registry

### 6. Encryption

- [ ] Per-column encryption produces BINARY columns
- [ ] `encrypt_exclude` leaves specified columns in plaintext
- [ ] Decryption with same key recovers original values
- [ ] Missing `encrypt_key` with `encrypt: True` raises error
- [ ] `FEATURE_ENCRYPTION = False` disables all encryption

### 7. Fast Mode

- [ ] `materialise_dataframe_fast` writes full-mode tables
- [ ] Non-full modes fall back to standard path
- [ ] Weight-sorted execution (heavy tables first/sequential)
- [ ] `materialise_bulk` processes all tables in dict

### 8. Partition Mode

- [ ] `materialise_partition` writes single partition via `replaceWhere`
- [ ] First write creates table with partition scheme
- [ ] Subsequent writes use replaceWhere (surgical)
- [ ] `flush_partition_logs()` writes buffered entries
- [ ] `PartitionLogBuffer.count()` tracks buffer size

### 9. Test Mode

- [ ] `test_mode=True` appends unique suffix to all table names
- [ ] Suffix includes timestamp + UUID (no collision)
- [ ] Test tables isolated from production
- [ ] Multiple concurrent test runs don't collide

### 10. Logging

- [ ] Log table created on first write
- [ ] Log table protected from `force_full_rebuild` (NEVER dropped)
- [ ] Success entries have BELLE-000, duration, row count
- [ ] Failure entries have appropriate error code + message
- [ ] `run_id` correlates all tables in one execution
- [ ] `parent_run_id` passed through from `external_run_id`
- [ ] `LOG_RETENTION_DAYS` cleanup works
- [ ] `purge_logs()` drops the log table
- [ ] Schema evolution on log table (new columns) handled

### 11. Maintenance

- [ ] Scheduled maintenance triggers on correct Nth-weekday
- [ ] VACUUM executes with correct retention hours
- [ ] OPTIMIZE executes (dry-run mode for testing)
- [ ] Intelligent auto-maintenance detects threshold conditions
- [ ] Maintenance dry-run previews without executing

### 12. Interactive vs Production Mode

- [ ] Interactive mode detected correctly (email user, no job.id)
- [ ] Production mode detected correctly (svc_aas prefix OR job.id)
- [ ] `_dev` database suffix applied in interactive mode
- [ ] CSV export suppressed in interactive mode
- [ ] Log table persistence suppressed in interactive mode

### 13. Error Handling

- [ ] All 18 error codes can be triggered (or at least referenced)
- [ ] Retry handler uses exponential backoff
- [ ] Failed tables don't crash the orchestrator
- [ ] Stack traces captured in error_message

### 14. Tracer

- [ ] `enable(full=True)` captures locals
- [ ] `report()` produces readable output
- [ ] `get_entries()` filters work (table, function, event, var)
- [ ] `to_dataframe()` returns valid Spark DataFrame
- [ ] `summary()` shows counts by function/event/table

---

## Production Smoke Test (After Deploy)

- [ ] Run Operations Reporting pipeline (smallest, fastest)
- [ ] Verify all 11 tables written successfully
- [ ] Verify log table has entries for this run
- [ ] Verify CSV export produced (if production mode)
- [ ] Compare row counts to previous run (within 1% tolerance)

---

## Sign-Off

| Field | Value |
| --- | --- |
| Belle version | |
| Tested by | |
| Date | |
| All tests PASS? | YES / NO |
| Production smoke test PASS? | YES / NO |
| Safe to deploy? | YES / NO |

---

*Last updated: June 2026*

---

## Validator Testing

Run `belle.Validator.validate()` against each pipeline before release:

```python
# Validate all known pipelines
pipelines = [
    "/path/to/sales_pipeline/sales_pipeline",
    "/path/to/crm_staging_pipeline/crm_staging",
    "/path/to/telephony_pipeline/telephony_semantic",
]

for p in pipelines:
    passed = belle.Validator.validate(p)
    assert passed, f"Validation failed for {p}"
```

- [ ] All pipelines pass with 0 errors
- [ ] Warnings are reviewed and understood (intentional anti-patterns documented)
- [ ] `belle.Validator.validate_config(TABLES_CONFIG)` passes for inline configs
