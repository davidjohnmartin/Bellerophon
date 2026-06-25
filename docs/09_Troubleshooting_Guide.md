# Troubleshooting Guide

**Audience:** Engineers debugging failed Belle runs.

---

## 1. First Response: Read the Error Output

Belle prints structured error information. Look for:

1. **Error code** (e.g., `BELLE-010`) — tells you the category
2. **Table name** — which table failed
3. **Error message** — specific details
4. **Stack trace** — printed automatically for failed tables

The orchestrator continues after failures — only dependents of the failed table are skipped.

---

## 2. Using the Tracer

### 2.1 Enable Before Your Run

```python
# Standard tracing (decision points only)
BellerophonTracer.enable()

# Full tracing (captures all local variables at each decision point)
BellerophonTracer.enable(full=True)

# Also increase verbosity for maximum output
belle.Config.VERBOSITY = 4  # DEBUG
```

### 2.2 After the Run: View the Report

```python
# Full report
BellerophonTracer.report()

# Filter by table
BellerophonTracer.report(table_filter="dim_customer")

# Filter by variable value
BellerophonTracer.report(var_filter="force_rebuild")

# Compact one-liner format
BellerophonTracer.report(compact=True)

# Summary (counts by function, event, table)
BellerophonTracer.summary()
```

### 2.3 Export to DataFrame (for SQL Analysis)

```python
trace_df = BellerophonTracer.to_dataframe(spark)
display(trace_df)

# Query specific events
trace_df.filter("event LIKE '%DROP%'").show(truncate=False)
```

### 2.4 What the Tracer Records

Key trace events:

| Event | Meaning |
| --- | --- |
| `CHECK_EXISTENCE` | Table existence check performed |
| `PATH_1_FORCE_DROP` | Table dropped (force rebuild) |
| `PATH_2_WILL_CREATE` | Table doesn't exist, will create |
| `PATH_3_VALIDATED_OK` | Table exists, partitions match |
| `PATH_3_MISMATCH_DROP` | Partition mismatch, dropped |
| `ENRICHED_METADATA_READ` | Validation metadata applied to materialise |
| `GLOBAL_REBUILD_DECISION` | Global force rebuild logic resolved |
| `PER_TABLE_REBUILD_DECISION` | Per-table rebuild decision |
| `TABLE_READINESS_COMPLETE` | All readiness checks done |

---

## 3. Common Failures

### 3.1 "Registry completely empty"

**Symptom:** "BELLE REGISTRY HEALTH CHECK FAILED — 0/N DataFrames found"

**Cause:** Cluster restart, notebook detach, or Python kernel restart cleared the OutputRegistry.

**Fix:** Rerun all cells that call `belle.OutputRegistry.set_output(...)`, then rerun orchestrator.

### 3.2 "Merge key duplicate warning / MERGE OPERATION BLOCKED"

**Symptom:** ValueError with BELLE-011 or merge key duplicate warning.

**Cause:** Source DataFrame has duplicate values in merge_keys columns.

**Fix options:**
1. Fix upstream: deduplicate your DataFrame before registration
2. Enable auto-dedup: `"merge_auto_deduplicate": True` in table config
3. Disable validation: `belle.Config.MERGE_VALIDATE_SOURCE_KEYS = False` (not recommended)

**Investigate:**
```python
df.groupBy("merge_key_col").count().filter("count > 1").show()
```

### 3.3 "Table does not exist, but load_mode 'merge' requires an existing table"

**Symptom:** BELLE-001 for merge/update/delete mode tables.

**Cause:** First run, or table was dropped.

**Fix:** Run once with `load_mode: "full"` to create the table, then switch to `merge`.

### 3.4 "Partition column 'X' not in DataFrame"

**Symptom:** Pre-validation error before any writes.

**Cause:** Your DataFrame doesn't contain the column specified in `partition_by`.

**Fix:** Add the column to your DataFrame, or correct the `partition_by` config.

### 3.5 OutOfMemoryError (BELLE-030)

**Symptom:** Py4JJavaError with OutOfMemoryError.

**Cause:** Not enough memory for the operation (large DataFrame, many encrypted columns, high parallelism).

**Fix options:**
1. Reduce `max_workers` in `orchestrator.run(max_workers=2)`
2. Increase cluster size (more worker nodes)
3. Set `disable_auto_persist: True` on large tables
4. Use partition mode for very large tables (write partition by partition)

### 3.6 "Permission denied" (BELLE-040/041)

**Symptom:** AnalysisException about access denied.

**Cause:** Service principal or user lacks write access to target database/catalog.

**Fix:** Grant appropriate permissions:
```sql
GRANT CREATE TABLE ON SCHEMA my_catalog.my_schema TO `service-principal-name`;
GRANT MODIFY ON SCHEMA my_catalog.my_schema TO `service-principal-name`;
```

### 3.7 "CSV export failed" (BELLE-021)

**Symptom:** Warning about CSV export failure, but tables wrote successfully.

**Impact:** Non-blocking. Delta tables are correct. Only CSV consumers (BI tools) affected.

**Cause:** Blob storage write permissions or path issues.

**Fix:** Check mount accessibility for the service principal. Check `BLOB_ROOT` path.

---

## 4. Dry Run Validation

Use `BellerophonDryRunValidator` to validate configs WITHOUT writing:

```python
validator = belle.DryRunValidator()
results = validator.validate(TABLES_CONFIG, spark)

# Check results
for table, result in results.items():
    if result['errors']:
        print(f"{table}: {result['errors']}")
```

This performs all pre-flight checks (table existence, partition matching, column validation) but writes nothing.

---

## 5. Data Quality Checker

Post-write validation:

```python
checker = belle.DataQualityChecker()
results = checker.validate(
    spark,
    TABLES_CONFIG,
    checks=["row_count", "null_check", "duplicate_check"]
)
```

---

## 6. Diagnostic Queries

### 6.1 Find Slowest Tables in Last Run

```sql
SELECT result_table_name, execution_duration_seconds, result_row_count
FROM my_db.bellerophon_log_table
WHERE run_id = (SELECT run_id FROM my_db.bellerophon_log_table ORDER BY execution_start_time DESC LIMIT 1)
ORDER BY execution_duration_seconds DESC;
```

### 6.2 Track Table Size Over Time

```sql
SELECT result_table_name, DATE(execution_start_time) AS run_date, result_row_count
FROM my_db.bellerophon_log_table
WHERE success = true AND result_table_name = 'fact_order'
ORDER BY execution_start_time;
```

### 6.3 Error Frequency

```sql
SELECT error_code, COUNT(*) AS occurrences, COLLECT_SET(result_table_name) AS affected_tables
FROM my_db.bellerophon_log_table
WHERE success = false AND execution_start_time >= current_date() - INTERVAL 30 DAYS
GROUP BY error_code
ORDER BY occurrences DESC;
```

---

## 7. Known Bugs & Hazards

### 7.1 KB-001: Infinite Retry Storm on Non-Transient Errors

**Status:** Open (v1.2.18)  
**Severity:** High — causes orchestrator runs to hang indefinitely.  
**Affected component:** `Orchestrator.process_one_table` → `RetryHandler.retry_with_backoff`

**Problem:** When a `ValueError` is raised during materialisation (e.g., schema drift detected, config validation error), the `RetryHandler` incorrectly classifies it as transient and retries indefinitely. Additionally, the Orchestrator's stage-level scheduler re-queues failed tables into successive stages (stage 1, stage 2, stage 3...) without a maximum stage limit.

**What `MAX_MATERIALISE_RETRIES=0` does NOT fix:** Setting this to 0 prevents the inner per-table retry loop, but does NOT stop the outer stage re-scheduling loop. The orchestrator will still create new stages with the failed table indefinitely.

**Observed symptoms:**
- 14+ stages printed, each with the same `ValueError`
- Cell never completes (must be manually cancelled)
- If running inside a `ThreadPoolExecutor`, the thread becomes an unkillable orphan

**Workaround for testing:**
- Call `belle.materialise_dataframe(...)` directly to bypass the Orchestrator's retry logic
- Do NOT use `ThreadPoolExecutor` to wrap orchestrator calls that may fail — orphan threads cannot be killed from Python when stuck in JVM calls

**Root cause fix required:**
1. `RetryHandler.is_transient_error()` must return `False` for `ValueError`
2. Orchestrator stage loop needs a maximum stage count (e.g., `max_stages = len(tables_config) + 1`)

### 7.2 Orphan Daemon Threads (ThreadPoolExecutor Hazard)

**Scenario:** When testing concurrent orchestrations with `ThreadPoolExecutor`, if a thread enters the KB-001 retry storm, it becomes permanently stuck in a JVM call (`saveAsTable`). Python's `PyThreadState_SetAsyncExc` cannot interrupt JVM-blocked threads.

**Impact:** The orphan thread continues writing tracebacks to stdout, polluting all subsequent cell outputs in the notebook session.

**Resolution:** Only a kernel restart (detach/reattach cluster) kills orphan threads. There is no Python-level fix.

**Prevention:**
- Use `concurrent.futures` with `timeout` on `future.result()`
- Only submit orchestrator calls that are expected to succeed (validated configs, no schema drift)
- For testing failure scenarios (schema drift, invalid config), call `belle.materialise_dataframe` directly — never via the Orchestrator inside a thread

### 7.3 KB-002: LOG_RETENTION_DAYS Default Mismatch

**Status:** Open (v1.2.18)  
**Impact:** Low — affects log cleanup schedule only.

**Problem:** `LOG_RETENTION_DAYS` is currently `90` but the documented/intended value is `730` (2 years). All production pipelines should explicitly set this to 730 until the default is updated in a future release.

---

## 8. Escalation Path

| Severity | Symptom | Action |
| --- | --- | --- |
| Low | Single table failed, non-critical | Fix and rerun next cycle |
| Medium | Multiple tables failed, downstream impacted | Investigate immediately, fix config |
| High | All tables failed (cluster/permission issue) | Escalate to platform team |
| Critical | Data corruption detected post-write | Stop pipeline, investigate, force rebuild |

---

---

## 9. Testing Mode Considerations

**Interactive mode only tests:**
- API contracts, config defaults, schema detection, write modes, DAG logic
- Orchestrator isolation, OutputRegistry, temp_config

**Production (ADF) only tests:**
- Service account detection (`Utils.detect_service_account()` → `(True, 'svc_aas_*')`)
- CSV export to blob storage
- External table writes (non-`_dev` databases)
- Non-interactive mode gating (`ran_in_interactive_mode = False` in logs)

A release is NOT validated until both modes have been tested. See `test_backwards_compatibility` notebook cell 2 for the full pre-release checklist.

---

*Last updated: June 2026*
