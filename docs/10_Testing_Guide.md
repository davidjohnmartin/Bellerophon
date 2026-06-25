# Testing Guide

**Audience:** Engineers modifying Belle or testing pipelines that use it.

---

## 0. Dual-Mode Testing Requirement

**A Belle release is NOT validated until tested in BOTH modes:**

| Mode | Where | What it validates |
| --- | --- | --- |
| Interactive | Databricks cluster via UI | API contracts, config defaults, schema drift, write modes, DAG logic, orchestrator isolation |
| Production | ADF scheduled job | Blob writes, CSV export, service account detection, external Delta, non-interactive gating |

**Interactive-only behaviours:**
- `Utils.is_interactive_notebook()` returns `True`
- CSV export is SKIPPED (gated behind `not interactive_mode`)
- Tables write to warehouse directory (managed tables), not blob
- Service account detection returns `(False, '<user_email>')`

**Production-only behaviours:**
- CSV export writes to `/mnt/internal/enhanced/.../_tmp_csv_cubes/`
- External table writes to blob (`BLOB_ROOT` paths)
- Log entries have `ran_in_interactive_mode = False`
- Service account detection returns `(True, 'svc_aas_*')`

See `test_backwards_compatibility` notebook cell 2 for the full pre-release checklist.

---

## 1. Test Mode (Built-In Isolation)

### 1.1 What Test Mode Does

When `test_mode=True`, Belle appends a unique suffix to ALL table names:

```
my_table  →  my_table_belle_test_20260623_143022_a1b2c3d4
```

The suffix includes a timestamp AND a UUID fragment, ensuring:
* Full isolation from production tables
* No collision between concurrent test runs
* Easy identification and cleanup

### 1.2 Enabling Test Mode

```python
orchestrator = belle.Orchestrator(
    TABLES_CONFIG,
    test_mode=True,  # Enables table suffix isolation
)
results = orchestrator.run()
```

Belle prints:
```
[Belle Test Mode] Instance ID: 20260623_143022_a1b2c3d4
   Tables will use suffix: _belle_test_20260623_143022_a1b2c3d4
```

### 1.3 Cleanup After Testing

Test tables are NOT auto-cleaned. Drop them manually:

```python
# Find test tables
test_tables = spark.sql("""
    SHOW TABLES IN my_database LIKE '*_belle_test_*'
""").collect()

# Drop them
for row in test_tables:
    spark.sql(f"DROP TABLE IF EXISTS my_database.{row.tableName}")
```

Or use the instance ID from the run output:
```python
suffix = "_belle_test_20260623_143022_a1b2c3d4"
for table_key in TABLES_CONFIG:
    conf = TABLES_CONFIG[table_key]
    full_name = f"{conf['target_database']}.{conf['result_table_name']}{suffix}"
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
```

---

## 2. Integration Testing Patterns

### 2.1 Minimal End-to-End Test

```python
%run ./bellerophon_core

# Small test DataFrames
df_dim = spark.createDataFrame([
    (1, "Customer A"), (2, "Customer B")
], ["customer_id", "name"])

df_fact = spark.createDataFrame([
    (1, 1, 100.0), (2, 1, 200.0), (3, 2, 150.0)
], ["order_id", "customer_id", "amount"])

# Register
belle.OutputRegistry.set_output("test_db_dim_customer", df_dim)
belle.OutputRegistry.set_output("test_db_fact_order", df_fact)

# Config
TEST_CONFIG = {
    "test_db.dim_customer": {
        "target_database": "test_db",
        "result_table_name": "dim_customer",
        "load_mode": "full",
        "dependencies": [],
    },
    "test_db.fact_order": {
        "target_database": "test_db",
        "result_table_name": "fact_order",
        "load_mode": "merge",
        "merge_keys": ["order_id"],
        "dependencies": ["test_db.dim_customer"],
    },
}

# Run with test mode
orchestrator = belle.Orchestrator(TEST_CONFIG, test_mode=True)
results = orchestrator.run(show_dag=True)

# Verify
assert all(r['status'] == 'success' for r in results), "Some tables failed!"
assert len(results) == 2, f"Expected 2 tables, got {len(results)}"
print("All tests passed.")
```

### 2.2 Testing Schema Evolution

> **⚠️ Important:** Schema drift detection (`SCHEMA_DRIFT_ACTION="fail"`, the default) will
> raise `ValueError` if columns are added or removed. You MUST override this setting
> when testing intentional schema evolution. See KB-001 in the Troubleshooting Guide —
> a drift error inside the Orchestrator triggers an infinite retry storm.

```python
# Run 1: Create table with columns A, B
df_v1 = spark.createDataFrame([(1, "a")], ["id", "col_a"])
belle.OutputRegistry.set_output("test_db_evolving_table", df_v1)

cfg = {"test_db.evolving_table": {
    "target_database": "test_db", "result_table_name": "evolving_table",
    "load_mode": "full", "dependencies": []
}}
orchestrator = belle.Orchestrator(cfg, test_mode=True)
orchestrator.run()

# Run 2: Add column C — MUST disable drift detection
df_v2 = spark.createDataFrame([(1, "a", "new")], ["id", "col_a", "col_c"])
belle.OutputRegistry.set_output("test_db_evolving_table", df_v2)

with belle.Config.temp_config(SCHEMA_DRIFT_ACTION="ignore"):
    orchestrator = belle.Orchestrator(cfg, test_mode=True)
    orchestrator.run()

# Verify column exists
cols = spark.table("test_db.evolving_table" + orchestrator.test_suffix).columns
assert "col_c" in cols
```

### 2.2.1 Testing Schema Drift Detection Itself

To verify that Belle DETECTS drift correctly, call `belle.materialise_dataframe` directly
(bypasses the Orchestrator's retry loop):

```python
# After creating base table with schema [id, col_a]:
df_drifted = spark.createDataFrame([(1, 99)], ["id", "new_col"])

try:
    with belle.Config.temp_config(SCHEMA_DRIFT_ACTION="fail"):
        belle.materialise_dataframe(
            df=df_drifted,
            target_database="test_db",
            result_table_name="evolving_table",
            load_mode="full",
            use_managed_table=True,
        )
    assert False, "Should have raised ValueError"
except ValueError as e:
    assert "Schema drift detected" in str(e)
    print("Schema drift correctly detected")
```

> **Never test drift detection via the Orchestrator** — KB-001 causes infinite retries.

### 2.3 Testing Encryption Round-Trip

```python
import base64, os
test_key = base64.b64encode(os.urandom(32)).decode()

df = spark.createDataFrame([(1, "secret", 42.0)], ["id", "name", "value"])
belle.OutputRegistry.set_output("test_db_encrypted", df)

config = {
    "test_db.encrypted": {
        "target_database": "test_db",
        "result_table_name": "encrypted",
        "load_mode": "full",
        "dependencies": [],
        "encrypt": True,
        "encrypt_key": test_key,
        "encrypt_exclude": ["id"],
    }
}

orchestrator = belle.Orchestrator(config, test_mode=True)
orchestrator.run()

# Verify: id is plaintext, name/value are BINARY
table_name = f"test_db.encrypted{belle.Config.TEST_MODE_SUFFIX}"
schema = spark.table(table_name).schema
assert schema["id"].dataType.simpleString() == "int"
assert schema["name"].dataType.simpleString() == "binary"

# Verify decryption works
from pyspark.sql import functions as F
decrypted = spark.table(table_name).select(
    "id",
    F.expr(f"aes_decrypt(name, unbase64('{test_key}'), 'GCM', 'DEFAULT')").alias("name")
).collect()
assert decrypted[0]["name"] == "secret"
```

---

## 3. Unit Testing Individual Functions

### 3.1 Config Validator

```python
# Test that invalid config raises
try:
    belle.ConfigValidator.validate({"bad_table": {"target_database": "db"}})
    assert False, "Should have raised"
except ValueError as e:
    assert "Missing key" in str(e)
    print("Validator correctly caught missing keys")
```

### 3.2 DAG Cycle Detection

```python
cyclic_config = {
    "db.a": {"target_database": "db", "result_table_name": "a",
             "load_mode": "full", "dependencies": ["db.b"]},
    "db.b": {"target_database": "db", "result_table_name": "b",
             "load_mode": "full", "dependencies": ["db.a"]},
}

try:
    belle.ConfigValidator.validate_dag_config(cyclic_config)
    assert False, "Should have raised"
except ValueError as e:
    assert "cycle" in str(e).lower()
    print("Cycle detection works")
```

### 3.3 Maintenance Scheduler

```python
import datetime

scheduler = belle.MaintenanceScheduler(interactive_mode=True)

# Test a known date (2nd Sunday of June 2026 = June 14)
test_date = datetime.date(2026, 6, 14)
belle.Config.ENABLE_SCHEDULED_VACUUM = True
belle.Config.SCHEDULED_VACUUM_DAY_OF_WEEK = 6  # Sunday
belle.Config.SCHEDULED_VACUUM_WEEK_OF_MONTH = 2  # 2nd

assert scheduler.should_run_vacuum(test_date) == True
assert scheduler.should_run_vacuum(datetime.date(2026, 6, 15)) == False
print("Scheduler logic correct")
```

---

## 4. Using Tracer for Test Assertions

```python
# Enable tracing
BellerophonTracer.enable(full=True)

# Run orchestrator
orchestrator.run()

# Assert specific decisions were made
drop_events = BellerophonTracer.get_entries(event_filter="DROP")
assert len(drop_events) == 0, "Unexpected table drops!"

create_events = BellerophonTracer.get_entries(event_filter="WILL_CREATE")
assert len(create_events) == 2, f"Expected 2 creates, got {len(create_events)}"

# Check no partition mismatches
mismatches = BellerophonTracer.get_entries(event_filter="MISMATCH")
assert len(mismatches) == 0, f"Unexpected partition mismatches: {mismatches}"

BellerophonTracer.clear()
```

---

## 5. Regression Test Scenarios

| Scenario | What to verify |
| --- | --- |
| Partition mismatch recovery | Change partition_by, run, verify table rebuilt correctly |
| Encryption round-trip | Encrypt, read back, decrypt, compare to original |
| OOM retry | Mock OOM on first attempt, verify retry succeeds |
| Dependency skip on failure | Force one table to fail, verify dependents skip cleanly |
| Schema evolution (add column) | Add column to DF, run insert mode, verify column appears |
| Merge idempotency | Run merge twice with same data, verify no duplicates |
| Force rebuild protection | Set force_rebuild, verify log table is NOT dropped |
| Concurrent test isolation | Run two test_mode instances, verify no table collision |
| Multi-orchestrator isolation | Two Orchestrator instances writing to same DB, verify no cross-contamination |
| Concurrent ThreadPool execution | Two orchestrators via ThreadPoolExecutor — only with configs expected to succeed (see KB-001) |

---

## 6. Temporary Config Overrides for Testing

```python
# Override config for duration of test, then revert
with belle.Config.temp_config(
    VERBOSITY=4,
    FEATURE_CSV_EXPORT=False,
    FEATURE_ENCRYPTION=False,
    LOG_RETENTION_DAYS=0,
):
    # Run tests with overrides
    orchestrator.run()

# Config is automatically reverted here
```

---

*Last updated: June 2026*
