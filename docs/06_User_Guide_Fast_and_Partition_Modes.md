# User Guide — Fast Mode & Partition Mode

**Audience:** Data engineers working with high-volume pipelines that need optimised write paths.  
**Prerequisites:** Read `05_User_Guide_Standard_Materialisation.md` first. These modes are alternatives to the standard orchestrator for specific workloads.

---

## 1. When to Use These Modes

| Mode | Use case | Typical scenario |
| --- | --- | --- |
| Standard (`orchestrator.run()`) | General purpose, mixed load modes, DAG dependencies | Most pipelines |
| Fast Mode (`materialise_dataframe_fast`) | Bulk full-mode writes, many tables, minimal per-table overhead | Initial loads, full rebuilds of 20+ tables |
| Partition Mode (`materialise_partition`) | Surgical partition-level writes at high frequency | Monthly partition refreshes, incremental loads |
| Bulk Mode (`materialise_bulk`) | Write multiple tables in one call with weight-sorted execution | Encryption-heavy pipelines needing optimal core utilisation |

---

## 2. Fast Mode

### 2.1 What It Does

`bellerophon_materialise_dataframe_fast` is a lean write path that skips per-table overhead:

* No schema drift checks
* No table existence validation
* No per-table row count verification
* No sample display
* Drops table unconditionally before write
* Still performs: encryption, Delta write, CSV export, logging

### 2.2 When to Use

* All tables are `load_mode: "full"` (non-full modes automatically fall back to standard path)
* You want maximum throughput for initial loads or full rebuilds
* You have 10+ tables and don't need per-table diagnostics

### 2.3 Constraints

* Only supports `load_mode: "full"` — non-full modes silently redirect to `bellerophon_materialise_dataframe`
* No DAG dependency management (tables write in weight-sorted order, not dependency order)
* No auto-persist/unpersist lifecycle
* If you need dependencies between tables, use the standard orchestrator

### 2.4 Direct Usage

```python
from pyspark.sql import functions as F

# Build your DataFrame
df = spark.sql("SELECT * FROM raw.big_table")

# Config for this table
conf = {
    "target_database": "my_catalog.my_schema",
    "result_table_name": "big_table",
    "load_mode": "full",
    "partition_by": ["date_key"],
    "export_csv": False,
    "use_managed_table": True,
    "encrypt": True,
    "encrypt_key": "<base64-key>",
    "encrypt_exclude": ["date_key"],
}

run_id = str(uuid.uuid4())

result_df, log_df, _ = belle.materialise_dataframe_fast(
    input_df=df,
    conf=conf,
    run_id=run_id,
    dag_stage=1,
    interactive_mode=True,
)
```

### 2.5 Bulk Fast Mode (Weight-Sorted Orchestration)

`bellerophon_materialise_bulk` writes multiple tables in a single call, sorted by "weight" (encrypted column count) so heavy tables get priority:

```python
# Define configs for all tables
all_configs = {
    "table_a": {"target_database": "db", "result_table_name": "table_a", ...},
    "table_b": {"target_database": "db", "result_table_name": "table_b", ...},
    "table_c": {"target_database": "db", "result_table_name": "table_c", ...},
}

# DataFrames dict (key = config key)
dataframes = {
    "table_a": df_a,
    "table_b": df_b,
    "table_c": df_c,
}

# Bulk materialise
results = belle.materialise_bulk(
    dataframes=dataframes,
    configs=all_configs,
    run_id=str(uuid.uuid4()),
    interactive_mode=False,
)
```

### 2.6 Weight-Sorted Execution

Fast mode sorts tables by "heaviness" (encrypted column count above `FAST_MODE_HEAVY_COL_THRESHOLD`):

* **Heavy tables** (>20 encrypted columns): Run sequentially (`FAST_MODE_HEAVY_WORKERS_RATIO = 0`), getting full cluster resources
* **Light tables**: Run in parallel

This prevents memory pressure from multiple large encryption operations competing for cores.

---

## 3. Partition Mode

### 3.1 What It Does

`bellerophon_materialise_partition` writes a single partition (or partition combination) to a Delta table using `replaceWhere`. It:

1. Filters the input DataFrame to the specified partition values
2. Applies encryption (if configured)
3. Creates the table if it doesn't exist (with partition scheme)
4. Uses `replaceWhere` for subsequent writes (surgical overwrite of one partition)
5. Batches log entries for efficiency

### 3.2 When to Use

* High-frequency partition writes (e.g., monthly data arriving daily)
* Processing one month at a time across many tables
* Incremental pipelines where only specific partitions change
* When full-table rewrite is too expensive

### 3.3 Constraints

* Table must be partitioned (partition_by must be specified)
* First write creates the table; subsequent writes use replaceWhere
* No DAG dependency management (call sequentially or manage externally)
* Log entries are buffered (call `flush_partition_logs()` when done)

### 3.4 Usage Pattern

```python
import uuid

run_id = str(uuid.uuid4())

# Process multiple months for one table
for year in [2024, 2025]:
    for month in range(1, 13):
        conf = {
            "target_database": "my_catalog.my_schema",
            "result_table_name": "fact_monthly_sales",
            "partition_by": ["_data_year", "_data_month"],
            "partition_filter": {"_data_year": year, "_data_month": month},
            "use_managed_table": True,
            "encrypt": False,
        }

        rows_written = belle.materialise_partition(
            input_df=df_all_sales,  # Full DataFrame; Belle filters to partition
            conf=conf,
            run_id=run_id,
            interactive_mode=False,
        )

        if rows_written == 0:
            print(f"  No data for {year}-{month:02d}, skipped")

# Flush all buffered log entries at the end
belle.flush_partition_logs(
    target_database="my_catalog.my_schema",
    run_id=run_id,
)
```

### 3.5 Partition Filter

The `partition_filter` dict specifies which partition to write:

```python
# Single partition column
"partition_filter": {"sale_date": "2024-06-15"}

# Multiple partition columns (compound partition)
"partition_filter": {"_data_year": 2024, "_data_month": 6}
```

Belle builds the `replaceWhere` clause from these key-value pairs.

### 3.6 Log Buffering

Partition mode buffers log entries (default: flush every 200 writes) to avoid per-partition log table overhead. You MUST call `flush_partition_logs()` at the end of your loop:

```python
# Access the buffer directly if needed
belle.PartitionLogBuffer.count()   # How many entries buffered
belle.PartitionLogBuffer.clear()   # Discard without writing
belle.flush_partition_logs(target_database="db", run_id=run_id)  # Write to log table
```

### 3.7 First Write vs Subsequent Writes

| Table state | Behaviour |
| --- | --- |
| Table does not exist | Creates table with `partitionBy()` from config |
| Table exists | Uses `replaceWhere` to surgically overwrite one partition |

This means you can start a fresh partition-mode pipeline and it will create the table on the first partition write, then incrementally fill subsequent partitions.

---

## 4. Combining Modes

### 4.1 Standard Orchestrator + Partition Mode (Hybrid)

A common pattern: use the standard orchestrator for dimension tables (full mode), then partition mode for large fact tables:

```python
# Phase 1: Dimensions via standard orchestrator
DIM_CONFIG = {
    "db.dim_customer": {..., "load_mode": "full"},
    "db.dim_product": {..., "load_mode": "full"},
}
orchestrator = belle.Orchestrator(DIM_CONFIG)
orchestrator.run()

# Phase 2: Facts via partition mode (month by month)
for year, month in months_to_process:
    for fact_table in ["fact_sales", "fact_returns"]:
        conf = {
            "target_database": "db",
            "result_table_name": fact_table,
            "partition_by": ["_data_year", "_data_month"],
            "partition_filter": {"_data_year": year, "_data_month": month},
            ...
        }
        belle.materialise_partition(df_facts[fact_table], conf, run_id)

belle.flush_partition_logs(target_database="db", run_id=run_id)
```

### 4.2 Sales Pipeline Pattern

The Sales Pipeline uses exactly this hybrid approach:
1. Dimensions built once (full mode, standard orchestrator)
2. Fact tables processed partition-by-partition (partition mode) per country per month
3. Monthly views rebuilt after facts (standard orchestrator or fast mode)

---

## 5. Performance Comparison

| Metric | Standard | Fast Mode | Partition Mode |
| --- | --- | --- | --- |
| Per-table overhead | High (validation, schema check, row count) | Minimal | Minimal |
| DAG support | Yes | No | No |
| Load modes | All 7 | Full only | Partition writes only |
| Encryption | Yes | Yes | Yes |
| CSV export | Yes | Yes | No |
| Logging | Per-table (immediate) | Per-table (immediate) | Batched (flush at end) |
| Memory management | Auto persist/unpersist | None | None |
| Best for | General pipelines | Bulk initial loads | Incremental monthly processing |

---

## 5.1 Checkpointing Before Registration

For fast mode and partition mode, your DataFrames are often the result of complex multi-join pipelines. If you experience driver OOM or very long compilation times, **checkpoint** your DataFrames before registration:

```python
sc.setCheckpointDir("/tmp/belle_checkpoints")

# After a complex pipeline with many joins/unions
df_heavy = build_complex_staging_query()
df_heavy = df_heavy.checkpoint()  # Truncates lineage, writes to disk

belle.OutputRegistry.set_output("db_heavy_table", df_heavy)
```

This breaks the lineage chain, preventing Spark from attempting to compile a plan with thousands of nodes. It is especially important in fast mode where many tables are materialised in sequence — without checkpointing, each table's write action can trigger re-compilation of the entire upstream lineage.

---

## 6. Requirements & Constraints Summary

### Fast Mode Requirements
- All tables must use `load_mode: "full"`
- DataFrames must be pre-built (no OutputRegistry integration)
- No dependency ordering needed between tables
- Tables are dropped unconditionally before write

### Partition Mode Requirements
- `partition_by` must be specified in config
- `partition_filter` must specify exact partition values
- Partition columns must exist in the DataFrame
- Call `flush_partition_logs()` after all writes complete
- First write creates the table; ensure partition scheme is correct from the start

### Bulk Mode Requirements
- `dataframes` dict keys must match `configs` dict keys
- All tables should be full mode (non-full falls back to standard)
- Weight-sorted: heavy encrypted tables execute first/sequentially

---

*Last updated: June 2026*
