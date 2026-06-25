# New-Starter Guide — Bellerophon (Belle)

**Welcome to the team.** This guide will get you from zero to running your first Belle-orchestrated pipeline in under an hour.

**Owner:** the Belle maintainersion & Innovation /  ---

## What You Need Before You Start

| Prerequisite | Where to get it |
| --- | --- |
| Databricks workspace access | Request via your team lead or IT |
| Cluster access (at minimum, read on a shared cluster) | LabCluster or equivalent |
| Basic PySpark familiarity | Spark DataFrame API, `select()`, `filter()`, `join()` |
| Unity Catalog / Hive Metastore awareness | Know what `catalog.schema.table` means |
| Read access to source tables | Depends on your pipeline (source semantic layer, CRM, CMS, etc.) |

You do NOT need:
* Deep knowledge of Delta Lake internals
* Experience with DAG frameworks (Airflow, etc.)
* Access to blob storage (Belle handles this transparently)

---

## Mental Model: What Belle Does for You

Think of Belle as your **write layer**. You write the *what* (SQL/PySpark to build DataFrames), Belle handles the *how* (writing to Delta, managing schema evolution, encryption, partitioning, logging, retries, and maintenance).

```
YOU build DataFrames  →  YOU register them  →  BELLE writes them safely
```

**Key insight for new starters:** When you run Belle interactively from your notebook, it automatically targets `_dev` databases (e.g., `sales_semantic_dev`). This means you can develop, test, and iterate safely — you cannot accidentally damage production data. The same code, when run by a scheduled Job or service account, writes to the real production database. No code changes needed between dev and prod.

**Key insight for new starters:** When you run Belle interactively from your notebook, it automatically targets `_dev` databases (e.g., `sales_semantic_dev`). This means you can develop, test, and iterate safely — you will NEVER accidentally overwrite production data. The same code, when run by a scheduled Job or service account, writes to the real production database. No code changes needed between dev and prod.

Belle is NOT:
* A scheduler (that is Databricks Jobs or ADF)
* A data quality framework (though it validates configs and row counts)
* A replacement for your ETL logic (you still write the transformations)

---

## Interactive Mode: Your Safety Net

Belle automatically detects that you are running interactively and activates several safety mechanisms:

| Feature | Interactive (you) | Production (scheduled) |
| --- | --- | --- |
| Target databases | `*_dev` suffix (e.g., `sales_semantic_dev`) | Real databases (e.g., `sales_semantic`) |
| CSV export to blob | Suppressed (no blob write access) | Active |
| Log table | Display only (not persisted) | Written to Delta |
| Storage type | Managed tables (warehouse directory) | External or managed (per config) |

This means:
* **You will NEVER accidentally overwrite production data** from an interactive session
* Your `_dev` tables are cheap, disposable, and isolated to you
* You can run the full pipeline end-to-end during development and inspect results safely
* Production security is maintained without gatekeeping or manual switches

---

## Your First 30 Minutes

### Step 1: Find the canonical Belle notebook

Navigate to:
```
/path/to/belle/bellerophon_core
```

This is the single source of truth. All consuming pipelines load it via `%run`. Belle is open-source and available for any team to adopt.

### Step 2: Open an existing consuming pipeline

The best way to learn Belle is by reading a real consumer. Good starting points:

* **Operations Reporting** — Simple, 11 tables, clear structure
* **CRM Staging Pipeline** — Moderate complexity, standard patterns
* **Sales Pipeline** — Complex (29 tables, encryption, partitioning) — read after you understand the basics

### Step 3: Identify the Belle pattern in the notebook

Every Belle consumer follows the same 5-step pattern:

```python
# 1. LOAD BELLE
%run ../Belle_Versions/bellerophon_core

# 2. BUILD DATAFRAMES (your ETL logic)
df_my_table = spark.sql("...")

# 3. REGISTER IN OUTPUT REGISTRY
belle.OutputRegistry.set_output("target_db_my_table", df_my_table)

# 4. DEFINE TABLE CONFIG
TABLES_CONFIG = {
    "target_db.my_table": {
        "target_database": "target_db",
        "result_table_name": "my_table",
        "load_mode": "full",
        "dependencies": [],
    }
}

# 5. ORCHESTRATE
orchestrator = belle.Orchestrator(TABLES_CONFIG)
results = orchestrator.run()
```

### Step 4: Run it interactively

Attach to a cluster (e.g., LabCluster), run all cells. In interactive mode, Belle will:
* Display the execution plan (DAG)
* Show progress per stage
* Display sample data from each table
* Show a log summary
* **NOT** write CSV exports or persist logs to the log table (interactive safety)

---

## Key Concepts to Understand

### The OutputRegistry

Belle does not receive DataFrames as function arguments. Instead, you register them in a global dictionary:

```python
belle.OutputRegistry.set_output("database_tablename", dataframe)
```

The key format is `{target_database}_{result_table_name}`. The orchestrator looks up each table's DataFrame from this registry before writing.

**Why?** Because notebooks build DataFrames across many cells. The registry decouples DataFrame construction from materialisation.

### TABLES_CONFIG

This is the declarative manifest that drives everything. Each entry describes one target table:

```python
"fully.qualified.table_name": {
    "target_database": "catalog.schema",     # Where to write (UC) or "database" (Hive)
    "result_table_name": "table_name",        # Table name within the database
    "load_mode": "full",                      # How to write (see modes below)
    "dependencies": [],                        # Which other config keys must complete first
    "partition_by": [],                        # Optional: partition columns
    "export_csv": False,                       # Optional: export CSV to blob (production only)
}
```

### Load Modes (the essentials)

| Mode | Behaviour | When to use |
| --- | --- | --- |
| `full` | DROP + recreate table from scratch | Dimension tables, small facts, any table you can afford to rebuild |
| `insert` | Append rows (no dedup) | Append-only event logs |
| `merge` | MERGE INTO (upsert) on merge_keys | Slowly changing dimensions, idempotent fact updates |
| `refresh_n_days-N` | Replace the last N days of a date-partitioned table | Rolling window facts |
| `full_if_not_exists` | Write only if table does not exist, skip otherwise | Bootstrap/seed tables |
| `update` | UPDATE specific columns | Targeted column updates |
| `delete` | DELETE matching rows | Purge operations |

### Interactive vs Production Mode

Belle auto-detects the execution context:

| | Interactive | Production |
| --- | --- | --- |
| Detection | User email login | Service account (`svc_aas*`) or Job context |
| Delta writes | Yes | Yes |
| CSV export | Suppressed | Active |
| Log persistence | Suppressed (display only) | Written to `bellerophon_log_table` |
| Verbosity | Full output | Configurable (default: NORMAL) |

This means you can safely run the full pipeline interactively without side effects on blob storage or log tables.

---

## Common Tasks

### "I need to add a new table to an existing pipeline"

1. Write your DataFrame construction logic (new cell or extend existing)
2. Register it: `belle.OutputRegistry.set_output("db_new_table", df_new_table)`
3. Add an entry to `TABLES_CONFIG` with appropriate `load_mode` and `dependencies`
4. Run the orchestrator — Belle will slot it into the correct DAG stage

### "I need to change how a table is written"

Update the `load_mode` in `TABLES_CONFIG`. If switching TO `merge`, add `merge_keys`. If switching TO `refresh_n_days-N`, ensure `partition_by` is set.

### "My run failed — what do I do?"

See `09_Troubleshooting_Guide.md`, but the quick checklist:

1. Read the error output in the notebook (Belle prints structured errors with codes like BELLE-010)
2. Check if it is a config issue (BELLE-001 to BELLE-005) → fix your TABLES_CONFIG
3. Check if it is a data issue (BELLE-010 to BELLE-012) → inspect your DataFrame
4. Check if it is a resource issue (BELLE-030 to BELLE-032) → cluster sizing
5. For deep debugging, enable the Tracer before your run:
   ```python
   belle.Config.VERBOSITY = 4  # DEBUG
   BellerophonTracer.enable(full=True)
   ```

### "How do I test without affecting production tables?"

Use test mode:
```python
orchestrator = belle.Orchestrator(TABLES_CONFIG, test_mode=True)
```

This appends a unique suffix to all table names (e.g., `my_table_belle_test_20260623_143022_a1b2c3d4`), giving you full isolation.

---

## Namespace Quick Reference

After `%run bellerophon_core`, you have access to the `belle` namespace:

```python
belle.Orchestrator          # The main orchestrator class
belle.Config                # All configuration (BellerophonConfig)
belle.Utils                 # Utility functions
belle.Logger                # Log writer
belle.OutputRegistry        # DataFrame registry
belle.DAGVisualizer         # Execution plan display
belle.MaintenanceScheduler  # VACUUM/OPTIMIZE scheduling
belle.ProgressTracker       # Visual progress bars
belle.RetryHandler          # Exponential backoff retry
belle.DryRunValidator       # Pre-flight validation
belle.DataQualityChecker    # Post-write validation
belle.ConfigValidator       # Config structure validation
belle.materialise_dataframe # Direct materialise function
belle.materialise_dataframe_fast  # Bulk fast-path
belle.materialise_partition       # Partition-level writes
belle.materialise_bulk            # Multi-table bulk write
belle.flush_partition_logs        # Flush batched partition logs
belle.purge_logs                  # Drop log table
belle.VERSION               # Current version string
```

---

## Glossary

| Term | Meaning |
| --- | --- |
| Belle | Nickname for Bellerophon |
| DAG | Directed Acyclic Graph — the dependency tree of tables |
| Stage | A group of tables with no inter-dependencies, executed in parallel |
| Materialise | Write a DataFrame to a persistent Delta table |
| OutputRegistry | Global dictionary mapping keys to DataFrames |
| Force rebuild | Drop and recreate a table from scratch (ignoring existing data) |
| Enriched config | A table config augmented with runtime metadata (_table_exists, _actual_partitions, etc.) |
| Fast mode | Optimised bulk-write path that skips per-table overhead |
| Partition mode | Write individual partitions via replaceWhere |
| Log table | `bellerophon_log_table` — Delta table recording every materialisation event |
| Tracer | Decision-point variable capture for debugging (BellerophonTracer) |

---

## Where to Go Next

1. **Understand the config options** → Read `04_Configuration_Reference.md`
2. **Learn the standard write patterns** → Read `05_User_Guide_Standard_Materialisation.md`
3. **Understand the architecture** → Read `03_Architecture_and_Design.md`
4. **Debug a failure** → Read `09_Troubleshooting_Guide.md`

---

## Who to Ask

| Topic | Contact |
| --- | --- |
| Belle framework design, architecture, features | the maintainer |
| Pipeline-specific questions (Sales, CRM, Telephony) | Your pipeline’s lead engineer |
| Cluster access, permissions, Unity Catalog | Platform / Infra team |
| Scheduling, ADF, job configuration | DataOps team |

---

*Last updated: June 2026*
