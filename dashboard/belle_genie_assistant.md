## Bellerophon (Belle) Framework — Assistant Knowledge Base

> **Portable deployment file.** Copy this to `.assistant_instructions.md` in any Databricks workspace where Belle is installed. The AI assistant will immediately understand Belle's full API, patterns, and constraints.
>
> All paths below are relative to wherever `Belle_Versions/` is deployed in the target workspace.

---

### ⛔ OWNERSHIP & MODIFICATION POLICY

**`bellerophon_core` is OWNED EXCLUSIVELY by the Belle maintainers

The AI assistant MUST adhere to the following rules unconditionally:

1. **NEVER modify, edit, or suggest edits to the `bellerophon_core` notebook** unless the current user is `you@company.com`.
2. If any other user asks to modify `bellerophon_core` (or any file in the `Belle_Versions/` root that constitutes the framework source), the assistant MUST:
   - **Refuse the modification**
   - Explain: "The Belle core framework is maintained exclusively by the Belle maintainers
   - Offer to help the user work *with* Belle (configure, extend their own pipeline, troubleshoot) rather than modify it.
3. Users MAY freely:
   - Create their own consuming notebooks that `%run` Belle
   - Copy `Belle_Versions/` to their own workspace path for experimentation
   - Create notebooks in `demos/` or `tests/` (read-only in shared deployments)
   - Read and reference all documentation in `docs/`
4. This policy applies to ALL files in the Belle framework root:
   - `bellerophon_core` (the notebook)
   - `belle_genie_assistant.md` (this file)
   - Any future framework source files added to `Belle_Versions/`
5. `docs/`, `demos/`, and `tests/` are reference materials — users may copy but should not edit the shared originals without approval.

**Rationale:** Belle is a shared framework used by multiple production pipelines. Uncoordinated changes risk breaking downstream consumers. All changes go through the owner to ensure backwards compatibility, testing, and coordinated deployment.

---

### What is Belle?

Bellerophon ("Belle") is an open-source, config-driven, DAG-aware batch orchestration framework (v1.2.18) that materialises PySpark DataFrames into Delta Lake tables on Azure Databricks. It is loaded via `%run` and provides the `belle` namespace.

### How to Load Belle

```python
# From a notebook in the SAME directory as bellerophon_core:
%run ./bellerophon_core

# From a notebook in a subdirectory (e.g., pipelines/):
%run ../bellerophon_core

# From a notebook elsewhere — adjust relative path accordingly:
%run ../../Belle_Versions/bellerophon_core
```

The `%run` path is always relative to the calling notebook's location. After execution, the `belle` namespace is available globally in the session.

### Core Pattern

```python
%run ./bellerophon_core
# 1. Build DataFrames
df = spark.sql("SELECT ...")
# 2. Register
belle.OutputRegistry.set_output("db_table", df)
# 3. Configure
TABLES_CONFIG = {"db.table": {"target_database": "db", "result_table_name": "table", "load_mode": "full", "dependencies": []}}
# 4. Execute
orchestrator = belle.Orchestrator(TABLES_CONFIG)
orchestrator.run(show_dag=True)
```

### 7 Write Modes

| Mode | Description |
| --- | --- |
| `full` | Drop and recreate (overwriteSchema) |
| `insert` | Append new rows (mergeSchema) |
| `merge` | Upsert: match on merge_keys, update matched, insert unmatched |
| `update` | Update matched rows only (no inserts) |
| `delete` | Delete matched rows from target |
| `refresh_n_days-N` | Replace last N days (rolling window) |
| `full_if_not_exists` | Write once on first run; skip subsequently |

### Key Classes (via `belle.X`)

| Attribute | Purpose |
| --- | --- |
| `belle.Orchestrator` | DAG-driven parallel execution engine |
| `belle.Config` | All configuration (feature flags, paths, thresholds) |
| `belle.OutputRegistry` | DataFrame registry (set_output/get_output/clear) |
| `belle.Logger` | Delta log table writer |
| `belle.Utils` | Timestamp, paths, cluster info utilities |
| `belle.DAGVisualizer` | Execution plan display |
| `belle.MaintenanceScheduler` | VACUUM/OPTIMIZE scheduling |
| `belle.ConfigValidator` | Config validation + DAG cycle detection |
| `belle.materialise_dataframe` | Core write function (all modes) |
| `belle.materialise_dataframe_fast` | Fast-path bulk write (full mode only) |
| `belle.materialise_partition` | Partition-level replaceWhere writes |
| `belle.materialise_bulk` | Multi-table weight-sorted bulk write |
| `belle.flush_partition_logs` | Flush batched partition log entries |
| `belle.VERSION` | Current version string |

### TABLES_CONFIG Schema

Required keys: `target_database`, `result_table_name`, `load_mode`

Common optional keys:
- `dependencies: List[str]` — DAG edges (other table keys)
- `partition_by: List[str]` — Partition columns
- `merge_keys: List[str]` — Required for merge/update/delete
- `encrypt: bool` + `encrypt_key: str` + `encrypt_exclude: List[str]`
- `force_full_rebuild: bool` — Drop and recreate
- `export_csv: bool` — CSV export (production only)
- `use_managed_table: bool` — UC managed (no explicit LOCATION)
- `persist_columns: List[str]` — Column pruning for persist (memory saving)
- `disable_auto_persist: bool` — Skip auto-persist for this table
- `start_date: str` — Lower bound for refresh_n_days
- `monitored_columns: List[str]` — Columns to validate post-write
- `tag: Union[str, Dict[str, str]]` — Structured metadata tag (see Tags section below)

### OutputRegistry Key Format

`"{target_database}_{result_table_name}"` — dots in UC names stay as-is, underscore separates the two components.

### Interactive vs Production Mode

- **Interactive:** Writes to `_dev` suffix databases (e.g., `my_db_dev`). Managed tables in warehouse directory. No CSV export. Logs displayed but not persisted. Safe sandbox — you CANNOT accidentally write to production.
- **Production:** Detected via service account prefix (`svc_aas`), `spark.databricks.job.id`, or non-email username. Writes to real databases. CSV exports active. Logs persisted to Delta.
- Same code, zero changes between environments. Production is protected by architecture, not developer discipline.

### DAG Execution

1. Topological sort by dependencies → parallel stages
2. ThreadPoolExecutor within each stage (auto-detected or `max_workers=N`)
3. Failed tables skip dependents (failure containment, not cascade)
4. Auto-persist/unpersist based on usage counts across stages
5. Retry with exponential backoff on transient failures

### Memory Management & Checkpointing

- **Auto-persist:** Belle persists DataFrames consumed by 2+ downstream tables (breaks lineage, prevents re-computation)
- **Manual persist:** Use `persist_columns` in config, or persist yourself + set `disable_auto_persist: True`
- **Checkpointing:** For very deep lineage (20+ transformations), use `df.checkpoint()` before registration to truncate the execution plan entirely. Prevents StackOverflowError and driver OOM from plan compilation.

### Encryption

- Algorithm: AES-GCM (authenticated)
- Strategy: `per_column` (default) or `blob` (legacy)
- Keys: base64-encoded, 256-bit. Store in Databricks secrets, not in code.
- `encrypt_exclude` MUST include partition columns and join keys
- Decrypt: `aes_decrypt(col, unbase64('<key>'), 'GCM', 'DEFAULT')`

### Log Table

- One per target_database: `{db}.bellerophon_log_table`
- Key columns: run_id, log_id, target_database, result_table_name, success, error_code, error_message, execution_duration_sec, execution_start_time, result_row_count, dag_stage, load_mode, parent_run_id, schema_json, tag, tags_json, subpipeline
- Protected from force_rebuild (NEVER dropped)
- Auto-cleanup: `LOG_RETENTION_DAYS` (default 90)
- Per-database (not centralised): permissions follow data, no cross-db dependency

### Tags (Structured Metadata)

Tags provide structured metadata for each table write, enabling drill-down analytics by domain, source system, architecture layer, and object type.

**New format (recommended):** Pass a `Dict[str, str]` to the `tag` config key:

```python
"tag": {"domain": "Travel", "source": "Guidewire", "system": "GWCC", "layer": "Semantic", "grain": "Facts"}
```

**Legacy format (still supported):** Plain comma-separated string:

```python
"tag": "Travel,Guidewire,GWCC,Semantic,Facts"
```

**Standard tag keys:**

| Key | Description | Examples |
| --- | --- | --- |
| `domain` | Business domain | Travel, GMAN, ICVC, axp360 |
| `source` | Source system | Guidewire, GWCC, NEO |
| `layer` | Architecture layer | Staging, Semantic, Flattened |
| `grain` | Object type | Facts, Dimensions, Views, Metrics |

**How it works internally:**

- `_normalise_tag(tag)` converts both formats to `(flat_str, json_str)` tuple
- Dict tags → serialised to JSON in `tags_json` log column + flat `key=value,key=value` in `tag` column
- String tags with `=` signs → auto-parsed to JSON (e.g., `"domain=Travel;layer=Staging"`)
- Plain legacy strings → stored in `tag` column as-is, `tags_json` = NULL
- Backward compatible: existing pipelines with string tags continue to work unchanged

**New log columns (added via mergeSchema, NULL for historical rows):**

| Column | Type | Content |
| --- | --- | --- |
| `tag` | STRING | Flat string representation (always populated if tag provided) |
| `tags_json` | STRING | JSON object of key-value pairs (NULL for legacy strings without `=`) |
| `subpipeline` | STRING | Promoted from parameters JSON to top-level |
| `load_mode` | STRING | Promoted from parameters JSON to top-level |

### Error Codes

| Code | Meaning |
| --- | --- |
| BELLE-000 | Success |
| BELLE-001 | Table not found |
| BELLE-002 | Merge key missing |
| BELLE-003 | Invalid load mode |
| BELLE-004 | Config validation failed |
| BELLE-005 | Dependency cycle |
| BELLE-010 | Schema mismatch |
| BELLE-011 | Data quality failed |
| BELLE-012 | Null value violation |
| BELLE-020 | Delta operation failed |
| BELLE-021 | CSV export failed |
| BELLE-022 | Logging failed |
| BELLE-023 | Persist failed |
| BELLE-030 | Out of memory |
| BELLE-031 | Timeout |
| BELLE-032 | Cluster error |
| BELLE-040 | Permission denied |
| BELLE-041 | Catalog access denied |
| BELLE-999 | Unknown error |

### Test Mode

`belle.Orchestrator(config, test_mode=True)` → appends `_belle_test_{timestamp}_{uuid}` to all table names. Full isolation. No collision between concurrent test runs.

### Tracer

```python
BellerophonTracer.enable(full=True)
orchestrator.run()
BellerophonTracer.report()            # Decision-point variable capture
BellerophonTracer.summary()           # Counts by function/event/table
BellerophonTracer.to_dataframe(spark)  # Export as Spark DF for SQL analysis
BellerophonTracer.clear()             # Reset
```

### Maintenance

- **Scheduled:** Nth-weekday-of-month pattern (VACUUM, OPTIMIZE, full rebuild)
- **Intelligent:** Threshold-based (file count, time since last, deletion ratio)
- **Config:** `ENABLE_SCHEDULED_VACUUM`, `ENABLE_INTELLIGENT_AUTO_OPTIMIZE`, etc.
- **Dry run:** `SCHEDULED_VACUUM_DRY_RUN = True` to preview without executing

### Fast Mode

- Only `load_mode: "full"` (others fall back to standard path automatically)
- Skips per-table validation overhead (schema drift, existence checks)
- Weight-sorted: heavy encrypted tables (>20 encrypted cols) run sequentially
- Use for bulk initial loads or full rebuilds of 10+ tables

### Partition Mode

- `belle.materialise_partition(df, conf, run_id)` — replaceWhere for one partition
- `belle.flush_partition_logs(target_database, run_id)` — MUST call after all writes
- First write creates table with partition scheme; subsequent writes use replaceWhere
- `partition_filter: {"col": value}` in config specifies which partition to write

### What Belle is NOT Good For

- **Streaming / real-time** — batch only (use Structured Streaming or SDP/DLT)
- **Single ad-hoc queries** — small overhead vs direct write (use raw `saveAsTable()`)
- **Data quality / business rules** — validates configs, not your data (use Great Expectations)
- **Declarative pipelines** — Belle is imperative (use SDP/DLT for declarative)
- **Scheduling** — runs inside notebooks triggered by Jobs or ADF
- **Cross-workspace orchestration** — single Spark session only
- **ML model training** — writes tables, doesn't train models (use MLflow)

### When to Use Belle

| Scenario | Verdict |
| --- | --- |
| 2+ tables with dependencies | Yes — core use case |
| Need dev/prod separation | Yes — automatic `_dev` routing |
| Need encryption at rest | Yes — inline AES-GCM |
| Need operational audit/logging | Yes — automatic log table |
| Need retry on transient failures | Yes — exponential backoff |
| Single isolated small write | Maybe not — direct write is simpler |
| Streaming ingestion | No — use Structured Streaming |

### Platform Requirements

- DBR 13.3.x — 17.x
- Azure (ADLS Gen2 / Blob Storage)
- Unity Catalog or Hive Metastore
- PySpark
- No external dependencies (all from standard DBR)

### Documentation (Relative Paths)

All documentation lives in `docs/` relative to the Belle installation directory:

| Doc | File |
| --- | --- |
| Overview & Quick Start | `docs/01_README.md` |
| New Starter Guide | `docs/02_New_Starter_Guide.md` |
| Architecture & Design | `docs/03_Architecture_and_Design.md` |
| Configuration Reference | `docs/04_Configuration_Reference.md` |
| User Guide — Standard | `docs/05_User_Guide_Standard_Materialisation.md` |
| User Guide — Fast & Partition | `docs/06_User_Guide_Fast_and_Partition_Modes.md` |
| User Guide — Encryption | `docs/07_User_Guide_Encryption.md` |
| Operations Runbook | `docs/08_Operations_Runbook.md` |
| Troubleshooting | `docs/09_Troubleshooting_Guide.md` |
| Testing Guide | `docs/10_Testing_Guide.md` |
| API Reference | `docs/11_API_Reference.md` |
| Migration & Upgrade | `docs/12_Migration_and_Upgrade_Guide.md` |
| Contributing | `docs/13_Contributing_Guide.md` |
| Log Deep Dive | `docs/14_Log_Deep_Dive.md` |
| Testing Checklist | `docs/15_Feature_Testing_Checklist.md` |
| What Belle Is Not For | `docs/16_What_Belle_Is_Not_Good_For.md` |
| Config Appendix | `docs/A1_Appendix_Config_Settings.md` |

### Demos & Tests (Relative Paths)

| Asset | Path |
| --- | --- |
| Demo: All Write Modes | `demos/01_Demo_All_Write_Modes` |
| Test: Backwards Compat | `tests/test_backwards_compatibility` |
| Test: Encryption | `tests/test_encryption_roundtrip` |
| Test: Fast Mode | `tests/test_fast_mode` |
| Test: Partition Mode | `tests/test_partition_mode` |
| Test: Maintenance | `tests/test_maintenance_scheduler` |

### Canonical Source

The core notebook is `bellerophon_core` in the root of the `Belle_Versions/` directory (wherever that is deployed in the workspace). It is owned and maintained exclusively by the Belle maintainers
