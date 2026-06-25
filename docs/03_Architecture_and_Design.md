# Architecture & Design Decisions

**Audience:** Architects, senior engineers, anyone wanting to understand *why* Belle is built the way it is.

---

## 1. Design Philosophy

Bellerophon exists because writing DataFrames to Delta tables in production is harder than it looks. You need:

* Dependency ordering (table B depends on table A)
* Parallel execution where possible
* Schema evolution handling
* Partition-aware writes
* Encryption at rest
* Logging and auditability
* Retry logic for transient failures
* Memory management (persist/unpersist)
* Maintenance (VACUUM, OPTIMIZE)
* Safe interactive testing without production side effects

Doing this ad-hoc in each notebook leads to inconsistency, bugs, and maintenance burden. Belle centralises all of this into a single, config-driven framework.

**Core principle:** Declare *what* you want (table name, write mode, dependencies, encryption). Belle handles *how*.

---

## 2. Module Structure (Cell Layout)

Belle lives in a single notebook (`bellerophon_core`) with 16 cells. This is deliberate — it is loaded via `%run`, which requires a single notebook. The cells are logically ordered:

| Cell | Name | Responsibility |
| --- | --- | --- |
| 1 | Header | Version string, changelog summary |
| 2 | Imports & Constants | All module-level imports, version constant, Tracer class |
| 3 | BellerophonConfig | All configuration (feature flags, paths, thresholds, maintenance) |
| 4 | ErrorCode & Table Readiness | Structured error codes, `ensure_table_ready()` |
| 5 | OutputRegistry | DataFrame registry for inter-stage data passing |
| 6 | BellerophonUtils | Utility functions (timestamps, paths, CSV, cluster info) |
| 7 | BellerophonLogger | Delta log table writer (4-case logic) |
| 8 | Validators & Production Features | ConfigValidator, ProgressTracker, RetryHandler, DryRunValidator, DataQualityChecker |
| 9 | DAGVisualizer | Execution plan display (ASCII art, HTML/SVG) |
| 10 | MaintenanceScheduler | VACUUM, OPTIMIZE, full rebuild scheduling |
| 11 | BellerophonOrchestrator | The main orchestration engine (DAG loop, ThreadPoolExecutor) |
| 12 | Materialise DataFrame | Core write function (all load modes, encryption, CSV, logging) |
| 13 | OOM Retry Wrapper | `resilient_materialise_table` |
| 14 | Fast Mode | Bulk write path + weight-sorted orchestration |
| 15 | Partition Materialisation | `replaceWhere` partition writes + batched log buffer |
| 16 | BelleNamespace & Module Init | `belle = BelleNamespace()` facade, startup banner |

**Why a single notebook?** Databricks `%run` is the only mechanism for sharing code between notebooks without packaging as a wheel. A single `%run ./bellerophon_core` loads the entire framework into the consuming notebook's session.

---

## 3. The DAG Execution Model

### 3.1 How Dependencies Work

Each table in `TABLES_CONFIG` declares its dependencies:

```python
"db.fact_order": {
    "dependencies": ["db.dim_customer", "db.dim_product"],
    ...
}
```

Belle performs a topological sort to determine **execution stages**:

```
Stage 1: [dim_customer, dim_product, dim_date]     ← no dependencies, run in parallel
Stage 2: [fact_order, fact_payment]                 ← depend on Stage 1, run in parallel
Stage 3: [fact_order_monthly_view]                  ← depends on Stage 2
```

### 3.2 Parallel Execution Within Stages

All tables within a stage have their dependencies satisfied, so they execute in parallel via `ThreadPoolExecutor(max_workers=N)`. The worker count is auto-detected from cluster size (capped at 16) or overridden manually.

### 3.3 Dependency Validation

Before execution, Belle validates:
1. All declared dependencies exist in `TABLES_CONFIG`
2. No circular dependencies (DFS cycle detection)
3. Dependencies in the current `tables_to_run` subset (others are warned and ignored)

### 3.4 Failed Dependency Handling

If a table fails, all downstream dependents are automatically **skipped** (not failed). The failure is contained to the affected branch of the DAG.

---

## 4. Storage Architecture

### 4.1 Dual Storage Model

Belle supports two storage paradigms simultaneously:

| Mode | target_database format | Table type | Location |
| --- | --- | --- | --- |
| Unity Catalog (managed) | `catalog.schema` | Managed table | UC-managed (automatic) |
| Hive Metastore (external) | `database_name` | External table | Explicit blob path |

Belle auto-detects which mode to use based on the dot count in `target_database`:
* 2+ dots → Unity Catalog (e.g., `my_catalog.my_schema`)
* 1 dot or no dots → Hive Metastore (e.g., `sales_semantic`)

### 4.2 Path Construction (Hive/External)

For external tables, Belle constructs storage paths:

```
{BLOB_ROOT}/{target_database}/{subpipeline?}/{DATA_FOLDER}/{table_name}/
```

Example:
```
/mnt/internal/enhanced/sales_semantic/data/sales_semantic.dim_customer/
```

### 4.3 Log Table Location

**Important:** The log table (`bellerophon_log_table`) is stored alongside the data tables in the same database/path. This means:

* Each target database has its own log table
* Log data lives on the same storage tier as the tables it describes
* In interactive mode (`_dev` databases), logs are NOT persisted (display only)
* In production, logs accumulate in the production database's log table

**Why not a centralised log table?** Because Belle can orchestrate tables across multiple databases in the same notebook. Each database owns its own audit trail, and permissions/access follow the same grants as the data itself.

**Caution:** If you have multiple Belle instances writing to the SAME target database from different notebooks running concurrently, the log table uses `mode("append")` with `mergeSchema=True` to handle parallel writers safely. However, avoid concurrent `force_full_rebuild` operations on the same database — the log table is protected from rebuild drops, but data tables are not.

---

## 5. Interactive vs Production Mode

### 5.1 How Mode is Detected

Belle uses three signals to determine if it is running in production:

1. **Service account prefix** — If `current_user()` starts with `svc_aas` → Production
2. **Job context** — If `spark.databricks.job.id` is set → Production
3. **Email pattern** — If username is not a valid email → Production

Any ONE of these triggers production mode. If all fail, mode defaults to Interactive.

### 5.2 The `_dev` Database Pattern: Dev/Prod on the Same Environment

This is one of Belle's most important architectural decisions for daily work:

* **Interactive runs** write to `_dev` suffix databases (e.g., `sales_semantic_dev`)
* **Production runs** write to the real databases (e.g., `sales_semantic`)
* **Both read from the same source data** — your dev results are computed from real, current data

**Why this matters:**

1. **Production is secure** — interactive users physically cannot write to production tables. There is no "are you sure?" prompt because the pathway doesn't exist.
2. **No separate dev environment needed** — dev and prod coexist on the same workspace, same cluster, same source tables. No data drift between environments.
3. **Zero code changes between dev and prod** — the same notebook, same config, same logic. Belle handles the routing.
4. **Each developer is isolated** — `_dev` databases use managed tables in the warehouse directory; they are cheap and disposable.
5. **Full pipeline testing is safe** — you can run 29 tables through the orchestrator interactively and inspect every one without risk.

### 5.3 Behaviour Differences

| Aspect | Interactive | Production |
| --- | --- | --- |
| Target databases | `*_dev` suffix (managed tables) | Real databases (external/managed per config) |
| Storage tier | Warehouse directory (cheap, disposable) | Blob / UC-managed (durable) |
| CSV export | Suppressed (no blob write in interactive) | Active (blob storage) |
| Log table persistence | Suppressed (display only) | Written to Delta |
| Progress output | Full (banners, samples, DAG) | Configurable via VERBOSITY |
| Force rebuild safety | Manual only | Can be scheduled |
| Risk to production | None — physically separate tables | Writes to live tables |

### 5.4 Why This Matters

You can run the EXACT same notebook code interactively and in production. The only difference is the `target_database` value in your config (e.g., `sales_semantic_dev` vs `sales_semantic`). Belle's mode detection ensures CSV exports and log persistence only happen in production, even if you forget to gate them yourself.

**This is not just convenience — it is a security model.** Production data integrity is guaranteed by architecture, not by developer discipline.

---

## 6. Namespace Isolation & Pollution Prevention

### 6.1 The Problem

When you `%run` a notebook, ALL of its top-level names are injected into the calling notebook's global namespace. If Belle defined `F = functions`, `col = functions.col`, etc. at module level, they would collide with any same-named variables in the consuming notebook.

### 6.2 Design Decisions to Prevent Pollution

1. **Local imports inside functions** — `pyspark.sql.functions`, `pyspark.sql.types`, `delta.tables`, and `pandas` are imported LOCALLY inside each function that uses them. This prevents `F`, `col`, `lit`, etc. from leaking into the caller's namespace.

2. **BelleNamespace facade** — All public API is accessed via a single `belle` object:
   ```python
   belle.Orchestrator
   belle.Config
   belle.OutputRegistry
   ```
   This means only ONE name (`belle`) is added to the caller's namespace.

3. **No module-level constant extraction** — Configuration is accessed via `BellerophonConfig.X`, not extracted into top-level variables.

4. **Underscore-prefixed internals** — Helper functions and internal state use `_` prefixes (e.g., `_PartitionLogBuffer`, `_strip_emoji`).

5. **Class-level state** — The OutputRegistry, Tracer, and Config are all class-level (not instance-level), avoiding the need for global variables.

### 6.3 What Users Must Be Mindful Of

Despite these precautions, `%run` still injects these names into your namespace:

* `belle` — The namespace facade
* `BellerophonConfig`, `BellerophonOrchestrator`, `BellerophonUtils`, `BellerophonLogger`, etc. — All class names
* `bellerophon_materialise_dataframe`, `bellerophon_materialise_dataframe_fast`, etc. — Function names
* `BELLEROPHON_VERSION`, `BELLEROPHON_ASCII_BANNER` — Module constants
* `belle_print`, `belle_banner`, `belle_emoji` — Helper functions
* `ensure_table_ready`, `ensure_all_tables_ready` — Readiness functions
* `BellerophonTracer` — Tracer class
* `resilient_materialise_table` — OOM wrapper

**Rules to follow:**
* Do NOT name your own variables `belle`, `BellerophonConfig`, etc.
* Do NOT define a function called `belle_print` or `ensure_table_ready` in your notebook
* If you use `from pyspark.sql import functions as F` in your own cells, that is fine — Belle does not define `F` at module level
* The `spark` variable is shared — Belle uses the same SparkSession as your notebook

### 6.4 The OutputRegistry is Global (By Design)

The `OutputRegistry` is a class-level dictionary. This is intentional — it allows DataFrames to be registered in early cells and consumed by the orchestrator in later cells. However, this means:

* If you run multiple Belle instances in the same notebook (see Section 7), they share the same registry
* If you `%run` Belle in multiple notebooks that share a cluster, each notebook has its own Python interpreter — no collision
* If you re-run cells that call `set_output()`, the registry is updated (no duplicate-key errors)

---

## 7. Running Multiple Belle Instances

### 7.1 Multiple Orchestrators in One Notebook

You CAN run multiple `belle.Orchestrator` instances in a single notebook. This is useful when you have tables targeting different databases:

```python
# Instance 1: Staging tables
staging_config = { ... }  # target_database = "my_staging"
orchestrator_staging = belle.Orchestrator(staging_config)
orchestrator_staging.run()

# Instance 2: Semantic tables (may depend on staging)
semantic_config = { ... }  # target_database = "my_semantic"
orchestrator_semantic = belle.Orchestrator(semantic_config)
orchestrator_semantic.run()
```

**Requirements:**
* Each instance operates on its own `TABLES_CONFIG` dictionary
* The OutputRegistry is shared — register ALL DataFrames before running ANY orchestrator
* Dependencies ONLY work within a single orchestrator instance (cross-instance dependencies require sequential execution)
* Each instance writes its logs to its own target database's log table

### 7.2 Parallel Belle Instances (Separate Notebooks)

For maximum parallelism, you can trigger multiple notebooks in parallel via `dbutils.notebook.run()` or Databricks Jobs:

```python
# Parent orchestrator notebook
from concurrent.futures import ThreadPoolExecutor

def run_country(country):
    return dbutils.notebook.run(
        f"./pipelines/{country}_pipeline",
        timeout_seconds=7200,
        arguments={"country": country}
    )

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(run_country, c): c 
               for c in ["germany", "france", "belgium", "cee", "uk"]}
```

Each child notebook loads Belle independently (`%run ../bellerophon_core`). Since each runs in its own Python interpreter, there is ZERO risk of namespace collision or registry contamination between them.

**This pattern is used by the Sales Pipeline** (country_runner) to process 5 countries in parallel.

### 7.3 Constraints for Parallel Execution

| Constraint | Reason |
| --- | --- |
| Different target tables | Two parallel writes to the SAME table will corrupt it |
| Same cluster OR separate clusters | Spark jobs from parallel notebooks share cluster resources |
| Independent configs | Each notebook has its own TABLES_CONFIG and OutputRegistry |
| Log table safety | Parallel appends to the same log table are safe (Delta ACID) |

---

## 8. Memory Management & Checkpointing

### 8.1 Auto-Persist/Unpersist

Belle tracks how many downstream tables depend on each table's DataFrame (usage count). When a DataFrame is needed by 2+ downstream tables:

1. **Persist** before the first consumer's stage
2. **Unpersist** after the last consumer completes

Storage level is chosen based on size:
* Large DataFrames (>5M rows or >30 columns) → `MEMORY_AND_DISK`
* Smaller DataFrames → `MEMORY_ONLY` (via `.persist()`)

### 8.2 Why Persist Matters (Breaking Lineage)

In Spark, DataFrames are lazy. Without persistence, every action re-executes the full lineage back to the source. For complex pipelines this creates **huge execution plans** that:
* Consume excessive driver memory (plan compilation)
* Cause long GC pauses
* Make Spark UI unreadable
* Risk StackOverflowErrors in deeply nested plans

Persisting (or checkpointing) acts as a **lineage breaker** — it materialises the DataFrame to memory/disk and creates a new, short lineage from that point forward.

**Belle manages this automatically.** If a DataFrame is consumed by multiple downstream tables, Belle persists it before the first consumer and unpersists after the last. This prevents the lineage explosion that occurs when the same wide DataFrame is evaluated N times independently.

### 8.3 Manual Persist (Specifying Tables)

You can explicitly control which DataFrames Belle persists:

```python
# Force persist via persist_columns (also acts as column pruning)
"db.large_staging_table": {
    ...
    "persist_columns": ["id", "date_key", "amount"],  # Persist only these columns for downstream joins
}
```

Or persist your DataFrame yourself before registering:
```python
df_heavy = build_heavy_query()
df_heavy.persist(StorageLevel.MEMORY_AND_DISK)
belle.OutputRegistry.set_output("db_heavy_table", df_heavy)

# Tell Belle not to double-persist
"db.heavy_table": { ..., "disable_auto_persist": True }
```

**When to manually persist:**
* A DataFrame is built from many joins/unions and used by 3+ downstream tables
* You see Spark plans exceeding 1000+ nodes in the Spark UI
* Stage times are dominated by re-computation rather than shuffle

### 8.4 Checkpointing (Breaking Very Deep Lineage)

For extremely deep lineage chains (e.g., iterative computations, 20+ sequential transformations), `.persist()` is not enough — the plan is still compiled even if data is cached. Use Spark **checkpointing** to truncate the plan entirely:

```python
sc.setCheckpointDir("/tmp/belle_checkpoints")

df_deep = long_chain_of_transformations()
df_deep = df_deep.checkpoint()  # Writes to disk, truncates lineage

belle.OutputRegistry.set_output("db_deep_table", df_deep)
```

**Checkpoint vs Persist:**

| Aspect | `.persist()` | `.checkpoint()` |
| --- | --- | --- |
| Lineage | Preserved (still compiled) | Truncated (fresh DAG) |
| Storage | Memory and/or disk (configurable) | Disk only (reliable) |
| Use case | Multi-consumer reuse | Deep plan complexity |
| Recovery | Can recompute from lineage | Cannot (data on disk) |

**Rule of thumb:** If your DataFrame is the result of <10 transformations and consumed by 2-3 tables, Belle's auto-persist is sufficient. If it's the result of a 50-step pipeline or recursive computation, checkpoint it before registering.

### 8.5 Persist Columns (Column Pruning)

If a table config includes `persist_columns`, Belle projects the DataFrame to only those columns BEFORE persisting. The full DataFrame is preserved internally for the actual write, but downstream consumers (who may only need a subset for joins) benefit from the reduced memory footprint.

### 8.6 Disabling Auto-Persist

Set `disable_auto_persist: True` in a table's config to opt out. Useful for DataFrames that are already cached or checkpointed by your own code.

---

## 9. The ADF Deployment Pattern

### 9.1 Why `.py` for ADF

When deploying a notebook to Azure Data Factory (ADF), the notebook is referenced by its workspace path. However, ADF's Databricks linked service can also execute Python scripts directly. The key difference:

* **Interactive / Databricks Jobs:** Reference the notebook path as-is (e.g., `/Users/.../my_pipeline`)
* **ADF Databricks Activity:** If using the "Python" activity type, you reference a `.py` file in DBFS or workspace. If using the "Notebook" activity type, you reference the notebook path.

The `%run` command in the consuming notebook does NOT need to change. What changes is how ADF references the *consuming* notebook:

```
Notebook Activity → path: /Users/.../my_pipeline        (works as-is)
Python Activity   → path: /Users/.../my_pipeline.py     (needs .py extension)
```

**Belle itself** (`bellerophon_core`) is never referenced directly by ADF. It is always loaded via `%run` from within the consuming notebook. The `.py` consideration only applies to the consuming notebook's ADF activity configuration.

### 9.2 Service Account Detection

When ADF triggers a notebook via a Databricks linked service, it runs under a service principal. Belle detects this via:
* Username starting with `svc_aas` (the configured `SERVICE_ACCOUNT_PREFIX`)
* OR the presence of `spark.databricks.job.id` in the Spark conf

This automatically switches Belle to production mode (CSV exports enabled, logs persisted).

---

## 10. Schema Evolution vs Force Rebuild

### 10.1 When Schema Evolution Handles It (No Purge Needed)

Belle uses `mergeSchema=True` / `overwriteSchema=True` depending on the write mode. The following changes are handled WITHOUT needing to drop and rebuild:

| Change | Handled by | Notes |
| --- | --- | --- |
| Adding new columns | `mergeSchema=True` | New columns appear as NULL in existing rows |
| Widening types (int→long) | `mergeSchema=True` | Delta handles type promotion |
| Renaming a column (full mode) | `overwriteSchema=True` | Full mode replaces the entire table |
| Adding partitions (full mode) | `overwriteSchema=True` | Full mode recreates with new schema |

### 10.2 When You MUST Purge (Force Rebuild Required)

| Change | Why purge is needed | How to do it |
| --- | --- | --- |
| Changing partition columns (non-full mode) | Delta cannot re-partition in place | Set `force_full_rebuild: True` in table config, or drop manually |
| Removing columns (merge/insert mode) | Delta merge doesn't drop columns | Set `force_full_rebuild: True` or `DROP TABLE` |
| Changing column types downward (long→int) | Delta rejects narrowing casts | Drop table, recreate |
| Switching load_mode from merge→full | Partition mismatch may exist | Belle auto-detects partition mismatch and drops automatically |
| Changing merge_keys on an existing merge table | Old keys embedded in table history | Drop table (Belle's pre-validation will warn) |

### 10.3 The `force_full_rebuild` Mechanism

Three levels of control:

1. **Per-table** (highest priority): `"force_full_rebuild": True` in a single table's config
2. **Global** (constructor): `belle.Orchestrator(config, global_force_rebuild=True)`
3. **Run-level** (deprecated): `orchestrator.run(force_full_refresh=True)`

When triggered, `ensure_table_ready()` drops the table before any writes. The log table is ALWAYS protected from force rebuild drops.

### 10.4 Automatic Partition Mismatch Recovery

If Belle detects that a table's actual partition columns differ from the config's `partition_by`, it automatically drops and recreates the table. No manual intervention required.

---

## 11. Error Handling Strategy

### 11.1 Error Code System

Belle uses structured error codes (BELLE-000 through BELLE-999) for categorised error reporting:

| Range | Category |
| --- | --- |
| BELLE-000 | Success |
| BELLE-001 to 005 | Configuration errors |
| BELLE-010 to 012 | Data errors |
| BELLE-020 to 023 | Execution errors |
| BELLE-030 to 032 | Resource errors (OOM, timeout, cluster) |
| BELLE-040 to 041 | Permission errors |
| BELLE-999 | Unknown |

### 11.2 Failure Containment

* A table failure does NOT abort the entire run
* Failed tables are recorded; their dependents are skipped
* The orchestrator continues with unaffected branches
* The final summary shows success/failure counts
* Error details are logged to the log table (production mode)

### 11.3 Retry Behaviour

* `MAX_MATERIALISE_RETRIES` (default: 2) — retries transient failures
* `BellerophonRetryHandler` — exponential backoff with configurable base/max delay
* Per-table override via `max_retries` in table config

---

## 12. Design Trade-offs & Known Limitations

| Decision | Trade-off | Rationale |
| --- | --- | --- |
| Single notebook, not a wheel | Cannot unit test in isolation; large cells | `%run` compatibility; no CI/CD packaging needed |
| Class-level state (Config, Registry) | Shared across all code in session | Simplicity; no dependency injection needed |
| ThreadPoolExecutor, not Spark parallelism | Limited by driver memory/threads | Table writes are driver-orchestrated; Spark parallelism is within each write |
| Local imports inside functions | Slight import overhead per call | Namespace isolation for `%run` consumers |
| Log table per database | Must query multiple tables for cross-pipeline audit | Permissions follow data; no cross-database dependency |
| Auto-detect UC vs Hive from dot count | Fragile if database names contain dots | Pragmatic; no real databases use dots in Hive names |

---

## 13. Security Model

### 13.1 Encryption

Belle encrypts DataFrame columns in-memory before writing to Delta. The encrypted data is what lands on disk. Two strategies:

* **Per-column** (default, v1.2.15+): Each column cast to string then AES-GCM encrypted individually
* **Blob** (legacy): All payload columns serialised to JSON, then single encrypted blob

Keys are passed in table config. Belle does NOT manage key rotation — that is the responsibility of the consuming pipeline.

### 13.2 Access Control

Belle respects whatever permissions are in place on the target database/catalog. It does not elevate privileges. If the executing user/service principal lacks write access to the target, the materialisation fails with BELLE-040/041.

---

## 14. Relationship to Consuming Pipelines

```
┌──────────────────────────────────────────────────────────────────────┐
│  Sales Pipeline         │  Telephony Pipeline    │  CRM Staging     │
│  (29 tables, 5 countries)│  (31 tables)           │  (N tables)      │
├──────────────────────────┼────────────────────────┼──────────────────┤
│  Own ETL logic           │  Own ETL logic         │  Own ETL logic   │
│  Own TABLES_CONFIG       │  Own TABLES_CONFIG     │  Own TABLES_CONFIG│
│  Own encryption keys     │  Own encryption keys   │  (no encryption) │
│  Own scheduling (ADF)    │  Own scheduling (Jobs) │  Own scheduling  │
└──────────┬───────────────┴───────────┬────────────┴────────┬─────────┘
           │                           │                     │
           └───────────────────────────┼─────────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────┐
                    │         BELLEROPHON CORE             │
                    │   (loaded via %run in each pipeline) │
                    └──────────────────────────────────────┘
```

Belle is a **library**, not a service. Each pipeline loads its own copy into memory. There is no shared state between pipelines unless they deliberately share a cluster and target database.

---

*Last updated: June 2026*

---

## Architectural Position — Where Belle Sits

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  EXTERNAL ORCHESTRATION (ADF / Databricks Jobs / Airflow)                    │
│  — Scheduling, triggers, inter-notebook dependencies                         │
└──────────────────────────────────────────────┬───────────────────────────────┘
                                               │
                                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  BELLE (inside the notebook)                                                 │
│  — Intra-notebook DAG ordering                                               │
│  — Parallel table materialisation                                            │
│  — Config-driven write modes (merge, refresh, full, partition)               │
│  — Per-table retry, validation, logging                                      │
│  — Inline encryption                                                         │
│  — Static validation (belle.Validator.validate)                              │
└──────────────────────────────────────────────┬───────────────────────────────┘
                                               │
                                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  DELTA LAKE + UNITY CATALOG                                                  │
│  — ACID transactions, time travel, governance                                │
└──────────────────────────────────────────────────────────────────────────────┘
```

Belle operates at the **middle layer**: above the storage engine, below the external scheduler. This is precisely the gap that causes most pipeline bugs, inconsistencies, and operational blind spots in teams that write Delta tables directly.

### Why This Layer Matters

Without a write orchestrator:
- Developers write ad-hoc retry logic (or none at all)
- Table write order is implicit, fragile, and invisible
- Logging is absent or inconsistent — failures are noticed by downstream consumers, not by the pipeline itself
- Cost is invisible at the table level — billing shows cluster cost, not what drove it
- Schema changes pass silently — consumers break downstream hours later

Belle eliminates this entire class of operational risk with zero changes to how you schedule or compute.
