# User Guide — Standard Materialisation

**Audience:** Data engineers and analysts building Belle-orchestrated pipelines.  
**Prerequisites:** Read `01_README.md` and `04_Configuration_Reference.md`.

This guide shows you how to implement each load mode, register DataFrames correctly, structure your TABLES_CONFIG, and handle common scenarios.

---

## 0. Should You Use Belle for This?

**Use Belle when:**
* You have 2+ tables with any dependency or sequencing
* You need logging, retry, or error tracking
* You want dev/prod separation (`_dev` databases) out of the box
* You need encryption, partition management, or schema validation
* You value automatic parallelism without managing threads yourself
* You want a self-documenting config that describes the pipeline

**Skip Belle (write Delta directly) when:**
* You have a single, isolated query writing one small table
* The table has no dependencies, no encryption, no partitioning
* You don't need logging, retry, or maintenance
* You are writing a quick ad-hoc export that won't become a pipeline

**The trade-off:** For that single-table case, Belle adds ~1-2 seconds of config validation and orchestrator setup overhead. You lose the safety net (logging, row-count validation, schema checks, error codes, progress tracking), but you avoid the framework.

**The moment you daisy-chain 3+ writes, the equation flips entirely.** Belle's parallel DAG execution, automatic retry, structured logging, and memory management provide quality, performance, and resilience far beyond what sequential raw `saveAsTable()` calls can offer. The cost of one failed run that Belle auto-recovers from (and a manual approach doesn't) exceeds the framework overhead of a thousand successful runs.

---

## 0.1 Interactive Mode: Safe Development

When you run your notebook interactively, Belle automatically:
* Writes to `_dev` databases (e.g., `sales_semantic_dev` instead of `sales_semantic`)
* Suppresses CSV export (no blob write access in interactive mode)
* Shows logs on screen but does NOT persist them
* Uses managed tables (warehouse directory) — cheap, disposable, and isolated

**This means you can run the full pipeline during development without any risk to production.** Your `_dev` tables are your personal sandbox — same source data, same transformations, complete isolation from production. Production security is guaranteed by architecture, not developer discipline.

To verify mode detection:
```python
print(f"Interactive mode: {belle.Utils.is_interactive_notebook()}")
```

---

## 1. The Complete Workflow

Every Belle pipeline follows this sequence:

```python
# ──────────────────────────────────────────────────────────────
# STEP 1: Load Belle
# ──────────────────────────────────────────────────────────────
%run ./bellerophon_core

# ──────────────────────────────────────────────────────────────
# STEP 2: Build DataFrames (your ETL logic)
# ──────────────────────────────────────────────────────────────
df_dim_customer = spark.sql("SELECT ...")
df_fact_order = spark.sql("SELECT ...")

# ──────────────────────────────────────────────────────────────
# STEP 3: Register in OutputRegistry
# ──────────────────────────────────────────────────────────────
belle.OutputRegistry.set_output("my_db_dim_customer", df_dim_customer)
belle.OutputRegistry.set_output("my_db_fact_order", df_fact_order)

# ──────────────────────────────────────────────────────────────
# STEP 4: Define TABLES_CONFIG
# ──────────────────────────────────────────────────────────────
TABLES_CONFIG = { ... }

# ──────────────────────────────────────────────────────────────
# STEP 5: Orchestrate
# ──────────────────────────────────────────────────────────────
orchestrator = belle.Orchestrator(TABLES_CONFIG)
results = orchestrator.run(show_dag=True)
```

---

## 2. Registering DataFrames (OutputRegistry)

### 2.1 The Key Format

The OutputRegistry key MUST match this pattern:
```
{target_database}_{result_table_name}
```

With dots replaced by underscores for UC namespaces:

| target_database | result_table_name | Registry key |
| --- | --- | --- |
| `sales_semantic` | `dim_customer` | `sales_semantic_dim_customer` |
| `my_catalog.my_schema` | `fact_order` | `my_catalog.my_schema_fact_order` |
| `operations_reporting` | `dim_date` | `operations_reporting_dim_date` |

### 2.2 Registration Rules

```python
# Register a DataFrame
belle.OutputRegistry.set_output("my_db_my_table", df)

# Re-registering is safe (overwrites silently)
belle.OutputRegistry.set_output("my_db_my_table", df_v2)

# Check what's registered
belle.OutputRegistry.get_all_keys()

# Retrieve a specific DataFrame
df = belle.OutputRegistry.get_output("my_db_my_table")

# Clear all (use with caution)
belle.OutputRegistry.clear_outputs()

# Health check (verify expected keys exist)
health = belle.OutputRegistry.check_health(["my_db_table1", "my_db_table2"])
print(health)  # {'healthy': True, 'found': 2, 'missing': 0, ...}
```

### 2.3 What Happens If a DataFrame Is NOT Registered

If a table's DataFrame is not in the OutputRegistry when the orchestrator runs:
* The table is **skipped** (not failed)
* A message is printed: "table skipped (no DataFrame registered — retained as-is)"
* The existing table in the target database is left untouched
* Downstream dependencies continue if THEY have DataFrames registered

This is by design — it allows you to run a subset of tables without removing entries from TABLES_CONFIG.

### 2.4 Dimension Fallback Loading

If a table is referenced as a dependency but has no DataFrame registered, Belle can optionally load it from the existing table:
```python
df = belle.Utils.try_load_from_table("my_db", "dim_customer")
```

This is useful for dimension tables that don't change on every run.

---

## 3. Implementing Each Load Mode

### 3.1 Full Mode (`load_mode: "full"`)

**What it does:** DROP TABLE IF EXISTS, then CREATE TABLE from DataFrame.

**When to use:**
* Dimension tables that are small enough to rebuild
* Any table where you want a clean slate every run
* Initial development/prototyping

**Requirements:**
* None beyond the 3 required keys

**Example:**
```python
# DataFrame
df_dim_date = spark.sql("""
    SELECT date_key, calendar_date, day_name, month_name, year
    FROM raw.calendar
    WHERE calendar_date BETWEEN '2015-01-01' AND '2030-12-31'
""")

# Register
belle.OutputRegistry.set_output("my_db_dim_date", df_dim_date)

# Config
"my_db.dim_date": {
    "target_database": "my_db",
    "result_table_name": "dim_date",
    "load_mode": "full",
    "dependencies": [],
    "partition_by": [],
    "export_csv": False,
}
```

**Schema change behaviour:** Full mode uses `overwriteSchema=True`. Column additions, removals, renames, and type changes are ALL handled automatically. No purge needed.

---

### 3.2 Insert Mode (`load_mode: "insert"`)

**What it does:** Append rows to existing table (no deduplication).

**When to use:**
* Append-only event logs
* Audit trails
* Any table where rows accumulate and are never updated

**Requirements:**
* Table must already exist (or be created on first run via a conditional in your code)

**Example:**
```python
df_new_events = spark.sql("""
    SELECT event_id, event_type, event_timestamp, payload
    FROM raw.events
    WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM my_db.event_log)
""")

belle.OutputRegistry.set_output("my_db_event_log", df_new_events)

"my_db.event_log": {
    "target_database": "my_db",
    "result_table_name": "event_log",
    "load_mode": "insert",
    "dependencies": [],
    "partition_by": ["event_date"],
    "monitored_date_column": "event_timestamp",
}
```

**Schema change behaviour:** Uses `mergeSchema=True`. New columns are added. Removed/renamed columns require a manual DROP or `force_full_rebuild`.

---

### 3.3 Merge Mode (`load_mode: "merge"`)

**What it does:** Delta MERGE INTO (upsert). Matches on `merge_keys`, updates matched rows, inserts unmatched rows.

**When to use:**
* Slowly changing dimensions (SCD Type 1)
* Fact tables with late-arriving corrections
* Any table needing idempotent upserts

**Requirements:**
* `merge_keys` — MUST be specified (list of column names)
* Table MUST already exist (create with `full` mode first, then switch to `merge`)
* Source DataFrame merge keys MUST be unique (Belle validates this by default)

**Example:**
```python
df_customer_updates = spark.sql("""
    SELECT customer_id, name, email, phone, last_updated
    FROM raw.customer_feed
    WHERE last_updated >= current_date() - INTERVAL 1 DAY
""")

belle.OutputRegistry.set_output("my_db_dim_customer", df_customer_updates)

"my_db.dim_customer": {
    "target_database": "my_db",
    "result_table_name": "dim_customer",
    "load_mode": "merge",
    "merge_keys": ["customer_id"],
    "dependencies": [],
    "partition_by": [],
}
```

**Optional: Limit update columns:**
```python
"merge_update_columns": ["name", "email", "phone", "last_updated"],
# Only these columns are updated on match. Omit to update ALL non-key columns.
```

**Optional: Auto-deduplicate source:**
```python
"merge_auto_deduplicate": True,
# If source has duplicate merge keys, keep first occurrence (instead of failing)
```

**Schema change behaviour:** Schema evolution applies to new columns. Removing columns requires a DROP or rebuild.

---

### 3.4 Refresh N Days Mode (`load_mode: "refresh_n_days-N"`)

**What it does:** Replaces the last N days of a date-partitioned table using `replaceWhere`.

**When to use:**
* Rolling window fact tables (e.g., replace last 7 days of daily sales)
* Tables where recent data is corrected but historical data is stable
* Much faster than full rebuild for large tables

**Requirements:**
* `partition_by` — MUST be exactly ONE date-based column
* Table should already exist with matching partitioning
* The N is specified IN the load_mode string (not a separate key)

**Example:**
```python
# Build DataFrame covering the refresh window (and potentially more)
df_daily_metrics = spark.sql("""
    SELECT metric_date, region, revenue, cost, margin
    FROM raw.daily_financials
    WHERE metric_date >= current_date() - INTERVAL 30 DAY
""")

belle.OutputRegistry.set_output("my_db_fact_daily_metrics", df_daily_metrics)

"my_db.fact_daily_metrics": {
    "target_database": "my_db",
    "result_table_name": "fact_daily_metrics",
    "load_mode": "refresh_n_days-30",
    "partition_by": ["metric_date"],
    "dependencies": [],
}
```

**Schema change behaviour:** `replaceWhere` respects existing schema. Adding columns requires `mergeSchema`. Changing partition column requires DROP.

---

### 3.5 Full If Not Exists Mode (`load_mode: "full_if_not_exists"`)

**What it does:** Write the table ONLY if it does not already exist. If it exists, skip entirely.

**When to use:**
* Bootstrap/seed tables (write once, never overwrite)
* Reference data that should not change between runs
* Safety net for tables that should only be created once

**Example:**
```python
df_seed = spark.sql("SELECT * FROM reference.country_codes")

belle.OutputRegistry.set_output("my_db_ref_countries", df_seed)

"my_db.ref_countries": {
    "target_database": "my_db",
    "result_table_name": "ref_countries",
    "load_mode": "full_if_not_exists",
    "dependencies": [],
}
```

---

### 3.6 Update Mode (`load_mode: "update"`)

**What it does:** UPDATE specific columns where merge_keys match.

**When to use:**
* Targeted column updates without touching other columns
* Setting status flags, updating timestamps

**Requirements:**
* `merge_keys` — Used as WHERE condition
* `update_set` — Dictionary of column:value pairs to SET

**Example:**
```python
"my_db.dim_customer": {
    "target_database": "my_db",
    "result_table_name": "dim_customer",
    "load_mode": "update",
    "merge_keys": ["customer_id"],
    "update_set": {"status": "inactive", "deactivated_date": "current_date()"},
    "dependencies": [],
}
```

---

### 3.7 Delete Mode (`load_mode: "delete"`)

**What it does:** DELETE rows matching a condition.

**When to use:**
* GDPR purge operations
* Removing test data
* Data lifecycle management

**Requirements:**
* `merge_keys` — Used in WHERE condition
* `delete_where` — SQL WHERE clause

**Example:**
```python
"my_db.dim_customer": {
    "target_database": "my_db",
    "result_table_name": "dim_customer",
    "load_mode": "delete",
    "merge_keys": ["customer_id"],
    "delete_where": "gdpr_delete_flag = true",
    "dependencies": [],
}
```

---

## 4. Schema Changes — When to Purge and When Not To

### 4.1 Decision Matrix

| Scenario | Load Mode | Purge needed? | How |
| --- | --- | --- | --- |
| Add column to DataFrame | `full` | NO | Automatic (overwriteSchema) |
| Add column to DataFrame | `merge` | NO | Automatic (mergeSchema) |
| Add column to DataFrame | `insert` | NO | Automatic (mergeSchema) |
| Remove column from DataFrame | `full` | NO | Automatic (overwriteSchema) |
| Remove column from DataFrame | `merge`/`insert` | YES | `force_full_rebuild: True` or DROP TABLE |
| Rename column | `full` | NO | Automatic (new schema replaces old) |
| Rename column | `merge`/`insert` | YES | Drop table first |
| Change partition columns | ANY | YES (if table exists) | Belle auto-detects and drops, OR set `force_full_rebuild` |
| Widen type (int→long) | ANY | NO | Delta handles type promotion |
| Narrow type (long→int) | ANY | YES | DROP TABLE manually |
| Change merge_keys | `merge` | YES | DROP TABLE (old key structure embedded in table) |
| Switch from merge→full | N/A | NO | Full mode does DROP+CREATE anyway |
| Switch from full→merge | N/A | MAYBE | Table must exist first (run once with full, then switch) |

### 4.2 How to Force Rebuild

Three options (in order of preference):

```python
# Option 1: Per-table in config (safest, explicit)
"my_db.my_table": {
    ...
    "force_full_rebuild": True,
}

# Option 2: Global in constructor (rebuilds ALL tables)
orchestrator = belle.Orchestrator(TABLES_CONFIG, global_force_rebuild=True)

# Option 3: Manual DROP before orchestration
spark.sql("DROP TABLE IF EXISTS my_db.my_table")
```

**Important:** After the rebuild run, REMOVE `force_full_rebuild: True` from your config. Otherwise it will drop and recreate every run.

---

## 5. Dependencies and DAG Structure

### 5.1 Declaring Dependencies

Dependencies reference other TABLES_CONFIG keys:

```python
TABLES_CONFIG = {
    "db.dim_a": { "dependencies": [], ... },
    "db.dim_b": { "dependencies": [], ... },
    "db.fact_x": { "dependencies": ["db.dim_a", "db.dim_b"], ... },
    "db.summary": { "dependencies": ["db.fact_x"], ... },
}
# Execution: Stage 1 [dim_a, dim_b] → Stage 2 [fact_x] → Stage 3 [summary]
```

### 5.2 Cross-Reference Formats

Dependencies can reference by EITHER:
* Config dict key (e.g., `"db.dim_a"`)
* Fully qualified name (e.g., `"my_catalog.my_schema.dim_a"`)

Belle normalises both formats internally.

### 5.3 Dependencies Are WITHIN a Single Orchestrator

Dependencies only work within one `belle.Orchestrator.run()` call. If you need cross-orchestrator ordering, run them sequentially:

```python
# Staging must complete before semantic
orchestrator_staging = belle.Orchestrator(STAGING_CONFIG)
orchestrator_staging.run()

orchestrator_semantic = belle.Orchestrator(SEMANTIC_CONFIG)
orchestrator_semantic.run()
```

---

## 6. Running Multiple Belle Instances in One Notebook

### 6.1 Pattern: Sequential Orchestrators

```python
%run ./bellerophon_core

# Build ALL DataFrames first
df_staging_a = ...
df_staging_b = ...
df_semantic_x = ...

# Register ALL DataFrames
belle.OutputRegistry.set_output("staging_db_table_a", df_staging_a)
belle.OutputRegistry.set_output("staging_db_table_b", df_staging_b)
belle.OutputRegistry.set_output("semantic_db_table_x", df_semantic_x)

# Orchestrator 1: Staging
STAGING_CONFIG = {
    "staging_db.table_a": { "target_database": "staging_db", ... },
    "staging_db.table_b": { "target_database": "staging_db", ... },
}
orchestrator_staging = belle.Orchestrator(STAGING_CONFIG)
orchestrator_staging.run()

# Orchestrator 2: Semantic (depends on staging output)
SEMANTIC_CONFIG = {
    "semantic_db.table_x": { "target_database": "semantic_db", ... },
}
orchestrator_semantic = belle.Orchestrator(SEMANTIC_CONFIG)
orchestrator_semantic.run()
```

### 6.2 Constraints

* The OutputRegistry is shared — register ALL DataFrames before the FIRST orchestrator runs
* Each orchestrator manages its own log table (one per target_database)
* BellerophonConfig is global — if you change settings between runs, they affect both
* Use `belle.Config.temp_config(...)` if you need different settings per orchestrator

### 6.3 Pattern: Parallel Notebooks (Maximum Throughput)

See `03_Architecture_and_Design.md` Section 7.2 for the `dbutils.notebook.run()` pattern.

---

## 7. Selective Execution (Running a Subset)

You don't need to remove entries from TABLES_CONFIG to skip tables. Use `tables_to_run`:

```python
# Only run these 2 tables (and their dependency chain)
results = orchestrator.run(
    tables_to_run=["my_db.fact_order", "my_db.dim_customer"]
)
```

Alternatively, simply don't register the DataFrames for tables you want to skip — they'll be auto-skipped with a message.

---

## 8. Monitoring and Observability

### 8.1 Row Count Validation

Belle validates post-write row counts against the source DataFrame. If the difference exceeds `WRITE_ROW_COUNT_TOLERANCE` (default 1%), a warning is printed.

### 8.2 Monitored Columns

```python
"monitored_id_column": "order_id",       # Tracks MAX value (for incremental bookkeeping)
"monitored_date_column": "order_date",   # Tracks MAX date (for freshness monitoring)
```

These values are recorded in the log table for each run.

### 8.3 Interactive Sample Display

In interactive mode, Belle displays the first N rows (default 10) of each table after writing. Control with:
```python
orchestrator.run(sample_rows=5)
```

---

## 9. Common Patterns

### 9.1 Dev/Prod Database Routing

```python
# Set at the top of your notebook
TARGET_DB = "sales_semantic_dev" if belle.Utils.is_interactive_notebook() else "sales_semantic"

TABLES_CONFIG = {
    f"{TARGET_DB}.dim_customer": {
        "target_database": TARGET_DB,
        "result_table_name": "dim_customer",
        ...
    },
}
```

### 9.2 Building Config Programmatically

```python
# Generate config for multiple similar tables
TABLES_CONFIG = {}
for table_name in ["dim_a", "dim_b", "dim_c", "dim_d"]:
    TABLES_CONFIG[f"{TARGET_DB}.{table_name}"] = {
        "target_database": TARGET_DB,
        "result_table_name": table_name,
        "load_mode": "full",
        "dependencies": [],
        "partition_by": [],
        "export_csv": False,
    }
```

### 9.3 External Run ID (ADF Correlation)

```python
# Pass ADF's run ID for log correlation
results = orchestrator.run(
    external_run_id=dbutils.widgets.get("adf_run_id"),
    execution_context={"pipeline": "daily_refresh", "trigger": "scheduled"}
)
```

---

## 10. Pre-Flight Validation

Belle validates your configuration BEFORE writing anything:

1. **Required keys** — Every table has target_database, result_table_name, load_mode
2. **Dependency existence** — All referenced dependencies exist in config
3. **Cycle detection** — No circular dependencies
4. **Merge keys present** — Merge mode has merge_keys defined
5. **DataFrame existence** — All registered DataFrames checked for expected columns
6. **Partition/merge/monitored columns** — Verified against DataFrame schema
7. **Table existence** — Checked against catalog (merge/update/delete modes require existing table)
8. **Partition consistency** — Existing table partitions match config

If validation fails:
* With `fail_on_validation_errors=True` (default): Raises ValueError with actionable error messages
* With `fail_on_validation_errors=False`: Logs warnings but proceeds (use with caution)

---

## 11. Constraints & Requirements Checklist

Before running your first orchestration, verify:

- [ ] Belle loaded via `%run` (not `import`)
- [ ] All DataFrames registered with correct key format (`{target_database}_{result_table_name}`)
- [ ] TABLES_CONFIG has all 3 required keys per entry
- [ ] Merge mode tables have `merge_keys` defined
- [ ] Merge keys exist in the DataFrame columns
- [ ] Partition columns (if specified) exist in the DataFrame
- [ ] Dependencies reference valid TABLES_CONFIG keys
- [ ] No circular dependencies
- [ ] Target database accessible (correct catalog/schema permissions)
- [ ] For merge/update/delete: target table already exists
- [ ] For refresh_n_days: exactly ONE partition column specified
- [ ] For encryption: encrypt_key provided and encrypt_exclude lists partition/join columns

---

*Last updated: June 2026*
