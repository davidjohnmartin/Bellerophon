# Configuration Reference

**Audience:** All engineers working with Belle.  
**Prerequisite:** Read `01_README.md` for basic orientation.

This document covers two configuration surfaces:
1. **BellerophonConfig** — Global framework settings (class-level attributes)
2. **TABLES_CONFIG** — Per-table declarative manifest (dictionary you define)

---

## Part 1: BellerophonConfig (Global Settings)

All settings are class-level attributes on `BellerophonConfig` (aliased as `belle.Config`). Override them directly before instantiating the Orchestrator:

```python
belle.Config.VERBOSITY = 3          # Increase output
belle.Config.FEATURE_CSV_EXPORT = False  # Disable CSV
```

Or use the context manager for temporary overrides:

```python
with belle.Config.temp_config(VERBOSITY=4, FEATURE_ENCRYPTION=False):
    orchestrator.run()  # Runs with overrides, then reverts
```

Or load from environment variables:

```python
belle.Config.from_env()  # Reads BELLE_BLOB_ROOT, BELLE_LOG_TABLE, etc.
```

---

### 1.1 Feature Flags

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `FEATURE_CSV_EXPORT` | bool | `True` | Master switch for CSV export. Set `False` to suppress all CSV output. |
| `FEATURE_LOG_SCHEMA_EVOLUTION` | bool | `True` | Allow log table schema to evolve (new columns added via `mergeSchema`). |
| `FEATURE_ENCRYPTION` | bool | `True` | Master switch for inline encryption. Set `False` to skip all encryption. |

---

### 1.2 Verbosity & Display

| Attribute | Type | Default | Valid Values | Description |
| --- | --- | --- | --- | --- |
| `VERBOSITY` | int | `2` | 0–4 | Output level. 0=SILENT, 1=MINIMAL, 2=NORMAL, 3=VERBOSE, 4=DEBUG |
| `ASCII_ART_ENABLED` | bool | `True` | | Show Pegasus banner and box-drawing art |
| `EMOJI_ENABLED` | bool | `True` | | Use emoji in output (set False for clean logs) |

Named constants for readability: `belle.Config.SILENT`, `belle.Config.MINIMAL`, `belle.Config.NORMAL`, `belle.Config.VERBOSE`, `belle.Config.DEBUG`.

---

### 1.3 Storage Paths

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `BLOB_ROOT` | str | `"/mnt/internal/enhanced"` | Root mount path for external Delta tables. MIGRATION NOTE: update for new platform. |
| `BLOB_ROOT_BASE` | str | `""` | If set, used as prefix with layer suffix appended (e.g., `"/mnt/datalake"` + `"_gold"`). If empty, `BLOB_ROOT` is used directly. |
| `CSV_TEMP_FOLDER` | str | `"_tmp_csv_cubes"` | Subfolder name for CSV exports on blob |
| `DATA_FOLDER` | str | `"data"` | Standard data subfolder name in the blob hierarchy |

**Path construction formula:**
```
{BLOB_ROOT}/{target_database}/{subpipeline?}/{DATA_FOLDER}/{table_name}/
```

---

### 1.4 Encryption

| Attribute | Type | Default | Valid Values | Description |
| --- | --- | --- | --- | --- |
| `FEATURE_ENCRYPTION` | bool | `True` | | Global kill switch |
| `ENCRYPTION_MODE` | str | `"GCM"` | `"GCM"`, `"CBC"` | AES mode. GCM provides authenticated encryption. |
| `ENCRYPTION_STRATEGY` | str | `"per_column"` | `"per_column"`, `"blob"` | Per-column encrypts each column individually. Blob serialises all to JSON then encrypts as one payload. |

---

### 1.5 Table Naming

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `LOG_TABLE_NAME` | str | `"bellerophon_log_table"` | Name of the execution log table (created per database) |
| `TEST_MODE_SUFFIX` | str | `"_bellerophon_test"` | Suffix appended to table names in test mode. Overridden with instance-specific ID when `test_mode=True`. |

---

### 1.6 Service Account Detection

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `SERVICE_ACCOUNT_PREFIX` | str | `"svc_aas"` | Username prefix that triggers production mode. MIGRATION NOTE: update for new platform service principals. |

---

### 1.7 CSV Export Settings

Only active when `FEATURE_CSV_EXPORT = True` AND running in production mode.

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `CSV_DELIMITER` | str | `";"` | Column separator |
| `CSV_ENCODING` | str | `"utf-8"` | File encoding |
| `CSV_QUOTE` | str | `""` | Quote character (empty = no quoting) |
| `CSV_QUOTE_ALL` | str | `"false"` | Quote all fields |
| `CSV_ESCAPE_QUOTES` | str | `"false"` | Escape quote characters |
| `CSV_LINE_SEP` | str | `"\n"` | Line separator |

---

### 1.8 Performance Thresholds

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `PERSIST_ROW_THRESHOLD` | int | `5,000,000` | Auto-persist DataFrames above this row count |
| `PERSIST_COL_THRESHOLD` | int | `30` | Auto-persist DataFrames above this column count |
| `DEFAULT_MAX_WORKERS` | int | `4` | Default thread pool size (auto-detected from cluster if not set) |
| `DEFAULT_SAMPLE_ROWS` | int | `10` | Rows to display in interactive sample output |
| `DEFAULT_JOB_QUEUE_LIMIT` | int | `10` | Spark job queue backpressure limit |
| `FAST_MODE_HEAVY_COL_THRESHOLD` | int | `20` | Tables with >N encrypted columns classified as "heavy" |
| `FAST_MODE_HEAVY_WORKERS_RATIO` | int | `0` | Worker ratio for heavy tables. 0 = sequential. |

---

### 1.9 Retry Configuration

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `DEFAULT_MAX_RETRIES` | int | `3` | Max retries for RetryHandler |
| `DEFAULT_BASE_DELAY` | float | `2.0` | Base delay (seconds) for exponential backoff |
| `DEFAULT_MAX_DELAY` | float | `60.0` | Maximum delay cap (seconds) |
| `MAX_MATERIALISE_RETRIES` | int | `2` | Retries for transient materialisation failures |

> **⚠️ KB-001 (Open):** Setting `MAX_MATERIALISE_RETRIES=0` prevents per-table inner retries but does NOT stop the Orchestrator's stage-level re-scheduling loop. Non-transient errors (`ValueError` from schema drift or config validation) still cause infinite stages. See `09_Troubleshooting_Guide.md` section 7.1.

---

### 1.10 Logging & Validation

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `PROGRESS_BAR_WIDTH` | int | `50` | Width of progress bar (characters) |
| `LOG_SCHEMA_JSON_MAX_LENGTH` | int | `4000` | Truncate schema_json in log table (0=unlimited) |
| `WRITE_ROW_COUNT_TOLERANCE` | float | `0.01` | Warn if source vs target row diff exceeds this fraction (1%) |
| `WRITE_ROW_COUNT_VALIDATION` | bool | `True` | Enable post-write row count validation |
| `LOG_RETENTION_DAYS` | int | `90` | Auto-cleanup logs older than N days (0=disabled). **KB-002:** planned change to `730` (2 years). |
| `LOG_STRIP_EMOJI` | bool | `True` | Strip emoji from log table values |
| `SCHEMA_DRIFT_ACTION` | str | `"fail"` | Action on schema drift: `"warn"`, `"fail"`, or `"ignore"`. **⚠️** Due to KB-001, `"fail"` triggers infinite retries via Orchestrator. Use `temp_config(SCHEMA_DRIFT_ACTION="ignore")` for intentional schema changes. |

---

### 1.11 Merge Validation

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `MERGE_VALIDATE_SOURCE_KEYS` | bool | `True` | Check source DataFrame for duplicate merge keys before merge |
| `MERGE_VALIDATE_TARGET_KEYS` | bool | `False` | Check target table for duplicate merge keys (slower) |
| `MERGE_AUTO_DEDUPLICATE_SOURCE` | bool | `False` | Auto-deduplicate source if duplicates found (keeps first) |
| `MERGE_FAIL_ON_DUPLICATE_KEYS` | bool | `True` | Raise error on duplicates (when validation enabled) |

---

### 1.12 Scheduled Maintenance

#### Full Rebuild Schedule

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `ENABLE_SCHEDULED_FULL_REBUILD` | bool | `False` | Activate scheduled full rebuilds |
| `SCHEDULED_REBUILD_DAY_OF_WEEK` | int | `6` | 0=Monday, 6=Sunday |
| `SCHEDULED_REBUILD_WEEK_OF_MONTH` | int | `2` | 1=First, 2=Second, 3=Third, 4=Fourth, 5=Last |
| `SCHEDULED_REBUILD_FROM_DATE` | str/None | `None` | Date string 'YYYY-MM-DD' to rebuild from (None=use table start_date) |

#### VACUUM Schedule

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `ENABLE_SCHEDULED_VACUUM` | bool | `False` | Activate scheduled VACUUM |
| `SCHEDULED_VACUUM_DAY_OF_WEEK` | int | `6` | 0=Monday, 6=Sunday |
| `SCHEDULED_VACUUM_WEEK_OF_MONTH` | int | `2` | 1=First, 2=Second, 3=Third, 4=Fourth, 5=Last |
| `SCHEDULED_VACUUM_RETENTION_HOURS` | int | `168` | Retention period (168h = 7 days, Delta minimum) |
| `SCHEDULED_VACUUM_DRY_RUN` | bool | `False` | Preview mode (shows what would be deleted) |

#### OPTIMIZE Schedule

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `ENABLE_SCHEDULED_OPTIMIZE` | bool | `False` | Activate scheduled OPTIMIZE |
| `SCHEDULED_OPTIMIZE_DAY_OF_WEEK` | int | `6` | 0=Monday, 6=Sunday |
| `SCHEDULED_OPTIMIZE_WEEK_OF_MONTH` | int | `2` | 1=First, 2=Second, 3=Third, 4=Fourth, 5=Last |
| `SCHEDULED_OPTIMIZE_ZORDER_COLUMNS` | dict | `{}` | Z-order columns per table: `{"table_name": ["col1", "col2"]}` |

---

### 1.13 Intelligent Auto-Maintenance

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `ENABLE_INTELLIGENT_AUTO_OPTIMIZE` | bool | `False` | Auto-OPTIMIZE based on file health metrics |
| `ENABLE_INTELLIGENT_AUTO_VACUUM` | bool | `False` | Auto-VACUUM based on retention metrics |
| `OPTIMIZE_MIN_SMALL_FILES` | int | `50` | Trigger OPTIMIZE when small file count exceeds this |
| `OPTIMIZE_SMALL_FILE_SIZE_MB` | int | `100` | Files below this size (MB) count as "small" |
| `OPTIMIZE_MIN_TOTAL_FILES` | int | `100` | Trigger OPTIMIZE when total file count exceeds this |
| `OPTIMIZE_MAX_DAYS_SINCE_LAST` | int | `7` | Trigger OPTIMIZE if last run was >N days ago (0=disabled) |
| `OPTIMIZE_MIN_TABLE_SIZE_GB` | int | `1` | Only optimize tables larger than this |
| `VACUUM_MIN_DAYS_SINCE_LAST` | int | `30` | Trigger VACUUM if last run was >N days ago (0=disabled) |
| `VACUUM_RETENTION_HOURS` | int | `168` | Retention for intelligent VACUUM (7 days) |
| `VACUUM_MIN_DELETIONS_THRESHOLD` | float | `0.10` | Trigger VACUUM if >10% of data was deleted/updated |
| `VACUUM_MIN_TABLE_SIZE_GB` | int | `5` | Only vacuum tables larger than this |
| `INTELLIGENT_MAINTENANCE_PARALLEL_WORKERS` | int | `4` | Parallel workers for multi-table maintenance |
| `INTELLIGENT_MAINTENANCE_DRY_RUN` | bool | `False` | Preview mode |
| `INTELLIGENT_MAINTENANCE_VERBOSE` | bool | `True` | Print detailed health analysis |

---

### 1.14 Runtime Compatibility

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `SUPPORTED_DBR_VERSIONS` | str | `"13.3.x, 14.x, 15.x, 16.x, 17.x"` | Informational |
| `MIN_DBR_VERSION` | str | `"13.3"` | Minimum supported DBR |

---

### 1.15 Validation Constants

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `REQUIRED_CONFIG_KEYS` | list | `["target_database", "result_table_name", "load_mode"]` | Keys that MUST exist in every TABLES_CONFIG entry |
| `VALID_LOAD_MODES` | list | See below | Allowed load_mode values |

Valid load modes: `full`, `insert`, `refresh_n_days`, `full_if_not_exists`, `merge`, `update`, `delete`

---

### 1.16 Environment Variable Overrides

Call `belle.Config.from_env()` to load overrides from environment variables:

| Env Variable | Overrides |
| --- | --- |
| `BELLE_BLOB_ROOT` | `BLOB_ROOT` |
| `BELLE_LOG_TABLE` | `LOG_TABLE_NAME` |
| `BELLE_SVC_PREFIX` | `SERVICE_ACCOUNT_PREFIX` |
| `BELLE_CSV_DELIMITER` | `CSV_DELIMITER` |
| `BELLE_CSV_EXPORT` | `FEATURE_CSV_EXPORT` (string "true"/"false") |

---

## Part 2: TABLES_CONFIG (Per-Table Configuration)

`TABLES_CONFIG` is a Python dictionary you define in your consuming notebook. Each key is a table identifier, each value is a configuration dictionary.

### 2.1 Required Keys

Every entry MUST have these three keys:

| Key | Type | Description |
| --- | --- | --- |
| `target_database` | str | Where to write. Format: `"catalog.schema"` (UC) or `"database"` (Hive) |
| `result_table_name` | str | Table name within the database |
| `load_mode` | str | Write strategy (see Section 2.3) |

### 2.2 Standard Optional Keys

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `dependencies` | list[str] | `[]` | Config keys this table depends on (must complete before this table starts) |
| `partition_by` | list[str] | `[]` | Partition columns for the Delta table |
| `export_csv` | bool | `True` | Export CSV to blob (production mode only) |
| `subpipeline` | str/None | `None` | Subpath in blob storage hierarchy |
| `tag` | str/None | `None` | Arbitrary tag for log table grouping |
| `extra_context` | any | `None` | Extra metadata stored in log table |
| `force_full_rebuild` | bool/None | `None` | Per-table override for force rebuild (None = use global) |
| `use_managed_table` | bool | auto | Force managed (True) or external (False) table. Auto-detected from target_database format if omitted. |
| `disable_auto_persist` | bool | `False` | Opt out of auto-persist/unpersist memory management |
| `persist_columns` | list[str]/None | `None` | Project DataFrame to these columns before persisting (full DF preserved for write) |
| `max_retries` | int | `Config.MAX_MATERIALISE_RETRIES` | Per-table retry count override |

### 2.3 Load Mode Keys

#### `load_mode: "full"`
Drop and recreate table from scratch. No additional keys required.

#### `load_mode: "insert"`
Append rows. No additional keys required.

#### `load_mode: "full_if_not_exists"`
Write only if table does not exist; skip on subsequent runs. No additional keys required.

#### `load_mode: "merge"`
MERGE INTO (upsert). Required additional keys:

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `merge_keys` | list[str] | YES | Columns to match on (MERGE condition) |
| `merge_update_columns` | list[str] | No | Columns to update on match. If omitted, all non-key columns are updated. |
| `merge_auto_deduplicate` | bool | No (default: False) | Auto-deduplicate source on merge keys before merge |

#### `load_mode: "refresh_n_days-N"`
Replace the last N days of a date-partitioned table. Required:

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `partition_by` | list[str] | YES | Must be exactly ONE date-based partition column |

Example: `"load_mode": "refresh_n_days-7"` replaces the last 7 days.

#### `load_mode: "update"`
UPDATE specific columns. Required:

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `merge_keys` | list[str] | YES | WHERE condition columns |
| `update_set` | dict | YES | Columns and values to SET |

#### `load_mode: "delete"`
DELETE matching rows. Required:

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `merge_keys` | list[str] | YES | WHERE condition columns |
| `delete_where` | str | YES | SQL WHERE clause for deletion |

### 2.4 Encryption Keys (in table config)

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `encrypt` | bool | `False` | Enable encryption for this table |
| `encrypt_key` | str | None | Base64-encoded AES key |
| `encrypt_exclude` | list[str] | `[]` | Columns to leave in plaintext (partition keys, join keys) |

### 2.5 Monitoring Keys

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `monitored_id_column` | str/None | `None` | Column to track MAX value for incremental logging |
| `monitored_date_column` | str/None | `None` | Date column to track for incremental logging |

### 2.6 CSV Export Override

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `csv_export_dir` | str/None | `None` | Override default CSV export path for this table |

### 2.7 Partition Materialisation Keys

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `partition_filter` | dict | `{}` | Column-value pairs to filter partition writes (e.g., `{"_data_year": 2024, "_data_month": 6}`) |

---

## Part 3: Orchestrator Constructor Parameters

```python
orchestrator = belle.Orchestrator(
    tables_config,                    # REQUIRED: Your TABLES_CONFIG dict
    logger=None,                      # Optional: Python logger instance
    custom_csv_removals="",           # Optional: Characters to strip from CSV output
    test_mode=False,                  # Optional: Isolate writes with unique suffix
    global_force_rebuild=False,       # Optional: Force rebuild ALL tables
    validate_configs=True,            # Optional: Run pre-flight validation
    fail_on_validation_errors=True,   # Optional: Raise on validation errors
)
```

---

## Part 4: Orchestrator.run() Parameters

```python
results = orchestrator.run(
    tables_to_run=None,               # Optional: List of config keys to process (None=all)
    max_workers=None,                  # Optional: Thread pool size (None=auto-detect)
    show_dag=False,                    # Optional: Display DAG visualization before execution
    job_queue_limit=10,                # Optional: Spark job queue backpressure limit
    sample_rows=10,                    # Optional: Rows to display in interactive samples
    external_run_id=None,              # Optional: ADF/external run ID for log correlation
    execution_context=None,            # Optional: Dict of external context for logging
    force_full_refresh=False,          # DEPRECATED: Use global_force_rebuild in constructor
)
```

**Returns:** List of per-table summary dictionaries with keys: `table_config_name`, `target_database`, `result_table_name`, `full_table_name`, `status`, `duration_seconds`, `start_time`, `end_time`, `row_count`.

---

## Part 5: Configuration Patterns (Examples)

### Minimal Config (1 table, full mode)

```python
TABLES_CONFIG = {
    "my_db.dim_date": {
        "target_database": "my_db",
        "result_table_name": "dim_date",
        "load_mode": "full",
        "dependencies": [],
    }
}
```

### Merge Mode with Dependencies

```python
TABLES_CONFIG = {
    "catalog.schema.dim_customer": {
        "target_database": "catalog.schema",
        "result_table_name": "dim_customer",
        "load_mode": "full",
        "dependencies": [],
        "partition_by": [],
    },
    "catalog.schema.fact_order": {
        "target_database": "catalog.schema",
        "result_table_name": "fact_order",
        "load_mode": "merge",
        "merge_keys": ["order_id"],
        "dependencies": ["catalog.schema.dim_customer"],
        "partition_by": ["order_date_key"],
    },
}
```

### Rolling Window Refresh

```python
"my_db.fact_daily_sales": {
    "target_database": "my_db",
    "result_table_name": "fact_daily_sales",
    "load_mode": "refresh_n_days-30",
    "partition_by": ["sale_date"],
    "dependencies": [],
}
```

### Encrypted Table

```python
"sales_semantic.factorder": {
    "target_database": "sales_semantic",
    "result_table_name": "factorder",
    "load_mode": "full",
    "dependencies": ["sales_semantic.dimclaim"],
    "partition_by": ["_data_year", "_data_month"],
    "encrypt": True,
    "encrypt_key": "<base64-encoded-256-bit-key>",
    "encrypt_exclude": ["_data_year", "_data_month", "claim_key"],
    "export_csv": True,
}
```

### Full Production Config (Kitchen Sink)

```python
"sales_semantic.factcontract": {
    "target_database": "sales_semantic",
    "result_table_name": "factcontract",
    "load_mode": "merge",
    "merge_keys": ["policy_key", "_data_year", "_data_month"],
    "merge_update_columns": ["premium", "status", "last_modified"],
    "dependencies": ["sales_semantic.dimpolicy", "sales_semantic.dimproduct"],
    "partition_by": ["_data_year", "_data_month"],
    "encrypt": True,
    "encrypt_key": "<base64-key>",
    "encrypt_exclude": ["_data_year", "_data_month", "policy_key"],
    "export_csv": True,
    "csv_export_dir": "/mnt/internal/enhanced/_tmp_csv_cubes/sales_semantic/factcontract",
    "subpipeline": None,
    "tag": "pipeline_v2",
    "extra_context": "country=germany",
    "monitored_id_column": "policy_key",
    "monitored_date_column": "_data_month",
    "force_full_rebuild": None,
    "use_managed_table": False,
    "max_retries": 3,
    "disable_auto_persist": False,
    "persist_columns": ["policy_key", "_data_year", "_data_month"],
}
```

---

*See `A1_Appendix_Config_Settings.md` for the full exhaustive matrix with every attribute, type, default, and example in one table.*

*Last updated: June 2026*
