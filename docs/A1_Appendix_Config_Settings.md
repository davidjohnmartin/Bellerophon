# Appendix A1 — Full Configuration Settings Matrix

**Version:** 1.2.19

Exhaustive listing of every `BellerophonConfig` class attribute and every `TABLES_CONFIG` key.

---

## Part 1: BellerophonConfig Attributes

### Feature Flags

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `FEATURE_CSV_EXPORT` | bool | `True` | Enable/disable CSV export functionality |
| `FEATURE_LOG_SCHEMA_EVOLUTION` | bool | `True` | Allow log table schema evolution (mergeSchema) |
| `FEATURE_ENCRYPTION` | bool | `True` | Global kill switch for inline encryption |

### Verbosity & Display

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `VERBOSITY` | int | `2` | Output level: 0=SILENT, 1=MINIMAL, 2=NORMAL, 3=VERBOSE, 4=DEBUG |
| `SILENT` | int | `0` | Named constant |
| `MINIMAL` | int | `1` | Named constant |
| `NORMAL` | int | `2` | Named constant |
| `VERBOSE` | int | `3` | Named constant |
| `DEBUG` | int | `4` | Named constant |
| `ASCII_ART_ENABLED` | bool | `True` | Show Pegasus banner and box-drawing art |
| `EMOJI_ENABLED` | bool | `True` | Use emoji in output (False = plain text) |

### Storage Paths

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `BLOB_ROOT` | str | `"/mnt/internal/enhanced"` | Root path for external Delta tables |
| `BLOB_ROOT_BASE` | str | `""` | Common prefix (layer suffix appended). If empty, BLOB_ROOT used |
| `CSV_TEMP_FOLDER` | str | `"_tmp_csv_cubes"` | Temporary CSV export folder name |
| `DATA_FOLDER` | str | `"data"` | Standard data folder name within blob root |

### Encryption

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `ENCRYPTION_MODE` | str | `"GCM"` | AES mode (GCM = authenticated encryption) |
| `ENCRYPTION_STRATEGY` | str | `"per_column"` | `"per_column"` (default) or `"blob"` (legacy to_json) |

### Table Naming

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `LOG_TABLE_NAME` | str | `"bellerophon_log_table"` | Name of execution log table |
| `TEST_MODE_SUFFIX` | str | `"_bellerophon_test"` | Base suffix for test mode tables |

### Service Account

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `SERVICE_ACCOUNT_PREFIX` | str | `"svc_aas"` | Username prefix for production detection |

### CSV Export

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `CSV_DELIMITER` | str | `";"` | CSV field separator |
| `CSV_ENCODING` | str | `"utf-8"` | CSV file encoding |
| `CSV_QUOTE` | str | `""` | Quote character (empty = none) |
| `CSV_QUOTE_ALL` | str | `"false"` | Quote all fields |
| `CSV_ESCAPE_QUOTES` | str | `"false"` | Escape quotes inside fields |
| `CSV_LINE_SEP` | str | `"\n"` | Line separator |

### Performance

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `PERSIST_ROW_THRESHOLD` | int | `5_000_000` | Auto-persist DataFrames above this row count |
| `PERSIST_COL_THRESHOLD` | int | `30` | Auto-persist above this column count |
| `DEFAULT_MAX_WORKERS` | int | `4` | Default ThreadPoolExecutor parallelism |
| `DEFAULT_SAMPLE_ROWS` | int | `10` | Rows displayed after write in interactive mode |
| `DEFAULT_JOB_QUEUE_LIMIT` | int | `10` | Max concurrent Spark jobs before throttling |

### Fast Mode

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `FAST_MODE_HEAVY_COL_THRESHOLD` | int | `20` | Encrypted columns above this = "heavy" table |
| `FAST_MODE_HEAVY_WORKERS_RATIO` | int | `0` | Workers for heavy tables (0 = sequential) |

### Retry

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `DEFAULT_MAX_RETRIES` | int | `3` | Max retries for transient failures |
| `DEFAULT_BASE_DELAY` | float | `2.0` | Base delay between retries (seconds) |
| `DEFAULT_MAX_DELAY` | float | `60.0` | Maximum delay cap (seconds) |
| `MAX_MATERIALISE_RETRIES` | int | `2` | Retries for materialise failures |

### Logging & Validation

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `PROGRESS_BAR_WIDTH` | int | `50` | Progress bar character width |
| `LOG_SCHEMA_JSON_MAX_LENGTH` | int | `4000` | Max length of schema_json in log (0=unlimited) |
| `WRITE_ROW_COUNT_TOLERANCE` | float | `0.01` | Warn if row count diff exceeds 1% |
| `WRITE_ROW_COUNT_VALIDATION` | bool | `True` | Enable post-write row count check |
| `LOG_RETENTION_DAYS` | int | `90` | Auto-cleanup logs older than N days (0=disabled) |
| `LOG_STRIP_EMOJI` | bool | `True` | Strip emoji from values written to log table |
| `SCHEMA_DRIFT_ACTION` | str | `"fail"` | On drift: `"fail"`, `"warn"`, or `"ignore"` |

### Validation Constants

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `REQUIRED_CONFIG_KEYS` | List[str] | `["target_database", "result_table_name", "load_mode"]` | Mandatory in every TABLES_CONFIG entry |
| `VALID_LOAD_MODES` | List[str] | `["full", "insert", "refresh_n_days", "full_if_not_exists", "merge", "update", "delete"]` | Accepted load_mode values |

### Scheduled Maintenance

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `ENABLE_SCHEDULED_FULL_REBUILD` | bool | `False` | Activate scheduled full rebuilds |
| `SCHEDULED_REBUILD_DAY_OF_WEEK` | int | `6` | 0=Mon, 6=Sun |
| `SCHEDULED_REBUILD_WEEK_OF_MONTH` | int | `2` | 1=First, 2=Second, 3=Third, 4=Fourth, 5=Last |
| `SCHEDULED_REBUILD_FROM_DATE` | str/None | `None` | Rebuild from this date (None=use table start_date) |
| `ENABLE_SCHEDULED_VACUUM` | bool | `False` | Activate scheduled VACUUM |
| `SCHEDULED_VACUUM_DAY_OF_WEEK` | int | `6` | Day of week |
| `SCHEDULED_VACUUM_WEEK_OF_MONTH` | int | `2` | Week of month |
| `SCHEDULED_VACUUM_RETENTION_HOURS` | int | `168` | VACUUM retention (168h = 7d minimum) |
| `SCHEDULED_VACUUM_DRY_RUN` | bool | `False` | Preview mode |
| `ENABLE_SCHEDULED_OPTIMIZE` | bool | `False` | Activate scheduled OPTIMIZE |
| `SCHEDULED_OPTIMIZE_DAY_OF_WEEK` | int | `6` | Day of week |
| `SCHEDULED_OPTIMIZE_WEEK_OF_MONTH` | int | `2` | Week of month |
| `SCHEDULED_OPTIMIZE_ZORDER_COLUMNS` | Dict | `{}` | `{"table": ["col1"]}` for Z-ordering |

### Intelligent Auto-Maintenance

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `ENABLE_INTELLIGENT_AUTO_OPTIMIZE` | bool | `False` | Threshold-based auto OPTIMIZE |
| `ENABLE_INTELLIGENT_AUTO_VACUUM` | bool | `False` | Threshold-based auto VACUUM |
| `OPTIMIZE_MIN_SMALL_FILES` | int | `50` | Small files to trigger OPTIMIZE |
| `OPTIMIZE_SMALL_FILE_SIZE_MB` | int | `100` | Definition of "small" file |
| `OPTIMIZE_MIN_TOTAL_FILES` | int | `100` | Total files threshold |
| `OPTIMIZE_MAX_DAYS_SINCE_LAST` | int | `7` | Days since last OPTIMIZE (0=disabled) |
| `OPTIMIZE_MIN_TABLE_SIZE_GB` | int | `1` | Skip tables smaller than this |
| `VACUUM_MIN_DAYS_SINCE_LAST` | int | `30` | Days since last VACUUM |
| `VACUUM_RETENTION_HOURS` | int | `168` | VACUUM retention hours |
| `VACUUM_MIN_DELETIONS_THRESHOLD` | float | `0.10` | Min deletion ratio to trigger |
| `VACUUM_MIN_TABLE_SIZE_GB` | int | `5` | Skip tables smaller than this |
| `INTELLIGENT_MAINTENANCE_PARALLEL_WORKERS` | int | `4` | Workers for maintenance ops |
| `INTELLIGENT_MAINTENANCE_DRY_RUN` | bool | `False` | Preview mode |
| `INTELLIGENT_MAINTENANCE_VERBOSE` | bool | `True` | Extra output during maintenance |

### Merge Validation

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `MERGE_VALIDATE_SOURCE_KEYS` | bool | `True` | Check source DataFrame for duplicate merge keys |
| `MERGE_VALIDATE_TARGET_KEYS` | bool | `False` | Check target table for duplicate merge keys |
| `MERGE_AUTO_DEDUPLICATE_SOURCE` | bool | `False` | Auto-deduplicate source on merge keys |
| `MERGE_FAIL_ON_DUPLICATE_KEYS` | bool | `True` | Raise error on duplicates (vs warning) |

### Runtime Compatibility

| Attribute | Type | Default | Description |
| --- | --- | --- | --- |
| `SUPPORTED_DBR_VERSIONS` | str | `"13.3.x, 14.x, 15.x, 16.x, 17.x"` | Documented support |
| `MIN_DBR_VERSION` | str | `"13.3"` | Minimum DBR |

---

## Part 2: TABLES_CONFIG Keys

### Required Keys

| Key | Type | Description |
| --- | --- | --- |
| `target_database` | str | Target database/schema (Hive: `"db"`, UC: `"catalog.schema"`) |
| `result_table_name` | str | Table name within target_database |
| `load_mode` | str | One of: `full`, `insert`, `merge`, `update`, `delete`, `refresh_n_days`, `full_if_not_exists` |

### Optional Keys

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `dependencies` | List[str] | `[]` | Tables this depends on (DAG edges) |
| `partition_by` | List[str] | `[]` | Partition columns |
| `merge_keys` | List[str] | `[]` | Required for merge/update/delete modes |
| `use_managed_table` | bool | `False` | Force managed table (no explicit LOCATION) |
| `export_csv` | bool | `False` | Export to CSV after write |
| `force_full_rebuild` | bool | `False` | Drop and recreate this table |
| `encrypt` | bool | `False` | Apply encryption |
| `encrypt_key` | str | None | Base64-encoded AES key |
| `encrypt_exclude` | List[str] | `[]` | Columns to leave in plaintext |
| `persist_columns` | List[str] | None | Project to these columns only (memory saving) |
| `disable_auto_persist` | bool | `False` | Skip auto-persist for this table |
| `start_date` | str | None | Used with refresh_n_days as lower bound |
| `delta_location` | str | None | Explicit LOCATION (overrides auto-generated path) |
| `custom_csv_name` | str | None | Override CSV filename |
| `partition_filter` | Dict | None | For partition mode: `{col: value}` |
| `monitored_columns` | List[str] | None | Columns to validate post-write |
| `merge_auto_deduplicate` | bool | `False` | Auto-dedup source for this table |
| `merge_update_columns` | List[str] | None | Columns to update on merge match (None=all) |
| `merge_insert_columns` | List[str] | None | Columns to insert on merge not-matched (None=all) |
| `merge_condition_override` | str | None | Custom merge condition (overrides merge_keys) |

### Enriched Metadata (Runtime, Added by `validate_and_enrich_table_config`)

| Key | Type | Description |
| --- | --- | --- |
| `_table_exists` | bool | Whether target table exists at validation time |
| `_actual_partitions` | List[str] | Current partition columns on existing table |
| `_partition_mismatch` | bool | Config partition_by differs from actual |
| `_effective_force_rebuild` | bool | Resolved rebuild flag (global OR per-table) |
| `_rebuild_source` | str | `"per_table_config"` or `"global_parameter"` |
| `_refresh_n_days` | int | N extracted from `refresh_n_days-N` mode |
| `_validation_errors` | List[str] | Blocking issues found |
| `_validation_warnings` | List[str] | Non-blocking issues found |
| `_missing_parameters` | List[str] | Missing optional but recommended keys |
| `_full_table_name` | str | Resolved `target_database.result_table_name` |

---

*Last updated: June 2026*

---

## Part 3: BelleValidator (Static Pipeline Analysis)

**Since:** v1.2.19  
**Access:** `belle.Validator`

### Class Methods

| Method | Arguments | Returns | Description |
| --- | --- | --- | --- |
| `validate(notebook_path, verbose=True)` | `notebook_path`: absolute workspace path, `verbose`: print report | `bool` | Static analysis of pipeline notebook. Returns True if no errors. |
| `validate_config(tables_config)` | `tables_config`: dict | `bool` | Validates a TABLES_CONFIG dict without notebook parsing. |

### Class Attributes (populated after validate/validate_config)

| Attribute | Type | Description |
| --- | --- | --- |
| `findings` | `list[Finding]` | All findings from last validation run |
| `tables` | `dict` | Extracted TABLES_CONFIG from last validation run |

### Finding Namedtuple

| Field | Type | Values |
| --- | --- | --- |
| `severity` | str | `ERROR`, `WARNING`, `INFO` |
| `category` | str | `STRUCTURE`, `REGISTRY`, `CONFIG`, `ANTI_PATTERN`, `ORCHESTRATOR` |
| `message` | str | Human-readable description |
| `cell_index` | int or None | Cell number where finding was detected |
| `line_number` | int or None | Line within the cell |

### Suppression

Add `# INTENTIONAL` on the same line as a flagged pattern to suppress it.
