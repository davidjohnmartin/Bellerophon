# API Reference

**Version:** 1.2.18

---

## BelleNamespace (`belle`)

After `%run bellerophon_core`, the `belle` object provides access to everything:

| Attribute | Resolves to |
| --- | --- |
| `belle.Orchestrator` | `BellerophonOrchestrator` |
| `belle.Config` | `BellerophonConfig` |
| `belle.Utils` | `BellerophonUtils` |
| `belle.Logger` | `BellerophonLogger` |
| `belle.OutputRegistry` | `BellerophonOutputRegistry` |
| `belle.DAGVisualizer` | `BellerophonDAGVisualizer` |
| `belle.MaintenanceScheduler` | `BellerophonMaintenanceScheduler` |
| `belle.ProgressTracker` | `BellerophonProgressTracker` |
| `belle.RetryHandler` | `BellerophonRetryHandler` |
| `belle.DryRunValidator` | `BellerophonDryRunValidator` |
| `belle.DataQualityChecker` | `BellerophonDataQualityChecker` |
| `belle.ConfigValidator` | `BellerophonConfigValidator` |
| `belle.materialise_dataframe` | `bellerophon_materialise_dataframe` |
| `belle.materialise_dataframe_fast` | `bellerophon_materialise_dataframe_fast` |
| `belle.materialise_partition` | `bellerophon_materialise_partition` |
| `belle.materialise_bulk` | `bellerophon_materialise_bulk` |
| `belle.materialise_table` | `resilient_materialise_table` |
| `belle.flush_partition_logs` | `bellerophon_flush_partition_logs` |
| `belle.purge_logs` | `BellerophonLogger.purge_logs` |
| `belle.PartitionLogBuffer` | `_PartitionLogBuffer` |
| `belle.VERSION` | `BELLEROPHON_VERSION` (str) |

---

## BellerophonOrchestrator

### Constructor

```python
BellerophonOrchestrator(
    tables_config: Dict[str, Dict],
    logger=None,
    custom_csv_removals: str = "",
    test_mode: bool = False,
    global_force_rebuild: bool = False,
    validate_configs: bool = True,
    fail_on_validation_errors: bool = True,
)
```

### Methods

| Method | Signature | Returns | Description |
| --- | --- | --- | --- |
| `run` | `(tables_to_run=None, max_workers=None, show_dag=False, job_queue_limit=10, sample_rows=10, external_run_id=None, execution_context=None, force_full_refresh=False)` | `List[Dict]` | Execute DAG orchestration |
| `materialise_table` | `(input_df, conf, run_id, interactive_mode, sample_rows, dag_stage, custom_csv_removals, max_workers=None, external_run_id=None, execution_context=None, retry_count=0)` | `Tuple[DF, DF, DF]` | Materialise single table |
| `display_dag` | `(tables_dependencies)` | None | Show DAG visualisation |
| `check_scheduled_maintenance` | `(force_full_refresh=False, check_date=None)` | `Tuple[bool, bool, bool]` | Check maintenance schedule |
| `run_post_processing_maintenance` | `(run_vacuum, run_optimize, tables_to_run=None)` | None | Execute VACUUM/OPTIMIZE |
| `validate_merge_keys` | `(df, merge_keys, df_name, auto_deduplicate=False, fail_on_duplicates=True)` | `Tuple[DF, int]` | Check merge key uniqueness |
| `get_sensible_max_workers` | `()` | `int` | Auto-detect worker count |

### Properties

| Property | Type | Description |
| --- | --- | --- |
| `tables_config` | Dict | Original config |
| `enriched_configs` | Dict | Config with validation metadata |
| `validation_summary` | Dict | Summary of validation results |
| `interactive_mode` | bool | Current mode |
| `instance_id` | str | Unique run instance |
| `target_database` | str | Primary target database |

---

## BellerophonConfig

See `04_Configuration_Reference.md` for full attribute listing.

### Class Methods

| Method | Returns | Description |
| --- | --- | --- |
| `from_env()` | cls | Load overrides from BELLE_* env vars |
| `reset_defaults()` | None | Reset all config to defaults |
| `temp_config(**overrides)` | ContextManager | Temporary override (reverts on exit) |
| `generate_instance_id()` | str | Create unique YYYYMMDD_HHMMSS_uuid8 |
| `build_data_path(target_database, subpipeline=None, layer=None)` | str | Construct blob data path |
| `build_log_path(target_database, layer=None)` | str | Construct log table path |
| `build_csv_export_path(target_database, subpipeline=None, layer=None)` | str | Construct CSV path |
| `validate_and_enrich_table_config(spark, config, table_key, global_force_rebuild=False)` | Dict | Validate and add metadata |

---

## BellerophonOutputRegistry

| Method | Signature | Returns | Description |
| --- | --- | --- | --- |
| `set_output` | `(key: str, value)` | None | Store DataFrame |
| `get_output` | `(key: str)` | DataFrame/None | Retrieve DataFrame |
| `clear_outputs` | `()` | None | Clear all registered DataFrames |
| `clear_output` | `(key: str)` | None | Remove single key from registry |
| `get_all_keys` | `()` | List[str] | List registered keys |
| `check_health` | `(expected_keys: List[str])` | Dict | Health check |

---

## BellerophonUtils

| Method | Returns | Description |
| --- | --- | --- |
| `nowstr()` | str | UTC timestamp string |
| `get_current_user()` | str | Current Spark user |
| `is_interactive_notebook()` | bool | Interactive mode detection |
| `apply_test_suffix(table_name)` | str | Apply test suffix if active |
| `build_blob_target_dir(target_database, subpipeline=None)` | str | Build storage path |
| `get_target_cube_csv_path(blob_dir, target_db, table_name)` | Tuple[str, str] | CSV dir + filename |
| `rename_csv_part_file(target_dir, desired_filename=None)` | None | Rename part-* to clean CSV |
| `get_spark_job_queue_size()` | int | Active Spark job count |
| `try_load_from_table(target_database, result_table_name)` | DataFrame/None | Load existing table |
| `check_optional_dependencies()` | Dict[str, bool] | Check networkx/matplotlib/pyvis |
| `get_cluster_info()` | Dict | Cluster ID, name, Spark/DBR version |
| `get_execution_context()` | Dict | Notebook path, job info |
| `get_table_row_count(table_name)` | int | Row count (0 if not exists) |
| `detect_service_account()` | Tuple[bool, str] | (is_service_acct, username) |
| `print_break(msg, level=2)` | None | Section separator |

---

## BellerophonLogger

| Method | Signature | Returns | Description |
| --- | --- | --- | --- |
| `write_log` | `(logging_df, target_database, logging_schema, run_id=None, interactive_mode=None)` | None | Write log DataFrame to Delta |
| `purge_logs` | `(spark_session, target_database)` | bool | Drop log table |
| `cleanup_old_logs` | `(spark_session, target_database, retention_days=None)` | int | Delete old entries |
| `reset_logging_messages` | `(target_database=None)` | None | Reset setup message flags |

---

## BellerophonTracer

| Method | Signature | Returns | Description |
| --- | --- | --- | --- |
| `enable` | `(full=False)` | None | Enable tracing |
| `disable` | `()` | None | Disable (entries preserved) |
| `clear` | `()` | None | Clear all entries |
| `is_enabled` | `()` | bool | Check if enabled |
| `trace` | `(function_name, table_name, event, variables, caller_locals=None)` | None | Record entry |
| `get_entries` | `(table_filter=None, function_filter=None, event_filter=None, var_filter=None)` | List[Dict] | Filter entries |
| `report` | `(table_filter=None, compact=False, var_filter=None, show_locals=False)` | None | Print report |
| `summary` | `()` | None | Print summary |
| `to_dataframe` | `(spark_session)` | DataFrame/None | Convert to Spark DF |

---

## BellerophonErrorCode

| Code | Name | Description |
| --- | --- | --- |
| `BELLE-000` | SUCCESS | Operation completed successfully |
| `BELLE-001` | TABLE_NOT_FOUND | Target table does not exist |
| `BELLE-002` | MERGE_KEY_MISSING | Merge missing required keys |
| `BELLE-003` | INVALID_LOAD_MODE | Unsupported load mode |
| `BELLE-004` | CONFIG_VALIDATION_FAILED | Config validation failed |
| `BELLE-005` | DEPENDENCY_CYCLE_DETECTED | Circular dependency |
| `BELLE-010` | SCHEMA_MISMATCH | Schema does not match |
| `BELLE-011` | DATA_QUALITY_FAILED | Data quality check failed |
| `BELLE-012` | NULL_VALUE_VIOLATION | Nulls in non-nullable columns |
| `BELLE-020` | DELTA_OPERATION_FAILED | Delta Lake operation failed |
| `BELLE-021` | CSV_EXPORT_FAILED | CSV export failed |
| `BELLE-022` | LOGGING_FAILED | Log write failed |
| `BELLE-023` | PERSIST_FAILED | Persist/cache failed |
| `BELLE-030` | OOM_ERROR | Out of memory |
| `BELLE-031` | TIMEOUT_ERROR | Operation timed out |
| `BELLE-032` | CLUSTER_ERROR | Cluster error |
| `BELLE-040` | PERMISSION_DENIED | Insufficient permissions |
| `BELLE-041` | CATALOG_ACCESS_DENIED | Unity Catalog access denied |
| `BELLE-999` | UNKNOWN_ERROR | Unclassified |

---

## Standalone Functions

> **Bypass note:** Calling these functions directly bypasses the Orchestrator's retry logic
> (`RetryHandler.retry_with_backoff`). This is the ONLY safe way to test failure scenarios
> (schema drift, invalid config) without triggering the KB-001 infinite retry storm.
> Config settings like `SCHEMA_DRIFT_ACTION` are still respected.

| Function | Signature | Returns | Description |
| --- | --- | --- | --- |
| `bellerophon_materialise_dataframe` | `(input_df, target_database, result_table_name, run_id, ...)` | `Tuple[DF, DF, DF]` | Full materialise (all modes). Raises `ValueError` on schema drift when `SCHEMA_DRIFT_ACTION="fail"`. |
| `bellerophon_materialise_dataframe_fast` | `(input_df, conf, run_id, dag_stage=None, interactive_mode=None)` | `Tuple[DF, DF, None]` | Fast-path materialise |
| `bellerophon_materialise_partition` | `(input_df, conf, run_id, interactive_mode=None)` | `int` | Partition write (returns row count) |
| `bellerophon_materialise_bulk` | `(dataframes, configs, run_id, interactive_mode=None)` | `List[Dict]` | Multi-table bulk write |
| `bellerophon_flush_partition_logs` | `(target_database, run_id)` | `int` | Flush buffered partition logs |
| `resilient_materialise_table` | `(materialise_func, input_df, conf, ...)` | `Tuple[DF, DF, DF]` | OOM retry wrapper |
| `ensure_table_ready` | `(spark_session, table_config, force_rebuild=False, verbose=None)` | `Dict` | Single table readiness |
| `ensure_all_tables_ready` | `(spark_session, tables_config, force_rebuild=False, verbose=None)` | `Dict` | All tables readiness |

---

*Last updated: June 2026*
