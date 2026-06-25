# Databricks notebook source
# DBTITLE 1,Bellerophon Core
# Bellerophon (Belle) v1.2.19 — Production batch orchestrator for Databricks.
# DAG-driven table materialisation | Delta Lake lifecycle | Config-driven validation.
# Features: parallel execution, partition-aware writes, SCD2 incremental, inline encryption.
#
# v1.2.18: CSV rename fix, delta_location path normalisation, namespace cleanup,
#          log table protection on force rebuild, schema evolution defaulted to True.
# v1.2.19: Integrated BelleValidator (belle.Validator.validate), LOG_RETENTION_DAYS=730.


# COMMAND ----------

# DBTITLE 1,Imports & Module Constants
# ============================================================================
# IMPORTS
# ============================================================================
import datetime
from datetime import timezone as _tz  # For timezone-aware timestamps
import uuid
import json
import time
import sys
import traceback
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple
from py4j.java_gateway import Py4JJavaError
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException
# pyspark.sql.functions, pyspark.sql.types, delta.tables, and pandas are
# imported LOCALLY inside each function that needs them. This prevents
# namespace pollution when users %run this notebook.
from functools import reduce

# Get Spark session
try:
    spark = SparkSession.builder.getOrCreate()
except Exception:
    spark = None  # Will be set by consuming notebook

# ============================================================================
# MODULE CONSTANTS
# ============================================================================
BELLEROPHON_VERSION = "1.2.19"

# ============================================================================
# BELLE OUTPUT HELPERS (Verbosity-Aware Printing)
# ============================================================================

def belle_print(msg, level=2, timestamp=False):
    """Print if verbosity >= level."""
    if BellerophonConfig.VERBOSITY >= level:
        if timestamp:
            import datetime as _dt
            ts = _dt.datetime.now().strftime("%H:%M:%S")
            print(f"[{ts}] {msg}")
        else:
            print(msg)


def belle_emoji(emoji_char, fallback=""):
    """Return emoji if EMOJI_ENABLED, else fallback text."""
    if BellerophonConfig.EMOJI_ENABLED:
        return emoji_char
    return fallback


def belle_banner(title="", char="=", width=80, level=2):
    """Print a banner line if verbosity allows. Suppressed when ASCII_ART_ENABLED=False."""
    if not BellerophonConfig.ASCII_ART_ENABLED:
        if title and BellerophonConfig.VERBOSITY >= level:
            print(title)
        return
    if BellerophonConfig.VERBOSITY >= level:
        print(char * width)
        if title:
            print(title)
            print(char * width)


# ============================================================================
# BELLEROPHON TRACER - Full Variable Tracking for All Decision Points
# ============================================================================

class BellerophonTracer:
    """Production trace facility for decision-point variable capture.
    Enable with .enable(full=True), query with .report() / .summary().
    """

    _enabled = False
    _capture_locals = False
    _entries = []

    # Types to skip when capturing locals (non-serializable/large objects)
    _SKIP_TYPES = ('SparkSession', 'DataFrame', 'DeltaTable', 'module',
                   'function', 'method', 'type', 'builtin_function_or_method')

    @classmethod
    def enable(cls, full=False):
        """Enable tracing. full=True also captures all locals() at each trace point."""
        cls._enabled = True
        cls._capture_locals = full
        cls._entries = []

    @classmethod
    def disable(cls):
        """Disable tracing (entries preserved until clear())."""
        cls._enabled = False

    @classmethod
    def clear(cls):
        """Clear all trace entries."""
        cls._entries = []

    @classmethod
    def is_enabled(cls):
        return cls._enabled

    @classmethod
    def _safe_repr(cls, val, max_len=200):
        """Safe string representation of a value, truncated."""
        try:
            r = repr(val)
            return r[:max_len] + '...' if len(r) > max_len else r
        except Exception:
            return f"<{type(val).__name__}>"

    @classmethod
    def _filter_locals(cls, local_vars):
        """Filter locals to serializable, interesting values."""
        filtered = {}
        for k, v in local_vars.items():
            if k.startswith('_') and k != '_table_exists':
                continue
            type_name = type(v).__name__
            if type_name in cls._SKIP_TYPES:
                continue
            # Skip very large objects
            try:
                if isinstance(v, (dict, list)) and len(v) > 50:
                    filtered[k] = f"<{type_name} len={len(v)}>"
                    continue
            except Exception:
                pass
            filtered[k] = v
        return filtered

    @classmethod
    def trace(cls, function_name, table_name, event, variables, caller_locals=None):
        """
        Record a trace entry.

        Args:
            function_name: Function emitting the trace.
            table_name: Fully qualified table name, or '*' for global events.
            event: Short event label (e.g., 'CHECK_EXISTENCE', 'PATH_1_FORCE_DROP').
            variables: Dict of explicitly named variable snapshots.
            caller_locals: Optional dict from locals() for full capture mode.
        """
        if not cls._enabled:
            return
        import datetime as _dt
        entry = {
            "timestamp": _dt.datetime.now().isoformat(),
            "function": function_name,
            "table": table_name,
            "event": event,
            "variables": {k: v for k, v in variables.items()},
        }
        if cls._capture_locals and caller_locals:
            entry["all_locals"] = cls._filter_locals(caller_locals)
        cls._entries.append(entry)

    @classmethod
    def get_entries(cls, table_filter=None, function_filter=None, event_filter=None, var_filter=None):
        """
        Retrieve trace entries with optional filters.

        Args:
            table_filter: Substring match on table name.
            function_filter: Substring match on function name.
            event_filter: Substring match on event label.
            var_filter: Substring match on any variable name or value.
        """
        entries = cls._entries
        if table_filter:
            entries = [e for e in entries if table_filter.lower() in e["table"].lower()]
        if function_filter:
            entries = [e for e in entries if function_filter.lower() in e["function"].lower()]
        if event_filter:
            entries = [e for e in entries if event_filter.lower() in e["event"].lower()]
        if var_filter:
            vf = var_filter.lower()
            def _match(e):
                for k, v in e["variables"].items():
                    if vf in k.lower() or vf in str(v).lower():
                        return True
                for k, v in e.get("all_locals", {}).items():
                    if vf in k.lower() or vf in str(v).lower():
                        return True
                return False
            entries = [e for e in entries if _match(e)]
        return entries

    @classmethod
    def report(cls, table_filter=None, compact=False, var_filter=None, show_locals=False):
        """
        Print formatted trace report.

        Args:
            table_filter: Filter entries by table name substring.
            compact: One-line-per-entry format.
            var_filter: Filter entries containing this variable name/value.
            show_locals: If True, include captured locals in output (can be verbose).
        """
        entries = cls.get_entries(table_filter=table_filter, var_filter=var_filter)
        if not entries:
            print("BellerophonTracer: No trace entries recorded.")
            if not cls._enabled:
                print("  (Tracing is DISABLED - call BellerophonTracer.enable() before execution)")
            return

        print("=" * 110)
        print(f"  BELLEROPHON TRACE REPORT  |  {len(entries)} entries"
              f"  |  locals={'ON' if cls._capture_locals else 'OFF'}")
        if table_filter:
            print(f"  Filter: table contains '{table_filter}'")
        if var_filter:
            print(f"  Filter: variable contains '{var_filter}'")
        print("=" * 110)

        if compact:
            for e in entries:
                ts = e["timestamp"].split("T")[1][:12]
                tbl = e["table"].split(".")[-1] if "." in e["table"] else e["table"]
                vars_str = ", ".join(f"{k}={v}" for k, v in e["variables"].items())
                print(f"  [{ts}] {tbl:<30} {e['function']:<35} {e['event']:<35} {vars_str}")
        else:
            current_table = None
            for e in entries:
                if e["table"] != current_table:
                    current_table = e["table"]
                    tbl_short = current_table.split(".")[-1] if "." in current_table else current_table
                    print(f"\n{'─' * 110}")
                    print(f"  TABLE: {current_table}")
                    print(f"{'─' * 110}")

                ts = e["timestamp"].split("T")[1][:12]
                print(f"\n  [{ts}] {e['function']}  →  {e['event']}")
                for k, v in e["variables"].items():
                    print(f"           {k}: {v}")
                if show_locals and e.get("all_locals"):
                    print(f"           --- all locals ---")
                    for k, v in e["all_locals"].items():
                        if k not in e["variables"]:
                            print(f"           {k}: {cls._safe_repr(v)}")

        print(f"\n{'=' * 110}")
        print(f"  END OF TRACE  |  {len(entries)} entries")
        print(f"{'=' * 110}")

    @classmethod
    def summary(cls):
        """Concise summary: counts by table and event, plus key decisions."""
        if not cls._entries:
            print("BellerophonTracer: No entries.")
            return

        from collections import Counter
        table_counts = Counter(e["table"] for e in cls._entries)
        event_counts = Counter(e["event"] for e in cls._entries)
        func_counts = Counter(e["function"] for e in cls._entries)

        print("=" * 80)
        print(f"  TRACE SUMMARY  |  {len(cls._entries)} total entries"
              f"  |  locals={'ON' if cls._capture_locals else 'OFF'}")
        print("=" * 80)

        print(f"\n  By function:")
        for f, c in func_counts.most_common():
            print(f"    {f:<45} {c:>4} entries")

        print(f"\n  By event:")
        for ev, c in event_counts.most_common():
            print(f"    {ev:<45} {c:>4} entries")

        print(f"\n  By table:")
        for t, c in table_counts.most_common():
            short = t.split(".")[-1] if "." in t else t
            print(f"    {short:<45} {c:>4} entries")

        drops = [e for e in cls._entries if "DROP" in e["event"]]
        creates = [e for e in cls._entries if "CREATE" in e["event"] or "WILL_CREATE" in e["event"]]
        mismatches = [e for e in cls._entries if "MISMATCH" in e["event"]]

        if drops or creates or mismatches:
            print(f"\n  Key decisions:")
            if drops:
                print(f"    Tables DROPPED:           {len(drops)}")
                for d in drops:
                    print(f"      - {d['table']}  ({d['event']})")
            if creates:
                print(f"    Tables to CREATE:         {len(creates)}")
                for c_ in creates:
                    print(f"      - {c_['table']}  ({c_['event']})")
            if mismatches:
                print(f"    Partition MISMATCHES:      {len(mismatches)}")
                for m in mismatches:
                    print(f"      - {m['table']}  ({m['event']})")
        print("=" * 80)

    @classmethod
    def to_dataframe(cls, spark_session):
        """Convert trace entries to a Spark DataFrame."""
        import json as _json
        rows = []
        for e in cls._entries:
            row = {
                "timestamp": e["timestamp"],
                "function": e["function"],
                "table": e["table"],
                "event": e["event"],
                "variables_json": _json.dumps(e["variables"], default=str),
            }
            if e.get("all_locals"):
                row["locals_json"] = _json.dumps(
                    {k: cls._safe_repr(v) for k, v in e["all_locals"].items()},
                    default=str
                )
            else:
                row["locals_json"] = None
            rows.append(row)
        if not rows:
            return None
        return spark_session.createDataFrame(rows)



# COMMAND ----------

# DBTITLE 1,BellerophonConfig — Environment & Paths
# ============================================================================
# ENVIRONMENT CONFIGURATION (Migration-Proof Design)
# ============================================================================

class BellerophonConfig:
    """Centralized configuration. Override via direct attribute, env var, or defaults."""
    
    # ========================================================================
    # FEATURE FLAGS (Easy On/Off for Future Migration)
    # ========================================================================
    FEATURE_CSV_EXPORT = True  # Set to False to disable all CSV export functionality
    FEATURE_LOG_SCHEMA_EVOLUTION = True

    # ========================================================================
    # VERBOSITY & DISPLAY CONTROL
    # ========================================================================
    # Verbosity levels:
    #   0 = SILENT  - No output at all
    #   1 = MINIMAL - Errors and final summary only
    #   2 = NORMAL  - Standard operational output (default)
    #   3 = VERBOSE - Detailed progress and decision logging
    #   4 = DEBUG   - Full variable dumps, all decision traces
    VERBOSITY = 2  # Default: NORMAL

    # Named constants for readability
    SILENT  = 0
    MINIMAL = 1
    NORMAL  = 2
    VERBOSE = 3
    DEBUG   = 4

    # Display toggles
    ASCII_ART_ENABLED = True   # Set False to suppress Pegasus banner and box-drawing art
    EMOJI_ENABLED = True       # Set False to replace emoji with plain-text markers
    
    # Storage Paths (Azure Blob Gen2 / ADLS Gen2)
    # MIGRATION NOTE: Update these paths for new platform
    BLOB_ROOT = "/mnt/internal/enhanced"  # Override for new platform
    BLOB_ROOT_BASE = ""  # Common prefix (layer suffix appended). If empty, BLOB_ROOT used.
    CSV_TEMP_FOLDER = "_tmp_csv_cubes"    # Temporary CSV export folder name
    DATA_FOLDER = "data"                   # Standard data folder name
    
    # Encryption Feature
    FEATURE_ENCRYPTION = True   # Global kill switch for inline encryption
    ENCRYPTION_MODE = "GCM"    # AES mode (GCM = authenticated encryption)
    ENCRYPTION_STRATEGY = "per_column"  # "per_column" (v1.2.15) or "blob" (legacy to_json)
    
    # Table Naming
    LOG_TABLE_NAME = "bellerophon_log_table"  # Execution log table name
    TEST_MODE_SUFFIX = "_bellerophon_test"     # Test mode table suffix
    
    # Service Account Detection (for interactive vs production mode)
    # MIGRATION NOTE: Update prefix pattern for new platform service accounts
    SERVICE_ACCOUNT_PREFIX = "svc_aas"  # Service account username pattern
    
    # CSV Export Configuration (Only used if FEATURE_CSV_EXPORT = True)
    CSV_DELIMITER = ";"
    CSV_ENCODING = "utf-8"
    CSV_QUOTE = ""
    CSV_QUOTE_ALL = "false"
    CSV_ESCAPE_QUOTES = "false"
    CSV_LINE_SEP = "\n"
    
    # Performance Thresholds
    PERSIST_ROW_THRESHOLD = 5_000_000  # Auto-persist above this row count
    PERSIST_COL_THRESHOLD = 30         # Auto-persist above this column count
    DEFAULT_MAX_WORKERS = 4
    DEFAULT_SAMPLE_ROWS = 10
    DEFAULT_JOB_QUEUE_LIMIT = 10
    
    # Fast Mode Execution (v1.2.14)
    FAST_MODE_HEAVY_COL_THRESHOLD = 20   # Tables with > N encrypted cols = "heavy"
    FAST_MODE_HEAVY_WORKERS_RATIO = 0    # 0 = sequential (heavy tables get all cores)
    
    # Retry Configuration
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_BASE_DELAY = 2.0   # seconds
    DEFAULT_MAX_DELAY = 60.0   # seconds
    
    # Progress Tracking
    PROGRESS_BAR_WIDTH = 50
    MAX_MATERIALISE_RETRIES = 2           # Retry transient failures (OOM, locks, timeouts)
    LOG_SCHEMA_JSON_MAX_LENGTH = 4000     # Truncate schema_json in log table (0=unlimited)
    WRITE_ROW_COUNT_TOLERANCE = 0.01      # Warn if source vs target row diff exceeds 1%
    WRITE_ROW_COUNT_VALIDATION = True      # Enable post-write row count validation
    LOG_RETENTION_DAYS = 730              # Auto-cleanup logs older than N days (0=disabled)
    LOG_STRIP_EMOJI = True                # Strip emoji from log table values
    SCHEMA_DRIFT_ACTION = "fail"          # Schema drift: "warn", "fail", or "ignore"
    
    # Validation
    REQUIRED_CONFIG_KEYS = [
        "target_database",
        "result_table_name",
        "load_mode"
    ]
    
    VALID_LOAD_MODES = [
        "full",
        "insert",
        "refresh_n_days",  # Use as: refresh_n_days-N (e.g., refresh_n_days-7)
        "full_if_not_exists",  # Write once on first run; skip on subsequent runs
        "merge",
        "update",
        "delete"
    ]
    
    # ========================================================================
    # SCHEDULED MAINTENANCE CONFIGURATION (v1.2.6+)
    # ========================================================================
    
    # Scheduled Full Rebuild (Failsafe for SCD Errors)
    ENABLE_SCHEDULED_FULL_REBUILD = False  # Enable to activate scheduled full rebuilds
    SCHEDULED_REBUILD_DAY_OF_WEEK = 6      # 0=Monday, 6=Sunday
    SCHEDULED_REBUILD_WEEK_OF_MONTH = 2    # 1=First, 2=Second, 3=Third, 4=Fourth, 5=Last
    SCHEDULED_REBUILD_FROM_DATE = None     # Date string 'YYYY-MM-DD' to rebuild from (None = use table config start_date)
    
    # Scheduled VACUUM (Clean up old data files)
    ENABLE_SCHEDULED_VACUUM = False        # Enable to activate scheduled VACUUM
    SCHEDULED_VACUUM_DAY_OF_WEEK = 6       # 0=Monday, 6=Sunday
    SCHEDULED_VACUUM_WEEK_OF_MONTH = 2     # 1=First, 2=Second, 3=Third, 4=Fourth, 5=Last
    SCHEDULED_VACUUM_RETENTION_HOURS = 168 # 168 hours = 7 days (Delta default minimum)
    SCHEDULED_VACUUM_DRY_RUN = False       # Set True for preview without deleting
    
    # Scheduled OPTIMIZE (Compact small files)
    ENABLE_SCHEDULED_OPTIMIZE = False      # Enable to activate scheduled OPTIMIZE
    SCHEDULED_OPTIMIZE_DAY_OF_WEEK = 6     # 0=Monday, 6=Sunday
    SCHEDULED_OPTIMIZE_WEEK_OF_MONTH = 2   # 1=First, 2=Second, 3=Third, 4=Fourth, 5=Last
    SCHEDULED_OPTIMIZE_ZORDER_COLUMNS = {} # Dict: {"table_name": ["col1", "col2"]} for Z-ordering
    
    # ========================================================================
    # INTELLIGENT AUTO-MAINTENANCE (v1.2.6)
    # ========================================================================
    # Automatically detect when tables need maintenance based on health metrics
    
    # Enable intelligent auto-maintenance (overrides scheduled maintenance)
    ENABLE_INTELLIGENT_AUTO_OPTIMIZE = False  # Auto-OPTIMIZE based on file count/size thresholds
    ENABLE_INTELLIGENT_AUTO_VACUUM = False    # Auto-VACUUM based on time/retention thresholds
    
    # OPTIMIZE Thresholds - tables meeting ANY threshold trigger OPTIMIZE
    OPTIMIZE_MIN_SMALL_FILES = 50              # Min number of small files to trigger OPTIMIZE
    OPTIMIZE_SMALL_FILE_SIZE_MB = 100          # Files smaller than this are "small" (default: 100 MB)
    OPTIMIZE_MIN_TOTAL_FILES = 100             # Total file count threshold (regardless of size)
    OPTIMIZE_MAX_DAYS_SINCE_LAST = 7           # Days since last OPTIMIZE (0 = disabled)
    OPTIMIZE_MIN_TABLE_SIZE_GB = 1             # Only optimize tables larger than this (GB)
    
    # VACUUM Thresholds - tables meeting ANY threshold trigger VACUUM
    VACUUM_MIN_DAYS_SINCE_LAST = 30            # Days since last VACUUM (0 = disabled)
    VACUUM_RETENTION_HOURS = 168               # Retention for intelligent VACUUM (7 days)
    VACUUM_MIN_DELETIONS_THRESHOLD = 0.10      # Vacuum if >10% of data was deleted/updated
    VACUUM_MIN_TABLE_SIZE_GB = 5               # Only vacuum tables larger than this (GB)
    
    # Advanced Configuration
    INTELLIGENT_MAINTENANCE_PARALLEL_WORKERS = 4  # Parallel workers for multi-table maintenance
    INTELLIGENT_MAINTENANCE_DRY_RUN = False    # Preview mode (shows what would be done)
    INTELLIGENT_MAINTENANCE_VERBOSE = True     # Print detailed health analysis
    
    # Merge Validation
    MERGE_VALIDATE_SOURCE_KEYS = True      # Check source DataFrame for duplicate merge keys
    MERGE_VALIDATE_TARGET_KEYS = False     # Check target table for duplicate merge keys (slower)
    
    # Auto-deduplicate source before merge (if duplicates found)
    MERGE_AUTO_DEDUPLICATE_SOURCE = False  # Keep first occurrence, drop duplicates
    
    # Merge behavior on validation failure
    MERGE_FAIL_ON_DUPLICATE_KEYS = True    # Raise error if duplicates found (when validation enabled)
    
    # Runtime Compatibility
    SUPPORTED_DBR_VERSIONS = "13.3.x, 14.x, 15.x, 16.x, 17.x"
    MIN_DBR_VERSION = "13.3"
    
    @classmethod
    def from_env(cls):
        """Load config overrides from BELLE_* environment variables."""
        import os
        cls.BLOB_ROOT = os.getenv('BELLE_BLOB_ROOT', cls.BLOB_ROOT)
        cls.LOG_TABLE_NAME = os.getenv('BELLE_LOG_TABLE', cls.LOG_TABLE_NAME)
        cls.SERVICE_ACCOUNT_PREFIX = os.getenv('BELLE_SVC_PREFIX', cls.SERVICE_ACCOUNT_PREFIX)
        cls.CSV_DELIMITER = os.getenv('BELLE_CSV_DELIMITER', cls.CSV_DELIMITER)
        cls.FEATURE_CSV_EXPORT = os.getenv('BELLE_CSV_EXPORT', str(cls.FEATURE_CSV_EXPORT)).lower() == 'true'
        return cls
    
    @classmethod
    def reset_defaults(cls):
        """Reset all configuration to defaults."""
        # Storage
        cls.BLOB_ROOT = "/mnt/internal/enhanced"
        cls.LOG_TABLE_NAME = "bellerophon_log_table"
        cls.SERVICE_ACCOUNT_PREFIX = "svc_aas"
        cls.TEST_MODE_SUFFIX = "_bellerophon_test"
        # Features
        cls.FEATURE_CSV_EXPORT = True
        cls.FEATURE_LOG_SCHEMA_EVOLUTION = True  # FIX: Enabled for production deployment - allows new columns to be added
        cls.VERBOSITY = 2  # NORMAL
        cls.ASCII_ART_ENABLED = True
        cls.EMOJI_ENABLED = True
        cls.PROGRESS_BAR_WIDTH = 50
        # Retry
        cls.MAX_MATERIALISE_RETRIES = 2
        # Logging
        cls.LOG_SCHEMA_JSON_MAX_LENGTH = 4000
        cls.WRITE_ROW_COUNT_TOLERANCE = 0.01
        cls.WRITE_ROW_COUNT_VALIDATION = True
        cls.LOG_RETENTION_DAYS = 90
        cls.LOG_STRIP_EMOJI = True
        cls.SCHEMA_DRIFT_ACTION = "warn"
        # Maintenance
        cls.ENABLE_INTELLIGENT_AUTO_VACUUM = True
        cls.ENABLE_INTELLIGENT_AUTO_OPTIMIZE = True
        cls.VACUUM_MIN_TABLE_SIZE_GB = 0.1
        cls.VACUUM_MIN_DAYS_SINCE_LAST = 7
        cls.VACUUM_MIN_DELETIONS_THRESHOLD = 0.10
        cls.OPTIMIZE_MIN_TABLE_SIZE_GB = 0.1
        cls.OPTIMIZE_MIN_SMALL_FILE_RATIO = 0.30
        # CSV
        cls.CSV_DELIMITER = ";"
        cls.CSV_ENCODING = "utf-8"
    
    class _ConfigContext:
        def __init__(self, **overrides):
            self._overrides = overrides
            self._saved = {}
        
        def __enter__(self):
            for key, value in self._overrides.items():
                if hasattr(BellerophonConfig, key):
                    self._saved[key] = getattr(BellerophonConfig, key)
                    setattr(BellerophonConfig, key, value)
            return self
        
        def __exit__(self, *exc):
            for key, value in self._saved.items():
                setattr(BellerophonConfig, key, value)
            return False
    
    @classmethod
    def temp_config(cls, **overrides):
        """Context manager for temporary config overrides (restored on exit)."""
        return cls._ConfigContext(**overrides)
    
    @classmethod
    def generate_instance_id(cls) -> str:
        """Generate unique instance ID for test mode isolation (YYYYMMDD_HHMMSS_uuid8)."""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        short_uuid = str(uuid.uuid4())[:8]
        return f"{timestamp}_{short_uuid}"
    
    @classmethod
    def _resolve_root(cls, layer: Optional[str] = None) -> str:
        if layer and cls.BLOB_ROOT_BASE:
            return f"{cls.BLOB_ROOT_BASE.rstrip('/')}_{layer}"
        return cls.BLOB_ROOT.rstrip('/')

    @classmethod
    def build_data_path(cls, target_database: str, subpipeline: Optional[str] = None,
                        layer: Optional[str] = None) -> str:
        root = cls._resolve_root(layer)
        if subpipeline:
            return f"{root}/{target_database}/{subpipeline}/{cls.DATA_FOLDER}/"
        else:
            return f"{root}/{target_database}/{cls.DATA_FOLDER}/"
    
    @classmethod
    def build_log_path(cls, target_database: str, layer: Optional[str] = None) -> str:
        root = cls._resolve_root(layer)
        return f"{root}/{target_database}/{cls.LOG_TABLE_NAME}"
    
    @classmethod
    def build_csv_export_path(cls, target_database: str, subpipeline: Optional[str] = None,
                             layer: Optional[str] = None) -> str:
        root = cls._resolve_root(layer)
        base = f"{root}/{cls.CSV_TEMP_FOLDER}/{target_database}"
        if subpipeline:
            base = f"{base}/{subpipeline}"
        return base
    
    @staticmethod
    def validate_and_enrich_table_config(
        spark,
        config: dict,
        table_key: str,
        global_force_rebuild: bool = False
    ) -> dict:
        """Validate config and enrich with runtime metadata (_table_exists, _actual_partitions, etc.)."""
        # Make a copy to avoid mutating the original
        enriched_config = config.copy()
        
        # Initialize metadata fields
        enriched_config['_table_exists'] = False
        enriched_config['_actual_partitions'] = []
        enriched_config['_partition_mismatch'] = False
        enriched_config['_missing_parameters'] = []
        enriched_config['_validation_errors'] = []
        enriched_config['_validation_warnings'] = []
        
        # ====================================================================
        # 1. VALIDATE REQUIRED FIELDS
        # ====================================================================
        required_fields = ['target_database', 'result_table_name', 'load_mode']
        for field in required_fields:
            if field not in config or not config[field]:
                enriched_config['_validation_errors'].append(
                    f"Missing required field: '{field}'"
                )
        
        # If basic validation failed, return early
        if enriched_config['_validation_errors']:
            return enriched_config
        
        # Extract config values
        target_database = config['target_database']
        result_table_name = config['result_table_name']
        load_mode = config['load_mode']
        partition_by = config.get('partition_by', [])
        merge_keys = config.get('merge_keys', [])
        merge_update_columns = config.get('merge_update_columns', [])
        
        # Construct full table name
        full_table_name = f"{target_database}.{result_table_name}"
        enriched_config['_full_table_name'] = full_table_name
        
        # ====================================================================
        # 2. CHECK TABLE EXISTENCE
        # ====================================================================
        try:
            detail_df = spark.sql(f"DESCRIBE DETAIL {full_table_name}")
            enriched_config['_table_exists'] = True
            
            # Extract partition columns from table metadata
            actual_partitions = detail_df.select("partitionColumns").first()[0]
            if actual_partitions is None:
                actual_partitions = []
            elif not isinstance(actual_partitions, list):
                actual_partitions = [actual_partitions]
            
            enriched_config['_actual_partitions'] = actual_partitions
            BellerophonTracer.trace(
                "validate_and_enrich", full_table_name, "TABLE_EXISTS",
                {"table_exists": True, "actual_partitions": actual_partitions,
                 "config_partition_by": partition_by,
                 "load_mode": load_mode}
            , caller_locals=locals())
            
        except Exception as e:
            # Table doesn't exist
            enriched_config['_table_exists'] = False
            enriched_config['_actual_partitions'] = []
            BellerophonTracer.trace(
                "validate_and_enrich", full_table_name, "TABLE_NOT_FOUND",
                {"table_exists": False, "config_partition_by": partition_by,
                 "load_mode": load_mode, "error": str(e)[:100]}
            , caller_locals=locals())
            
            # Check if table is required to exist for this load_mode
            requires_table = load_mode in ['merge', 'update', 'delete']
            if requires_table:
                enriched_config['_validation_errors'].append(
                    f"Table '{full_table_name}' does not exist, but load_mode '{load_mode}' "
                    f"requires an existing table. Create it first with 'full' or 'insert' mode."
                )
        
        # ====================================================================
        # 3. VALIDATE PARTITION CONSISTENCY (if table exists)
        # ====================================================================
        if enriched_config['_table_exists'] and partition_by:
            actual_partitions = enriched_config['_actual_partitions']
            
            # Check if partitions match
            if len(actual_partitions) == 0 and len(partition_by) > 0:
                # Table exists without partitions, but config specifies partitions
                enriched_config['_partition_mismatch'] = True
                BellerophonTracer.trace(
                    "validate_and_enrich", full_table_name, "PARTITION_MISMATCH_NO_PARTITIONS",
                    {"actual_partitions": [], "config_partition_by": partition_by,
                     "mismatch_type": "table_has_none_config_has_some"}
                , caller_locals=locals())
                enriched_config['_validation_errors'].append(
                    f"❌ PARTITIONING MISMATCH: Table '{full_table_name}' exists WITHOUT partitions, "
                    f"but config specifies partition_by={partition_by}.\n"
                    f"   Solutions:\n"
                    f"   1. DROP and recreate: DROP TABLE {full_table_name}; (then re-run with partition_by)\n"
                    f"   2. Change to 'full' mode: Use load_mode='full' to recreate with partitions\n"
                    f"   3. Remove partition_by: If partitions not needed, remove from config\n"
                    f"   4. Set force_full_rebuild=True: Will recreate table with correct partitions"
                )
            elif actual_partitions != partition_by and len(actual_partitions) > 0:
                # Table partitioned but with different columns
                enriched_config['_partition_mismatch'] = True
                BellerophonTracer.trace(
                    "validate_and_enrich", full_table_name, "PARTITION_MISMATCH_DIFFERENT",
                    {"actual_partitions": actual_partitions, "config_partition_by": partition_by,
                     "mismatch_type": "different_partition_columns"}
                , caller_locals=locals())
                enriched_config['_validation_errors'].append(
                    f"❌ PARTITIONING MISMATCH: Table '{full_table_name}' partitioned by {actual_partitions}, "
                    f"but config specifies partition_by={partition_by}.\n"
                    f"   Solutions:\n"
                    f"   1. Update config to match table: partition_by={actual_partitions}\n"
                    f"   2. DROP and recreate: DROP TABLE {full_table_name}; (then re-run)\n"
                    f"   3. Set force_full_rebuild=True: Will recreate with new partitioning"
                )
            else:
                # Partitions match or table doesn't have partitions and config doesn't specify any
                enriched_config['_partition_mismatch'] = False
                BellerophonTracer.trace(
                    "validate_and_enrich", full_table_name, "PARTITIONS_OK",
                    {"actual_partitions": actual_partitions, "config_partition_by": partition_by,
                     "match": True}
                , caller_locals=locals())
        
        # ====================================================================
        # 4. VALIDATE LOAD_MODE SPECIFIC REQUIREMENTS
        # ====================================================================
        
        # Parse load_mode for refresh_n_days
        if load_mode.startswith("refresh_n_days"):
            # Validate refresh_n_days format
            if '-' not in load_mode:
                enriched_config['_validation_errors'].append(
                    f"Invalid refresh_n_days format: '{load_mode}'. "
                    f"Must be 'refresh_n_days-N' (e.g., 'refresh_n_days-7')"
                )
            else:
                try:
                    n_days = int(load_mode.split('-')[-1])
                    enriched_config['_refresh_n_days'] = n_days
                except ValueError:
                    enriched_config['_validation_errors'].append(
                        f"Invalid refresh_n_days format: '{load_mode}'. "
                        f"N must be an integer (e.g., 'refresh_n_days-7')"
                    )
            
            # Validate partition_by for refresh_n_days
            if not partition_by or len(partition_by) != 1:
                enriched_config['_validation_errors'].append(
                    f"refresh_n_days mode requires exactly ONE partition column (e.g., ['date_key']). "
                    f"Got: {partition_by}"
                )
        
        # Validate merge mode requirements
        elif load_mode == "merge":
            if not merge_keys:
                enriched_config['_missing_parameters'].append('merge_keys')
                enriched_config['_validation_errors'].append(
                    f"load_mode 'merge' requires 'merge_keys' parameter"
                )
            # merge_update_columns auto-derived at materialization if omitted
            # (all DataFrame columns minus merge_keys)
        
        # Validate update mode requirements
        elif load_mode == "update":
            if not merge_keys:
                enriched_config['_missing_parameters'].append('merge_keys')
                enriched_config['_validation_errors'].append(
                    f"load_mode 'update' requires 'merge_keys' parameter (used as WHERE condition)"
                )
        
        # Validate delete mode requirements
        elif load_mode == "delete":
            if not merge_keys:
                enriched_config['_missing_parameters'].append('merge_keys')
                enriched_config['_validation_errors'].append(
                    f"load_mode 'delete' requires 'merge_keys' parameter (used as WHERE condition)"
                )
        
        # ====================================================================
        # 5. DETERMINE EFFECTIVE FORCE_REBUILD
        # ====================================================================
        # Per-table setting overrides global setting
        # If not specified in config, use global setting
        per_table_rebuild = config.get('force_full_rebuild', None)
        
        if per_table_rebuild is not None:
            # Explicit per-table setting
            enriched_config['_effective_force_rebuild'] = per_table_rebuild
            enriched_config['_rebuild_source'] = 'per_table_config'
        else:
            # Use global setting
            enriched_config['_effective_force_rebuild'] = global_force_rebuild
            enriched_config['_rebuild_source'] = 'global_parameter'
        
        BellerophonTracer.trace(
            "validate_and_enrich", full_table_name, "FINAL_ENRICHED_STATE",
            {"_table_exists": enriched_config.get('_table_exists'),
             "_partition_mismatch": enriched_config.get('_partition_mismatch'),
             "_effective_force_rebuild": enriched_config.get('_effective_force_rebuild'),
             "_rebuild_source": enriched_config.get('_rebuild_source'),
             "_actual_partitions": enriched_config.get('_actual_partitions', []),
             "config_partition_by": partition_by,
             "load_mode": load_mode}
        , caller_locals=locals())

        # ====================================================================
        # 6. WARNINGS FOR BEST PRACTICES
        # ====================================================================
        
        # Warn if using insert mode without monitored_date_column
        if load_mode == 'insert' and not config.get('monitored_date_column'):
            enriched_config['_validation_warnings'].append(
                f"Table '{result_table_name}' uses load_mode='insert' without monitored_date_column. "
                f"Cannot track last load date for incremental logging."
            )
        
        # Warn if partition_by specified but not used by load_mode
        if partition_by and load_mode not in ['full', 'full_if_not_exists', 'insert'] and not load_mode.startswith('refresh_n_days'):
            enriched_config['_validation_warnings'].append(
                f"partition_by={partition_by} specified, but load_mode '{load_mode}' may not use it. "
                f"Partitioning is primarily used by 'full', 'insert', and 'refresh_n_days-N' modes."
            )
        
        # Warn if force_full_rebuild will recreate table with partition mismatch
        if (enriched_config['_effective_force_rebuild'] and 
            enriched_config['_table_exists'] and 
            enriched_config['_partition_mismatch']):
            enriched_config['_validation_warnings'].append(
                f"force_full_rebuild=True will recreate table '{full_table_name}' with new partitioning: "
                f"{partition_by} (current: {enriched_config['_actual_partitions']})"
            )
        
        return enriched_config

# All config accessed via BellerophonConfig.X or belle.Config.X
# No module-level constant extraction — prevents namespace pollution on %run.



# COMMAND ----------

# DBTITLE 1,ErrorCode & Table Readiness
# ============================================================================
# STRUCTURED ERROR CODES
# ============================================================================

class BellerophonErrorCode:
    """Structured error codes for logging and alerting."""
    
    # Success
    SUCCESS = "BELLE-000"
    
    # Configuration Errors (001-009)
    TABLE_NOT_FOUND = "BELLE-001"
    MERGE_KEY_MISSING = "BELLE-002"
    INVALID_LOAD_MODE = "BELLE-003"
    CONFIG_VALIDATION_FAILED = "BELLE-004"
    DEPENDENCY_CYCLE_DETECTED = "BELLE-005"
    
    # Data Errors (010-019)
    SCHEMA_MISMATCH = "BELLE-010"
    DATA_QUALITY_FAILED = "BELLE-011"
    NULL_VALUE_VIOLATION = "BELLE-012"
    
    # Execution Errors (020-029)
    DELTA_OPERATION_FAILED = "BELLE-020"
    CSV_EXPORT_FAILED = "BELLE-021"
    LOGGING_FAILED = "BELLE-022"
    PERSIST_FAILED = "BELLE-023"
    
    # Resource Errors (030-039)
    OOM_ERROR = "BELLE-030"
    TIMEOUT_ERROR = "BELLE-031"
    CLUSTER_ERROR = "BELLE-032"
    
    # Permission Errors (040-049)
    PERMISSION_DENIED = "BELLE-040"
    CATALOG_ACCESS_DENIED = "BELLE-041"
    
    # Unknown/Generic
    UNKNOWN_ERROR = "BELLE-999"
    
    @classmethod
    def get_description(cls, code: str) -> str:
        """Get human-readable description for error code."""
        descriptions = {
            cls.SUCCESS: "Operation completed successfully",
            cls.TABLE_NOT_FOUND: "Target table does not exist",
            cls.MERGE_KEY_MISSING: "Merge operation missing required merge keys",
            cls.INVALID_LOAD_MODE: "Invalid or unsupported load mode specified",
            cls.CONFIG_VALIDATION_FAILED: "Configuration validation failed",
            cls.DEPENDENCY_CYCLE_DETECTED: "Circular dependency detected in DAG",
            cls.SCHEMA_MISMATCH: "DataFrame schema does not match target table",
            cls.DATA_QUALITY_FAILED: "Data quality validation failed",
            cls.NULL_VALUE_VIOLATION: "Null values found in non-nullable columns",
            cls.DELTA_OPERATION_FAILED: "Delta Lake operation failed",
            cls.CSV_EXPORT_FAILED: "CSV export operation failed",
            cls.LOGGING_FAILED: "Failed to write execution log",
            cls.PERSIST_FAILED: "DataFrame persist/cache operation failed",
            cls.OOM_ERROR: "Out of memory error",
            cls.TIMEOUT_ERROR: "Operation timed out",
            cls.CLUSTER_ERROR: "Cluster communication or execution error",
            cls.PERMISSION_DENIED: "Insufficient permissions",
            cls.CATALOG_ACCESS_DENIED: "Unity Catalog access denied",
            cls.UNKNOWN_ERROR: "Unknown or unclassified error"
        }
        return descriptions.get(code, "No description available")



# ============================================================================
# TABLE READINESS - Force Rebuild & Config Validation (v1.2.8)
# ============================================================================
# Single point of truth for table lifecycle before materialization.
# Called by BellerophonOrchestrator.run() before any writes.
# ============================================================================

def ensure_table_ready(spark_session, table_config, force_rebuild=False, verbose=None):
    """Ensure table is ready: force_rebuild drops it, else validate partitions match config."""
    if verbose is None:
        verbose = BellerophonConfig.VERBOSITY >= BellerophonConfig.VERBOSE
    db = table_config["target_database"]
    tbl = table_config["result_table_name"]
    full_name = f"{db}.{tbl}"
    config_partitions = table_config.get("partition_by", []) or []

    result = {
        "table_name": full_name,
        "action": None,
        "existed_before": False,
        "partition_match": None,
        "actual_partitions": [],
        "config_partitions": config_partitions,
    }

    # --- Check existence & current partitions ---
    table_exists = False
    actual_partitions = []
    try:
        detail = spark_session.sql(f"DESCRIBE DETAIL {full_name}").collect()
        if detail:
            table_exists = True
            raw = detail[0]["partitionColumns"]
            actual_partitions = list(raw) if raw else []
    except Exception:
        table_exists = False

    result["existed_before"] = table_exists
    result["actual_partitions"] = actual_partitions

    BellerophonTracer.trace(
        "ensure_table_ready", full_name, "CHECK_EXISTENCE",
        {"table_exists": table_exists, "actual_partitions": actual_partitions,
         "config_partitions": config_partitions, "force_rebuild": force_rebuild}
    , caller_locals=locals())


    # -- PATH 1: Force rebuild ------------------------------------------------
    if force_rebuild:
        if table_exists:
            # Guard: NEVER drop the log table during force rebuild
            if tbl == BellerophonConfig.LOG_TABLE_NAME:
                result["action"] = "validated_ok"
                if verbose:
                    print(f"  [prep] {full_name}: PROTECTED (log table excluded from force rebuild)")
                return result
            spark_session.sql(f"DROP TABLE IF EXISTS {full_name}")
            result["action"] = "dropped_for_rebuild"
            BellerophonTracer.trace(
                "ensure_table_ready", full_name, "PATH_1_FORCE_DROP",
                {"action": "dropped_for_rebuild", "table_existed": True,
                 "config_partitions": config_partitions}
            , caller_locals=locals())
            if verbose:
                part_info = f", partitioned by {config_partitions}" if config_partitions else ""
                print(f"  [prep] {full_name}: DROPPED (force_rebuild=True). Will recreate{part_info}")
        else:
            result["action"] = "will_create_new"
            BellerophonTracer.trace(
                "ensure_table_ready", full_name, "PATH_1_WILL_CREATE",
                {"action": "will_create_new", "table_existed": False,
                 "force_rebuild": True, "config_partitions": config_partitions}
            , caller_locals=locals())
            if verbose:
                part_info = f", partitioned by {config_partitions}" if config_partitions else ""
                print(f"  [prep] {full_name}: Does not exist. Will create{part_info}")
        return result

    # -- PATH 2: Table doesn't exist (no force) -------------------------------
    if not table_exists:
        result["action"] = "will_create_new"
        BellerophonTracer.trace(
            "ensure_table_ready", full_name, "PATH_2_WILL_CREATE",
            {"action": "will_create_new", "table_existed": False,
             "force_rebuild": False, "config_partitions": config_partitions}
        , caller_locals=locals())
        if verbose:
            part_info = f", partitioned by {config_partitions}" if config_partitions else ""
            print(f"  [prep] {full_name}: Does not exist. Will create{part_info}")
        return result

    # -- PATH 3: Table exists, validate config vs actual -----------------------
    partitions_match = (actual_partitions == config_partitions)
    result["partition_match"] = partitions_match

    if partitions_match:
        result["action"] = "validated_ok"
        BellerophonTracer.trace(
            "ensure_table_ready", full_name, "PATH_3_VALIDATED_OK",
            {"action": "validated_ok", "actual_partitions": actual_partitions,
             "config_partitions": config_partitions, "match": True}
        , caller_locals=locals())
        if verbose:
            print(f"  [prep] {full_name}: OK - partitions match config "
                  f"{config_partitions}")
    else:
        # Mismatch -> auto-fix by dropping
        spark_session.sql(f"DROP TABLE IF EXISTS {full_name}")
        result["action"] = "dropped_for_partition_mismatch"
        BellerophonTracer.trace(
            "ensure_table_ready", full_name, "PATH_3_MISMATCH_DROP",
            {"action": "dropped_for_partition_mismatch",
             "actual_partitions": actual_partitions,
             "config_partitions": config_partitions, "match": False}
        , caller_locals=locals())
        if verbose:
            print(f"  [prep] {full_name}: PARTITION MISMATCH - "
                  f"config={config_partitions}, actual={actual_partitions}. DROPPED.")

    return result


def ensure_all_tables_ready(spark_session, tables_config, force_rebuild=False, verbose=None):
    """Apply ensure_table_ready to all tables_config entries and update metadata."""
    if verbose is None:
        verbose = BellerophonConfig.VERBOSITY >= BellerophonConfig.VERBOSE
    if verbose:
        belle_banner("TABLE READINESS CHECK", level=BellerophonConfig.VERBOSE)
        mode = "FORCE REBUILD" if force_rebuild else "INCREMENTAL (validate only)"
        print(f"Mode: {mode}")
        print("=" * 80)

    results = {}
    counters = {"dropped_for_rebuild": 0, "will_create_new": 0,
                "validated_ok": 0, "dropped_for_partition_mismatch": 0}

    for key, config in tables_config.items():
        # Determine per-table effective force rebuild
        per_table = config.get("force_full_rebuild", None)
        effective = per_table if per_table is not None else force_rebuild

        BellerophonTracer.trace(
            "ensure_all_tables_ready", f"{config.get('target_database','?')}.{config.get('result_table_name','?')}",
            "PER_TABLE_REBUILD_DECISION",
            {"config_key": key, "per_table_force": per_table,
             "global_force_rebuild": force_rebuild, "effective_force": effective,
             "config_partition_by": config.get("partition_by", []),
             "config_load_mode": config.get("load_mode", "?")}
        , caller_locals=locals())
        r = ensure_table_ready(spark_session, config, force_rebuild=effective, verbose=verbose)
        results[key] = r
        counters[r["action"]] += 1

        # Update enriched metadata so materialise_dataframe sees correct state
        if r["action"] in ("dropped_for_rebuild", "dropped_for_partition_mismatch", "will_create_new"):
            config["_table_exists"] = False
            config["_partition_mismatch"] = False  # resolved by drop
            if r["action"] in ("dropped_for_rebuild", "dropped_for_partition_mismatch"):
                config["_effective_force_rebuild"] = True
        elif r["action"] == "validated_ok":
            config["_table_exists"] = True
            config["_partition_mismatch"] = False
        BellerophonTracer.trace(
            "ensure_all_tables_ready", f"{config.get('target_database','?')}.{config.get('result_table_name','?')}",
            "METADATA_UPDATED",
            {"action": r["action"],
             "_table_exists": config.get("_table_exists"),
             "_partition_mismatch": config.get("_partition_mismatch"),
             "_effective_force_rebuild": config.get("_effective_force_rebuild")}
        , caller_locals=locals())

    if verbose:
        print("-" * 80)
        print(f"  Dropped (force rebuild):       {counters['dropped_for_rebuild']}")
        print(f"  Will create (new):             {counters['will_create_new']}")
        print(f"  Validated OK (no change):      {counters['validated_ok']}")
        print(f"  Dropped (partition mismatch):  {counters['dropped_for_partition_mismatch']}")
        belle_banner(level=BellerophonConfig.VERBOSE)

    return results



# COMMAND ----------

# DBTITLE 1,BellerophonOutputRegistry
# ============================================================================
# BELLEROPHON OUTPUT REGISTRY
# ============================================================================

class BellerophonOutputRegistry:
    """Centralized DataFrame registry for inter-stage DAG data passing."""
    _outputs = {}
    
    @staticmethod
    def set_output(key: str, value):
        """Store a DataFrame in the registry."""
        # Note: Overwrite check removed - DataFrames are populated by cells before orchestration,
        # not by Belle itself. Overwrite warnings were misleading to users.
        BellerophonOutputRegistry._outputs[key] = value
    
    @staticmethod
    def get_output(key: str):
        """Retrieve a DataFrame from the registry."""
        return BellerophonOutputRegistry._outputs.get(key)
    
    @staticmethod
    def clear_outputs():
        """Clear all outputs from the registry."""
        BellerophonOutputRegistry._outputs.clear()
    
    @staticmethod
    def get_all_keys():
        """Get all registered output keys."""
        return list(BellerophonOutputRegistry._outputs.keys())
    
    @staticmethod
    def check_health(expected_keys):
        """
        Check registry health and detect if it has been cleared.
        
        Args:
            expected_keys: List of expected DataFrame keys (format: "database_tablename")
            
        Returns:
            Dict with health status and actionable guidance
        """
        found_keys = [k for k in expected_keys if k in BellerophonOutputRegistry._outputs 
                      and BellerophonOutputRegistry._outputs[k] is not None]
        missing_keys = [k for k in expected_keys if k not in found_keys]
        
        found_count = len(found_keys)
        missing_count = len(missing_keys)
        total = len(expected_keys)
        
        is_healthy = missing_count == 0
        
        return {
            'healthy': is_healthy,
            'found': found_count,
            'missing': missing_count,
            'total': total,
            'missing_keys': missing_keys,
            'found_keys': found_keys
        }


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def resilient_materialise_table(
    materialise_func,
    input_df,
    conf,
    run_id,
    interactive_mode,
    sample_rows,
    dag_stage,
    custom_csv_removals,
    max_workers
):
    """
    OOM Retry Logic (Belle's resilience layer).
    Attempts to materialise a table, catching OutOfMemoryError and retrying once with reduced parallelism.
    
    NOTE Issue #36: This function is NOT currently used by the orchestrator.
    - Orchestrator calls self.materialise_table() (class method at line 2736)
    - Class method directly calls bellerophon_materialise_dataframe without OOM retry
    - This function has signature mismatches (missing external_run_id, execution_context, retry_count)
    
    To fix: Add OOM retry logic directly in the class method or update this function's signature.
    Also address Issue #37: OOM detection only catches Py4JJavaError, not SparkException or Python OOMs.
    """
    try:
        return materialise_func(
            input_df,
            conf,
            run_id,
            interactive_mode,
            sample_rows,
            dag_stage,
            custom_csv_removals
        )
    except Py4JJavaError as e:
        if 'OutOfMemoryError' in str(e):
            print("🔄 [Belle OOM Recovery] OutOfMemoryError detected! Retrying with reduced parallelism (max_workers=1).")
            try:
                return materialise_func(
                    input_df,
                    conf,
                    run_id,
                    interactive_mode,
                    sample_rows,
                    dag_stage,
                    custom_csv_removals,
                    1
                )
            except Exception as e2:
                print("❌ [Belle OOM Recovery] Still failed after reducing parallelism.")
                raise
        else:
            raise



# COMMAND ----------

# DBTITLE 1,BellerophonUtils
# ============================================================================
# BELLE UTILS CLASS
# ============================================================================

class BellerophonUtils:
    """General utility functions for Bellerophon orchestration."""

    @staticmethod
    def nowstr() -> str:
        """Return current timestamp with UTC timezone indicator"""
        return datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')

    @staticmethod
    def get_current_user() -> str:
        try:
            return spark.sql("SELECT current_user()").collect()[0][0]
        except Exception as e:
            return f"ERROR: {e}"

    @staticmethod
    def is_interactive_notebook() -> bool:
        """
        Determine if code is running in interactive notebook mode or production.
        Uses enhanced service account detection for ADF compatibility.
        """
        try:
            is_service_acct, _ = BellerophonUtils.detect_service_account()
            return not is_service_acct
        except Exception:
            # Default to interactive if detection fails
            return True
    
    @staticmethod
    def apply_test_suffix(table_name: str) -> str:
        """Appends test mode suffix to the table name if test mode is activated."""
        try:
            test_mode = globals().get('force_bellerophon_test_mode', False)
            suffix = BellerophonConfig.TEST_MODE_SUFFIX
            if test_mode and not table_name.endswith(suffix):
                return f"{table_name}{suffix}"
            return table_name
        except Exception:
            return table_name

    @staticmethod
    def build_blob_target_dir(target_database: str, subpipeline: Optional[str]=None) -> str:
        """
        Build blob storage target directory path.
        Delegates to BellerophonConfig for centralized path management.
        """
        return BellerophonConfig.build_data_path(target_database, subpipeline)

    @staticmethod
    def get_target_cube_csv_path(blob_target_dir: str, target_database: str, result_table_name: str) -> Tuple[str, str]:
        from pathlib import Path
        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        sequence = "0001"
        full_table_name_local = f"{target_database}.{result_table_name}"
        blob_path = Path(blob_target_dir.rstrip('/'))
        parts = blob_path.parts
        data_folder = BellerophonConfig.DATA_FOLDER
        csv_temp_folder = BellerophonConfig.CSV_TEMP_FOLDER
        try:
            engine_idx = parts.index(target_database)
        except ValueError:
            raise ValueError("target_database not found in blob_target_dir")
        try:
            data_idx = parts.index(data_folder)
        except ValueError:
            raise ValueError(f"'{data_folder}' not found in blob_target_dir")
        subpipeline_local = None
        if data_idx - engine_idx == 2:
            subpipeline_local = parts[engine_idx + 1]
        csv_base = blob_path / csv_temp_folder / target_database
        if subpipeline_local:
            csv_base = csv_base / subpipeline_local
        csv_dir = csv_base / full_table_name_local
        csv_filename = f"{full_table_name_local}_{now}_{sequence}.csv"
        return str(csv_dir), csv_filename

    @staticmethod
    def rename_csv_part_file(target_dir: str, desired_filename: str = None) -> None:
        """Rename Spark's part-* output to a clean single CSV file and remove metadata."""
        try:
            import builtins as _bi
            _dbutils = getattr(_bi, 'dbutils', None) or globals().get('dbutils')
            if not _dbutils:
                return
            _files = _dbutils.fs.ls(target_dir)
            _part = next((f for f in _files if f.name.startswith("part-")), None)
            if not _part:
                return
            _final_name = desired_filename or "data.csv"
            _final_path = f"{target_dir.rstrip('/')}/{_final_name}"
            _dbutils.fs.mv(_part.path, _final_path)
            for _f in _files:
                if _f.name.startswith("_"):
                    try:
                        _dbutils.fs.rm(_f.path)
                    except Exception:
                        pass
        except Exception:
            pass

    @staticmethod
    def get_spark_job_queue_size() -> int:
        try:
            job_ids = spark.sparkContext.statusTracker.getActiveJobIds()
            return len(job_ids)
        except Exception:
            return 0

    @staticmethod
    def try_load_from_table(target_database: str, result_table_name: str):
        """
        Dimension Fallback Loading.
        Try to load DataFrame from existing table when not in output registry.
        """
        try:
            full_table_name = f"{target_database}.{result_table_name}"
            print(f"[Bellerophon][LOAD FROM TABLE] Attempting to load {full_table_name}...")
            df = spark.table(full_table_name)
            row_count = df.count()
            print(f"[Bellerophon][LOAD FROM TABLE] Successfully loaded {full_table_name} ({row_count:,} rows)")
            return df
        except Exception as e:
            print(f"[Bellerophon][LOAD FROM TABLE] Could not load {target_database}.{result_table_name}: {str(e)[:100]}")
            return None

    @staticmethod
    def check_optional_dependencies() -> Dict[str, bool]:
        """Checks for optional visualization dependencies."""
        deps = {}
        for mod in ['networkx', 'matplotlib', 'pyvis']:
            try:
                __import__(mod)
                deps[mod] = True
            except ImportError:
                deps[mod] = False
        return deps

    @staticmethod
    def get_cluster_info() -> Dict[str, str]:
        """
        Get Spark cluster information for logging and debugging.
        
        Returns:
            Dictionary with cluster_id, cluster_name, spark_version, dbr_version
        """
        try:
            cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown")
            cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "unknown")
            spark_version = spark.version
            
            # Try to get DBR version
            try:
                dbr_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "unknown")
            except Exception:
                dbr_version = "unknown"
            
            return {
                "cluster_id": cluster_id,
                "cluster_name": cluster_name,
                "spark_version": spark_version,
                "dbr_version": dbr_version
            }
        except Exception as e:
            return {
                "cluster_id": "unknown",
                "cluster_name": "unknown",
                "spark_version": "unknown",
                "dbr_version": "unknown"
            }
    
    @staticmethod
    def get_execution_context() -> Dict[str, Any]:
        """
        Get execution context information (notebook path, job info, etc.).
        Useful for tracking where Belle is executed from.
        
        Returns:
            Dictionary with notebook_path, job_id, job_run_id, run_name
        """
        try:
            # Try to get notebook path
            try:
                dbutils_available = 'dbutils' in globals()
                if dbutils_available:
                    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
                else:
                    notebook_path = "unknown"
            except Exception:
                notebook_path = "unknown"
            
            # Try to get job information
            try:
                job_id = spark.conf.get("spark.databricks.job.id", None)
                job_run_id = spark.conf.get("spark.databricks.job.runId", None)
                run_name = spark.conf.get("spark.databricks.job.name", None)
            except Exception:
                job_id = None
                job_run_id = None
                run_name = None
            
            return {
                "notebook_path": notebook_path,
                "job_id": job_id,
                "job_run_id": job_run_id,
                "run_name": run_name
            }
        except Exception as e:
            return {
                "notebook_path": "unknown",
                "job_id": None,
                "job_run_id": None,
                "run_name": None
            }
    
    @staticmethod
    def get_table_row_count(table_name: str) -> int:
        """
        Get current row count of a table.
        Used for before/after comparisons in logging.
        
        Args:
            table_name: Fully qualified table name (catalog.schema.table or database.table)
            
        Returns:
            Row count or 0 if table doesn't exist
        """
        try:
            return spark.table(table_name).count()
        except Exception:
            return 0
    
    @staticmethod
    def detect_service_account() -> Tuple[bool, str]:
        """
        Enhanced service account detection for ADF and production environments.
        
        Returns:
            Tuple of (is_service_account: bool, account_name: str)
        """
        try:
            current_user = BellerophonUtils.get_current_user()
            
            # Check if username starts with service account prefix
            if current_user.lower().startswith(BellerophonConfig.SERVICE_ACCOUNT_PREFIX.lower()):
                return (True, current_user)
            
            # Check if running as a job (non-interactive)
            try:
                job_id = spark.conf.get("spark.databricks.job.id", None)
                if job_id:
                    return (True, current_user)
            except Exception:
                pass
            
            # Check for email pattern (interactive users typically have emails)
            if not re.match(r"[^@]+@[^@]+\.[^@]+", current_user):
                return (True, current_user)
            
            return (False, current_user)
            
        except Exception as e:
            return (False, "unknown")
    
    @staticmethod
    def print_break(msg: str = '', level: int = 2) -> None:
        """Print a section break. Respects VERBOSITY and ASCII_ART_ENABLED."""
        if BellerophonConfig.VERBOSITY < level:
            return
        if BellerophonConfig.ASCII_ART_ENABLED:
            print('\n' + '='*40)
            if msg:
                print(f"[{BellerophonUtils.nowstr()}] === {msg} ===")
            print('='*40 + '\n')
        elif msg:
            print(f"\n[{BellerophonUtils.nowstr()}] {msg}\n")



# COMMAND ----------

# DBTITLE 1,BellerophonLogger
# ============================================================================
# BELLEROPHON LOGGER CLASS
# ============================================================================

class BellerophonLogger:
    """Writes orchestration logs to a Delta log table per target database."""
    
    # Class-level flag to track if setup messages have been shown per database
    _logging_setup_shown = {}
    
    @staticmethod
    def reset_logging_messages(target_database=None):
        """Reset the logging setup message flag for a new orchestration run."""
        if target_database:
            BellerophonLogger._logging_setup_shown[target_database] = False
        else:
            BellerophonLogger._logging_setup_shown.clear()

    @staticmethod
    def write_log(logging_df, target_database, logging_schema, run_id=None, interactive_mode=None):
        log_db = target_database
        log_table = f"{log_db}.{BellerophonConfig.LOG_TABLE_NAME}"
        log_table_path = BellerophonConfig.build_log_path(target_database)

        def table_exists():
            try:
                return spark.catalog.tableExists(log_table)
            except Exception:
                return False

        def delta_data_exists():
            # FIX #13: Guard dbutils dependency - fail gracefully in non-Databricks environments
            try:
                if 'dbutils' not in globals():
                    return False
                files = dbutils.fs.ls(log_table_path)
                return any(f.name.endswith('.parquet') or f.name.startswith('_delta_log') for f in files)
            except Exception:
                return False

        # Check if setup messages have been shown for this database
        show_setup_messages = target_database not in BellerophonLogger._logging_setup_shown
        
        if show_setup_messages:
            print(f"[LOGGING] Checking table and data existence for '{log_table}' at '{log_table_path}'...")
            BellerophonLogger._logging_setup_shown[target_database] = True
        
        tbl_exists = table_exists()
        data_exists = delta_data_exists()

        # Interactive mode: logs displayed only (not persisted to tables)
        # Note: Individual table success messages already shown by orchestrator
        if interactive_mode:
            return

        # CASE 1: Neither table nor data exists - first run
        if not tbl_exists and not data_exists:
            try:
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {log_db}")
                # FIX: Use append (not overwrite) to avoid protocol conflict when
                # multiple parallel threads hit CASE 1 simultaneously on an empty path.
                # mergeSchema ensures the first writer's schema wins, latecomers merge.
                logging_df.write.format("delta").mode("append") \
                    .option("mergeSchema", "true").save(log_table_path)
                spark.sql(f"CREATE TABLE IF NOT EXISTS {log_table} USING DELTA LOCATION '{log_table_path}'")
            except Exception as e:
                # Retry once: another thread may have just created the path
                import time; time.sleep(0.5)
                try:
                    logging_df.write.format("delta").mode("append") \
                        .option("mergeSchema", "true").save(log_table_path)
                    spark.sql(f"CREATE TABLE IF NOT EXISTS {log_table} USING DELTA LOCATION '{log_table_path}'")
                except Exception:
                    pass  # Non-fatal: log write failed but pipeline continues
                return

        # CASE 2: Data exists but table does not - register table
        elif data_exists and not tbl_exists:
            pass  # Silent: register and append
            try:
                # FIX #9: Unity Catalog managed schemas don't support LOCATION
                # UC automatically manages locations - don't specify custom path
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {log_db}")
                spark.sql(f"CREATE TABLE IF NOT EXISTS {log_table} USING DELTA LOCATION '{log_table_path}'")
                print(f"[LOGGING] Table registered in metastore.")
            except Exception:
                pass  # Table registered by another thread — continue to append
            try:
                # FIX #11-12: Controlled schema evolution - only allow if explicitly enabled
                # Prevents schema drift from mixed success/failure schemas
                # Only use mergeSchema if BellerophonConfig allows it
                if BellerophonConfig.FEATURE_LOG_SCHEMA_EVOLUTION:
                    logging_df.write.format("delta").mode("append") \
                        .option("mergeSchema", "true") \
                        .save(log_table_path)
                    print(f"[LOGGING] Appended new log after table registration (schema evolution enabled).")
                else:
                    # Strict schema enforcement - will fail if schemas don't match
                    logging_df.write.format("delta").mode("append").save(log_table_path)
                    print(f"[LOGGING] Appended new log after table registration (strict schema).")
            except Exception as e:
                print(f"[ERROR] Could not append log after registering table: {e}")
                return

        # CASE 3: Table exists, but data does not - treat as first run
        elif tbl_exists and not data_exists:
            print(f"[LOGGING] Table exists but no delta data found. Writing data as first run.")
            try:
                logging_df.write.format("delta").mode("overwrite").save(log_table_path)
                print(f"[LOGGING] Log data written (first run with empty table).")
            except Exception as e:
                print(f"[ERROR] Could not write log data to existing but empty table: {e}")
                return

        # CASE 4: Both table and data exist - append with controlled schema evolution
        elif tbl_exists and data_exists:
            try:
                # FIX #11-12: Controlled schema evolution - only allow if explicitly enabled
                # Prevents schema drift from mixed success/failure schemas
                if BellerophonConfig.FEATURE_LOG_SCHEMA_EVOLUTION:
                    # Schema evolution enabled: allows adding new columns without dropping table
                    logging_df.write.format("delta").mode("append") \
                        .option("mergeSchema", "true") \
                        .save(log_table_path)
                    if show_setup_messages:
                        print(f"[LOGGING] Appending logs with schema evolution support to {log_table}.")
                else:
                    # FIX #14: Strict schema enforcement - ensures stable schema contract
                    # Will fail if schemas don't match, preventing silent drift
                    logging_df.write.format("delta").mode("append").save(log_table_path)
                    if show_setup_messages:
                        print(f"[LOGGING] Appending logs with strict schema to {log_table}.")
            except Exception as e:
                print(f"[ERROR] Could not append logs to table '{log_table}': {e}")
                return


    @staticmethod
    def purge_logs(spark_session, target_database: str) -> bool:
        """Explicitly drop the log table. Only call when you truly want to lose all history."""
        log_table = f"{target_database}.{BellerophonConfig.LOG_TABLE_NAME}"
        try:
            spark_session.sql(f"DROP TABLE IF EXISTS {log_table}")
            print(f"[LOGGING] Purged log table: {log_table}")
            return True
        except Exception as e:
            print(f"[LOGGING] Purge failed: {e}")
            return False

    @staticmethod
    def cleanup_old_logs(spark_session, target_database: str, retention_days: int = None):
        """Delete log entries older than retention_days.
        
        Args:
            spark_session: Active SparkSession
            target_database: Database containing the log table
            retention_days: Days to retain (None = use config default)
        
        Returns:
            Number of rows deleted, or -1 on error
        """
        retention = retention_days or BellerophonConfig.LOG_RETENTION_DAYS
        if retention <= 0:
            return 0
        
        log_table = f"{target_database}.{BellerophonConfig.LOG_TABLE_NAME}"
        try:
            if not spark_session.catalog.tableExists(log_table):
                return 0
            
            cutoff = f"current_date() - INTERVAL {retention} DAYS"
            count_before = spark_session.table(log_table).count()
            spark_session.sql(f"""
                DELETE FROM {log_table}
                WHERE execution_start_time < {cutoff}
            """)
            count_after = spark_session.table(log_table).count()
            deleted = count_before - count_after
            if deleted > 0:
                print(f"[LOGGING] Cleaned up {deleted:,} log entries older than {retention} days")
            return deleted
        except Exception as e:
            print(f"[LOGGING] Log cleanup failed (non-blocking): {e}")
            return -1


# v1.2.12: Emoji stripping utility for log sanitization
import re as _re_module
_EMOJI_PATTERN = _re_module.compile(
    "["
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F680-\U0001F6FF"  # transport & map
    "\U0001F1E0-\U0001F1FF"  # flags
    "\U00002702-\U000027B0"  # dingbats
    "\U0001F900-\U0001F9FF"  # supplemental symbols
    "\U00002600-\U000026FF"  # misc symbols
    "\U0000FE00-\U0000FE0F"  # variation selectors
    "\U0000200D"              # zero width joiner
    "]+", flags=_re_module.UNICODE
)

def _strip_emoji(text: str) -> str:
    """Remove emoji characters from text for clean log storage."""
    if not isinstance(text, str):
        return text
    return _EMOJI_PATTERN.sub('', text).strip()



# COMMAND ----------

# DBTITLE 1,Validators & Production Features
# ============================================================================
# BELLEROPHON CONFIG VALIDATOR
# ============================================================================

class BellerophonConfigValidator:
    """Validates TABLES_CONFIG structure and dependencies."""
    
    @staticmethod
    def validate(config: Dict[str, Any]) -> None:
        """Validates the TABLES_CONFIG dictionary for required keys and proper structure."""
        # Build normalized lookup
        normalized_lookup = {}
        for table, conf in config.items():
            for key in ['target_database', 'result_table_name', 'dependencies']:
                if key not in conf:
                    raise ValueError(f"Missing key '{key}' in table config for {table}")
            
            normalized_name = f"{conf['target_database']}.{conf['result_table_name']}"
            normalized_lookup[table] = conf
            normalized_lookup[normalized_name] = conf
        
        # Validate each table's configuration
        for table, conf in config.items():
            load_mode = conf.get("load_mode", "full")
            
            # Dependency validation
            for dep in conf.get("dependencies", []):
                if dep not in normalized_lookup:
                    raise ValueError(
                        f"Dependency '{dep}' in {table} not found in TABLES_CONFIG."
                    )
            
            # DML mode-specific checks
            if load_mode == "merge":
                if "merge_keys" not in conf or not conf.get("merge_keys"):
                    raise ValueError(f"Table '{table}' with load_mode 'merge' requires 'merge_keys'")
                # merge_update_columns auto-derived at materialization if omitted
                pass  # (all DataFrame columns minus merge_keys)")
            elif load_mode == "update":
                if "update_set" not in conf or not conf.get("update_set"):
                    raise ValueError(f"Table '{table}' with load_mode 'update' requires 'update_set'")
            elif load_mode == "delete":
                if "delete_where" not in conf or not conf.get("delete_where"):
                    raise ValueError(f"Table '{table}' with load_mode 'delete' requires 'delete_where'")
            elif load_mode.startswith("refresh_n_days"):
                if "partition_by" not in conf or not conf.get("partition_by"):
                    raise ValueError(f"Table '{table}' with load_mode 'refresh_n_days' requires 'partition_by'")
    
    @staticmethod
    def validate_dag_config(local_config: Dict[str, Dict]) -> None:
        """Validates that all dependencies exist and that there are no cycles."""
        print("\n" + "="*80)
        print("🔍 DEPENDENCY VALIDATION")
        print("="*80)
        visited = set()
        rec_stack = set()
        nodes = set(local_config.keys())
        edge_targets = set()
        edges = []

        def visit(node):
            if node in rec_stack:
                cycle_list = list(rec_stack) + [node]
                raise ValueError(
                    "\n" + "="*60 + "\n"
                    "🚨 BELLEROPHON DAG VALIDATION FAILURE 🚨\n"
                    "Dependency cycle detected!\n"
                    f"Cycle: {' -> '.join(cycle_list)}\n"
                    "="*60
                )
            if node in visited:
                return
            rec_stack.add(node)
            for dep in local_config[node].get("dependencies", []):
                edge_targets.add(dep)
                edges.append((dep, node))
                if dep not in local_config:
                    raise ValueError(
                        "\n" + "="*60 + "\n"
                        "🚨 BELLEROPHON DAG VALIDATION FAILURE 🚨\n"
                        f"Table '{node}' depends on unknown table '{dep}'\n"
                        "="*60
                    )
                visit(dep)
            rec_stack.remove(node)
            visited.add(node)

        for n in local_config:
            visit(n)

        if edges:
            print(f"\n📋 Found {len(edges)} dependency relationship(s)")
        else:
            print("\n📋 No explicit dependencies")

        referenced = edge_targets
        orphans = [n for n in nodes if not local_config[n].get("dependencies") and n not in referenced]
        if orphans:
            print(f"\n⚠️  Warning: {len(orphans)} isolated table(s) found (no dependencies/dependents)")

        print("✅ Dependency validation passed - no cycles detected")
        print("="*80 + "\n")


# ============================================================================
# PRODUCTION FEATURES - PROGRESS TRACKER
# ============================================================================

class BellerophonProgressTracker:
    """Visual progress tracking for long-running orchestrations (Belle's progress reporter)"""
    
    def __init__(self, total_tables: int, width: int = 50):
        self.total = total_tables
        self.current = 0
        self.width = width
        self.start_time = time.time()
        self.completed_tables = []
        self.failed_tables = []
        self.current_stage = 0
        self.stage_start_time = None
        self._previous_durations = {}  # v1.2.9: ETAs from previous run
    
    def load_previous_durations(self, spark_session, target_database: str):
        """Load table durations from the most recent successful run for ETA hints."""
        try:
            log_table = f"{target_database}.bellerophon_log_table"
            if spark_session.catalog.tableExists(log_table):
                rows = spark_session.sql(f"""
                    SELECT target_table_name, execution_duration_seconds
                    FROM {log_table}
                    WHERE success = true
                    AND execution_start_time = (
                        SELECT MAX(execution_start_time) FROM {log_table} WHERE success = true
                    )
                """).collect()
                for r in rows:
                    self._previous_durations[r['target_table_name']] = r['execution_duration_seconds']
                if self._previous_durations:
                    total_prev = sum(self._previous_durations.values())
                    belle_print(f"Loaded ETAs from previous run ({len(self._previous_durations)} tables, "
                                f"total {total_prev:.0f}s)", level=3)
        except Exception:
            pass  # Silently skip if log table unavailable
        
    def start_stage(self, stage_num: int, tables_in_stage: List[str]):
        """Visual indicator for stage start"""
        self.current_stage = stage_num
        self.stage_start_time = time.time()
        print(f"\n{'─'*80}")
        print(f"🎯 Stage {stage_num} | {len(tables_in_stage)} table(s) in parallel")
        print(f"{'─'*80}")
    
    def complete_stage(self, stage_num: int):
        """Visual indicator for stage completion"""
        if self.stage_start_time:
            stage_duration = time.time() - self.stage_start_time
            print(f"✅ Stage {stage_num} complete | {stage_duration:.2f}s\n")
        
    def update(self, table_name: str, status: str = "success", duration: float = 0.0):
        """Update progress after table completion (no progress bars)."""
        self.current += 1
        if status == "success":
            self.completed_tables.append(table_name)
            emoji = belle_emoji("✅", "[OK]")
        else:
            self.failed_tables.append(table_name)
            emoji = belle_emoji("❌", "[FAIL]")
        
        short_table = table_name.split('.')[-1] if '.' in table_name else table_name
        
        # v1.2.9 - Show previous run ETA if available, otherwise just elapsed
        prev_eta = self._previous_durations.get(table_name, None)
        eta_hint = f" (prev: {prev_eta:.0f}s)" if prev_eta else ""
        
        belle_print(f"{emoji} {short_table} ({duration:.1f}s){eta_hint}  "
                     f"[{self.current}/{self.total}]", level=2)
    
    def get_summary(self) -> str:
        """Return visually formatted execution summary"""
        elapsed = time.time() - self.start_time
        success_rate = (len(self.completed_tables) / self.total * 100) if self.total > 0 else 0
        
        summary = "\n"
        summary += "╔" + "═"*78 + "╗\n"
        summary += "║" + " "*25 + "🎭 BELLE EXECUTION SUMMARY" + " "*28 + "║\n"
        summary += "╠" + "═"*78 + "╣\n"
        summary += f"║  📊 Total Tables:          {self.total:<50}║\n"
        summary += f"║  ✅ Completed Successfully: {len(self.completed_tables):<50}║\n"
        summary += f"║  ❌ Failed:                 {len(self.failed_tables):<50}║\n"
        summary += f"║  📈 Success Rate:                   {success_rate:.1f}%{' '*46}║\n"
        summary += f"║  ⏱  Total Time:             {elapsed:.2f} seconds{' '*(38-len(f'{elapsed:.2f}'))}║\n"
        summary += "╠" + "═"*78 + "╣\n"
        
        if self.failed_tables:
            summary += "║  ⚠️  FAILED TABLES:" + " "*58 + "║\n"
            for table in self.failed_tables:
                table_display = table[:70] if len(table) > 70 else table
                summary += f"║     • {table_display:<70}║\n"
        else:
            summary += "║  🎉 All tables materialized successfully!" + " "*36 + "║\n"
        
        summary += "╚" + "═"*78 + "╝\n"
        return summary


# ============================================================================
# PRODUCTION FEATURES - RETRY HANDLER
# ============================================================================

class BellerophonRetryHandler:
    """Exponential backoff retry for transient failures (OOM, locks, timeouts)."""
    
    TRANSIENT_ERROR_PATTERNS = [
        # OOM (Issue #37: expanded patterns)
        "OutOfMemoryError",
        "java.lang.OutOfMemoryError",
        "Container killed by YARN",
        "MemoryError",
        # Network / connectivity
        "Connection refused",
        "SocketTimeoutException",
        "Temporary failure",
        "Connection reset",
        "TTransportException",
        # Delta concurrency
        "Table is locked",
        "ConcurrentModificationException",
        "ConcurrentAppendException",
        "ConcurrentDeleteReadException",
        "DELTA_CONCURRENT_WRITE",
        # Transient Spark errors
        "SparkUpgradeException",
        "FetchFailedException",
        "TaskKilledException",
        "ExecutorLostFailure",
    ]
    
    @staticmethod
    def is_transient_error(error: Exception) -> bool:
        """Check if error is likely transient"""
        error_str = str(error)
        return any(pattern in error_str for pattern in BellerophonRetryHandler.TRANSIENT_ERROR_PATTERNS)
    
    @staticmethod
    def retry_with_backoff(func, *args, max_retries: int = 3, base_delay: float = 2.0, **kwargs):
        """Execute function with exponential backoff on transient failures"""
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                
                if BellerophonRetryHandler.is_transient_error(e):
                    delay = base_delay * (2 ** attempt)
                    print(f"🔄 [Belle Retry] Transient error: {str(e)[:100]}")
                    print(f"⏳ [Belle Retry] Waiting {delay:.1f}s before retry {attempt + 1}/{max_retries}...")
                    time.sleep(delay)
                else:
                    raise


# ============================================================================
# PRODUCTION FEATURES - DRY RUN VALIDATOR & DATA QUALITY CHECKER
# ============================================================================

class BellerophonDryRunValidator:
    """Validate configuration without executing table materialisation"""
    
    @staticmethod
    def validate_config_deep(tables_config: Dict[str, Any]) -> Dict[str, List[str]]:
        issues = {}
        for table_name, conf in tables_config.items():
            table_issues = []
            required_keys = ['target_database', 'result_table_name', 'dependencies']
            for key in required_keys:
                if key not in conf:
                    table_issues.append(f"Missing required key: '{key}'")
            if table_issues:
                issues[table_name] = table_issues
        return issues
    
    @staticmethod
    def run_dry_run(tables_config: Dict[str, Any]) -> bool:
        print("\n🎭 BELLE DRY RUN MODE")
        try:
            BellerophonConfigValidator.validate(tables_config)
            print("✅ Basic validation: PASSED")
            BellerophonConfigValidator.validate_dag_config(tables_config)
            print("✅ DAG validation: PASSED")
            return True
        except Exception as e:
            print(f"❌ Validation FAILED: {e}")
            return False


class BellerophonDataQualityChecker:
    """Post-execution data quality verification"""
    
    @staticmethod
    def check_row_count(df: DataFrame, min_rows: int = 0) -> bool:
        try:
            count = df.count()
            return count >= min_rows
        except Exception:
            return False


# ============================================================================
# BANNER
# ============================================================================

BELLEROPHON_ASCII_BANNER = r"""
                                      =@@.                                                            
                                   .@*:.                                                              
                            .-*%@@@%%@@@@%=                                                           
                     .:+%%@%*=@@@@*                                                                   
               :=#%@%*=:.  .*@@+-:                                                                    
              ==:.        -@@%.       :*%%%%%*:                                                       
                         *@@@:     .*@@@@@@@@@@@#.           -@##@@@@@%%%*:                           
                       .#@@@=     :@@@@%=:::-*@@@@:          :@@@@@@@@#%@@@-                          
                       #@@@%      %@@@- =#%%*: :       =%@@@@@%**%%%%@@:+@@-.:                        
                      =@@@@+     :@@@+ #@@@@@@@*.     *%@@@@@@@@*=:+%@@@@@#+%+                        
                      #@@@@:     :@@@= @@@@@@@@@%       -+*#*+::#@@@@@@@@@@@%.                        
                      +@@@@@@*.   %%: .@@@@@@@@:    ::*@@@@@%:%@@@@@@@@@@@#*@@*.                      
                       .#@@@+: .=-:.  +%@@@@@@=.    %@@@@@@*+@@@@@@@@@@@@@@@@@@@*                     
                         :%@@@@=-@@%@@@@%#@@#*       *%%%@**@@@@@@@@@@@@@@@@@@@@@@+                   
            .=%@@@@@%=.    -@@@@@@@@::@%:  +@%=.      =@@%=@@@@@@@@@@@@@@@@@@@@@@@@@=                 
     .=%@@@@@@@@@@@@@@@@:     -@@@@@@. -@@@@@@+@@=   #@@@:%@@@@@@@@@@@@*.:-::*@@@@@@*                 
   =@@@@@@@@@@@@%=@@@@%@@=      =@@@@@-    .    *@%  %@@%=@@@@@@@@@@@@@@:     =@@==@:                 
 :%@@@@@@@@@@@@@@@.%@@%:%@=     :@@@@@@@:       :@@*  =@=%@@@@@@@@@@@@@@%                             
 .     .#@@@@@@@@@+:@@@%.%@@:  -@@@@@#%@::=*.   :@@@@=   %@@@@@@@@@@@@@@@%.                           
         *@@@@@@@@%.-@@@@+:*@@@@@@@@@@@@==@@+     :@@@@@*  %@@@@@@@@@@@@@@@@%.                          
           .   =@@@=  -@@@@@@@@@@@@@@@@@@# -@@#.  :@=*@@@@%#@@@@@@@@@@@@@@@@@*                          
               =@@@@+ :*=::-*%@@#@@@@@@@@#..-*%* #@. =@@@@@@@@@@@@@@@@@@@@@@:                         
               +@@@@@@+ .:===::#@@@@@@@%%@@@#=.  .%@=  :+@@@@@@@@@@@@@@@@@@@+                         
               :@@@@@@@@@@@@@@@@@@@@@@@@@@@-   .=*%@@@@@@@@@@@@@@@@@@@@@@@@@@+                        
                :%@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@#                        
                  :+#%@@@@@@@@@@@@@@- :@@@@@@@@#.++ == +@+  .-%@@@@@@@@@*-@@@@@@:                     
                       .*@@@@@@@@@*    #@%*==::*@#.%@.=@%==-. #@@@@@@@@@@@*@@@@@#                     
                         :*%%%#+:      -@@@@@@@@-=@@.=@@@@@@# =@@@@@@@@@@@@@@@@@@:       .:=+=:       
                                  :-=*%%@@@@@#+#@@@=*@@@@@@@@: @@@@@@@@@@@@@@@@@@#*%%@@@@@@@@@@=      
          .-*#*+-.            :*@@@@@@@@@@@@@@@@@@%@@@@@@@@:=* =@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@:     
        :%@@@@@@@@@=.       -%@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%.  .@@@@@@@@@@%=::.   :-+#%@%=  :%@@@-    
       .@@@@@@@@@@@@@@+:..:%@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%.  :@@@@@@@@@@@@@@@@@@%#+-:::::  =@@@=   
       :@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%.  *@@@@@@@@@@@@@@@@@@@@@@@@@@@: :@@@#  
        *@@@@@@@@@@#:    #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@: :@@@@@@@@@@@@@@@@@@@@@@@@@@@#  +@@@@:
       -%@@@@@@@@*      :@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@= -@@@@@@@@@@@@@@@%*-.   :@@@@.  %@@@=
    =@@@@@@@@@@@%       =@@@@@@@@@@@@@=%@@@@@@@@@@@@@@@@@@@@@@@@@%.+@@%:.                 :@@@-  -@@@:
   =@@@@@+@@@@@@=       :@@@@@@@@@@@@@@@%.%@@@@@@@@@@@@@@@%=.      %@@@:                    #@@@* .#@+ 
   *@@@@**@@@@@@*        #@@@@@@@@@@@@@@@::@@@@@@@@@@@@@@@@@%+:+@@*                      *@@@: #@@% 
   :@@@@#:@@@@@@@:       .%@@@@@@@@@@@@@@: %@@@@@@@@%*+=:.      %@@@:                    #@@@* .#@+ 
    .+@@@#*@@@@@@*         =@@@@@@@@@@@@@: %@@@@@@@%            %@@@@+                   #@@@%.  :: 
   :   .*@@@@@@@@%           #@@@@@@@@@@%.:@@@@@@@@@              :%@@%:              =%@@@@%:      
  =%.   :@@@@@@@@*           *@@@@@@@@@%..%@@@@@@@@%                :+%@=             %@@@%:        
  *@@%*#@@@@@@@@%:         :#@@@@@@@@@*.  :+@@@@@@@*                                 .@@%-          
  :@@@@@@@@@@@#-       .*%@@@@@@@@@@@%+.      .%@@@@@*                                 :*:            
   :%@@@@@@%:          :@@@@@@@%*=:.           .@@@@@@:                                               
      :==:.             -@@@@@=                .@@@@@@@#:               .-=*#%%%*+=:                  
                  ::======@@@@%.                =#@@@@@@@@@#=:.     :*@@@@@@@@@@@@@@@@*:              
             .-#@@@@@@@@@@@@@@@+   :-=*%@@@@@@%+-:  .=#@@@@@@@@@@@@@@#=.         :*%@@@*:           
           :%@@@%*:        -@@@@#. :%@@@@%%%%%@@@@@@#:.  :*%@@@@@@%*:  .:=%@@@@@@@*:.  =%@@%:         
         -@@@#:  .:*@@@@@@@@%@@@@@#:  :%@@@=:.   :#@@@@@#:.       ..=%@@@@@@%%%%%@@@@@=.  =@@@:       
       .%%+.   =%%%**+=--=+%@@@@@@@@@#.   +@@@@@#:   .*@@@@@@@@@@@@@@@@*.            :+%%.  .+%*.     
                                 -%@@@@@@:    +@@@@@%.    :+%@@@@*-                                   
                                       :+%@@%.     -*@@@*                                             
                                                                                                      
                                                                      
    .@@@%*: :@@@@@# +@:    %%    :@@@@@+ *@@@%*   =%@@%=  :@@@@#- :@*   *@:  =%@@%=  :%%.  +@:        
    .@# :@# :@#     *@:    @@    :@*     #%. :@% %@-  :@% :@*  *@-:@*   *@: %@-  :@%.:@@@: +@:        
    .@@@@@= :@@@@@: *@:    @@    :@@@@@. #@%%@* .@%    #@.:@%+*@%.:@@@@@@@::@*    *@::@*+@++@:        
    .@#  *@::@#     *@:    @@    :@*     #%. =@= @@:  :%%.:@*..   :@*   *@:.@@:  .%@.:@* =@@@:        
    .@@@@%+ :@@@@@# *@@@@#.@@@@@*:@@@@@+ #%.  %@..*@@@@*  :@+     :@*   *@:  *@@@@*  :@+  :%@:        
                         
                         "Belle" for short  🎯
"""



# COMMAND ----------

# DBTITLE 1,DAGVisualizer
# ============================================================================
# DAG VISUALIZER
# ============================================================================

class BellerophonDAGVisualizer:
    """Lightweight DAG visualization using ASCII art and HTML/SVG."""
    
    @staticmethod
    def build_execution_stages(tables_config: Dict[str, Any]) -> List[List[str]]:
        """Topological sort into parallel execution stages."""
        normalized_lookup = {}
        for table, conf in tables_config.items():
            normalized_name = f"{conf['target_database']}.{conf['result_table_name']}"
            normalized_lookup[table] = conf
            normalized_lookup[normalized_name] = table
        
        graph = {}
        for table, conf in tables_config.items():
            deps = []
            for dep in conf.get('dependencies', []):
                actual_table = normalized_lookup.get(dep, dep)
                if actual_table in tables_config:
                    deps.append(actual_table)
            graph[table] = set(deps)
        
        stages = []
        remaining = set(tables_config.keys())
        processed = set()
        
        while remaining:
            ready = []
            for table in remaining:
                table_deps = graph[table]
                if all(dep in processed for dep in table_deps):
                    ready.append(table)
            
            if not ready:
                ready = list(remaining)
            
            stages.append(sorted(ready))
            processed.update(ready)
            remaining -= set(ready)
        
        return stages
    
    @staticmethod
    def analyze_dag_structure(tables_config: Dict[str, Any]) -> Dict[str, str]:
        """Categorize nodes: source/intermediate/sink/isolated."""
        # Build dependency graph
        normalized_lookup = {}
        for table, conf in tables_config.items():
            normalized_name = f"{conf['target_database']}.{conf['result_table_name']}"
            normalized_lookup[table] = table
            normalized_lookup[normalized_name] = table
        
        # Track dependencies (who depends on whom)
        dependencies = {}  # table -> list of tables it depends on
        dependents = {}    # table -> list of tables that depend on it
        
        for table in tables_config.keys():
            dependencies[table] = []
            dependents[table] = []
        
        for table, conf in tables_config.items():
            for dep in conf.get('dependencies', []):
                dep_table = normalized_lookup.get(dep)
                if dep_table and dep_table in tables_config:
                    dependencies[table].append(dep_table)
                    dependents[dep_table].append(table)
        
        # Categorize nodes
        categories = {}
        for table in tables_config.keys():
            has_dependencies = len(dependencies[table]) > 0
            has_dependents = len(dependents[table]) > 0
            
            if not has_dependencies and not has_dependents:
                categories[table] = 'isolated'  # No connections
            elif not has_dependencies and has_dependents:
                categories[table] = 'source'     # Root nodes (no upstream dependencies)
            elif has_dependencies and not has_dependents:
                categories[table] = 'sink'       # Leaf nodes (no downstream dependents)
            else:
                categories[table] = 'intermediate'  # Middle layer
        
        return categories
    
    @staticmethod
    def print_ascii_dag(tables_config: Dict[str, Any], show_load_mode: bool = True):
        """Print ASCII art tree visualization of the DAG with execution stages."""
        stages = BellerophonDAGVisualizer.build_execution_stages(tables_config)
        structure = BellerophonDAGVisualizer.analyze_dag_structure(tables_config)
        
        # Count by structure type
        structure_counts = {}
        for cat in structure.values():
            structure_counts[cat] = structure_counts.get(cat, 0) + 1
        
        print("\n" + "="*80)
        print("📊 EXECUTION PLAN")
        print("="*80)
        print(f"Tables: {len(tables_config)} | Stages: {len(stages)} | Max parallelism: {max(len(stage) for stage in stages)}")
        
        total_deps = sum(len(conf.get('dependencies', [])) for conf in tables_config.values())
        print(f"Total Dependencies: {total_deps}")
        
        # Show structure analysis
        print(f"\nDAG Structure:")
        print(f"  🌱 Source Tables (no dependencies): {structure_counts.get('source', 0)}")
        print(f"  🔄 Intermediate Tables: {structure_counts.get('intermediate', 0)}")
        print(f"  🎯 Sink Tables (no dependents): {structure_counts.get('sink', 0)}")
        if structure_counts.get('isolated', 0) > 0:
            print(f"  ⚪ Isolated Tables: {structure_counts.get('isolated', 0)}")
        
        print(f"\n{'─'*80}")
        print("🔹 Execution order (parallel within each stage)")
        print(f"{'─'*80}")
        
        for stage_num, stage_tables in enumerate(stages, 1):
            print(f"\n╔{'='*96}╗")
            print(f"║ STAGE {stage_num:2d} - {len(stage_tables):2d} table(s) in parallel{' '*61}║")
            print(f"╠{'='*96}╣")
            
            for idx, table in enumerate(stage_tables):
                conf = tables_config[table]
                load_mode = conf.get('load_mode', 'full')
                deps = conf.get('dependencies', [])
                table_type = structure[table]
                
                # Icon based on structure type
                type_icons = {
                    'source': '🌱',
                    'intermediate': '🔄',
                    'sink': '🎯',
                    'isolated': '⚪'
                }
                type_icon = type_icons.get(table_type, '▶')
                
                branch = "╠──" if idx < len(stage_tables) - 1 else "╚──"
                
                table_display = table
                if len(table_display) > 48:
                    table_display = table_display[:45] + "..."
                
                mode_indicator = f"[{load_mode}]" if show_load_mode else ""
                
                print(f"║ {branch}{type_icon} {table_display:48s} {mode_indicator:20s} ║")
                
                if deps:
                    prefix = "║    " if idx < len(stage_tables) - 1 else "║    "
                    dep_text = ", ".join(deps) if len(deps) <= 2 else f"{deps[0]}, {deps[1]}, +{len(deps)-2} more"
                    if len(dep_text) > 85:
                        dep_text = dep_text[:82] + "..."
                    print(f"{prefix}  └─ depends on: {dep_text:80s} ║")
            
            print(f"╚{'='*96}╝")
        
        print("="*80 + "\n")
    
    @staticmethod
    def get_table_color(table_name: str, config: Dict[str, Any], structure_type: str) -> str:
        """Hex color for node based on config override or structural position."""
        # Check for explicit color override in config
        if 'color' in config:
            return config['color']
        
        # Default colors based on DAG structure
        structure_colors = {
            'source': '#3498DB',       # Blue - source/root nodes
            'intermediate': '#27AE60', # Green - intermediate processing
            'sink': '#E74C3C',         # Red - final output/sink nodes
            'isolated': '#95A5A6'      # Gray - isolated tables
        }
        
        return structure_colors.get(structure_type, '#34495E')  # Dark gray default
    
    @staticmethod
    def generate_html_dag(tables_config: Dict[str, Any], width: int = 1600, height: int = 1200) -> str:
        """Generate interactive HTML/SVG visualization."""
        import math
        
        stages = BellerophonDAGVisualizer.build_execution_stages(tables_config)
        structure = BellerophonDAGVisualizer.analyze_dag_structure(tables_config)
        
        stage_width = width / (len(stages) + 1)
        node_radius = 25
        
        normalized_lookup = {}
        for table, conf in tables_config.items():
            normalized_name = f"{conf['target_database']}.{conf['result_table_name']}"
            normalized_lookup[table] = table
            normalized_lookup[normalized_name] = table
        
        nodes = {}
        node_id = 0
        for stage_idx, stage_tables in enumerate(stages):
            x = stage_width * (stage_idx + 1)
            vertical_padding = 80
            available_height = height - (2 * vertical_padding)
            stage_height = available_height / len(stage_tables)
            
            for table_idx, table in enumerate(stage_tables):
                y = vertical_padding + stage_height * (table_idx + 0.5)
                conf = tables_config[table]
                load_mode = conf.get('load_mode', 'full')
                table_type = structure[table]
                
                color = BellerophonDAGVisualizer.get_table_color(table, conf, table_type)
                full_name = f"{conf['target_database']}.{conf['result_table_name']}"
                
                nodes[table] = {
                    'id': node_id,
                    'x': x,
                    'y': y,
                    'label': full_name,
                    'load_mode': load_mode,
                    'color': color,
                    'type': table_type,
                    'dependencies': conf.get('dependencies', [])
                }
                node_id += 1
        
        edges = []
        for table, node_data in nodes.items():
            for dep in node_data['dependencies']:
                dep_table = normalized_lookup.get(dep)
                
                if dep_table and dep_table in nodes:
                    from_x = nodes[dep_table]['x']
                    from_y = nodes[dep_table]['y']
                    to_x = node_data['x']
                    to_y = node_data['y']
                    
                    dx = to_x - from_x
                    dy = to_y - from_y
                    angle = math.atan2(dy, dx)
                    
                    from_x_adj = from_x + node_radius * math.cos(angle)
                    from_y_adj = from_y + node_radius * math.sin(angle)
                    to_x_adj = to_x - node_radius * math.cos(angle)
                    to_y_adj = to_y - node_radius * math.sin(angle)
                    
                    edges.append({
                        'from': nodes[dep_table]['id'],
                        'to': node_data['id'],
                        'x1': from_x_adj,
                        'y1': from_y_adj,
                        'x2': to_x_adj,
                        'y2': to_y_adj
                    })
        
        svg_edges = ""
        for edge in edges:
            svg_edges += f'<line x1="{edge["x1"]}" y1="{edge["y1"]}" x2="{edge["x2"]}" y2="{edge["y2"]}" stroke="#7F8C8D" stroke-width="2" marker-end="url(#arrowhead)" />\n'
        
        svg_nodes = ""
        for table, node in nodes.items():
            svg_nodes += f'<circle cx="{node["x"]}" cy="{node["y"]}" r="{node_radius}" fill="{node["color"]}" stroke="white" stroke-width="2" />\n'
            svg_nodes += f'<text x="{node["x"]}" y="{node["y"] - node_radius - 10}" text-anchor="middle" font-size="12" fill="black">{node["label"]}</text>\n'
            svg_nodes += f'<text x="{node["x"]}" y="{node["y"] + node_radius + 20}" text-anchor="middle" font-size="10" fill="#555">[{node["load_mode"]}]</text>\n'
            svg_nodes += f'<text x="{node["x"]}" y="{node["y"] + node_radius + 35}" text-anchor="middle" font-size="9" fill="#999">{node["type"]}</text>\n'
        
        # Add legend
        legend_x = 50
        legend_y = height - 150
        legend = f"""
        <g id="legend">
            <text x="{legend_x}" y="{legend_y}" font-size="14" font-weight="bold">Legend:</text>
            <circle cx="{legend_x + 10}" cy="{legend_y + 20}" r="8" fill="#3498DB" stroke="white" stroke-width="1" />
            <text x="{legend_x + 25}" y="{legend_y + 25}" font-size="11">Source (no dependencies)</text>
            <circle cx="{legend_x + 10}" cy="{legend_y + 40}" r="8" fill="#27AE60" stroke="white" stroke-width="1" />
            <text x="{legend_x + 25}" y="{legend_y + 45}" font-size="11">Intermediate</text>
            <circle cx="{legend_x + 10}" cy="{legend_y + 60}" r="8" fill="#E74C3C" stroke="white" stroke-width="1" />
            <text x="{legend_x + 25}" y="{legend_y + 65}" font-size="11">Sink (no dependents)</text>
            <circle cx="{legend_x + 10}" cy="{legend_y + 80}" r="8" fill="#95A5A6" stroke="white" stroke-width="1" />
            <text x="{legend_x + 25}" y="{legend_y + 85}" font-size="11">Isolated</text>
        </g>
        """
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Bellerophon DAG Visualization</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        h2 {{ color: #2C3E50; }}
        svg {{ border: 1px solid #ddd; background: white; border-radius: 5px; }}
        .info {{ margin: 20px 0; padding: 15px; background: white; border-left: 4px solid #3498DB; }}
    </style>
</head>
<body>
    <h2>🎯 Bellerophon DAG Visualization (Belle v1.2.6)</h2>
    <div class="info">
        <strong>Total Tables:</strong> {len(tables_config)} | 
        <strong>Stages:</strong> {len(stages)} | 
        <strong>Max Parallelism:</strong> {max(len(stage) for stage in stages)}
    </div>
    <svg width="{width}" height="{height}">
        <defs>
            <marker id="arrowhead" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto">
                <polygon points="0 0, 10 3, 0 6" fill="#7F8C8D" />
            </marker>
        </defs>
        {svg_edges}
        {svg_nodes}
        {legend}
    </svg>
</body>
</html>
        """
        return html
    
    @staticmethod
    def display_dag(tables_config: Dict[str, Any], mode: str = 'both'):
        """Display DAG using ASCII and/or HTML visualization."""
        if mode in ['ascii', 'both']:
            BellerophonDAGVisualizer.print_ascii_dag(tables_config)
        
        if mode in ['html', 'both']:
            html = BellerophonDAGVisualizer.generate_html_dag(tables_config)
            try:
                displayHTML(html)
            except:
                print("\n[INFO] HTML visualization not available in this environment")
    
    @staticmethod
    def visualize_dag_ascii(tables_config: Dict[str, Dict]):
        """Alias for backward compatibility with existing code."""
        BellerophonDAGVisualizer.print_ascii_dag(tables_config)



# COMMAND ----------

# DBTITLE 1,MaintenanceScheduler
# ============================================================================
# BELLEROPHON MAINTENANCE SCHEDULER
# ============================================================================

class BellerophonMaintenanceScheduler:
    """Scheduled VACUUM, OPTIMIZE, and rebuild operations. Uses Nth-weekday-of-month pattern."""
    
    VERSION = "1.0.0"
    
    def __init__(self, interactive_mode: bool = True):
        self.interactive_mode = interactive_mode
    
    @staticmethod
    def get_nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> Optional[datetime.date]:
        """Get Nth occurrence of weekday in month (n=5 means last)."""
        import calendar
        
        # Get first day of month
        first_day = datetime.date(year, month, 1)
        first_weekday = first_day.weekday()
        
        # Calculate days until first occurrence of target weekday
        days_until_target = (weekday - first_weekday) % 7
        first_occurrence = first_day + datetime.timedelta(days=days_until_target)
        
        # Handle "last occurrence" special case
        if n == 5:
            # Find all occurrences
            occurrences = []
            current = first_occurrence
            last_day_of_month = calendar.monthrange(year, month)[1]
            
            while current.day <= last_day_of_month:
                occurrences.append(current)
                current += datetime.timedelta(days=7)
            
            return occurrences[-1] if occurrences else None
        
        # Calculate Nth occurrence
        target_date = first_occurrence + datetime.timedelta(days=7 * (n - 1))
        
        # Verify it's still in the same month
        if target_date.month != month:
            return None
        
        return target_date
    
    @staticmethod
    def is_scheduled_day(
        check_date: datetime.date,
        day_of_week: int,
        week_of_month: int,
        enabled: bool = True
    ) -> bool:
        if not enabled:
            return False
        
        target_date = BellerophonMaintenanceScheduler.get_nth_weekday_of_month(
            check_date.year,
            check_date.month,
            day_of_week,
            week_of_month
        )
        
        return target_date == check_date if target_date else False
    
    def should_run_full_rebuild(self, check_date: Optional[datetime.date] = None) -> bool:
        if check_date is None:
            check_date = datetime.date.today()
        
        return self.is_scheduled_day(
            check_date,
            BellerophonConfig.SCHEDULED_REBUILD_DAY_OF_WEEK,
            BellerophonConfig.SCHEDULED_REBUILD_WEEK_OF_MONTH,
            BellerophonConfig.ENABLE_SCHEDULED_FULL_REBUILD
        )
    
    def should_run_vacuum(self, check_date: Optional[datetime.date] = None) -> bool:
        if check_date is None:
            check_date = datetime.date.today()
        
        return self.is_scheduled_day(
            check_date,
            BellerophonConfig.SCHEDULED_VACUUM_DAY_OF_WEEK,
            BellerophonConfig.SCHEDULED_VACUUM_WEEK_OF_MONTH,
            BellerophonConfig.ENABLE_SCHEDULED_VACUUM
        )
    
    def should_run_optimize(self, check_date: Optional[datetime.date] = None) -> bool:
        if check_date is None:
            check_date = datetime.date.today()
        
        return self.is_scheduled_day(
            check_date,
            BellerophonConfig.SCHEDULED_OPTIMIZE_DAY_OF_WEEK,
            BellerophonConfig.SCHEDULED_OPTIMIZE_WEEK_OF_MONTH,
            BellerophonConfig.ENABLE_SCHEDULED_OPTIMIZE
        )
    
    def run_vacuum(
        self,
        tables_config: Dict[str, Dict[str, Any]],
        retention_hours: Optional[int] = None,
        dry_run: Optional[bool] = None,
        specific_tables: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        retention_hours = retention_hours if retention_hours is not None else BellerophonConfig.SCHEDULED_VACUUM_RETENTION_HOURS
        dry_run = dry_run if dry_run is not None else BellerophonConfig.SCHEDULED_VACUUM_DRY_RUN
        
        results = {}
        
        if self.interactive_mode and BellerophonConfig.VERBOSITY >= BellerophonConfig.NORMAL:
            print("=" * 80)
            print(f"[VACUUM MAINTENANCE] {'(DRY RUN)' if dry_run else ''}")
            print("=" * 80)
            print(f"Retention: {retention_hours} hours ({retention_hours/24:.1f} days)")
            print(f"Target tables: {'All' if not specific_tables else len(specific_tables)}")
            print(f"Parallel workers: {BellerophonConfig.INTELLIGENT_MAINTENANCE_PARALLEL_WORKERS}")
            print("=" * 80)
        
        tables_to_process = []
        for table_key, config in tables_config.items():
            table_name = config.get('result_table_name')
            target_database = config.get('target_database')
            full_table_name = f"{target_database}.{table_name}"
            if specific_tables and full_table_name not in specific_tables:
                continue
            tables_to_process.append((table_name, full_table_name))
        
        if not tables_to_process:
            if self.interactive_mode:
                print("\n⚠️  No tables to process")
            return results
        
        def vacuum_single_table(table_info):
            table_name, full_table_name = table_info
            try:
                start_time = time.time()
                
                if self.interactive_mode:
                    print(f"\n🗑️  {full_table_name}...")
                
                # Build VACUUM command
                vacuum_sql = f"VACUUM {full_table_name} RETAIN {retention_hours} HOURS"
                if dry_run:
                    vacuum_sql += " DRY RUN"
                
                # Execute VACUUM
                vacuum_result = spark.sql(vacuum_sql)
                
                if dry_run:
                    files_count = vacuum_result.count()
                else:
                    files_count = 0
                duration = time.time() - start_time
                
                result = {
                    "success": True,
                    "files_info": files_count if dry_run else "N/A",
                    "duration_sec": round(duration, 2),
                    "dry_run": dry_run
                }
                
                if self.interactive_mode:
                    if dry_run:
                        print(f"   ✓ Would delete {files_count} file(s) ({duration:.1f}s)")
                    else:
                        print(f"   ✓ Completed in {duration:.1f}s")
                
                return table_name, result
                
            except Exception as e:
                result = {
                    "success": False,
                    "error": str(e),
                    "duration_sec": 0
                }
                
                if self.interactive_mode:
                    print(f"   ✗ ERROR: {e}")
                
                return table_name, result
        
        with ThreadPoolExecutor(max_workers=BellerophonConfig.INTELLIGENT_MAINTENANCE_PARALLEL_WORKERS) as executor:
            # Submit all tasks
            future_to_table = {executor.submit(vacuum_single_table, table_info): table_info for table_info in tables_to_process}
            
            # Collect results as they complete
            for future in as_completed(future_to_table):
                table_name, result = future.result()
                results[table_name] = result
        
        
        if self.interactive_mode:
            print("\n" + "=" * 80)
            success_count = sum(1 for r in results.values() if r["success"])
            print(f"\u2705 VACUUM Complete: {success_count}/{len(results)} tables successful")
            print("=" * 80)
        
        return results
    
    def run_optimize(
        self,
        tables_config: Dict[str, Dict[str, Any]],
        zorder_columns: Optional[Dict[str, List[str]]] = None,
        specific_tables: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        zorder_columns = zorder_columns if zorder_columns is not None else BellerophonConfig.SCHEDULED_OPTIMIZE_ZORDER_COLUMNS
        
        results = {}
        
        if self.interactive_mode and BellerophonConfig.VERBOSITY >= BellerophonConfig.NORMAL:
            print("=" * 80)
            print("\u26a1 OPTIMIZE MAINTENANCE")
            print("=" * 80)
            print(f"Target tables: {'All' if not specific_tables else len(specific_tables)}")
            if zorder_columns:
                print(f"Z-order configurations: {len(zorder_columns)} table(s)")
            print("=" * 80)
            print(f"Parallel workers: {BellerophonConfig.INTELLIGENT_MAINTENANCE_PARALLEL_WORKERS}")
        
        tables_to_process = []
        for table_key, config in tables_config.items():
            table_name = config.get('result_table_name')
            target_database = config.get('target_database')
            full_table_name = f"{target_database}.{table_name}"
            if specific_tables and full_table_name not in specific_tables:
                continue
            tables_to_process.append((table_name, full_table_name))
        
        if not tables_to_process:
            if self.interactive_mode:
                print("\n⚠️  No tables to process")
            return results
        
        def optimize_single_table(table_info):
            table_name, full_table_name = table_info
            try:
                start_time = time.time()
                
                if self.interactive_mode:
                    print(f"\n⚡ {full_table_name}...")
                
                # Build OPTIMIZE command
                optimize_sql = f"OPTIMIZE {full_table_name}"
                
                # Z-order if configured for this table
                _zo_cols = (zorder_columns.get(table_name)
                           or zorder_columns.get(full_table_name)
                           or zorder_columns.get(full_table_name.split('.')[-1]))
                if _zo_cols:
                    cols = _zo_cols
                    
                    try:
                        table_columns = [f.name for f in spark.table(full_table_name).schema.fields]
                        missing = [c for c in cols if c not in table_columns]
                        if missing:
                            if self.interactive_mode:
                                print(f"   ⚠️  Z-order columns not in schema: {missing}")
                                print(f"   Available: {table_columns}")
                            # Remove invalid columns, proceed with valid ones
                            valid_cols = [c for c in cols if c in table_columns]
                            if not valid_cols:
                                if self.interactive_mode:
                                    print(f"   ⏭️  Skipping Z-order (no valid columns)")
                                cols = []
                            else:
                                cols = valid_cols
                                if self.interactive_mode:
                                    print(f"   Using valid columns only: {cols}")
                    except Exception:
                        pass  # If schema check fails, let the SQL fail naturally
                    
                    if cols:
                        optimize_sql += f" ZORDER BY ({', '.join(cols)})"
                        if self.interactive_mode:
                            print(f"   Z-ordering on: {', '.join(cols)}")
                
                # Execute OPTIMIZE
                optimize_result = spark.sql(optimize_sql)
                
                # Collect metrics
                metrics = optimize_result.collect()
                duration = time.time() - start_time
                
                # Parse metrics if available
                files_added = files_removed = 0
                if metrics:
                    for row in metrics:
                        if hasattr(row, 'num_added_files'):
                            files_added += row.num_added_files
                        if hasattr(row, 'num_removed_files'):
                            files_removed += row.num_removed_files
                
                result = {
                    "success": True,
                    "files_added": files_added,
                    "files_removed": files_removed,
                    "duration_sec": round(duration, 2),
                    "zorder": table_name in zorder_columns
                }
                
                if self.interactive_mode:
                    print(f"   ✓ Completed in {duration:.1f}s")
                    if files_removed > 0:
                        print(f"   📄 Compacted {files_removed} → {files_added} file(s)")
                
                return table_name, result
                
            except Exception as e:
                result = {
                    "success": False,
                    "error": str(e),
                    "duration_sec": 0
                }
                
                if self.interactive_mode:
                    print(f"   ✗ ERROR: {e}")
                
                return table_name, result
        
        with ThreadPoolExecutor(max_workers=BellerophonConfig.INTELLIGENT_MAINTENANCE_PARALLEL_WORKERS) as executor:
            # Submit all tasks
            future_to_table = {executor.submit(optimize_single_table, table_info): table_info for table_info in tables_to_process}
            
            # Collect results as they complete
            for future in as_completed(future_to_table):
                table_name, result = future.result()
                results[table_name] = result
        
        
        if self.interactive_mode:
            print("\n" + "=" * 80)
            success_count = sum(1 for r in results.values() if r["success"])
            total_removed = sum(r.get("files_removed", 0) for r in results.values() if r["success"])
            total_added = sum(r.get("files_added", 0) for r in results.values() if r["success"])
            print(f"\u2705 OPTIMIZE Complete: {success_count}/{len(results)} tables successful")
            if total_removed > 0:
                print(f"\ud83d\udcc4 Total files compacted: {total_removed} \u2192 {total_added}")
            print("=" * 80)
        
        return results
    
    def get_schedule_description(self) -> str:
        """Human-readable summary of active maintenance schedules."""
        today = datetime.date.today()
        
        weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        ordinal_names = ["", "1st", "2nd", "3rd", "4th", "Last"]
        
        lines = ["[SCHEDULED MAINTENANCE CONFIGURATION]", "=" * 80, ""]
        
        # Full Rebuild
        if BellerophonConfig.ENABLE_SCHEDULED_FULL_REBUILD:
            dow = BellerophonConfig.SCHEDULED_REBUILD_DAY_OF_WEEK
            wom = BellerophonConfig.SCHEDULED_REBUILD_WEEK_OF_MONTH
            next_run = self.get_nth_weekday_of_month(today.year, today.month, dow, wom)
            if not next_run or next_run < today:
                # Try next month
                next_month = today.month % 12 + 1
                next_year = today.year + (1 if next_month == 1 else 0)
                next_run = self.get_nth_weekday_of_month(next_year, next_month, dow, wom)
            
            lines.append("Full Rebuild: ENABLED")
            lines.append(f"  \u2192 {ordinal_names[wom]} {weekday_names[dow]} of each month")
            if next_run:
                lines.append(f"  \u2192 Next run: {next_run}")
            lines.append("")
        else:
            lines.append("Full Rebuild: DISABLED")
            lines.append("")
        
        # VACUUM
        if BellerophonConfig.ENABLE_SCHEDULED_VACUUM:
            dow = BellerophonConfig.SCHEDULED_VACUUM_DAY_OF_WEEK
            wom = BellerophonConfig.SCHEDULED_VACUUM_WEEK_OF_MONTH
            retention = BellerophonConfig.SCHEDULED_VACUUM_RETENTION_HOURS
            next_run = self.get_nth_weekday_of_month(today.year, today.month, dow, wom)
            if not next_run or next_run < today:
                next_month = today.month % 12 + 1
                next_year = today.year + (1 if next_month == 1 else 0)
                next_run = self.get_nth_weekday_of_month(next_year, next_month, dow, wom)
            
            lines.append("VACUUM: ENABLED")
            lines.append(f"  \u2192 {ordinal_names[wom]} {weekday_names[dow]} of each month")
            lines.append(f"  \u2192 Retention: {retention} hours ({retention/24:.1f} days)")
            if BellerophonConfig.SCHEDULED_VACUUM_DRY_RUN:
                lines.append("  \u2192 Mode: DRY RUN (preview only)")
            if next_run:
                lines.append(f"  \u2192 Next run: {next_run}")
            lines.append("")
        else:
            lines.append("VACUUM: DISABLED")
            lines.append("")
        
        # OPTIMIZE
        if BellerophonConfig.ENABLE_SCHEDULED_OPTIMIZE:
            dow = BellerophonConfig.SCHEDULED_OPTIMIZE_DAY_OF_WEEK
            wom = BellerophonConfig.SCHEDULED_OPTIMIZE_WEEK_OF_MONTH
            zorder = BellerophonConfig.SCHEDULED_OPTIMIZE_ZORDER_COLUMNS
            next_run = self.get_nth_weekday_of_month(today.year, today.month, dow, wom)
            if not next_run or next_run < today:
                next_month = today.month % 12 + 1
                next_year = today.year + (1 if next_month == 1 else 0)
                next_run = self.get_nth_weekday_of_month(next_year, next_month, dow, wom)
            
            lines.append("OPTIMIZE: ENABLED")
            lines.append(f"  \u2192 {ordinal_names[wom]} {weekday_names[dow]} of each month")
            if zorder:
                lines.append(f"  \u2192 Z-order on {len(zorder)} table(s)")
            if next_run:
                lines.append(f"  \u2192 Next run: {next_run}")
            lines.append("")
        else:
            lines.append("OPTIMIZE: DISABLED")
            lines.append("")
        
        return "\n".join(lines)
    
    # ========================================================================
    # INTELLIGENT AUTO-MAINTENANCE
    # ========================================================================
    
    @staticmethod
    def _normalize_delta_operation(op_name: str) -> str:
        """Normalize DESCRIBE HISTORY operation names to uppercase canonical form."""
        if not op_name:
            return ""
        normalized = op_name.upper().strip()
        # Map compound operations to canonical form
        if normalized.startswith('OPTIMIZE'):
            return 'OPTIMIZE'
        if normalized.startswith('VACUUM'):
            return 'VACUUM'
        if normalized in ('CREATE TABLE AS SELECT', 'CREATE TABLE', 'WRITE', 'APPEND', 'OVERWRITE'):
            return 'WRITE'
        if normalized.startswith('MERGE'):
            return 'MERGE'
        if normalized.startswith('DELETE'):
            return 'DELETE'
        if normalized.startswith('UPDATE'):
            return 'UPDATE'
        if normalized.startswith('RESTORE'):
            return 'RESTORE'
        if normalized.startswith('CONVERT'):
            return 'CONVERT'
        if normalized.startswith('SET TBLPROPERTIES'):
            return 'SET TBLPROPERTIES'
        return normalized
    
    def get_table_statistics(self, table_name: str, list_files: bool = False) -> Optional[Dict[str, Any]]:
        """Get Delta table stats (files, size, last OPTIMIZE/VACUUM, deletion ratio)."""
        try:
            # Get table details
            detail_df = spark.sql(f"DESCRIBE DETAIL {table_name}")
            detail = detail_df.first()
            
            if not detail:
                return None
            
            # Extract key metrics
            num_files = detail.numFiles
            size_bytes = detail.sizeInBytes
            size_gb = size_bytes / (1024 ** 3)
            
            # Get history for last operations
            history_df = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 100")
            history = history_df.collect()
            
            # Find last OPTIMIZE and VACUUM
            last_optimize = None
            last_vacuum = None
            
            dml_rows_affected = 0
            total_rows_snapshot = 0
            
            for record in history:
                operation = record.operation
                operation = self._normalize_delta_operation(operation)
                timestamp = record.timestamp
                
                if operation == 'OPTIMIZE' and not last_optimize:
                    last_optimize = timestamp
                elif operation == 'VACUUM' and not last_vacuum:
                    last_vacuum = timestamp
                
                # Accumulate DML metrics for deletion estimate (since last VACUUM)
                if last_vacuum is None:  # Only count ops since last vacuum
                    metrics = record.operationMetrics if hasattr(record, 'operationMetrics') and record.operationMetrics else {}
                    if operation in ('DELETE', 'UPDATE', 'MERGE'):
                        dml_rows_affected += int(metrics.get('numTargetRowsDeleted', 0))
                        dml_rows_affected += int(metrics.get('numTargetRowsUpdated', 0))
                    if operation in ('WRITE', 'MERGE'):
                        rows_written = int(metrics.get('numOutputRows', 0))
                        if rows_written > total_rows_snapshot:
                            total_rows_snapshot = rows_written
            
            # Calculate days since last operations
            now = datetime.datetime.now()
            days_since_optimize = None
            days_since_vacuum = None
            
            if last_optimize:
                days_since_optimize = (now - last_optimize).days
            if last_vacuum:
                days_since_vacuum = (now - last_vacuum).days
            
            # Calculate average file size
            avg_file_size_mb = (size_bytes / num_files) / (1024 ** 2) if num_files > 0 else 0
            small_file_threshold_mb = BellerophonConfig.OPTIMIZE_SMALL_FILE_SIZE_MB
            
            if list_files and num_files > 0:
                # Accurate count via file listing (expensive but precise)
                try:
                    # Use dbutils to list files in the table location
                    from pyspark.dbutils import DBUtils
                    dbutils = DBUtils(spark)
                    file_list = dbutils.fs.ls(detail.location)
                    small_file_threshold_bytes = small_file_threshold_mb * 1024 * 1024
                    # Count files below threshold (excluding _delta_log directory)
                    small_files_estimate = sum(
                        1 for f in file_list 
                        if not f.path.endswith('/') 
                        and '_delta_log' not in f.path 
                        and f.size < small_file_threshold_bytes
                    )
                except Exception as e:
                    # Fallback to estimation if listing fails
                    small_files_estimate = num_files if avg_file_size_mb < small_file_threshold_mb else int(num_files * 0.3)
            else:
                # Fast estimation (crude but quick) - suitable for health checks
                small_files_estimate = num_files if avg_file_size_mb < small_file_threshold_mb else int(num_files * 0.3)
            
            deletion_pct = 0.0
            if total_rows_snapshot > 0:
                deletion_pct = dml_rows_affected / total_rows_snapshot
            
            return {
                'table_name': table_name,
                'num_files': num_files,
                'size_bytes': size_bytes,
                'size_gb': round(size_gb, 2),
                'avg_file_size_mb': round(avg_file_size_mb, 2),
                'small_files_count': small_files_estimate,
                'last_optimize': last_optimize,
                'last_vacuum': last_vacuum,
                'days_since_optimize': days_since_optimize,
                'days_since_vacuum': days_since_vacuum,
                'deletion_pct': round(deletion_pct, 4),
                'dml_rows_affected': dml_rows_affected,
                'location': detail.location,
                'format': detail.format
            }
            
        except Exception as e:
            if self.interactive_mode:
                print(f"⚠️ Could not get statistics for {table_name}: {e}")
            return None
    
    def needs_optimize(self, table_name: str, stats: Optional[Dict[str, Any]] = None) -> tuple[bool, List[str]]:
        """Check thresholds; returns (needs_optimize, reasons)."""
        if not BellerophonConfig.ENABLE_INTELLIGENT_AUTO_OPTIMIZE:
            return False, ["Intelligent auto-optimize disabled"]
        
        # Get statistics
        if stats is None:
            stats = self.get_table_statistics(table_name)
        
        if not stats:
            return False, ["Could not retrieve table statistics"]
        
        reasons = []
        
        # Check minimum table size
        if stats['size_gb'] < BellerophonConfig.OPTIMIZE_MIN_TABLE_SIZE_GB:
            return False, [f"Table too small ({stats['size_gb']} GB < {BellerophonConfig.OPTIMIZE_MIN_TABLE_SIZE_GB} GB threshold)"]
        
        # Check total file count
        if stats['num_files'] >= BellerophonConfig.OPTIMIZE_MIN_TOTAL_FILES:
            reasons.append(f"{stats['num_files']} files >= {BellerophonConfig.OPTIMIZE_MIN_TOTAL_FILES} threshold")
        
        # Check small files count
        if stats['small_files_count'] >= BellerophonConfig.OPTIMIZE_MIN_SMALL_FILES:
            reasons.append(f"{stats['small_files_count']} small files >= {BellerophonConfig.OPTIMIZE_MIN_SMALL_FILES} threshold")
        
        # Check days since last optimize
        if BellerophonConfig.OPTIMIZE_MAX_DAYS_SINCE_LAST > 0:
            if stats['days_since_optimize'] is None:
                reasons.append("Never been optimized")
            elif stats['days_since_optimize'] >= BellerophonConfig.OPTIMIZE_MAX_DAYS_SINCE_LAST:
                reasons.append(f"{stats['days_since_optimize']} days since last OPTIMIZE >= {BellerophonConfig.OPTIMIZE_MAX_DAYS_SINCE_LAST} day threshold")
        
        return len(reasons) > 0, reasons
    
    def needs_vacuum(self, table_name: str, stats: Optional[Dict[str, Any]] = None) -> tuple[bool, List[str]]:
        """Check thresholds; returns (needs_vacuum, reasons)."""
        if not BellerophonConfig.ENABLE_INTELLIGENT_AUTO_VACUUM:
            return False, ["Intelligent auto-vacuum disabled"]
        
        # Get statistics
        if stats is None:
            stats = self.get_table_statistics(table_name)
        
        if not stats:
            return False, ["Could not retrieve table statistics"]
        
        reasons = []
        
        # Check minimum table size
        if stats['size_gb'] < BellerophonConfig.VACUUM_MIN_TABLE_SIZE_GB:
            return False, [f"Table too small ({stats['size_gb']} GB < {BellerophonConfig.VACUUM_MIN_TABLE_SIZE_GB} GB threshold)"]
        
        # Check days since last vacuum
        if BellerophonConfig.VACUUM_MIN_DAYS_SINCE_LAST > 0:
            if stats['days_since_vacuum'] is None:
                reasons.append("Never been vacuumed")
            elif stats['days_since_vacuum'] >= BellerophonConfig.VACUUM_MIN_DAYS_SINCE_LAST:
                reasons.append(f"{stats['days_since_vacuum']} days since last VACUUM >= {BellerophonConfig.VACUUM_MIN_DAYS_SINCE_LAST} day threshold")
        
        if BellerophonConfig.VACUUM_MIN_DELETIONS_THRESHOLD > 0:
            try:
                deletion_pct = stats.get('deletion_pct', 0.0)
                if deletion_pct >= BellerophonConfig.VACUUM_MIN_DELETIONS_THRESHOLD:
                    reasons.append(
                        f"Deletion ratio {deletion_pct:.1%} >= "
                        f"{BellerophonConfig.VACUUM_MIN_DELETIONS_THRESHOLD:.0%} threshold")
            except Exception:
                pass  # Skip if stats unavailable
        
        return len(reasons) > 0, reasons
    
    def analyze_table_health(
        self,
        tables_config: Dict[str, Dict[str, Any]],
        specific_tables: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Analyze all tables; returns per-table health dict with needs_optimize/needs_vacuum flags."""
        results = {}
        
        if self.interactive_mode:
            print("=" * 80)
            print("[TABLE HEALTH ANALYSIS]")
            print("=" * 80)
            print(f"Parallel workers: {BellerophonConfig.INTELLIGENT_MAINTENANCE_PARALLEL_WORKERS}")
        
        tables_to_process = []
        for table_key, config in tables_config.items():
            table_name = config.get('result_table_name')
            target_database = config.get('target_database')
            full_table_name = f"{target_database}.{table_name}"
            if specific_tables and full_table_name not in specific_tables:
                continue
            tables_to_process.append((table_name, full_table_name))
        
        if not tables_to_process:
            if self.interactive_mode:
                print("\n⚠️  No tables to process")
            return results
        
        def analyze_single_table(table_info):
            table_name, full_table_name = table_info
            
            if self.interactive_mode:
                print(f"\n🔍 Analyzing: {full_table_name}")
            
            # Get statistics
            stats = self.get_table_statistics(full_table_name)
            
            if not stats:
                result = {
                    'error': 'Could not retrieve statistics',
                    'needs_optimize': False,
                    'needs_vacuum': False
                }
                return table_name, result
            
            # Check if maintenance needed
            needs_opt, opt_reasons = self.needs_optimize(full_table_name, stats)
            needs_vac, vac_reasons = self.needs_vacuum(full_table_name, stats)
            
            result = {
                'stats': stats,
                'needs_optimize': needs_opt,
                'optimize_reasons': opt_reasons,
                'needs_vacuum': needs_vac,
                'vacuum_reasons': vac_reasons
            }
            
            if self.interactive_mode and BellerophonConfig.INTELLIGENT_MAINTENANCE_VERBOSE:
                print(f"   Files: {stats['num_files']} ({stats['size_gb']} GB, avg {stats['avg_file_size_mb']} MB/file)")
                print(f"   Last OPTIMIZE: {stats['days_since_optimize']} days ago" if stats['days_since_optimize'] else "   Last OPTIMIZE: Never")
                print(f"   Last VACUUM: {stats['days_since_vacuum']} days ago" if stats['days_since_vacuum'] else "   Last VACUUM: Never")
                
                if needs_opt:
                    print(f"   ⚠️ OPTIMIZE NEEDED: {', '.join(opt_reasons)}")
                else:
                    print(f"   ✓ No optimize needed")
                
                if needs_vac:
                    print(f"   ⚠️ VACUUM NEEDED: {', '.join(vac_reasons)}")
                else:
                    print(f"   ✓ No vacuum needed")
            
            return table_name, result
        
        with ThreadPoolExecutor(max_workers=BellerophonConfig.INTELLIGENT_MAINTENANCE_PARALLEL_WORKERS) as executor:
            # Submit all tasks
            future_to_table = {executor.submit(analyze_single_table, table_info): table_info for table_info in tables_to_process}
            
            # Collect results as they complete
            for future in as_completed(future_to_table):
                table_name, result = future.result()
                results[table_name] = result
        
        
        if self.interactive_mode:
            print("\n" + "=" * 80)
            optimize_count = sum(1 for r in results.values() if r.get('needs_optimize'))
            vacuum_count = sum(1 for r in results.values() if r.get('needs_vacuum'))
            print(f"📊 SUMMARY: {len(results)} tables analyzed")
            print(f"   {optimize_count} need OPTIMIZE")
            print(f"   {vacuum_count} need VACUUM")
            print("=" * 80)
        
        return results
    
    def run_intelligent_optimize(
        self,
        tables_config: Dict[str, Dict[str, Any]],
        zorder_columns: Optional[Dict[str, List[str]]] = None,
        specific_tables: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """OPTIMIZE only tables exceeding health thresholds."""
        if self.interactive_mode:
            print("=" * 80)
            print("[INTELLIGENT AUTO-OPTIMIZE]")
            print("=" * 80)
        
        # Analyze table health
        health_analysis = self.analyze_table_health(tables_config, specific_tables)
        
        # Filter to tables that need optimize
        tables_to_optimize = [
            table_name for table_name, health in health_analysis.items()
            if health.get('needs_optimize', False)
        ]
        
        if not tables_to_optimize:
            if self.interactive_mode:
                print("\n✓ No tables need OPTIMIZE at this time")
                print("=" * 80)
            return {}
        
        if self.interactive_mode:
            print(f"\n🎯 Running OPTIMIZE on {len(tables_to_optimize)} table(s)")
            for table in tables_to_optimize:
                reasons = health_analysis[table]['optimize_reasons']
                print(f"   • {table}: {', '.join(reasons)}")
            print()
        
        # Run OPTIMIZE on filtered tables
        if BellerophonConfig.INTELLIGENT_MAINTENANCE_DRY_RUN:
            if self.interactive_mode:
                print("🧪 DRY RUN MODE: No actual OPTIMIZE will be executed\n")
            return {table: {'dry_run': True} for table in tables_to_optimize}
        
        return self.run_optimize(
            tables_config,
            zorder_columns=zorder_columns,
            specific_tables=tables_to_optimize
        )
    
    def run_intelligent_vacuum(
        self,
        tables_config: Dict[str, Dict[str, Any]],
        retention_hours: Optional[int] = None,
        specific_tables: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """VACUUM only tables exceeding health thresholds."""
        if self.interactive_mode:
            print("=" * 80)
            print("[INTELLIGENT AUTO-VACUUM]")
            print("=" * 80)
        
        # Analyze table health
        health_analysis = self.analyze_table_health(tables_config, specific_tables)
        
        # Filter to tables that need vacuum
        tables_to_vacuum = [
            table_name for table_name, health in health_analysis.items()
            if health.get('needs_vacuum', False)
        ]
        
        if not tables_to_vacuum:
            if self.interactive_mode:
                print("\n✓ No tables need VACUUM at this time")
                print("=" * 80)
            return {}
        
        if self.interactive_mode:
            print(f"\n🎯 Running VACUUM on {len(tables_to_vacuum)} table(s)")
            for table in tables_to_vacuum:
                reasons = health_analysis[table]['vacuum_reasons']
                print(f"   • {table}: {', '.join(reasons)}")
            print()
        
        # Run VACUUM on filtered tables
        if BellerophonConfig.INTELLIGENT_MAINTENANCE_DRY_RUN:
            if self.interactive_mode:
                print("🧪 DRY RUN MODE: No actual VACUUM will be executed\n")
            return {table: {'dry_run': True} for table in tables_to_vacuum}
        
        retention = retention_hours if retention_hours is not None else BellerophonConfig.VACUUM_RETENTION_HOURS
        
        return self.run_vacuum(
            tables_config,
            retention_hours=retention,
            dry_run=False,
            specific_tables=tables_to_vacuum
        )



# COMMAND ----------

# DBTITLE 1,BellerophonOrchestrator
# ============================================================================
# BELLEROPHON ORCHESTRATOR
# ============================================================================

class BellerophonOrchestrator:
    """DAG-driven parallel table materialisation with logging, retry, and memory management."""
    VERSION = "1.2.18"

    def __init__(
        self, 
        tables_config, 
        logger=None, 
        custom_csv_removals="", 
        test_mode=False,
        global_force_rebuild=False,
        validate_configs=True,
        fail_on_validation_errors=True
    ):
        """Initialize with tables_config. validate_configs=True enriches metadata pre-run."""
        # Basic validation (legacy validator)
        BellerophonConfigValidator.validate(tables_config)
        
        # Store original configs
        self.tables_config = tables_config
        self.global_force_rebuild = global_force_rebuild
        self.validate_configs_flag = validate_configs
        self.fail_on_validation_errors = fail_on_validation_errors
        
        # Extract target_database from first table configuration
        self.target_database = next(iter(tables_config.values()))["target_database"]
        import logging as logging_pkg
        self.logger = logger or logging_pkg.getLogger("BellerophonOrchestrator")
        if not self.logger.handlers:
            logging_pkg.basicConfig(level=logging_pkg.INFO)
        self.logger.setLevel(logging_pkg.INFO)
        self.interactive_mode = BellerophonUtils.is_interactive_notebook()
        self.custom_csv_removals = custom_csv_removals
        
        self.instance_id = BellerophonConfig.generate_instance_id()
        self.test_mode = test_mode
        
        self.scheduler = BellerophonMaintenanceScheduler(interactive_mode=self.interactive_mode)
        
        if test_mode:
            # Override global test suffix with instance-specific suffix
            original_suffix = BellerophonConfig.TEST_MODE_SUFFIX
            BellerophonConfig.TEST_MODE_SUFFIX = f"_belle_test_{self.instance_id}"
            if self.interactive_mode:
                print(f"🧪 [Belle Test Mode] Instance ID: {self.instance_id}")
                print(f"   Tables will use suffix: {BellerophonConfig.TEST_MODE_SUFFIX}")
                print(f"   This ensures isolation from other concurrent test runs.")
        
        # Config validation & enrichment
        
        self.enriched_configs = {}
        self.validation_summary = {
            'total_tables': len(tables_config),
            'tables_with_errors': 0,
            'tables_with_warnings': 0,
            'tables_to_rebuild': 0,
            'tables_existing': 0,
            'tables_new': 0,
            'partition_mismatches': 0
        }
        
        if validate_configs:
            # Import spark from globals (should be available in Databricks)
            try:
                spark_session = globals().get('spark')
                if not spark_session:
                    raise RuntimeError("SparkSession not found in globals. Cannot validate configs.")
            except Exception as e:
                if fail_on_validation_errors:
                    raise RuntimeError(f"Cannot validate configs: {e}")
                else:
                    self.logger.warning(f"Config validation skipped: {e}")
                    self.enriched_configs = tables_config
                    return
            
            # Validate and enrich each table config
            for table_key, config in tables_config.items():
                enriched = BellerophonConfig.validate_and_enrich_table_config(
                    spark_session,
                    config,
                    table_key,
                    global_force_rebuild=global_force_rebuild
                )
                
                self.enriched_configs[table_key] = enriched
                
                # Update summary statistics
                if enriched.get('_validation_errors'):
                    self.validation_summary['tables_with_errors'] += 1
                if enriched.get('_validation_warnings'):
                    self.validation_summary['tables_with_warnings'] += 1
                if enriched.get('_effective_force_rebuild'):
                    self.validation_summary['tables_to_rebuild'] += 1
                if enriched.get('_table_exists'):
                    self.validation_summary['tables_existing'] += 1
                else:
                    self.validation_summary['tables_new'] += 1
                if enriched.get('_partition_mismatch'):
                    self.validation_summary['partition_mismatches'] += 1
            
            # Display validation summary if interactive
            if self.interactive_mode:
                self._display_validation_summary()
            
            # Handle validation errors
            if self.validation_summary['tables_with_errors'] > 0:
                if fail_on_validation_errors:
                    # Collect all error messages
                    error_messages = []
                    for table_key, config in self.enriched_configs.items():
                        if config.get('_validation_errors'):
                            error_messages.append(f"\n❌ Table '{table_key}':")
                            for error in config['_validation_errors']:
                                error_messages.append(f"   {error}")
                    
                    error_msg = "\n".join([
                        "="*80,
                        "CONFIG VALIDATION FAILED",
                        "="*80,
                        f"Found errors in {self.validation_summary['tables_with_errors']} table(s):",
                    ] + error_messages + [
                        "",
                        "="*80,
                        "Fix the configuration errors above before running the orchestrator.",
                        "Alternatively, set fail_on_validation_errors=False to continue anyway."
                    ])
                    raise ValueError(error_msg)
                else:
                    # Log errors but continue
                    self.logger.warning(
                        f"Config validation found errors in {self.validation_summary['tables_with_errors']} table(s). "
                        f"Continuing anyway (fail_on_validation_errors=False)."
                    )
        else:
            # Validation disabled - use original configs
            self.enriched_configs = tables_config
    
    def _display_validation_summary(self):
        """Display config validation summary in interactive mode."""
        s = self.validation_summary
        _sep = "=" * 80

        print(f"\n{_sep}")
        print("CONFIG VALIDATION SUMMARY")
        print(_sep)

        # Checks performed
        print("  Checks performed:")
        print("    1. Table existence (catalog lookup)")
        print("    2. Partition layout vs config")
        print("    3. DataFrame registered in OutputRegistry")
        print("    4. Partition / monitored / merge columns present in DataFrame")

        # Table inventory
        n_exist   = s['tables_existing']
        n_new     = s['tables_new']
        n_total   = s['total_tables']
        n_rebuild = s['tables_to_rebuild']
        n_kept    = n_exist - max(0, n_rebuild - n_new)

        print(f"\n  Tables: {n_total}")
        if n_kept > 0:
            print(f"    {n_kept} existing \u2192 reuse (incremental)")
        if n_rebuild - n_new > 0:
            print(f"    {n_rebuild - n_new} existing \u2192 drop + rebuild (force_rebuild)")
        if n_new > 0:
            print(f"    {n_new} new \u2192 create")

        if s['partition_mismatches'] > 0:
            print(f"    \u26A0\uFE0F  {s['partition_mismatches']} partition mismatch(es) \u2192 auto-fixed")

        # Errors
        if s['tables_with_errors'] > 0:
            print(f"\n  \u274C {s['tables_with_errors']} table(s) with config errors:")
            for table_key, config in self.enriched_configs.items():
                if config.get('_validation_errors'):
                    short = config.get('result_table_name', table_key)
                    print(f"    \u274C {short}:")
                    for error in config['_validation_errors']:
                        for ln in error.split('\n'):
                            print(f"       {ln}")

        # Warnings
        if s['tables_with_warnings'] > 0:
            print(f"\n  \u26A0\uFE0F  {s['tables_with_warnings']} table(s) with warnings:")
            for table_key, config in self.enriched_configs.items():
                if config.get('_validation_warnings'):
                    short = config.get('result_table_name', table_key)
                    print(f"    \u26A0\uFE0F  {short}:")
                    for warning in config['_validation_warnings']:
                        print(f"       {warning}")

        if s['tables_with_errors'] == 0:
            print(f"\n  \u2705 All {n_total} table configs validated.")

        print(_sep + "\n")

    @staticmethod
    def validate_merge_keys(
        df, 
        merge_keys: List[str], 
        df_name: str = "DataFrame",
        auto_deduplicate: bool = False,
        fail_on_duplicates: bool = True
    ):
        """Check merge key uniqueness; optionally auto-deduplicate. Returns (df, dup_count)."""
        from pyspark.sql import functions as F
        
        # Count total rows
        total_rows = df.count()
        
        # Count distinct merge key combinations
        distinct_keys = df.select(merge_keys).distinct().count()
        
        duplicate_count = total_rows - distinct_keys
        
        if duplicate_count > 0:
            # Duplicates found
            duplicate_pct = (duplicate_count / total_rows) * 100 if total_rows > 0 else 0
            
            warning_msg = (
                f"\n⚠️  MERGE KEY DUPLICATE WARNING\n"
                f"   DataFrame: {df_name}\n"
                f"   Merge keys: {merge_keys}\n"
                f"   Total rows: {total_rows:,}\n"
                f"   Distinct keys: {distinct_keys:,}\n"
                f"   Duplicates: {duplicate_count:,} ({duplicate_pct:.2f}%)\n"
            )
            
            if auto_deduplicate:
                # Auto-deduplicate by keeping first occurrence
                df = df.dropDuplicates(subset=merge_keys)
                print(warning_msg + f"   ✅ Auto-deduplicated: kept first occurrence of each key\n")
                return df, duplicate_count
            elif fail_on_duplicates:
                # Raise error
                error_msg = (
                    warning_msg +
                    f"\n"
                    f"🚨 MERGE OPERATION BLOCKED\n"
                    f"   Duplicate merge keys will cause multiple matches and data corruption.\n"
                    f"\n"
                    f"📋 SOLUTIONS:\n"
                    f"   1. Fix data quality: Deduplicate {df_name} before merge\n"
                    f"   2. Enable auto-dedup: Set merge_auto_deduplicate=True in config\n"
                    f"   3. Disable validation: Set MERGE_VALIDATE_SOURCE_KEYS=False in BellerophonConfig\n"
                    f"\n"
                    f"Example - Find duplicates:\n"
                    f"   df.groupBy({merge_keys}).count().filter('count > 1').show()\n"
                )
                raise ValueError(error_msg)
            else:
                # Just warn, don't fail
                print(warning_msg + f"   ⚠️  Proceeding with duplicates (validation disabled)\n")
                return df, duplicate_count
        
        # No duplicates - all good
        return df, 0

    @staticmethod
    def get_sensible_max_workers():
        """Auto-detect sensible max_workers based on cluster configuration."""
        import builtins
        try:
            num_workers = int(spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers", "2"))
            return builtins.max(1, builtins.min(num_workers * 2, 16))
        except Exception:
            try:
                return builtins.max(1, builtins.min(spark.sparkContext.defaultParallelism, 16))
            except Exception:
                return 4

    def _count_stages(self, dag: Dict[str, List[str]], tables: List[str]) -> int:
        """Count the number of DAG stages for estimation."""
        import builtins
        depths = {t: 0 for t in tables}
        tables_set = set(tables)
        for _ in range(len(tables)):
            for node in tables:
                for dep in dag.get(node, []):
                    if dep in depths:
                        depths[node] = builtins.max(depths[node], depths[dep]+1)
        return builtins.max(depths.values())+1 if depths else 1
    
    def materialise_table(
        self,
        input_df,
        conf,
        run_id,
        interactive_mode,
        sample_rows,
        dag_stage,
        custom_csv_removals,
        max_workers=None,
        external_run_id=None,
        execution_context=None,
        retry_count=0
    ):
        """Materialise a single table using bellerophon_materialise_dataframe."""
        return bellerophon_materialise_dataframe(
            input_df,
            conf['target_database'],
            BellerophonUtils.apply_test_suffix(conf['result_table_name']),
            run_id=run_id,
            log_id=None,
            subpipeline=conf.get('subpipeline', None),
            export_csv=conf.get('export_csv', True),
            interactive_mode=interactive_mode,
            tag=conf.get('tag', None),
            extra_context=conf.get('extra_context', None),
            monitored_id_column=conf.get('monitored_id_column', None),
            monitored_date_column=conf.get('monitored_date_column', None),
            conf=conf,
            print_status=False,
            collect_sample=True,
            sample_rows=sample_rows,
            dag_stage=dag_stage,
            custom_csv_removals=custom_csv_removals,
            external_run_id=external_run_id,
            execution_context=execution_context,
            retry_count=retry_count
        )

    def display_dag(self, tables_dependencies):
        """Display DAG visualization (ASCII + HTML/SVG)."""
        config_for_viz = {}
        for table, deps in tables_dependencies.items():
            if table in self.tables_config:
                config_for_viz[table] = self.tables_config[table]
            else:
                # Reconstruct from available info
                config_for_viz[table] = {
                    'target_database': table.split('.')[0] if '.' in table else 'unknown',
                    'result_table_name': table.split('.')[-1] if '.' in table else table,
                    'dependencies': deps,
                    'load_mode': 'unknown'
                }
        BellerophonDAGVisualizer.display_dag(config_for_viz, mode='both')
    
    def check_scheduled_maintenance(
        self,
        force_full_refresh: bool = False,
        check_date: Optional[datetime.date] = None
    ) -> tuple:
        """Returns (force_rebuild, run_vacuum, run_optimize) for today's schedule."""
        if check_date is None:
            check_date = datetime.date.today()
        
        # Check scheduled full rebuild
        scheduled_rebuild = self.scheduler.should_run_full_rebuild(check_date)
        force_refresh = force_full_refresh or scheduled_rebuild
        
        # Check VACUUM and OPTIMIZE schedules
        run_vacuum = self.scheduler.should_run_vacuum(check_date)
        run_optimize = self.scheduler.should_run_optimize(check_date)
        
        # Print maintenance schedule status if interactive
        if self.interactive_mode and (scheduled_rebuild or run_vacuum or run_optimize):
            print("\n" + "=" * 80)
            print("[SCHEDULED MAINTENANCE TRIGGERED]")
            print("=" * 80)
            if scheduled_rebuild:
                print("✓ Full Rebuild: SCHEDULED for today")
            if run_vacuum:
                print("✓ VACUUM: SCHEDULED for today")
            if run_optimize:
                print("✓ OPTIMIZE: SCHEDULED for today")
            print("=" * 80 + "\n")
        
        return force_refresh, run_vacuum, run_optimize
    
    def run_post_processing_maintenance(
        self,
        run_vacuum: bool,
        run_optimize: bool,
        tables_to_run: Optional[List[str]] = None
    ):
        """Execute VACUUM/OPTIMIZE after DAG completion."""
        if not run_vacuum and not run_optimize:
            return
        
        if self.interactive_mode:
            print("\n" + "=" * 80)
            print("[POST-PROCESSING MAINTENANCE]")
            print("=" * 80)
        
        # Run VACUUM if scheduled
        if run_vacuum:
            try:
                vacuum_results = self.scheduler.run_vacuum(
                    self.tables_config,
                    specific_tables=tables_to_run
                )
                
                # Log results
                if self.interactive_mode:
                    success_count = sum(1 for r in vacuum_results.values() if r.get("success", False))
                    print(f"\n✅ VACUUM: {success_count}/{len(vacuum_results)} tables successful")
            
            except Exception as e:
                if self.interactive_mode:
                    print(f"\n❌ VACUUM failed: {e}")
                self.logger.error(f"VACUUM operation failed: {e}")
        
        # Run OPTIMIZE if scheduled
        if run_optimize:
            try:
                optimize_results = self.scheduler.run_optimize(
                    self.tables_config,
                    specific_tables=tables_to_run
                )
                
                # Log results
                if self.interactive_mode:
                    success_count = sum(1 for r in optimize_results.values() if r.get("success", False))
                    print(f"\n✅ OPTIMIZE: {success_count}/{len(optimize_results)} tables successful")
            
            except Exception as e:
                if self.interactive_mode:
                    print(f"\n❌ OPTIMIZE failed: {e}")
                self.logger.error(f"OPTIMIZE operation failed: {e}")
        
        if self.interactive_mode:
            print("=" * 80)

    def run(
        self,
        tables_to_run: Optional[List[str]] = None,
        max_workers: Optional[int] = None,
        show_dag: bool = False,
        job_queue_limit: int = 10,
        sample_rows: int = 10,
        external_run_id: Optional[str] = None,
        execution_context: Optional[Dict[str, Any]] = None,
        force_full_refresh: bool = False
    ) -> List[Dict[str, Any]]:
        """Execute DAG orchestration. Returns list of per-table summary dicts."""
        import sys
        import traceback
        import builtins

        if max_workers is None:
            max_workers = self.get_sensible_max_workers()

        def ts_print(msg, level=2):
            """Verbosity-aware timestamped print."""
            if BellerophonConfig.VERBOSITY >= level:
                prefix = f"[{BellerophonUtils.nowstr()}] " if BellerophonConfig.VERBOSITY >= BellerophonConfig.VERBOSE else ""
                print(f"{prefix}{msg}")

        # Use the ASCII art banner from module constants
        if BellerophonConfig.ASCII_ART_ENABLED:
            print(BELLEROPHON_ASCII_BANNER)

        BellerophonUtils.print_break(f"Bellerophon Orchestrator Startup (v{self.VERSION})")
        ts_print(f"Interactive mode: {self.interactive_mode}")
        run_id = str(uuid.uuid4())
        ts_print(f"run_id: {run_id}")
        
        if external_run_id:
            ts_print(f"🔗 external_run_id: {external_run_id} (ADF/External orchestration)")
        if execution_context:
            ts_print(f"📋 execution_context: {json.dumps(execution_context, indent=2)}")
        
        # Display mode-specific information
        if self.interactive_mode:
            ts_print("🔬 Interactive mode: logs displayed only (not persisted to tables)")
        
        # Force-rebuild fallback logic (per-table > global > run-param)
        
        # If force_full_refresh is True and global_force_rebuild was False,
        # treat force_full_refresh as the effective global setting
        effective_global_rebuild = self.global_force_rebuild or force_full_refresh
        BellerophonTracer.trace(
            "orchestrator.run", "*", "GLOBAL_REBUILD_DECISION",
            {"global_force_rebuild": self.global_force_rebuild,
             "force_full_refresh_param": force_full_refresh,
             "effective_global_rebuild": effective_global_rebuild,
             "interactive_mode": self.interactive_mode}
        , caller_locals=locals())
        
        if force_full_refresh and not self.global_force_rebuild:
            ts_print("⚠️  force_full_refresh parameter is DEPRECATED (use global_force_rebuild in __init__ instead)")
            ts_print(f"   Using force_full_refresh={force_full_refresh} as fallback for tables without explicit setting")
            
            # Re-enrich configs with the new effective global setting
            if self.validate_configs_flag:
                spark_session = globals().get('spark')
                for table_key, config in self.enriched_configs.items():
                    # Re-determine effective rebuild if no per-table setting
                    if config.get('force_full_rebuild') is None:
                        config['_effective_force_rebuild'] = effective_global_rebuild
                        config['_rebuild_source'] = 'force_full_refresh_parameter'
        
        # Reset logging setup messages for this orchestration run
        BellerophonLogger.reset_logging_messages()
        
        # v1.2.6 - Check scheduled maintenance operations
        force_full_refresh_for_maintenance, run_vacuum, run_optimize = self.check_scheduled_maintenance(effective_global_rebuild)

        # ====================================================================
        # BUILD CONFIG_BY_FULLNAME (using enriched configs)
        # ====================================================================
        config_by_fullname = {}
        for conf in self.enriched_configs.values():
            full_name = f"{conf['target_database']}.{conf['result_table_name']}"
            config_by_fullname[full_name] = conf

        if tables_to_run is None:
            tables_to_run = list(config_by_fullname.keys())
        else:
            normalized_tables_to_run = []
            for t in tables_to_run:
                if t in config_by_fullname:
                    normalized_tables_to_run.append(t)
                else:
                    ts_print(f"Table '{t}' is not in your TABLES_CONFIG and will be skipped.")
            tables_to_run = normalized_tables_to_run

        local_config = {k: config_by_fullname[k] for k in tables_to_run if k in config_by_fullname}
        BellerophonConfigValidator.validate_dag_config(local_config)
        
        # Pre-validation: catches config errors BEFORE any writes
        local_dataframes = {}
        for base_name in tables_to_run:
            conf = local_config[base_name]
            db, base_tbl = conf['target_database'], conf['result_table_name']
            key = f"{db}_{base_tbl}"
            if key in BellerophonOutputRegistry._outputs:
                local_dataframes[base_name] = BellerophonOutputRegistry.get_output(key)
        
        ts_print("Pre-validating DataFrames...")
        prevalidation_errors = []
        prevalidation_warnings = []
        for base_name in tables_to_run:
            conf = local_config[base_name]
            short = conf.get('result_table_name', base_name)
            
            # Check 1: DataFrame exists in OutputRegistry
            if base_name not in local_dataframes or local_dataframes[base_name] is None:
                continue  # No DataFrame registered — will appear in skip summary below

            
            df = local_dataframes[base_name]
            df_cols = set(df.columns)
            
            # Check 2: Partition columns exist in DataFrame
            partition_cols = conf.get('partition_by', []) or []
            for pc in partition_cols:
                if pc not in df_cols:
                    prevalidation_errors.append(
                        f"{short}: Partition column '{pc}' not in DataFrame. "
                        f"Available: {sorted(df_cols)}")
            
            # Check 3: Monitored columns exist in DataFrame
            for col_key in ['monitored_id_column', 'monitored_date_column']:
                col_val = conf.get(col_key)
                if col_val:
                    cols_to_check = [col_val] if isinstance(col_val, str) else list(col_val)
                    for c in cols_to_check:
                        if c not in df_cols:
                            prevalidation_warnings.append(
                                f"{short}: Monitored column '{c}' ({col_key}) not in DataFrame")
            
            # Check 4: Merge keys exist for merge mode
            if conf.get('load_mode') == 'merge':
                merge_keys = conf.get('merge_keys', [])
                for mk in merge_keys:
                    if mk not in df_cols:
                        prevalidation_errors.append(
                            f"{short}: Merge key '{mk}' not in DataFrame")
        
        if prevalidation_errors:
            ts_print(f"❌ Pre-validation found {len(prevalidation_errors)} error(s):")
            for err in prevalidation_errors:
                ts_print(f"   {err}")
            if self.fail_on_validation_errors:
                raise ValueError(
                    f"Pre-validation failed with {len(prevalidation_errors)} error(s). "
                    f"Fix DataFrame/config mismatches before running.")
        
        if prevalidation_warnings:
            ts_print(f"⚠️  Pre-validation warnings ({len(prevalidation_warnings)}):")
            for w in prevalidation_warnings:
                ts_print(f"   {w}")
        
        if not prevalidation_errors and not prevalidation_warnings:
            ts_print(f"✅ All {len(tables_to_run)} DataFrames validated against configs")
        elif not prevalidation_errors:
            ts_print(f"✅ {len(tables_to_run)} DataFrames validated ({len(prevalidation_warnings)} warnings)")
        
        # Single point of truth: drops tables for force rebuild or partition
        # mismatch BEFORE any writes. Updates enriched config metadata so
        # materialise_dataframe sees correct _table_exists / _partition_mismatch.
        ts_print("Running table readiness checks...")
        ensure_all_tables_ready(
            spark,
            local_config,
            force_rebuild=effective_global_rebuild,
            verbose=self.interactive_mode
        )
        BellerophonTracer.trace(
            "orchestrator.run", "*", "TABLE_READINESS_COMPLETE",
            {"tables_checked": len(local_config),
             "effective_global_rebuild": effective_global_rebuild}
        , caller_locals=locals())

        # ====================================================================
        # PRODUCTION MODE PREVIEW (interactive only)
        # ====================================================================
        if self.interactive_mode:
            _any_csv = any(c.get('export_csv', True) for c in local_config.values())
            _first_db = list(local_config.values())[0]['target_database']
            _prod_db = _first_db.replace("_dev", "").replace("_test", "")
            _data_path = BellerophonConfig.build_data_path(_prod_db)
            _csv_path = BellerophonConfig.build_csv_export_path(_prod_db)

            print("\n" + "=" * 80)
            print("PRODUCTION MODE PREVIEW")
            print("=" * 80)
            print(f"  Target database:  {_prod_db}")
            print(f"  Data path:        {_data_path}<table>/")
            if _any_csv:
                print(f"  CSV path:         {_csv_path}/<table>.csv")
            print(f"\n  Tables ({len(local_config)}):")
            for _bname, _conf in local_config.items():
                _tbl = _conf.get('result_table_name', _bname)
                _mode = _conf.get('load_mode', 'full').upper()
                _csv_flag = " + CSV" if _conf.get('export_csv', True) else ""
                _part = f", partitioned by {_conf['partition_by']}" if _conf.get('partition_by') else ""
                _rebuild = " [REBUILD]" if _conf.get('_effective_force_rebuild') else ""
                _td = getattr(BellerophonOutputRegistry, 'table_deltas', {}).get(_tbl)
                _delta_str = f" | {_td['start']} \u2192 {_td['end']}" if _td else ""
                print(f"    {_tbl:<35} {_mode}{_csv_flag}{_part}{_rebuild}{_delta_str}")
            print("=" * 80 + "\n")



        
        # ====================================================================
        # REGISTRY HEALTH CHECK
        # ====================================================================
        # Check if registry has been cleared (cluster restart/resize)
        expected_keys = [f"{conf['target_database']}_{conf['result_table_name']}" for conf in local_config.values()]
        
        health = BellerophonOutputRegistry.check_health(expected_keys)
        
        if not health['healthy']:
            print("\n" + "!"*80)
            print("⚠️  BELLE REGISTRY HEALTH CHECK FAILED")
            print("!"*80)
            print(f"\nRegistry status: {health['found']}/{health['total']} DataFrames found")
            print(f"Missing: {health['missing']} DataFrame(s)")
            
            if health['found'] == 0:
                print("\n🔴 CAUSE: Registry completely empty")
                print("   Likely reasons:")
                print("   • Cluster was restarted or resized")
                print("   • Notebook was detached from compute")
                print("   • Python kernel was restarted")
                print("   • Driver memory was cleared")
            else:
                print(f"\n🟡 CAUSE: Registry partially populated ({health['found']}/{health['total']})")
                print("   Some DataFrames were lost or not created")
            
            print("\n💡 SOLUTION:")
            print("   1. Rerun cells that create DataFrames (typically cells 8-28)")
            print("   2. Each cell should end with:")
            print("      BellerophonOutputRegistry.set_output('key', dataframe)")
            print("   3. Then rerun this orchestration cell")
            
            if health['missing'] <= 10:
                print(f"\n📋 Missing DataFrames ({health['missing']}):")
                for key in health['missing_keys'][:10]:
                    # Extract table name from key (format: database_tablename)
                    table_name = key.split('_', 1)[-1] if '_' in key else key
                    print(f"   • {table_name}")
            
            print("\n" + "!"*80 + "\n")
        
        local_dataframes = {}
        _full_refs = {}  # Full DFs preserved for write when persist_columns projects
        _skipped_tables = []
        _table_deltas = getattr(BellerophonOutputRegistry, 'table_deltas', {})
        _skip_reasons = getattr(BellerophonOutputRegistry, 'skip_reasons', {})
        for base_name in tables_to_run:
            conf = local_config[base_name]
            db, base_tbl = conf['target_database'], conf['result_table_name']
            key = f"{db}_{base_tbl}"
            if key in BellerophonOutputRegistry._outputs:
                local_dataframes[base_name] = BellerophonOutputRegistry.get_output(key)
            else:
                _skip_reason = _skip_reasons.get(base_tbl, 'no DataFrame registered')
                _skip_delta = _table_deltas.get(base_tbl)
                _skipped_tables.append((base_tbl, _skip_reason, _skip_delta))
        if _skipped_tables:
            ts_print(f"\u23ED\uFE0F  {len(_skipped_tables)} table(s) skipped (no DataFrame registered — retained as-is):")
            for _st, _sr, _sd in _skipped_tables:
                _dh = f" | delta: {_sd['start']} \u2192 {_sd['end']}" if _sd else ""
                ts_print(f"    {_st:<35} {_sr}{_dh}")
        tables_to_run = [t for t in tables_to_run if t in local_dataframes]
        if not tables_to_run:
            ts_print("No tables to process. Exiting.")
            return []

        tables_dependencies = {}
        usage_counts = {}
        for base_name in tables_to_run:
            conf = local_config[base_name]
            deps = []
            for dep in conf.get("dependencies", []):
                if dep not in config_by_fullname:
                    ts_print(f"'{base_name}' declares dependency '{dep}', which is not in TABLES_CONFIG and will be ignored.")
                elif dep not in tables_to_run:
                    ts_print(f"'{base_name}' declares dependency '{dep}', which is not in this run and will be ignored.")
                else:
                    deps.append(dep)
                    usage_counts[dep] = usage_counts.get(dep, 0) + 1
            tables_dependencies[base_name] = deps

        persisted = {df: False for df in usage_counts.keys()}
        usage_remaining = usage_counts.copy()

        def disable_persist_management(table_name, conf):
            return conf.get('disable_auto_persist', False)

        def persist_df_if_needed(df_name):
            conf = local_config[df_name]
            if disable_persist_management(df_name, conf):
                return
            if usage_counts.get(df_name, 0) > 0 and not persisted.get(df_name, False):
                df = local_dataframes[df_name]
                # persist_columns: project before persisting (opt-in column pruning).
                # Full DF preserved in _full_refs for the write; downstream gets projected.
                _pc = conf.get('persist_columns')
                if _pc:
                    _full_refs[df_name] = df
                    df = df.select(*_pc)
                    local_dataframes[df_name] = df
                row_threshold = 5_000_000
                col_threshold = 30
                if df.count() > row_threshold or len(df.columns) > col_threshold:
                    df.persist(StorageLevel.MEMORY_AND_DISK)
                else:
                    df.persist()
                persisted[df_name] = True

        def unpersist_df_if_possible(df_name):
            usage_remaining[df_name] -= 1
            if usage_remaining[df_name] == 0 and persisted.get(df_name, False):
                local_dataframes[df_name].unpersist(blocking=True)
                persisted[df_name] = False

        # DAG STAGE LOGIC
        BellerophonUtils.print_break("Execution Overview")
        stage_count = self._count_stages(tables_dependencies, tables_to_run)
        _skip_msg = f" | {len(_skipped_tables)} skipped" if _skipped_tables else ""
        ts_print(f"Tables: {len(tables_to_run)} to write{_skip_msg} | Estimated stages: {stage_count}")

        if show_dag and self.interactive_mode and stage_count > 1:
            BellerophonUtils.print_break("Dependency Graph Visualization (DAG)")
            self.display_dag(tables_dependencies)

        from concurrent.futures import ThreadPoolExecutor, as_completed

        tables_to_process = set(tables_to_run)
        tables_processed = set()
        tables_failed = set()
        tables_skipped = set()
        logs_to_display = []
        summary_rows = []
        samples_to_display = []

        table_start_times = {}
        table_end_times = {}
        table_durations = {}
        stage_durations = []
        stage_names = []
        stage_tables = {}

        # LOGGING SETUP - Verify log table readiness before orchestration
        BellerophonUtils.print_break("Logging Setup")
        if not self.interactive_mode:
            # Production mode: verify log table exists and is accessible
            log_table = f"{self.target_database}.bellerophon_log_table"
            log_table_path = BellerophonConfig.build_log_path(self.target_database)
            ts_print(f"Checking log table '{log_table}' at '{log_table_path}'...")
            
            # Mark as shown so write_log won't print it again
            BellerophonLogger._logging_setup_shown[self.target_database] = True
            
            # Check table existence
            try:
                table_exists = spark.catalog.tableExists(log_table)
                from pathlib import Path
                import os
                # Check if path is accessible (basic check)
                if log_table_path.startswith('/mnt/') or log_table_path.startswith('/dbfs/'):
                    path_accessible = True  # Assume accessible
                else:
                    path_accessible = True
                
                if table_exists:
                    ts_print(f"✅ Log table exists and ready")
                else:
                    ts_print(f"📝 Log table will be created on first write")
            except Exception as e:
                ts_print(f"⚠️  Log table check failed: {str(e)[:100]}")
        else:
            # Interactive mode: logs displayed only (not persisted)
            ts_print("Interactive mode: logs will be displayed only (not persisted to tables)")
            # Mark as shown so write_log won't print the message during Stage 1
            BellerophonLogger._logging_setup_shown[self.target_database] = True
        
        BellerophonUtils.print_break("Orchestration")
        stage_number = 0
        
        progress_tracker = BellerophonProgressTracker(total_tables=len(tables_to_run))
        progress_tracker.load_previous_durations(spark, self.target_database)

        while tables_to_process - tables_processed - tables_skipped:
            stage_number += 1
            stage_ready = []
            for tbl in tables_to_process - tables_processed - tables_skipped:
                failed_deps = [dep for dep in tables_dependencies[tbl] if dep in tables_failed or dep in tables_skipped]
                if failed_deps:
                    ts_print(f"  ⏭️  Skipping {tbl}: dependency failed → {', '.join(failed_deps)}")
                    tables_skipped.add(tbl)
                    continue
                if all(dep in tables_processed for dep in tables_dependencies[tbl]):
                    stage_ready.append(tbl)

            if not stage_ready:
                remaining = tables_to_process - tables_processed - tables_skipped
                if remaining:
                    ts_print(f"ERROR: No tables ready but {len(remaining)} remain unprocessed. Possible cycle in dependencies.")
                    ts_print(f"  Remaining: {', '.join(remaining)}")
                break

            stage_start_time = time.time()
            stage_tables[stage_number] = stage_ready
            stage_names.append(f"Stage {stage_number}")

            _stage_names = [local_config[t].get("result_table_name", t.split(".")[-1]) for t in stage_ready]
            _stage_label = ", ".join(_stage_names)
            ts_print(f"\n--- Stage {stage_number}: {_stage_label} ---")
            for tbl in stage_ready:
                persist_df_if_needed(tbl)
            
            progress_tracker.start_stage(stage_number, stage_ready)

            def process_one_table(base_name, conf, stage_number):
                nonlocal run_id, local_dataframes, self
                start = time.time()
                table_start_times[base_name] = start
                try:
                    input_df = _full_refs.pop(base_name, None) or local_dataframes[base_name]
                    
                    def _do_materialise():
                        return self.materialise_table(
                            input_df, conf, run_id, self.interactive_mode,
                            sample_rows, stage_number, self.custom_csv_removals,
                            external_run_id=external_run_id,
                            execution_context=execution_context
                        )
                    
                    max_retries = conf.get('max_retries', BellerophonConfig.MAX_MATERIALISE_RETRIES)
                    if max_retries > 1:
                        result_df, log_df, sample_df = BellerophonRetryHandler.retry_with_backoff(
                            _do_materialise, max_retries=max_retries, base_delay=5.0)
                    else:
                        result_df, log_df, sample_df = _do_materialise()
                    duration = time.time() - start
                    table_end_times[base_name] = time.time()
                    table_durations[base_name] = duration
                    
                    return {
                        'status': 'success',
                        'table': base_name,
                        'duration': duration,
                        'log_df': log_df,
                        'sample_df': sample_df,
                        'result_df': result_df
                    }
                except Exception as e:
                    duration = time.time() - start
                    table_end_times[base_name] = time.time()
                    table_durations[base_name] = duration
                    ts_print(f"❌ {base_name} FAILED: {str(e)[:200]}")
                    traceback.print_exc()
                    error_log_data = [(
                        run_id, str(uuid.uuid4()), conf['target_database'],
                        conf['result_table_name'], None, None, None,
                        0, 0, stage_number, duration, "ERROR", str(e)[:500],
                        conf.get('subpipeline', ''), conf.get('tag', ''),
                        conf.get('extra_context', ''), conf.get('load_mode', ''), None
                    )]
                    error_log_schema = "run_id STRING, log_id STRING, target_database STRING, result_table_name STRING, monitored_id_max LONG, monitored_date_max STRING, sample_data STRING, sample_rows_count LONG, result_row_count LONG, dag_stage INT, duration_seconds DOUBLE, status STRING, message STRING, subpipeline STRING, tag STRING, extra_context STRING, load_mode STRING, timestamp STRING"
                    error_log_df = spark.createDataFrame(error_log_data, error_log_schema)
                    return {
                        'status': 'failed',
                        'table': base_name,
                        'duration': duration,
                        'log_df': error_log_df,
                        'sample_df': None,
                        'result_df': None,
                        'error': str(e)
                    }

            results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                for base_name in stage_ready:
                    short = local_config[base_name].get('result_table_name', base_name.split('.')[-1])
                    prev = progress_tracker._previous_durations.get(base_name)
                    eta_hint = f" (prev: {prev:.0f}s)" if prev else ""
                    _lm = local_config[base_name].get('load_mode', 'full')
                    _d = _table_deltas.get(short)
                    _dh = f" | {_d['start']} \u2192 {_d['end']}" if _d else ""
                    ts_print(f"  \u25B6 {short} [{_lm}]{eta_hint}{_dh}")
                    futures[executor.submit(process_one_table, base_name, local_config[base_name], stage_number)] = base_name
                
                ts_print(f"  {len(futures)} task(s) submitted, awaiting completion...")
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)
                    
                    if result['status'] == 'success':
                        tbl = result['table']
                        logs_to_display.append((tbl, result['log_df']))
                        if result['sample_df'] is not None and not result['sample_df'].empty:
                            samples_to_display.append((tbl, result['sample_df']))
                        db = local_config[tbl]['target_database']
                        base_tbl = local_config[tbl]['result_table_name']
                        output_key = f"{db}_{base_tbl}"
                        BellerophonOutputRegistry.set_output(output_key, result['result_df'])
                        local_dataframes[tbl] = result['result_df']
                        
                        for dep in tables_dependencies[tbl]:
                            if dep in usage_remaining:
                                unpersist_df_if_possible(dep)

                        tables_processed.add(tbl)
                        progress_tracker.update(tbl, status="success", duration=result['duration'])
                    else:
                        tbl = result['table']
                        logs_to_display.append((tbl, result['log_df']))
                        tables_failed.add(tbl)
                        progress_tracker.update(tbl, status="failed", duration=result['duration'])

            stage_duration = time.time() - stage_start_time
            stage_durations.append(stage_duration)
            ts_print(f"Stage {stage_number} completed in {stage_duration:.2f}s")
            progress_tracker.complete_stage(stage_number)

        total_duration = sum(stage_durations)
        
        # Consolidate logs with schema tolerance for heterogeneous tables
        if logs_to_display:
            all_logs_df = reduce(
                lambda a, b: a.unionByName(b[1], allowMissingColumns=True),
                logs_to_display[1:],
                logs_to_display[0][1]
            )
            first_db = list(local_config.values())[0]['target_database']
            BellerophonLogger.write_log(
                all_logs_df,
                first_db,
                all_logs_df.schema,
                run_id,
                self.interactive_mode
            )

        # Summary
        BellerophonUtils.print_break("Summary")
        ts_print(f"✅ Processed {len(tables_processed)}/{len(tables_to_run)} tables in {stage_number} stages | {total_duration:.2f}s total")
        
        # Display sample data in interactive mode
        if self.interactive_mode and samples_to_display:
            print("\n" + "="*80)
            print("📊 SAMPLE DATA FROM PROCESSED TABLES")
            print("="*80)
            # Show ALL table samples (removed 5-table limit per user request)
            for table_name, sample_df in samples_to_display:
                print(f"\n▶️ {table_name.split('.')[-1]} (first {len(sample_df)} rows)")
                print("-"*80)
                try:
                    display(spark.table(table_name).limit(len(sample_df)))
                except Exception:
                    print(sample_df.to_string() if hasattr(sample_df, 'to_string') else str(sample_df))
        
        # Display log summary in interactive mode
        if self.interactive_mode and logs_to_display:
            print("\n" + "="*80)
            print("📋 LOG SUMMARY")
            print("="*80)
            try:
                # Reuse consolidated all_logs_df (Spark DF) from above.
                # Avoids toPandas() which fails with CANNOT_DETERMINE_TYPE
                # on the many-nullable-column union (LongType/BooleanType
                # columns that are all-null trigger Arrow inference errors).
                display(all_logs_df)
            except Exception as e:
                print(f"Could not display log summary: {e}")
        
        # Performance analysis
        if stage_durations and len(stage_durations) > 1:
            print("\n" + "="*80)
            print("PERFORMANCE ANALYSIS")
            print("="*80)
            
            # Find bottleneck stages (>30% of total time)
            bottlenecks = []
            for idx, dur in enumerate(stage_durations, 1):
                stage_pct = (dur / total_duration) * 100 if total_duration > 0 else 0
                if stage_pct > 30:
                    bottlenecks.append((idx, dur, stage_pct))
            
            if bottlenecks:
                print("\n⚠️  Bottleneck stages detected:")
                for stage_num, dur, pct in bottlenecks:
                    print(f"   Stage {stage_num}: {dur:.1f}s ({pct:.1f}% of total)")
                    stage_table_list = stage_tables.get(stage_num, [])
                    if stage_table_list:
                        slowest = max(stage_table_list, key=lambda t: table_durations.get(t, 0))
                        slowest_dur = table_durations.get(slowest, 0)
                        print(f"      Slowest table: {slowest.split('.')[-1]} ({slowest_dur:.1f}s)")
                
                # Advanced optimization recommendations
                print("\n💡 Advanced Optimization Recommendations:")
                print("   📊 Data Skew & Partitioning:")
                print("      • Check partition distribution: DESCRIBE DETAIL <table> | SELECT * WHERE num_files > 1000")
                print("      • For skewed joins: Use broadcast() for small tables (<100MB) or salting for large ones")
                print("      • Repartition before expensive operations: df.repartition(col('partition_key'))")
                print("\n   [OPTIMIZE] Query Optimization:")
                print("      • Add filter pushdown: WHERE clauses on partitioned columns (date_key, etc.)")
                print("      • Use column pruning: SELECT only needed columns, not SELECT *")
                print("      • Enable Adaptive Query Execution: spark.conf.set('spark.sql.adaptive.enabled', 'true')")
                print("\n   🔄 Dependency & Parallelism:")
                print("      • Review DAG: Can any dependencies be removed or weakened?")
                print("      • Stage with many tables: Consider splitting into sub-stages if not all need same deps")
                print("      • Slow history tables: Investigate SCD logic - consider incremental SCD updates")
                print("\n   💾 Caching Strategy:")
                print("      • Cache dimension tables read multiple times: df.cache() before multiple joins")
                print("      • Unpersist after use: df.unpersist() to free memory")
                print("      • Monitor cache hit rate: Check Spark UI Storage tab")
                print("\n   🔍 Table-Specific:")
                slow_tables = sorted(table_durations.items(), key=lambda x: x[1], reverse=True)[:3]
                for tbl, dur in slow_tables:
                    if dur > 60:  # Tables taking >1 minute
                        print(f"      • {tbl.split('.')[-1]} ({dur:.1f}s): Profile with EXPLAIN ANALYZE")
            else:
                print("✅ Well-balanced execution - no significant bottlenecks")
                print("\n💡 Optimization opportunities:")
                print("   • Review total execution time: Can any stage be further parallelized?")
                print("   • Check Spark UI: Look for shuffle spills or GC overhead")
                print("   • Consider Z-ordering: OPTIMIZE <table> ZORDER BY (commonly_filtered_columns)")
        
        self.run_post_processing_maintenance(run_vacuum, run_optimize, tables_to_run)
        
        if BellerophonConfig.LOG_RETENTION_DAYS > 0 and not self.interactive_mode:
            BellerophonLogger.cleanup_old_logs(spark, self.target_database)
        
        summary_rows = []
        for tbl in tables_to_run:
            db = local_config[tbl]['target_database']
            result_table = local_config[tbl]['result_table_name']
            full_table_name = f"{db}.{result_table}"
            
            # Get execution stats
            duration = table_durations.get(tbl, 0.0)
            start_time = table_start_times.get(tbl, 0)
            end_time = table_end_times.get(tbl, 0)
            status = "success" if tbl in tables_processed else "failed"
            
            # Try to get row count from table
            try:
                if status == "success":
                    row_count = spark.table(full_table_name).count()
                else:
                    row_count = None
            except Exception:
                row_count = None
            
            summary_rows.append({
                'table_config_name': tbl,
                'target_database': db,
                'result_table_name': result_table,
                'full_table_name': full_table_name,
                'status': status,
                'duration_seconds': duration,
                'start_time': start_time,
                'end_time': end_time,
                'row_count': row_count
            })
        
        return summary_rows



# COMMAND ----------

# DBTITLE 1,Materialise DataFrame Function
# ============================================================================
# BELLEROPHON (BELLE) MATERIALISE DATAFRAME - Multi-Runtime Compatible
# Compatible with DBR 13.3.x through 17.x | Unity Catalog + Hive Metastore
# ============================================================================

def _normalise_tag(tag):
    """Normalise tag to (flat_str, json_str) tuple. Accepts str or Dict[str, str]."""
    if tag is None:
        return None, None
    if isinstance(tag, dict):
        # Structured KV tags: {"domain": "Travel", "layer": "Staging"}
        flat = ",".join(f"{k}={v}" for k, v in tag.items())
        return flat, json.dumps(tag)
    if isinstance(tag, str):
        # Legacy string tag — attempt to detect KV format for parsing
        # Supports: "domain=Travel;layer=Staging" or "domain=Travel,layer=Staging"
        if "=" in tag:
            sep = ";" if ";" in tag else ","
            pairs = {}
            for part in tag.split(sep):
                if "=" in part:
                    k, v = part.split("=", 1)
                    pairs[k.strip()] = v.strip()
                else:
                    pairs[part.strip()] = part.strip()
            return tag, json.dumps(pairs)
        # Plain legacy string — no structured parse possible
        return tag, None
    return str(tag), None


def bellerophon_materialise_dataframe(
    input_df: Any,
    target_database: str,
    result_table_name: str,
    run_id: str,
    log_id: Optional[str]=None,
    subpipeline: Optional[str]=None,
    export_csv: bool=True,
    interactive_mode: Optional[bool]=None,
    tag=None,
    extra_context: Optional[Any]=None,
    monitored_id_column: Optional[str]=None,
    monitored_date_column: Optional[str]=None,
    conf: Optional[Dict[str, Any]]=None,
    print_status: bool=True,
    collect_sample: bool=True,
    sample_rows: int=10,
    dag_stage=None,
    custom_csv_removals: str="",
    external_run_id: Optional[str]=None,
    execution_context: Optional[Dict[str, Any]]=None,
    retry_count: int=0
) -> Tuple[Any, Any, Any]:
    """Materialise DataFrame to Delta (UC or Hive). Supports full/insert/refresh/merge/update/delete."""
    import builtins
    import datetime
    import traceback
    import re
    from pathlib import Path
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType, DateType, TimestampType
    from pyspark.sql import functions as bellerophon_F
    from delta.tables import DeltaTable
    import pandas as bellerophon_pd

    # ========================================================================
    # INITIALIZATION & CONFIGURATION
    # ========================================================================
    
    # Normalise tag: supports str (legacy), Dict[str, str] (structured KV), or None
    tag_flat, tags_json = _normalise_tag(tag)

    output_table_name = BellerophonUtils.apply_test_suffix(result_table_name)
    test_mode = globals().get('force_bellerophon_test_mode', False)
    
    if run_id is None:
        run_id = str(uuid.uuid4())
    if log_id is None:
        log_id = str(uuid.uuid4())
    
    execution_start_time = datetime.datetime.now(datetime.timezone.utc)
    error_message = ""
    error_code = BellerophonErrorCode.SUCCESS
    success = False

    # Detect Unity Catalog usage (3-level namespace: catalog.schema.table)
    namespace_parts = target_database.split('.')
    is_unity_catalog = len(namespace_parts) >= 2
    
    if is_unity_catalog:
        # Unity Catalog: catalog.schema format
        if len(namespace_parts) == 2:
            catalog_name, schema_name = namespace_parts
            full_table_name = f"{catalog_name}.{schema_name}.{output_table_name}"
        else:
            # Already includes table name or single catalog (assume default schema)
            full_table_name = f"{target_database}.{output_table_name}"
    else:
        # Legacy Hive metastore: database.table format
        full_table_name = f"{target_database}.{output_table_name}"
    
    # Storage configuration
    blob_target_dir = BellerophonUtils.build_blob_target_dir(target_database, subpipeline)
    
    # For UC managed tables, we let UC handle location; for external tables, use explicit location
    use_managed_table = conf.get("use_managed_table", is_unity_catalog) if conf else is_unity_catalog
    
    if use_managed_table:
        # UC managed table - no explicit location needed
        delta_location = None
    else:
        # External table or legacy - use explicit location
        delta_location = f"{blob_target_dir.rstrip('/')}/{full_table_name.replace('.', '/')}/".replace('//', '/')
    
    # CSV export paths
    # Per-config override: csv_export_dir bypasses default path construction.
    # Backward-compatible — only active if explicitly set in table config.
    if conf and conf.get('csv_export_dir'):
        csv_dir = conf['csv_export_dir']
        _now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"{target_database}.{output_table_name}_{_now}_0001.csv"
    else:
        csv_dir, csv_filename = BellerophonUtils.get_target_cube_csv_path(blob_target_dir, target_database, output_table_name)
    csv_full_path = f"{csv_dir}/{csv_filename}"

    # Load mode and partitioning
    partition_cols = conf.get("partition_by") if conf else None
    load_mode = conf.get("load_mode", "full") if conf else "full"
    interactive_mode = interactive_mode if interactive_mode is not None else BellerophonUtils.is_interactive_notebook()
    
    # Extract enriched validation metadata (if available from config validation)
    table_exists_from_validation = conf.get("_table_exists") if conf else None
    effective_force_rebuild = conf.get("_effective_force_rebuild", False) if conf else False
    partition_mismatch_detected = conf.get("_partition_mismatch", False) if conf else False
    actual_partitions_from_validation = conf.get("_actual_partitions", []) if conf else []
    refresh_n_days_from_validation = conf.get("_refresh_n_days") if conf else None
    validation_errors = conf.get("_validation_errors", []) if conf else []

    BellerophonTracer.trace(
        "materialise_dataframe", full_table_name, "ENRICHED_METADATA_READ",
        {"table_exists_from_validation": table_exists_from_validation,
         "effective_force_rebuild": effective_force_rebuild,
         "partition_mismatch_detected": partition_mismatch_detected,
         "partition_cols": partition_cols, "load_mode": load_mode,
         "use_managed_table": use_managed_table,
         "actual_partitions_from_validation": actual_partitions_from_validation,
         "validation_errors": validation_errors}
    , caller_locals=locals())
    
    # If config was validated and has errors, log them but allow execution to proceed
    # (orchestrator should have already warned/failed during init if fail_on_validation_errors=True)
    if validation_errors and print_status:
        print(f"   ⚠️  Table config has validation errors: {validation_errors}")
        print(f"   ℹ️  Proceeding with materialization (orchestrator may have been initialized with fail_on_validation_errors=False)")
    

    
    try:
        cluster_info = BellerophonUtils.get_cluster_info()
        exec_context = BellerophonUtils.get_execution_context()
        is_service_acct, account_name = BellerophonUtils.detect_service_account()
        
        user_name = account_name if not is_service_acct else None
        service_account = account_name if is_service_acct else None
        cluster_id = cluster_info.get("cluster_id", "unknown")
        cluster_name = cluster_info.get("cluster_name", "unknown")
        spark_version = cluster_info.get("spark_version", "unknown")
        dbr_version = cluster_info.get("dbr_version", "unknown")
        notebook_path = exec_context.get("notebook_path", "unknown")
        
        # Use external_run_id if provided (from ADF), otherwise use internal run_id
        parent_run_id = external_run_id if external_run_id else run_id
        
        # Serialize execution_context to JSON
        execution_context_json = json.dumps(execution_context) if execution_context else None
        
        # Initialize DML metrics (will be populated if we can track them)
        rows_before = 0
        rows_inserted = None
        rows_updated = None
        rows_deleted = None
        
        # Try to get current row count before operation
        try:
            rows_before = BellerophonUtils.get_table_row_count(full_table_name)
        except Exception:
            rows_before = 0
            
    except Exception as e:
        # Fallback values if collection fails
        user_name = "unknown"
        service_account = None
        cluster_id = "unknown"
        cluster_name = "unknown"
        spark_version = "unknown"
        dbr_version = "unknown"
        notebook_path = "unknown"
        parent_run_id = external_run_id if external_run_id else run_id
        execution_context_json = None
        rows_before = 0
        rows_inserted = None
        rows_updated = None
        rows_deleted = None
    
    # Monitoring columns
    monitored_id_max_value = ""
    monitored_date_max_value = ""
    file_size_bytes = 0
    row_count = None
    schema_json = ""
    return_value = None
    
    parameters_dict = {
        "target_database": target_database,
        "result_table_name": output_table_name,
        "run_id": run_id,
        "log_id": log_id,
        "subpipeline": subpipeline,
        "export_csv": export_csv,
        "interactive_mode": interactive_mode,
        "tag": tag_flat,
        "tags_json": tags_json,
        "extra_context": extra_context,
        "monitored_id_column": monitored_id_column,
        "monitored_date_column": monitored_date_column,
        "is_unity_catalog": is_unity_catalog,
        "use_managed_table": use_managed_table
    }
    sample_data = None

    # ========================================================================
    # HELPER FUNCTIONS
    # ========================================================================
    
    def sanitize_for_csv(df, delimiter=";", extra_remove=""):
        """Strip control chars, delimiter, and newlines from all string columns for CSV safety."""
        control_regex = r"[\x00-\x1F\x7F]"
        specials = delimiter + "\n\r\t" + (extra_remove or "")
        specials_regex = "[" + re.escape(specials) + "]"
        
        def clean_col(col):
            col = bellerophon_F.regexp_replace(col, '"', '""')
            col = bellerophon_F.regexp_replace(col, specials_regex, "")
            col = bellerophon_F.regexp_replace(col, control_regex, "")
            return col
        
        for f in df.schema.fields:
            if f.dataType.typeName() == "string":
                df = df.withColumn(f.name, clean_col(bellerophon_F.col(f.name)))
        return df
    
    def table_exists_check(table_name: str) -> bool:
        """Check if table exists - works for both UC and Hive metastore."""
        try:
            return spark.catalog.tableExists(table_name)
        except Exception as e:
            # Fallback: try to describe the table
            try:
                spark.sql(f"DESCRIBE TABLE {table_name}")
                return True
            except Exception:
                return False
    
    def get_table_location(table_name: str) -> Optional[str]:
        """Get table location if it's an external table."""
        try:
            details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()
            if details and len(details) > 0:
                return details[0].get("location")
        except Exception:
            pass
        return None

    # ========================================================================
    # MAIN MATERIALIZATION LOGIC
    # ========================================================================
    
    try:
        if print_status:
            short_name = full_table_name.split('.')[-1] if '.' in full_table_name else full_table_name
            print(f"\n▶️ [Belle] Starting: {short_name}")
            if is_unity_catalog:
                mode_icon = "🏢" if use_managed_table else "💾"
                mode_text = "UC Managed" if use_managed_table else "External"
                print(f"   {mode_icon} {mode_text} | Mode: {load_mode.upper()}")
            
            if interactive_mode and not is_unity_catalog:
                prod_db = target_database.replace("_dev", "").replace("_test", "")
                prod_blob = f"/mnt/internal/enhanced/{prod_db}/data/{prod_db}/{output_table_name}/"
                prod_csv = f"/mnt/internal/enhanced/{prod_db}/cubes/{prod_db}/{output_table_name}.csv"
                part_info = f", partitioned by {partition_cols}" if partition_cols else ""
                print(f"   [prod] Would write to: {prod_blob}{part_info}")
                if export_csv:
                    print(f"   [prod] Would export CSV: {prod_csv}")
                else:
                    print(f"   [prod] CSV export: disabled for this table")
        
        if test_mode and print_status:
            print(f"   🧪 [Belle] TEST MODE ACTIVE")

        # Check table existence
        if table_exists_from_validation is not None:
            table_exists = table_exists_from_validation
            if print_status and False:  # Debug mode only
                print(f"   ℹ️  Using cached table existence: {table_exists}")
        else:
            table_exists = table_exists_check(full_table_name)
            if print_status and False:  # Debug mode only
                print(f"   ℹ️  Checked table existence inline: {table_exists}")

        BellerophonTracer.trace(
            "materialise_dataframe", full_table_name, "TABLE_EXISTS_RESOLVED",
            {"table_exists": table_exists, "source": "cached" if table_exists_from_validation is not None else "inline_check",
             "effective_force_rebuild": effective_force_rebuild,
             "load_mode": load_mode}
        , caller_locals=locals())

        if effective_force_rebuild:
            if table_exists:
                spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                table_exists = False
                if print_status:
                    print(f"   [prep] Force rebuild - dropped {full_table_name}")

            # Production (non-interactive): purge blob for external tables.
            # Runs regardless of table_exists — orchestrator may have pre-dropped
            # the metastore entry, but blob data persists until explicitly removed.
            if not interactive_mode and not use_managed_table and delta_location:
                import builtins as _bi
                _dbutils = (
                    getattr(_bi, 'dbutils', None)
                    or globals().get('dbutils')
                )
                if _dbutils:
                    try:
                        _dbutils.fs.rm(delta_location, True)
                        if print_status:
                            print(
                                f"   [prep] Force rebuild"
                                f" - purged blob: {delta_location}"
                            )
                    except Exception as e:
                        if print_status:
                            print(
                                f"   ⚠️  Force rebuild"
                                f" - blob purge failed: {e}"
                            )

            original_load_mode = load_mode
            if load_mode != "full":
                load_mode = "full"
            BellerophonTracer.trace(
                "materialise_dataframe", full_table_name, "FORCE_REBUILD_MODE_SWITCH",
                {"effective_force_rebuild": True, "table_exists": table_exists,
                 "original_load_mode": original_load_mode, "final_load_mode": load_mode,
                 "partition_cols": partition_cols}
            , caller_locals=locals())
            if print_status and original_load_mode != "full":
                print(f"   [prep] Force rebuild: load_mode {original_load_mode} -> full")
        
        # ====================================================================
        # LOAD MODE EXECUTION
        # ====================================================================
        
        # v1.2.12: Schema drift detection before write
        # Bypassed when force_rebuild=True (table already dropped; schema change is intentional)
        if table_exists and not effective_force_rebuild and BellerophonConfig.SCHEMA_DRIFT_ACTION != "ignore":
            try:
                existing_cols = {f.name: str(f.dataType) for f in spark.table(full_table_name).schema.fields}
                incoming_cols = {f.name: str(f.dataType) for f in input_df.schema.fields}
                
                added = set(incoming_cols.keys()) - set(existing_cols.keys())
                removed = set(existing_cols.keys()) - set(incoming_cols.keys())
                changed = {c for c in (set(incoming_cols.keys()) & set(existing_cols.keys()))
                          if incoming_cols[c] != existing_cols[c]}
                
                if added or removed or changed:
                    drift_msg = []
                    if added: drift_msg.append(f"added={list(added)}")
                    if removed: drift_msg.append(f"removed={list(removed)}")
                    if changed: drift_msg.append(f"type_changed={{{', '.join(f'{c}: {existing_cols[c]}→{incoming_cols[c]}' for c in changed)}}}")
                    drift_str = "; ".join(drift_msg)
                    
                    BellerophonTracer.trace(
                        "materialise_dataframe", full_table_name, "SCHEMA_DRIFT",
                        {"added": list(added), "removed": list(removed),
                         "type_changed": list(changed), "action": BellerophonConfig.SCHEMA_DRIFT_ACTION})
                    
                    if print_status:
                        print(f"  ⚠️  Schema drift detected: {drift_str}")
                    
                    if BellerophonConfig.SCHEMA_DRIFT_ACTION == "fail":
                        raise ValueError(f"Schema drift detected for {full_table_name}: {drift_str}")
            except ValueError:
                raise  # Re-raise the fail action
            except Exception:
                pass  # Don't block writes for schema check failures
        
        BellerophonTracer.trace(
            "materialise_dataframe", full_table_name, "WRITE_MODE_EXECUTION",
            {"load_mode": load_mode, "table_exists": table_exists,
             "partition_cols": partition_cols, "use_managed_table": use_managed_table,
             "is_unity_catalog": is_unity_catalog,
             "effective_force_rebuild": effective_force_rebuild}
        , caller_locals=locals())
        # ── Inline Encryption (config-driven, v1.2.15) ────────────────────
        # Per-column strategy: cast each column to string, encrypt individually.
        # Eliminates to_json serialization bottleneck (3-5x faster on wide tables).
        # Blob strategy (legacy): to_json(struct) → single encrypted_payload.
        if conf and conf.get('encrypt') and BellerophonConfig.FEATURE_ENCRYPTION:
            import pyspark.sql.functions as _F
            _enc_key = conf.get('encrypt_key')
            _enc_exclude = conf.get('encrypt_exclude', [])
            if not _enc_key:
                raise ValueError(
                    f"Table '{result_table_name}' has encrypt=True "
                    f"but no encrypt_key provided in config"
                )
            _payload_cols = [c for c in input_df.columns if c not in _enc_exclude]
            if BellerophonConfig.ENCRYPTION_STRATEGY == "per_column":
                # Per-column: each column cast to string, encrypted to BINARY.
                # No struct, no JSON serialization, no single-blob bottleneck.
                _enc_exprs = [_F.col(c) for c in _enc_exclude]
                for _c in _payload_cols:
                    _enc_exprs.append(
                        _F.expr(
                            f"aes_encrypt(cast(`{_c}` as string), "
                            f"unbase64('{_enc_key}'), "
                            f"'{BellerophonConfig.ENCRYPTION_MODE}', 'DEFAULT')"
                        ).alias(_c)
                    )
                input_df = input_df.select(*_enc_exprs)
            else:
                # Legacy blob: to_json → single encrypted_payload column.
                input_df = (
                    input_df
                    .withColumn("_payload", _F.to_json(_F.struct(*_payload_cols)))
                    .withColumn(
                        "encrypted_payload",
                        _F.expr(
                            f"aes_encrypt(_payload, unbase64('{_enc_key}'), "
                            f"'{BellerophonConfig.ENCRYPTION_MODE}', 'DEFAULT')"
                        ),
                    )
                    .select(
                        *[_F.col(c) for c in _enc_exclude],
                        "encrypted_payload"
                    )
                )
            if print_status:
                _strategy = BellerophonConfig.ENCRYPTION_STRATEGY
                belle_print(
                    f"   🔐 Encrypted {len(_payload_cols)} columns "
                    f"(strategy={_strategy}, kept: {_enc_exclude or 'none'})",
                    level=2)

        # full_if_not_exists: create table only on first run, skip on subsequent runs
        if load_mode == "full_if_not_exists":
            if table_exists:
                if print_status:
                    _existing_count = spark.table(full_table_name).count()
                    print(f"   ⏭️  Table already exists ({_existing_count:,} rows) — skipped (full_if_not_exists)")
                # load_mode stays as full_if_not_exists — handled by elif below
            else:
                load_mode = "full"  # Table missing — create with full mode
                if print_status:
                    print(f"   📝  Table does not exist — creating (full_if_not_exists → full)")

        if load_mode == "full":
            # Drop existing table
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            
            # FIX #10: Safe external location deletion with validation
            if not use_managed_table and delta_location:
                # Safety checks before deletion
                safe_to_delete = False
                
                # Check 1: Location is not empty
                if delta_location and len(delta_location.strip()) > 0:
                    # Check 2: Not a root path (must have at least 3 path segments)
                    # e.g., /mnt/data/mydb/mytable (good), /mnt/data (bad - too short)
                    path_parts = [p for p in delta_location.split('/') if p]
                    if len(path_parts) >= 3:
                        # Check 3: Path looks like a Delta table location (contains catalog/schema/table structure)
                        # Typical Unity Catalog pattern: /.../{catalog}/{schema}/{table}
                        # Or external: /mnt/{mount}/{path}/{table}
                        safe_to_delete = True
                    else:
                        if print_status:
                            print(f"   ⚠️ Skipping deletion: Path too short (possible root path): {delta_location}")
                else:
                    if print_status:
                        print(f"   ⚠️ Skipping deletion: Empty or invalid path")
                
                if safe_to_delete:
                    try:
                        if print_status:
                            print(f"   🗑️ Deleting external location: {delta_location}")
                        # FIX #13+: Guard dbutils usage
                        # dbutils is injected by Databricks at notebook level,
                        # not in module globals(). Check builtins too.
                        import builtins as _bi
                        _dbutils = getattr(_bi, 'dbutils', None) or globals().get('dbutils')
                        if _dbutils:
                            _dbutils.fs.rm(delta_location, True)
                        else:
                            if print_status:
                                print(f"   ⚠️ dbutils not available, cannot delete external location")
                    except Exception as e:
                        if print_status:
                            print(f"   ⚠️ Could not delete data at {delta_location}: {e}")
            
            # Write table
            writer = input_df.write.format("delta").mode("overwrite")
            if partition_cols:
                writer = writer.partitionBy(partition_cols)
            
            if use_managed_table:
                writer.saveAsTable(full_table_name)
            else:
                # v1.2.8 FIX: For external tables, allow schema/partition overwrite.
                # DROP TABLE only removes metadata; Delta files at the location may
                # persist if dbutils cleanup failed. overwriteSchema=true lets Delta
                # accept partition/schema changes on the existing files.
                writer.option("overwriteSchema", "true").save(delta_location)
                # Register table in metastore from existing Delta location.
                # Schema and partition info are already in the Delta log,
                # so no PARTITIONED BY clause is needed (or allowed).
                spark.sql(f"""CREATE TABLE IF NOT EXISTS {full_table_name}
                              USING DELTA
                              LOCATION '{delta_location}'""")
                
                # v1.2.12: Validate external table partition alignment
                if partition_cols:
                    try:
                        _detail = spark.sql(f"DESCRIBE DETAIL {full_table_name}").first()
                        _actual_parts = list(_detail['partitionColumns']) if _detail['partitionColumns'] else []
                        if sorted(_actual_parts) != sorted(partition_cols):
                            print(f"  ⚠️  Partition mismatch for {full_table_name}: "
                                  f"expected={partition_cols}, actual={_actual_parts}")
                    except Exception:
                        pass

        elif load_mode == "insert":
            writer = input_df.write.format("delta")
            if partition_cols:
                writer = writer.partitionBy(partition_cols)
            
            if not table_exists:
                if use_managed_table:
                    writer.mode("overwrite").saveAsTable(full_table_name)
                else:
                    writer.mode("overwrite").option("overwriteSchema", "true").save(delta_location)
                    # Register table from Delta location (schema/partitions in Delta log)
                    spark.sql(f"""CREATE TABLE IF NOT EXISTS {full_table_name}
                                  USING DELTA
                                  LOCATION '{delta_location}'""")
            else:
                if use_managed_table:
                    writer.mode("append").saveAsTable(full_table_name)
                else:
                    writer.mode("append").save(delta_location)

        elif load_mode.startswith("refresh_n_days"):
            # FIX #1: Robust N extraction with validation
            # v1.2.7 - Use enriched metadata if available
            if refresh_n_days_from_validation is not None:
                N = refresh_n_days_from_validation
                if print_status and False:  # Debug mode only
                    print(f"   ℹ️  Using cached refresh window: {N} days")
            else:
                # Fallback: extract N from load_mode string
                try:
                    if '-' in load_mode:
                        N = int(load_mode.split('-')[-1])
                    else:
                        raise ValueError(f"refresh_n_days mode must specify N as 'refresh_n_days-N' (e.g., 'refresh_n_days-7'), got '{load_mode}'")
                except (ValueError, IndexError) as e:
                    raise ValueError(f"Invalid refresh_n_days format '{load_mode}': {e}")
            
            # FIX #7: Better validation for single partition requirement
            if not partition_cols or len(partition_cols) != 1:
                raise ValueError(f"refresh_n_days mode requires exactly ONE partition column (e.g., ['date_key']). Got: {partition_cols}")
            partition_col = partition_cols[0]
            
            # FIX #PARTITIONING-GUARD: Validate existing table has correct partitioning
            # CRITICAL: If table exists without partitions, fail fast with clear guidance
            # v1.2.7 - Skip inline validation if config was already validated and partitions match
            skip_inline_validation = (
                table_exists_from_validation is not None and  # Config was validated
                not partition_mismatch_detected  # No partition mismatch found
            )
            
            if table_exists and not skip_inline_validation:
                try:
                    # Check actual partition columns from table metadata
                    detail_df = spark.sql(f"DESCRIBE DETAIL {full_table_name}")
                    actual_partitions = detail_df.select("partitionColumns").first()[0]
                    
                    # Convert to list if needed
                    if actual_partitions is None:
                        actual_partitions = []
                    elif not isinstance(actual_partitions, list):
                        actual_partitions = [actual_partitions]
                    
                    # Validate partitioning matches expected
                    if len(actual_partitions) == 0:
                        # Table exists but is NOT partitioned
                        raise ValueError(
                            f"❌ PARTITIONING MISMATCH: Table '{full_table_name}' exists WITHOUT partitions, "
                            f"but refresh_n_days mode requires partition_by={partition_cols}.\n"
                            f"   Solutions:\n"
                            f"   1. DROP and recreate: DROP TABLE {full_table_name}; (then re-run with partition_by)\n"
                            f"   2. Change to 'full' mode: Use load_mode='full' instead of 'refresh_n_days'\n"
                            f"   3. Disable partitioning: Remove partition_by from config (if appropriate)\n"
                            f"   \n"
                            f"   🔍 Current state: Table has partitionColumns={actual_partitions} (empty)"
                        )
                    elif actual_partitions != partition_cols:
                        # Table partitioned but with different columns
                        raise ValueError(
                            f"❌ PARTITIONING MISMATCH: Table '{full_table_name}' partitioned by {actual_partitions}, "
                            f"but config specifies partition_by={partition_cols}.\n"
                            f"   Solutions:\n"
                            f"   1. Update config to match table: partition_by={actual_partitions}\n"
                            f"   2. DROP and recreate with new partitioning: DROP TABLE {full_table_name};\n"
                            f"   \n"
                            f"   🔍 Cannot change partitioning of existing table without recreating it."
                        )
                    # else: Partitioning matches - proceed normally
                    
                except Exception as e:
                    # If we can't read table metadata, something is wrong
                    if "PARTITIONING MISMATCH" in str(e):
                        # Re-raise our validation errors
                        raise
                    else:
                        # Some other error reading metadata - warn but don't block
                        if print_status:
                            print(f"   ⚠️  Could not validate partitioning: {e}")
                            print(f"   ℹ️  Proceeding with refresh_n_days, but this may fail if partitioning is incorrect")
            elif table_exists and skip_inline_validation:
                # v1.2.7 - Using cached validation results
                if print_status and False:  # Debug mode only
                    print(f"   ✅ Using cached partition validation (partitions match: {actual_partitions_from_validation})")
            
            # FIX #5 & #6: Delete last N partitions with robust type handling
            if table_exists:
                try:
                    # FIX #4: Check table exists before using DeltaTable.forName
                    df_existing = spark.table(full_table_name)
                    max_date = df_existing.agg(bellerophon_F.max(partition_col)).first()[0]
                    
                    # FIX #5: Guard against None max_date (empty table)
                    if max_date is not None:
                        import pandas as pd
                        # Removed: `from datetime import datetime` shadowed module-level import
                        
                        # FIX #3 & #6: Robust partition format handling
                        # Handle int (20260306), string ("20260306"), date, or timestamp
                        if isinstance(max_date, int):
                            # Already YYYYMMDD integer
                            max_date_str = str(max_date)
                        elif isinstance(max_date, str):
                            # String - try to parse as YYYYMMDD or YYYY-MM-DD
                            max_date_str = max_date.replace('-', '')  # Remove dashes if present
                        else:
                            # Date or timestamp object
                            max_date_str = pd.Timestamp(max_date).strftime('%Y%m%d')
                        
                        # Convert to pandas Timestamp for date arithmetic
                        try:
                            max_date_ts = pd.Timestamp(datetime.datetime.strptime(max_date_str, '%Y%m%d'))
                        except Exception as e:
                            raise ValueError(f"Could not parse partition value '{max_date}' as YYYYMMDD date: {e}")
                        
                        # Generate list of partitions to delete (as integers)
                        to_delete = [int((max_date_ts - pd.Timedelta(days=i)).strftime('%Y%m%d')) for i in range(N)]
                        
                        # FIX #4: Only call DeltaTable.forName if table exists
                        delta_tbl = DeltaTable.forName(spark, full_table_name)
                        for val in to_delete:
                            # FIX #6: Cast partition column to int for comparison (handles both int and string)
                            delta_tbl.delete(bellerophon_F.col(partition_col).cast("int") == val)
                        
                        if print_status:
                            print(f"   🗑️  Deleted {N} partitions: {to_delete}")
                    elif print_status:
                        print(f"   ℹ️  Table exists but is empty - skipping partition deletion")
                        
                except Exception as e:
                    if print_status:
                        print(f"   ⚠️ Could not delete partitions: {e}")
                    # Don't fail the entire job if partition deletion fails
            
            # FIX #2 & #8: Use overwrite mode for first write (NO pre-create needed)
            # First write: overwrite mode creates table WITH partitions automatically
            # Subsequent writes: append mode adds to existing partitions
            # v1.2.7 FIX: Use overwrite for BOTH managed AND external tables when table doesn't exist
            write_mode = "overwrite" if not table_exists else "append"
            
            writer = input_df.write.format("delta").mode(write_mode)
            if partition_cols:
                writer = writer.partitionBy(partition_cols)
            
            if use_managed_table:
                # FIX #8: Simple saveAsTable - no pre-create needed!
                # Overwrite mode on first write creates table WITH partitions
                writer.saveAsTable(full_table_name)
                if print_status and not table_exists:
                    print(f"   ✅ Created managed table with partitions: {partition_cols}")
            else:
                # External table: write data first, then create table pointing to location
                if write_mode == "overwrite":
                    writer.option("overwriteSchema", "true").save(delta_location)
                else:
                    writer.save(delta_location)
                if not table_exists:
                    # Register table from Delta location (schema/partitions in Delta log)
                    spark.sql(f"""CREATE TABLE IF NOT EXISTS {full_table_name}
                                  USING DELTA
                                  LOCATION '{delta_location}'""")

        elif load_mode == "merge":
            merge_keys = conf.get("merge_keys", [])
            update_columns = conf.get("merge_update_columns", [])
            # Auto-derive update columns: every column except the merge keys
            if not update_columns and merge_keys:
                update_columns = [c for c in input_df.columns if c not in set(merge_keys)]
            if not merge_keys or not update_columns:
                raise ValueError("merge_keys and merge_update_columns must be specified in config for merge mode.")
            
            # FIX #4: Check table exists before merge operation
            if not table_exists:
                raise ValueError(f"Cannot merge into table '{full_table_name}': table does not exist. Create it first with 'full' or 'insert' mode.")
            
            # FIX #34: Validate merge key uniqueness to prevent duplicate matches
            if BellerophonConfig.MERGE_VALIDATE_SOURCE_KEYS:
                if print_status:
                    print(f"   🔍 Validating source merge keys: {merge_keys}")
                
                input_df, source_dup_count = BellerophonOrchestrator.validate_merge_keys(
                    input_df,
                    merge_keys=merge_keys,
                    df_name="source",
                    auto_deduplicate=BellerophonConfig.MERGE_AUTO_DEDUPLICATE_SOURCE,
                    fail_on_duplicates=BellerophonConfig.MERGE_FAIL_ON_DUPLICATE_KEYS
                )
                
                if source_dup_count == 0 and print_status:
                    print(f"   ✅ Source keys are unique")
            
            # FIX #34: Optional target key validation (can be expensive for large tables)
            if BellerophonConfig.MERGE_VALIDATE_TARGET_KEYS:
                if print_status:
                    print(f"   🔍 Validating target merge keys (this may take time)...")
                
                target_df = spark.table(full_table_name)
                _, target_dup_count = BellerophonOrchestrator.validate_merge_keys(
                    target_df,
                    merge_keys=merge_keys,
                    df_name="target",
                    auto_deduplicate=False,  # Can't auto-fix target in-place
                    fail_on_duplicates=BellerophonConfig.MERGE_FAIL_ON_DUPLICATE_KEYS
                )
                
                if target_dup_count == 0 and print_status:
                    print(f"   ✅ Target keys are unique")
                elif target_dup_count > 0:
                    print(f"   ⚠️  Target has {target_dup_count:,} duplicate keys - consider running OPTIMIZE or manual dedup")
            
            merge_condition = " AND ".join([f"source.{k} = target.{k}" for k in merge_keys])
            updates = {col: f"source.{col}" for col in update_columns}
            
            # FIX #35: Get target table schema to align insert columns
            delta_tbl = DeltaTable.forName(spark, full_table_name)
            target_columns = [field.name for field in spark.table(full_table_name).schema.fields]
            
            # Build insert values dict - only include columns that exist in target
            insert_values = {col: f"source.{col}" for col in target_columns if col in input_df.columns}
            
            # Execute merge with explicit column mapping
            delta_tbl.alias("target").merge(
                input_df.alias("source"),
                merge_condition
            ).whenMatchedUpdate(
                set=updates
            ).whenNotMatchedInsert(
                values=insert_values  # FIX #35: Explicit columns instead of insertAll
            ).execute()

        elif load_mode == "update":
            update_set = conf.get("update_set", {})
            update_where = conf.get("update_where", None)
            if not update_set:
                raise ValueError("update_set must be specified in config for update mode.")
            
            # FIX #4: Check table exists before update operation
            if not table_exists:
                raise ValueError(f"Cannot update table '{full_table_name}': table does not exist. Create it first with 'full' or 'insert' mode.")
            
            delta_tbl = DeltaTable.forName(spark, full_table_name)
            delta_tbl.update(
                condition=update_where,
                set=update_set
            )

        elif load_mode == "delete":
            delete_where = conf.get("delete_where", None)
            if not delete_where:
                raise ValueError("delete_where must be specified in config for delete mode.")
            
            # FIX #4: Check table exists before delete operation
            if not table_exists:
                raise ValueError(f"Cannot delete from table '{full_table_name}': table does not exist. Create it first with 'full' or 'insert' mode.")
            
            delta_tbl = DeltaTable.forName(spark, full_table_name)
            delta_tbl.delete(condition=delete_where)

        elif load_mode == "full_if_not_exists":
            pass  # Table exists — write skipped (handled above)

        else:
            raise ValueError(f"Unsupported load_mode: {load_mode}")

        # ====================================================================
        # POST-WRITE OPERATIONS
        # ====================================================================
        
        # Collect sample data
        if collect_sample:
            try:
                sample_data = spark.table(full_table_name).limit(sample_rows).toPandas()
            except Exception as e:
                if print_status:
                    print(f"   ⚠️ Could not collect sample data: {e}")
                sample_data = None

        # ====================================================================
        # CSV EXPORT (Optional Feature - Can be disabled via BellerophonConfig)
        # ====================================================================
        # MIGRATION NOTE: Set BellerophonConfig.FEATURE_CSV_EXPORT = False to disable
        
        if export_csv and BellerophonConfig.FEATURE_CSV_EXPORT and not interactive_mode:
            # ================================================================
            # CSV EXPORT PERFORMANCE NOTES (Issues #18-20)
            # ================================================================
            # FIX #18: coalesce(1) bottleneck - Single-file output is intentional for:
            #   - Simple consumption by external systems expecting single files
            #   - Backward compatibility with existing integrations
            #   TRADE-OFF: Bottleneck for large datasets (all data through 1 executor)
            #   FUTURE: Add BellerophonConfig.CSV_MAX_FILES to control file count
            #
            # FIX #19: partition collect() memory - Collects distinct partition values
            #   to driver. Can blow driver memory with many partitions.
            #   FUTURE: Use partitionBy() for file-based partitioning instead
            #
            # FIX #20: sanitize_for_csv performance - Regex on ALL string columns (full scan)
            #   FUTURE: Add BellerophonConfig.CSV_SANITIZE_ENABLED flag or sampling
            # ================================================================
            
            if print_status:
                print(f"   📥 Exporting CSV...")

            if load_mode in ["insert", "merge", "update", "delete"]:
                # Export only the changed data (batch mode)
                df_to_export = input_df
                df_clean = sanitize_for_csv(df_to_export, delimiter=";", extra_remove=custom_csv_removals)
                batch_csv_dir = f"{csv_dir}/batch_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                try:
                    # FIX #13: Guard dbutils usage
                    if 'dbutils' in globals():
                        dbutils.fs.mkdirs(batch_csv_dir)
                except Exception:
                    pass
                
                if partition_cols:
                    # FIX #19: collect() on partition keys can blow driver memory with many partitions
                    # Consider file-based partitioning: .write.partitionBy(*partition_cols)
                    partitions = df_clean.select(*partition_cols).distinct().collect()
                    for partition in partitions:
                        part_df = df_clean
                        partition_key = {}
                        for col_name, value in zip(partition_cols, partition):
                            part_df = part_df.filter(bellerophon_F.col(col_name) == value)
                            partition_key[col_name] = value
                        
                        partition_csv_subdir = f"{batch_csv_dir}/" + "_".join([f"{col}={val}" for col, val in partition_key.items()])
                        # FIX #16-17: Use BellerophonConfig CSV options and fix invalid quote/boolean strings
                        # FIX #18: coalesce(1) performance bottleneck - kept for backward compatibility
                        csv_options = {
                            "encoding": BellerophonConfig.CSV_ENCODING,
                            "header": "true",
                            "sep": BellerophonConfig.CSV_DELIMITER,
                            "lineSep": BellerophonConfig.CSV_LINE_SEP
                        }
                        # Only add quote options if not empty (quote="" is invalid in some Spark versions)
                        if BellerophonConfig.CSV_QUOTE:
                            csv_options["quote"] = BellerophonConfig.CSV_QUOTE
                            csv_options["quoteAll"] = BellerophonConfig.CSV_QUOTE_ALL
                            csv_options["escapeQuotes"] = BellerophonConfig.CSV_ESCAPE_QUOTES
                        part_df.coalesce(1).write.mode("overwrite").format("csv").options(**csv_options).save(partition_csv_subdir)
                        BellerophonUtils.rename_csv_part_file(partition_csv_subdir)
                else:
                    # FIX #16-17: Use BellerophonConfig CSV options and fix invalid quote/boolean strings
                    # FIX #18: coalesce(1) performance bottleneck - kept for backward compatibility
                    csv_options = {
                        "encoding": BellerophonConfig.CSV_ENCODING,
                        "header": "true",
                        "sep": BellerophonConfig.CSV_DELIMITER,
                        "lineSep": BellerophonConfig.CSV_LINE_SEP
                    }
                    # Only add quote options if not empty (quote="" is invalid in some Spark versions)
                    if BellerophonConfig.CSV_QUOTE:
                        csv_options["quote"] = BellerophonConfig.CSV_QUOTE
                        csv_options["quoteAll"] = BellerophonConfig.CSV_QUOTE_ALL
                        csv_options["escapeQuotes"] = BellerophonConfig.CSV_ESCAPE_QUOTES
                    df_clean.coalesce(1).write.mode("overwrite").format("csv").options(**csv_options).save(batch_csv_dir)
                    BellerophonUtils.rename_csv_part_file(batch_csv_dir, csv_filename)

            else:  # full or refresh_n_days - export complete table
                df = spark.table(full_table_name)
                df_clean = sanitize_for_csv(df, delimiter=";", extra_remove=custom_csv_removals)
                
                try:
                    # FIX #13: Guard dbutils usage
                    if 'dbutils' in globals():
                        dbutils.fs.rm(csv_dir, recurse=True)
                        dbutils.fs.mkdirs(csv_dir)
                    else:
                        if print_status:
                            print(f"   ⚠️ dbutils not available, cannot manage CSV directory")
                except Exception as e:
                    if print_status:
                        print(f"   ⚠️ Could not manage CSV directory (may be UC managed): {e}")
                
                if partition_cols:
                    # FIX #19: collect() on partition keys can blow driver memory with many partitions
                    # Consider file-based partitioning: .write.partitionBy(*partition_cols)
                    partitions = df_clean.select(*partition_cols).distinct().collect()
                    for partition in partitions:
                        part_df = df_clean
                        partition_key = {}
                        for col_name, value in zip(partition_cols, partition):
                            part_df = part_df.filter(bellerophon_F.col(col_name) == value)
                            partition_key[col_name] = value
                        
                        partition_csv_subdir = f"{csv_dir}/" + "_".join([f"{col}={val}" for col, val in partition_key.items()])
                        # FIX #16-17: Use BellerophonConfig CSV options and fix invalid quote/boolean strings
                        # FIX #18: coalesce(1) performance bottleneck - kept for backward compatibility
                        csv_options = {
                            "encoding": BellerophonConfig.CSV_ENCODING,
                            "header": "true",
                            "sep": BellerophonConfig.CSV_DELIMITER,
                            "lineSep": BellerophonConfig.CSV_LINE_SEP
                        }
                        # Only add quote options if not empty (quote="" is invalid in some Spark versions)
                        if BellerophonConfig.CSV_QUOTE:
                            csv_options["quote"] = BellerophonConfig.CSV_QUOTE
                            csv_options["quoteAll"] = BellerophonConfig.CSV_QUOTE_ALL
                            csv_options["escapeQuotes"] = BellerophonConfig.CSV_ESCAPE_QUOTES
                        part_df.coalesce(1).write.mode("overwrite").format("csv").options(**csv_options).save(partition_csv_subdir)
                        BellerophonUtils.rename_csv_part_file(partition_csv_subdir)
                else:
                    # FIX #16-17: Use BellerophonConfig CSV options and fix invalid quote/boolean strings
                    # FIX #18: coalesce(1) performance bottleneck - kept for backward compatibility
                    csv_options = {
                        "encoding": BellerophonConfig.CSV_ENCODING,
                        "header": "true",
                        "sep": BellerophonConfig.CSV_DELIMITER,
                        "lineSep": BellerophonConfig.CSV_LINE_SEP
                    }
                    # Only add quote options if not empty (quote="" is invalid in some Spark versions)
                    if BellerophonConfig.CSV_QUOTE:
                        csv_options["quote"] = BellerophonConfig.CSV_QUOTE
                        csv_options["quoteAll"] = BellerophonConfig.CSV_QUOTE_ALL
                        csv_options["escapeQuotes"] = BellerophonConfig.CSV_ESCAPE_QUOTES
                    df_clean.coalesce(1).write.mode("overwrite").format("csv").options(**csv_options).save(csv_dir)
                    BellerophonUtils.rename_csv_part_file(csv_dir, csv_filename)
                    
            if print_status:
                print(f"   ✅ CSV export complete")

        # ====================================================================
        # COLLECT METRICS
        # ====================================================================
        
        try:
            df_table = spark.table(full_table_name)
            row_count = df_table.count()
            
            # v1.2.10 FIX: Post-write row count validation (Issue backlog HIGH PRIORITY)
            if BellerophonConfig.WRITE_ROW_COUNT_VALIDATION and input_df is not None:
                try:
                    source_count = input_df.count()
                    if source_count > 0 and row_count is not None:
                        # For full/insert modes, target should >= source
                        # For refresh_n_days, target may be larger (historical + new)
                        if load_mode in ('full', 'full_if_not_exists') and source_count > 0:
                            diff_ratio = abs(row_count - source_count) / source_count
                            if diff_ratio > BellerophonConfig.WRITE_ROW_COUNT_TOLERANCE:
                                _rc_msg = (f"⚠️  Row count mismatch for {full_table_name}: "
                                          f"source={source_count:,}, target={row_count:,}, "
                                          f"diff={diff_ratio:.2%} > tolerance={BellerophonConfig.WRITE_ROW_COUNT_TOLERANCE:.0%}")
                                if print_status:
                                    print(_rc_msg)
                                BellerophonTracer.trace(
                                    "WRITE_ROW_COUNT_MISMATCH", full_table_name,
                                    source_count=source_count, target_count=row_count,
                                    diff_ratio=round(diff_ratio, 4), load_mode=load_mode)
                except Exception:
                    pass  # Don't fail materialise over validation
            
            schema_json = df_table.schema.json()
            # v1.2.9: Truncate schema_json to prevent log table bloat
            _max_len = BellerophonConfig.LOG_SCHEMA_JSON_MAX_LENGTH
            if _max_len > 0 and len(schema_json) > _max_len:
                schema_json = schema_json[:_max_len] + f"... [TRUNCATED from {len(schema_json)} chars]"
            
            # v1.2.9 - Multiple column monitoring: accepts string or list
            def _collect_max_values(df, col_spec, col_type="id"):
                """Collect MAX values for one or more monitoring columns."""
                if not col_spec:
                    return ""
                cols = [col_spec] if isinstance(col_spec, str) else list(col_spec)
                results = []
                for c in cols:
                    if c in df.columns:
                        try:
                            val = df.agg({c: "max"}).collect()[0][0]
                            if col_type == "date" and val is not None:
                                # v1.2.12: Expanded date format handling
                                if isinstance(val, (datetime.date,)):
                                    pass  # Already a date
                                elif isinstance(val, datetime.datetime):
                                    val = val.date()
                                elif isinstance(val, str):
                                    # Try multiple formats
                                    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y%m%d", "%d/%m/%Y"):
                                        try:
                                            val = datetime.datetime.strptime(val[:10], fmt).date()
                                            break
                                        except ValueError:
                                            continue
                                elif isinstance(val, int):
                                    val = datetime.datetime.strptime(str(val), "%Y%m%d").date()
                            results.append(f"{c}={val}")
                        except Exception:
                            results.append(f"{c}=ERROR")
                return "; ".join(results) if results else ""

            monitored_id_max_value = _collect_max_values(df_table, monitored_id_column, "id")
            monitored_date_max_value = _collect_max_values(df_table, monitored_date_column, "date")

        except Exception as e:
            row_count = None
            schema_json = ""
            if monitored_id_column:
                monitored_id_max_value = f"ERROR: {e}"
            if monitored_date_column:
                monitored_date_max_value = f"ERROR: {e}"

        # Get table size
        try:
            tbl_info = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()
            if tbl_info and len(tbl_info) > 0:
                tbl_dict = tbl_info[0].asDict()
                if "sizeInBytes" in tbl_dict:
                    file_size_bytes = tbl_dict["sizeInBytes"]
                elif "location" in tbl_dict:
                    tbl_path = tbl_dict["location"]
                    try:
                        # FIX #13: Guard dbutils usage - fail gracefully in non-Databricks environments
                        if 'dbutils' not in globals():
                            file_size_bytes = 0
                        else:
                            files = dbutils.fs.ls(tbl_path)
                            file_size_bytes = builtins.sum(file_info.size for file_info in files)
                    except Exception:
                        file_size_bytes = 0
        except Exception as e:
            file_size_bytes = 0

        success = True
        # FIX #26: Return DataFrame instead of string to prevent OutputRegistry pollution
        # OutputRegistry.set_output() expects DataFrames, not table name strings
        # This ensures health checks and downstream operations get actual DataFrames
        return_value = df_table  # Changed from: full_table_name
        
        if print_status:
            print(f"   📊 {row_count:,} rows | {file_size_bytes/1024/1024:.1f} MB")

    except Exception as e:
        error_message = str(e)
        
        # v1.2.9 FIX Issue #39: Proper error classification with type-first, then patterns
        import re as _re
        error_type_name = type(e).__name__
        error_code = BellerophonErrorCode.UNKNOWN_ERROR
        
        # Priority 1: Exception type matching (most precise)
        _type_map = {
            'AnalysisException': BellerophonErrorCode.TABLE_NOT_FOUND,
            'PermissionError': BellerophonErrorCode.PERMISSION_DENIED,
            'MemoryError': BellerophonErrorCode.OOM_ERROR,
        }
        if error_type_name in _type_map:
            error_code = _type_map[error_type_name]
        else:
            # Priority 2: Ordered pattern matching (first match wins)
            _patterns = [
                # OOM - check specific Java exception, then keywords
                (_re.compile(r'java\.lang\.OutOfMemoryError|Container killed|executor OOM', _re.I),
                 BellerophonErrorCode.OOM_ERROR),
                # Permission
                (_re.compile(r'Permission denied|Access denied|FORBIDDEN|UnauthorizedException', _re.I),
                 BellerophonErrorCode.PERMISSION_DENIED),
                # Catalog access
                (_re.compile(r'CATALOG_ACCESS_DENIED|catalog.*not found', _re.I),
                 BellerophonErrorCode.CATALOG_ACCESS_DENIED),
                # Table not found
                (_re.compile(r'TABLE_OR_VIEW_NOT_FOUND|Table or view.*not found|table.*does not exist', _re.I),
                 BellerophonErrorCode.TABLE_NOT_FOUND),
                # Schema mismatch
                (_re.compile(r'schema.*mismatch|cannot.*evolve|overwriteSchema|Partition columns do not match', _re.I),
                 BellerophonErrorCode.SCHEMA_MISMATCH),
                # Delta operation failures
                (_re.compile(r'ConcurrentModification|ConcurrentAppend|DELTA_CONCURRENT', _re.I),
                 BellerophonErrorCode.DELTA_OPERATION_FAILED),
                # Timeout
                (_re.compile(r'TimeoutException|SocketTimeout|deadline exceeded', _re.I),
                 BellerophonErrorCode.TIMEOUT_ERROR),
                # Cluster
                (_re.compile(r'ExecutorLostFailure|FetchFailed|TaskKilled|SparkUpgrade', _re.I),
                 BellerophonErrorCode.CLUSTER_ERROR),
                # Merge key
                (_re.compile(r'merge.*key|duplicate.*match|MERGE.*ON.*condition', _re.I),
                 BellerophonErrorCode.MERGE_KEY_MISSING),
            ]
            for pattern, code in _patterns:
                if pattern.search(error_message):
                    error_code = code
                    break
        
        if print_status:
            print(f"\n❌ {full_table_name.split('.')[-1]}: {error_message[:150]}")
        traceback.print_exc()
        raise

    finally:
        execution_end_time = datetime.datetime.now(datetime.timezone.utc)
        try:
            execution_duration_seconds = (execution_end_time - execution_start_time).total_seconds()
        except Exception as e:
            execution_duration_seconds = f"ERROR: {e}"

        # ====================================================================
        # LOGGING
        # ====================================================================
        
        logging_schema = StructType([
            StructField("run_id", StringType(), True),
            StructField("log_id", StringType(), True),
            StructField("target_table_name", StringType(), True),
            StructField("target_table_blob_dir", StringType(), True),
            StructField("csv_path", StringType(), True),
            StructField("execution_start_time", TimestampType(), True),
            StructField("execution_end_time", TimestampType(), True),
            StructField("execution_duration_seconds", DoubleType(), True),
            StructField("ran_in_interactive_mode", BooleanType(), True),
            StructField("success", BooleanType(), True),
            StructField("error_message", StringType(), True),
            StructField("row_count", LongType(), True),
            StructField("file_size_bytes", LongType(), True),
            StructField("parameters", StringType(), True),
            StructField("schema_json", StringType(), True),
            StructField("monitored_id_max_value", StringType(), True),
            StructField("monitored_date_max_value", StringType(), True),
            StructField("dag_stage", IntegerType(), True),
            StructField("is_unity_catalog", BooleanType(), True),
            StructField("use_managed_table", BooleanType(), True),
            StructField("user_name", StringType(), True),
            StructField("service_account", StringType(), True),
            StructField("cluster_id", StringType(), True),
            StructField("cluster_name", StringType(), True),
            StructField("spark_version", StringType(), True),
            StructField("dbr_version", StringType(), True),
            StructField("rows_inserted", LongType(), True),
            StructField("rows_updated", LongType(), True),
            StructField("rows_deleted", LongType(), True),
            StructField("rows_before", LongType(), True),
            StructField("error_code", StringType(), True),  # Belle status code (e.g., BELLE0=success, BELLE101=table_not_found)
            StructField("retry_count", IntegerType(), True),
            StructField("parent_run_id", StringType(), True),
            StructField("notebook_path", StringType(), True),
            StructField("execution_context", StringType(), True),
            StructField("tag", StringType(), True),
            StructField("tags_json", StringType(), True),
            StructField("subpipeline", StringType(), True),
            StructField("load_mode", StringType(), True),
        ])

        if BellerophonConfig.LOG_STRIP_EMOJI and error_message:
            error_message = _strip_emoji(error_message)
        
        if success and not error_message:
            error_message = None
        
        logging_data = [(
            run_id,
            log_id,
            full_table_name,
            delta_location if delta_location else "UC_MANAGED",
            csv_full_path,
            execution_start_time,
            execution_end_time,
            execution_duration_seconds if isinstance(execution_duration_seconds, float) else 0.0,
            interactive_mode,
            success,
            error_message,
            row_count,
            file_size_bytes,
            json.dumps(parameters_dict),
            schema_json,
            str(monitored_id_max_value),
            str(monitored_date_max_value),
            dag_stage,
            is_unity_catalog,
            use_managed_table,
            user_name,
            service_account,
            cluster_id,
            cluster_name,
            spark_version,
            dbr_version,
            rows_inserted,
            rows_updated,
            rows_deleted,
            rows_before,
            error_code,
            retry_count,
            parent_run_id,
            notebook_path,
            execution_context_json,
            tag_flat,
            tags_json,
            subpipeline,
            load_mode,
        )]

        logging_df = spark.createDataFrame(logging_data, schema=logging_schema)
        
        try:
            BellerophonLogger.write_log(
                logging_df=logging_df,
                target_database=target_database,
                logging_schema=logging_schema,
                run_id=run_id,
                interactive_mode=interactive_mode
            )
        except Exception as log_error:
            if print_status:
                print(f"   ⚠️ Could not write to Belle log table: {log_error}")

    # Return tuple: (table_name, logging_dataframe, sample_data)
    return return_value, logging_df, sample_data

# COMMAND ----------

# DBTITLE 1,OOM Retry Wrapper
# ── OOM Retry Wrapper ────────────────────────────────────────────────────────

def resilient_materialise_table(
    materialise_func,
    input_df,
    conf,
    run_id,
    interactive_mode,
    sample_rows,
    dag_stage,
    custom_csv_removals,
    max_workers
):
    """OOM retry wrapper: catches OutOfMemoryError and retries with max_workers=1."""
    try:
        return materialise_func(
            input_df, conf, run_id, interactive_mode,
            sample_rows, dag_stage, custom_csv_removals
        )
    except Exception as e:
        if 'OutOfMemoryError' in str(e):
            belle_print(
                "  [OOM Recovery] Retrying with reduced parallelism...",
                level=1)
            return materialise_func(
                input_df, conf, run_id, interactive_mode,
                sample_rows, dag_stage, custom_csv_removals, 1
            )
        raise

# COMMAND ----------

# DBTITLE 1,Fast Mode — Bulk Write & Weight-Sorted Orchestration
# ============================================================================
# FAST MODE — Lean materialise path for bulk full-mode writes.
# Skips per-table overhead (schema drift, existence checks, post-write stats).
# Same encryption, Delta write, and logging as standard path.
# ============================================================================


def bellerophon_materialise_dataframe_fast(
    input_df,
    conf: Dict[str, Any],
    run_id: str,
    dag_stage: int = None,
    interactive_mode: bool = None,
) -> Tuple[Any, Any, None]:
    """Fast-path materialise: encrypt → write → minimal log. No per-table overhead."""
    import datetime as _dt
    from pyspark.sql import functions as _F
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        LongType, DoubleType, BooleanType, TimestampType,
    )

    execution_start = _dt.datetime.now(_dt.timezone.utc)
    log_id = str(uuid.uuid4())

    # ── Config extraction ────────────────────────────────────────────────────
    target_database = conf['target_database']
    result_table_name = conf['result_table_name']
    output_table_name = BellerophonUtils.apply_test_suffix(result_table_name)
    load_mode = conf.get('load_mode', 'full')
    partition_cols = conf.get('partition_by')
    export_csv = conf.get('export_csv', False)
    use_managed_table = conf.get('use_managed_table', False)
    subpipeline = conf.get('subpipeline')

    # ── Namespace resolution ─────────────────────────────────────────────────
    ns_parts = target_database.split('.')
    is_unity_catalog = len(ns_parts) >= 2
    if is_unity_catalog and len(ns_parts) == 2:
        full_table_name = f"{ns_parts[0]}.{ns_parts[1]}.{output_table_name}"
    else:
        full_table_name = f"{target_database}.{output_table_name}"

    # ── Storage location (external tables only) ──────────────────────────────
    if use_managed_table:
        delta_location = None
    else:
        blob_dir = BellerophonUtils.build_blob_target_dir(
            target_database, subpipeline)
        delta_location = (
            f"{blob_dir.rstrip('/')}/{full_table_name.replace('.', '/')}/"
            .replace('//', '/')
        )

    if interactive_mode is None:
        interactive_mode = BellerophonUtils.is_interactive_notebook()

    # ── Execution ────────────────────────────────────────────────────────────
    success = False
    error_message = ""
    error_code = BellerophonErrorCode.SUCCESS
    return_value = None
    row_count = None

    try:
        # ── Inline encryption (v1.2.15 — per-column or blob) ────────────────
        if conf.get('encrypt') and BellerophonConfig.FEATURE_ENCRYPTION:
            _enc_key = conf.get('encrypt_key')
            _enc_exclude = conf.get('encrypt_exclude', [])
            if not _enc_key:
                raise ValueError(
                    f"encrypt=True but no encrypt_key for '{result_table_name}'")
            _payload_cols = [
                c for c in input_df.columns if c not in _enc_exclude]
            if BellerophonConfig.ENCRYPTION_STRATEGY == "per_column":
                # Per-column: each column → cast string → AES-GCM → BINARY.
                _enc_exprs = [_F.col(c) for c in _enc_exclude]
                for _c in _payload_cols:
                    _enc_exprs.append(
                        _F.expr(
                            f"aes_encrypt(cast(`{_c}` as string), "
                            f"unbase64('{_enc_key}'), "
                            f"'{BellerophonConfig.ENCRYPTION_MODE}', 'DEFAULT')"
                        ).alias(_c)
                    )
                input_df = input_df.select(*_enc_exprs)
            else:
                # Legacy blob: to_json → single encrypted_payload.
                input_df = (
                    input_df
                    .withColumn("_payload", _F.to_json(_F.struct(*_payload_cols)))
                    .withColumn(
                        "encrypted_payload",
                        _F.expr(
                            f"aes_encrypt(_payload, unbase64('{_enc_key}'), "
                            f"'{BellerophonConfig.ENCRYPTION_MODE}', 'DEFAULT')"
                        ),
                    )
                    .select(
                        *[_F.col(c) for c in _enc_exclude],
                        "encrypted_payload",
                    )
                )

        # ── Write (full mode — fast_mode only supports full) ─────────────────
        if load_mode != 'full':
            # Fallback to standard path for non-full modes
            return bellerophon_materialise_dataframe(
                input_df, target_database, result_table_name, run_id,
                conf=conf, collect_sample=False, print_status=False,
                dag_stage=dag_stage, interactive_mode=interactive_mode,
            )

        # DROP already done by run_fast() pre-drop, but idempotent safety:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

        writer = input_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(partition_cols)

        if use_managed_table:
            writer.option("overwriteSchema", "true").saveAsTable(full_table_name)
        else:
            writer.option("overwriteSchema", "true").save(delta_location)
            spark.sql(
                f"""CREATE TABLE IF NOT EXISTS {full_table_name}
                    USING DELTA LOCATION '{delta_location}'"""
            )

        # ── CSV export (only if enabled — typically only worldwide views) ────
        if export_csv and BellerophonConfig.FEATURE_CSV_EXPORT and not interactive_mode:
            _csv_dir = conf.get('csv_export_dir')
            if _csv_dir:
                _ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                _csv_fn = f"{target_database}.{output_table_name}_{_ts}_0001.csv"
            else:
                blob_dir_csv = BellerophonUtils.build_blob_target_dir(
                    target_database, subpipeline)
                _csv_dir, _csv_fn = BellerophonUtils.get_target_cube_csv_path(
                    blob_dir_csv, target_database, output_table_name)
            try:
                _df_export = spark.table(full_table_name)
                _df_export.coalesce(1).write.mode("overwrite").format("csv").options(
                    encoding=BellerophonConfig.CSV_ENCODING,
                    header="true",
                    sep=BellerophonConfig.CSV_DELIMITER,
                    lineSep=BellerophonConfig.CSV_LINE_SEP,
                ).save(_csv_dir)
                BellerophonUtils.rename_csv_part_file(_csv_dir, _csv_fn)
            except Exception as _csv_err:
                belle_print(
                    f"  ⚠️  CSV export failed for {output_table_name}: {_csv_err}",
                    level=2)

        success = True
        return_value = input_df  # fast mode: skip re-read (avoids metastore lag)


    except Exception as e:
        error_message = str(e)[:500]
        # Classify error
        _etype = type(e).__name__
        _emap = {
            'AnalysisException': BellerophonErrorCode.TABLE_NOT_FOUND,
            'PermissionError': BellerophonErrorCode.PERMISSION_DENIED,
            'MemoryError': BellerophonErrorCode.OOM_ERROR,
        }
        error_code = _emap.get(_etype, BellerophonErrorCode.UNKNOWN_ERROR)
        raise

    finally:
        execution_end = _dt.datetime.now(_dt.timezone.utc)
        duration = (execution_end - execution_start).total_seconds()

        # ── Log row (same schema as standard path) ───────────────────────────
        logging_schema = StructType([
            StructField("run_id", StringType(), True),
            StructField("log_id", StringType(), True),
            StructField("target_table_name", StringType(), True),
            StructField("target_table_blob_dir", StringType(), True),
            StructField("csv_path", StringType(), True),
            StructField("execution_start_time", TimestampType(), True),
            StructField("execution_end_time", TimestampType(), True),
            StructField("execution_duration_seconds", DoubleType(), True),
            StructField("ran_in_interactive_mode", BooleanType(), True),
            StructField("success", BooleanType(), True),
            StructField("error_message", StringType(), True),
            StructField("row_count", LongType(), True),
            StructField("file_size_bytes", LongType(), True),
            StructField("parameters", StringType(), True),
            StructField("schema_json", StringType(), True),
            StructField("monitored_id_max_value", StringType(), True),
            StructField("monitored_date_max_value", StringType(), True),
            StructField("dag_stage", IntegerType(), True),
            StructField("is_unity_catalog", BooleanType(), True),
            StructField("use_managed_table", BooleanType(), True),
            StructField("user_name", StringType(), True),
            StructField("service_account", StringType(), True),
            StructField("cluster_id", StringType(), True),
            StructField("cluster_name", StringType(), True),
            StructField("spark_version", StringType(), True),
            StructField("dbr_version", StringType(), True),
            StructField("rows_inserted", LongType(), True),
            StructField("rows_updated", LongType(), True),
            StructField("rows_deleted", LongType(), True),
            StructField("rows_before", LongType(), True),
            StructField("error_code", StringType(), True),
            StructField("retry_count", IntegerType(), True),
            StructField("parent_run_id", StringType(), True),
            StructField("notebook_path", StringType(), True),
            StructField("execution_context", StringType(), True),
        ])

        # Minimal params dict (skip expensive serialisation)
        _params = json.dumps({
            "load_mode": load_mode,
            "fast_mode": True,
            "encrypt": bool(conf.get('encrypt')),
        })

        logging_data = [(
            run_id, log_id, full_table_name,
            delta_location or "UC_MANAGED",
            None,  # csv_path (not tracked in fast mode)
            execution_start, execution_end, duration,
            interactive_mode, success,
            error_message if error_message else None,
            row_count, None,  # file_size_bytes skipped
            _params, None,  # schema_json skipped
            None, None,  # monitored columns skipped
            dag_stage, is_unity_catalog, use_managed_table,
            None, None,  # user/service_account (batch-level)
            None, None, None, None,  # cluster info (batch-level)
            None, None, None, 0,  # DML metrics
            error_code, 0, run_id, None, None,
        )]

        logging_df = spark.createDataFrame(logging_data, schema=logging_schema)

    return return_value, logging_df, None


# ============================================================================
# ORCHESTRATOR run_fast() — Weight-Sorted Two-Phase Execution
# ============================================================================


def _orchestrator_run_fast(
    self,
    max_workers: int = None,
    heavy_workers: int = None,
    external_run_id: str = None,
    execution_context: dict = None,
) -> List[Dict[str, Any]]:
    """Two-phase fast orchestration: light tables first (full parallelism), then heavy (reduced workers)."""
    import builtins
    import threading

    _heavy_threshold = BellerophonConfig.FAST_MODE_HEAVY_COL_THRESHOLD

    if max_workers is None:
        max_workers = self.get_sensible_max_workers()
    if heavy_workers is None:
        heavy_workers = max(
            1, int(max_workers * BellerophonConfig.FAST_MODE_HEAVY_WORKERS_RATIO))

    run_id = str(uuid.uuid4())
    interactive_mode = self.interactive_mode
    tables_config = self.tables_config

    belle_banner(f"Bellerophon Fast Orchestrator (v{self.VERSION})")
    belle_print(f"run_id: {run_id}")
    belle_print(
        f"Tables: {len(tables_config)} | Workers: {max_workers} "
        f"(heavy: {heavy_workers}) | Mode: FAST (weight-sorted)")

    # ── Build config_by_fullname ──────────────────────────────────────────
    config_by_fullname = {}
    for conf in tables_config.values():
        fn = f"{conf['target_database']}.{conf['result_table_name']}"
        config_by_fullname[fn] = conf
    tables_to_run = list(config_by_fullname.keys())

    # ── Resolve DataFrames from registry ─────────────────────────────────
    local_dataframes = {}
    _full_refs = {}
    _skipped = []
    for base_name in tables_to_run:
        conf = config_by_fullname[base_name]
        key = f"{conf['target_database']}_{conf['result_table_name']}"
        if key in BellerophonOutputRegistry._outputs:
            local_dataframes[base_name] = BellerophonOutputRegistry.get_output(key)
        else:
            _skipped.append(base_name)

    tables_to_run = [t for t in tables_to_run if t in local_dataframes]
    if _skipped:
        belle_print(f"  ⏭️  {len(_skipped)} table(s) skipped (no DataFrame)")
    if not tables_to_run:
        belle_print("No tables to process.")
        return []

    # ── Pre-drop all tables in parallel ──────────────────────────────────
    _drop_start = time.time()
    _drop_stmts = [
        f"DROP TABLE IF EXISTS {config_by_fullname[t]['target_database']}."
        f"{config_by_fullname[t]['result_table_name']}"
        for t in tables_to_run
    ]
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        list(pool.map(spark.sql, _drop_stmts))
    belle_print(
        f"  Pre-dropped {len(_drop_stmts)} tables in "
        f"{time.time() - _drop_start:.1f}s")

    # ── DAG dependency resolution ────────────────────────────────────────
    tables_dependencies = {}
    usage_counts = {}
    for base_name in tables_to_run:
        conf = config_by_fullname[base_name]
        deps = [
            d for d in conf.get('dependencies', [])
            if d in config_by_fullname and d in local_dataframes
        ]
        tables_dependencies[base_name] = deps
        for d in deps:
            usage_counts[d] = usage_counts.get(d, 0) + 1

    persisted = {}
    usage_remaining = usage_counts.copy()

    def _persist_if_needed(name):
        if usage_counts.get(name, 0) == 0 or persisted.get(name):
            return
        df = local_dataframes[name]
        conf = config_by_fullname[name]
        _pc = conf.get('persist_columns')
        if _pc:
            _full_refs[name] = df
            df = df.select(*_pc)
            local_dataframes[name] = df
        df.persist(StorageLevel.MEMORY_AND_DISK)
        persisted[name] = True

    def _unpersist_if_done(name):
        usage_remaining[name] -= 1
        if usage_remaining[name] == 0 and persisted.get(name):
            local_dataframes[name].unpersist(blocking=True)
            persisted[name] = False

    # ── Weight estimation ────────────────────────────────────────────────
    def _col_weight(base_name):
        """Number of columns to encrypt = cost proxy."""
        df = local_dataframes[base_name]
        conf = config_by_fullname[base_name]
        if conf.get('encrypt'):
            _exclude = conf.get('encrypt_exclude', [])
            return len([c for c in df.columns if c not in _exclude])
        return len(df.columns)

    # ── Two-phase stage execution ────────────────────────────────────────
    tables_processed = set()
    tables_failed = set()
    logs_to_display = []
    summary_rows = []
    stage_number = 0
    total_start = time.time()
    _progress_lock = threading.Lock()
    _progress_count = [0]  # mutable for closure access
    _total_tables = len(tables_to_run)

    while set(tables_to_run) - tables_processed - tables_failed:
        stage_number += 1
        stage_ready = [
            t for t in tables_to_run
            if t not in tables_processed
            and t not in tables_failed
            and all(d in tables_processed for d in tables_dependencies[t])
            and not any(d in tables_failed for d in tables_dependencies[t])
        ]
        if not stage_ready:
            remaining = set(tables_to_run) - tables_processed - tables_failed
            if remaining:
                belle_print(
                    f"  ⚠️  Deadlock: {len(remaining)} tables cannot proceed")
            break

        # Sort by weight: lightest first
        stage_ready.sort(key=_col_weight)

        # Split into light and heavy
        _light = [t for t in stage_ready
                  if _col_weight(t) <= _heavy_threshold]
        _heavy = [t for t in stage_ready
                  if _col_weight(t) > _heavy_threshold]

        stage_start = time.time()
        belle_print(
            f"\n  Stage {stage_number}: {len(stage_ready)} tables "
            f"({len(_light)} light + {len(_heavy)} heavy)")

        for t in stage_ready:
            _persist_if_needed(t)

        def _process_one(base_name):
            conf = config_by_fullname[base_name]
            input_df = (
                _full_refs.pop(base_name, None)
                or local_dataframes[base_name])
            t0 = time.time()
            try:
                result_df, log_df, _ = bellerophon_materialise_dataframe_fast(
                    input_df, conf, run_id,
                    dag_stage=stage_number,
                    interactive_mode=interactive_mode,
                )
                dur = time.time() - t0
                with _progress_lock:
                    _tbl = conf['result_table_name']
                    _w = _col_weight(base_name)
                    _progress_count[0] += 1
                    print(
                        f"    ✓ [{_progress_count[0]}/{_total_tables}] {_tbl} ({dur:.1f}s, {_w} cols)",
                        flush=True)
                return {
                    'status': 'success', 'table': base_name,
                    'duration': dur, 'log_df': log_df,
                    'result_df': result_df,
                }
            except Exception as e:
                dur = time.time() - t0
                with _progress_lock:
                    _progress_count[0] += 1
                    print(
                        f"    ✗ [{_progress_count[0]}/{_total_tables}] {conf['result_table_name']} FAILED "
                        f"({dur:.1f}s): {str(e)[:100]}", flush=True)
                return {
                    'status': 'failed', 'table': base_name,
                    'duration': dur, 'log_df': None,
                    'error': str(e)[:200],
                }

        def _run_batch(batch, workers, label):
            """Execute a batch with given parallelism."""
            if not batch:
                return
            belle_print(
                f"    [{label}] {len(batch)} tables, {workers} workers")
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(_process_one, t): t for t in batch}
                for f in as_completed(futures):
                    result = f.result()
                    tbl = result['table']
                    if result['status'] == 'success':
                        tables_processed.add(tbl)
                        if result.get('log_df') is not None:
                            logs_to_display.append(result['log_df'])
                        conf = config_by_fullname[tbl]
                        key = (
                            f"{conf['target_database']}_"
                            f"{conf['result_table_name']}")
                        BellerophonOutputRegistry.set_output(
                            key, result['result_df'])
                        local_dataframes[tbl] = result['result_df']
                        for dep in tables_dependencies[tbl]:
                            if dep in usage_remaining:
                                _unpersist_if_done(dep)
                    else:
                        tables_failed.add(tbl)
                    summary_rows.append(result)

        # Phase A: Light tables — full parallelism
        _run_batch(_light, max_workers, "LIGHT")
        # Phase B: Heavy tables — reduced parallelism
        _run_batch(_heavy, heavy_workers, "HEAVY")

        belle_print(
            f"  Stage {stage_number} done in "
            f"{time.time() - stage_start:.1f}s")

    # ── Consolidated log write ──────────────────────────────────────────
    if logs_to_display:
        try:
            all_logs_df = reduce(
                lambda a, b: a.unionByName(b, allowMissingColumns=True),
                logs_to_display[1:],
                logs_to_display[0],
            )
            BellerophonLogger.write_log(
                all_logs_df,
                next(iter(tables_config.values()))['target_database'],
                all_logs_df.schema,
                run_id,
                interactive_mode,
            )
        except Exception as _log_err:
            belle_print(f"  ⚠️  Log write failed: {_log_err}")

    # ── Summary ────────────────────────────────────────────────────────
    total_dur = time.time() - total_start
    belle_banner("Fast Orchestrator Summary")
    belle_print(
        f"  ✅ {len(tables_processed)}/{len(tables_to_run)} tables written "
        f"in {stage_number} stage(s) | {total_dur:.1f}s total")
    if tables_failed:
        belle_print(f"  ❌ {len(tables_failed)} failure(s):")
        for r in summary_rows:
            if r['status'] == 'failed':
                belle_print(f"     {r['table']}: {r.get('error', '?')}")
        raise RuntimeError(
            f"Fast orchestrator: {len(tables_failed)} table(s) failed")

    return summary_rows


# Attach to Orchestrator class
BellerophonOrchestrator.run_fast = _orchestrator_run_fast

# COMMAND ----------

# DBTITLE 1,Partition Materialisation
# ============================================================================
# PARTITION MATERIALISATION — High-frequency partition writes.
# replaceWhere for existing tables, full write for new. Batched logging.
# ============================================================================
#

# ── Partition Log Buffer ─────────────────────────────────────────────────────

class _PartitionLogBuffer:
    """Accumulates partition write logs for batched persistence."""
    _buffer: List = []
    _flush_threshold: int = 200

    @classmethod
    def append(cls, row: tuple):
        cls._buffer.append(row)
        if len(cls._buffer) >= cls._flush_threshold:
            cls.flush()

    @classmethod
    def flush(cls, target_database: str = None, schema=None):
        """Write all buffered log rows to the Bellerophon log table."""
        if not cls._buffer:
            return 0
        _count = len(cls._buffer)
        try:
            if target_database and schema:
                _log_df = spark.createDataFrame(cls._buffer, schema=schema)
                BellerophonLogger.write_log(
                    logging_df=_log_df,
                    target_database=target_database,
                    logging_schema=schema,
                )
        except Exception:
            pass  # Non-fatal: logging failure never breaks pipeline
        cls._buffer.clear()
        return _count

    @classmethod
    def count(cls) -> int:
        return len(cls._buffer)

    @classmethod
    def clear(cls):
        cls._buffer.clear()


def bellerophon_materialise_partition(
    input_df,
    conf: Dict[str, Any],
    run_id: str,
    interactive_mode: bool = None,
) -> int:
    """Partition-level materialise: filter → encrypt → replaceWhere → batched log."""
    import time as _t
    from pyspark.sql import functions as _F

    _write_start = _t.time()

    # ── Config extraction ────────────────────────────────────────────────────
    target_database = conf['target_database']
    result_table_name = conf['result_table_name']
    output_table_name = BellerophonUtils.apply_test_suffix(
        result_table_name)
    partition_cols = conf.get('partition_by', ['_data_year', '_data_month'])
    partition_filter = conf.get('partition_filter', {})
    use_managed_table = conf.get('use_managed_table', True)
    subpipeline = conf.get('subpipeline')

    if interactive_mode is None:
        interactive_mode = BellerophonUtils.is_interactive_notebook()

    # ── Namespace resolution ─────────────────────────────────────────────────
    ns_parts = target_database.split('.')
    if len(ns_parts) >= 2:
        full_table_name = f"{ns_parts[0]}.{ns_parts[1]}.{output_table_name}"
    else:
        full_table_name = f"{target_database}.{output_table_name}"

    # ── Storage location (external tables only) ──────────────────────────────
    if use_managed_table:
        delta_location = None
    else:
        blob_dir = BellerophonUtils.build_blob_target_dir(
            target_database, subpipeline)
        delta_location = (
            f"{blob_dir.rstrip('/')}/{full_table_name.replace('.', '/')}/"
            .replace('//', '/')
        )

    # ── Filter to partition ──────────────────────────────────────────────────
    _part_df = input_df
    for _col_name, _val in partition_filter.items():
        _part_df = _part_df.filter(_F.col(_col_name) == _val)

    # Ensure partition columns exist
    for _pc in partition_cols:
        if _pc not in _part_df.columns:
            _pval = partition_filter.get(_pc)
            if _pval is not None:
                _part_df = _part_df.withColumn(_pc, _F.lit(_pval))

    _count = _part_df.count()
    if _count == 0:
        return 0

    # ── Inline encryption ────────────────────────────────────────────────────
    if conf.get('encrypt') and BellerophonConfig.FEATURE_ENCRYPTION:
        _enc_key = conf.get('encrypt_key')
        _enc_exclude = conf.get('encrypt_exclude', [])
        if _enc_key:
            if BellerophonConfig.ENCRYPTION_STRATEGY == "per_column":
                _enc_exprs = [_F.col(c) for c in _enc_exclude
                              if c in _part_df.columns]
                _payload_cols = [
                    c for c in _part_df.columns
                    if c not in _enc_exclude
                ]
                for _c in _payload_cols:
                    _enc_exprs.append(
                        _F.expr(
                            f"aes_encrypt(cast(`{_c}` as string), "
                            f"unbase64('{_enc_key}'), "
                            f"'{BellerophonConfig.ENCRYPTION_MODE}',"
                            f" 'DEFAULT')"
                        ).alias(_c)
                    )
                _part_df = _part_df.select(*_enc_exprs)
            else:
                # Legacy blob encryption
                _payload_cols = [
                    c for c in _part_df.columns
                    if c not in _enc_exclude
                ]
                _part_df = (
                    _part_df
                    .withColumn("_payload",
                        _F.to_json(_F.struct(*_payload_cols)))
                    .withColumn("encrypted_payload",
                        _F.expr(
                            f"aes_encrypt(_payload, "
                            f"unbase64('{_enc_key}'), "
                            f"'{BellerophonConfig.ENCRYPTION_MODE}',"
                            f" 'DEFAULT')"
                        ))
                    .select(
                        *[_F.col(c) for c in _enc_exclude
                          if c in _part_df.columns],
                        "encrypted_payload")
                )

    # ── Write ────────────────────────────────────────────────────────────────
    _tbl_exists = spark.catalog.tableExists(full_table_name)

    if not _tbl_exists:
        # First write: create table with partition scheme
        writer = (
            _part_df.write.format("delta").mode("overwrite")
            .partitionBy(*partition_cols)
            .option("overwriteSchema", "true")
        )
        if use_managed_table:
            writer.saveAsTable(full_table_name)
        else:
            writer.save(delta_location)
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {full_table_name} "
                f"USING DELTA LOCATION '{delta_location}'")
    else:
        # Subsequent writes: replaceWhere
        _where_parts = [
            f"{_c} = {_v}" if isinstance(_v, int)
            else f"{_c} = '{_v}'"
            for _c, _v in partition_filter.items()
        ]
        _where = " AND ".join(_where_parts)

        writer = (
            _part_df.write.format("delta").mode("overwrite")
            .option("replaceWhere", _where)
            .option("overwriteSchema", "true")
        )
        if use_managed_table:
            writer.saveAsTable(full_table_name)
        else:
            writer.save(delta_location)

    # ── Batched logging ──────────────────────────────────────────────────────
    _write_dur = _t.time() - _write_start
    _now = datetime.datetime.now(datetime.timezone.utc)
    _start_ts = datetime.datetime.fromtimestamp(
        _now.timestamp() - _write_dur, tz=datetime.timezone.utc)

    _params = ",".join(
        f"{k}={v}" for k, v in partition_filter.items())

    _log_row = (
        run_id,                               # run_id
        str(uuid.uuid4())[:8],                # log_id
        full_table_name,                      # target_table_name
        delta_location or "UC_MANAGED",       # target_table_blob_dir
        None,                                 # csv_path
        _start_ts,                            # execution_start_time
        _now,                                 # execution_end_time
        _write_dur,                           # execution_duration_seconds
        interactive_mode,                     # ran_in_interactive_mode
        True,                                 # success
        None,                                 # error_message
        int(_count),                          # row_count
        None,                                 # file_size_bytes
        _params,                              # parameters
        None,                                 # schema_json
        None,                                 # monitored_id_max_value
        None,                                 # monitored_date_max_value
        None,                                 # dag_stage
        False,                                # is_unity_catalog
        use_managed_table,                    # use_managed_table
        None, None, None, None,               # user/service/cluster/notebook
        None, None,                           # spark/dbr version
        int(_count),                          # rows_inserted
        None, None, None,                     # rows_updated/deleted/before
        BellerophonErrorCode.SUCCESS,         # error_code
        0,                                    # retry_count
        None,                                 # parent_run_id
        None,                                 # notebook_path
        "TRUST_PIPELINE",                     # execution_context
    )
    _PartitionLogBuffer.append(_log_row)

    return _count


def bellerophon_flush_partition_logs(
    target_database: str,
    log_schema=None,
) -> int:
    """Flush buffered partition logs to the log table. Call at end of country/run."""
    return _PartitionLogBuffer.flush(target_database, log_schema)



# ── Bulk Table Materialise (bootstrap optimisation) ────────────────

def bellerophon_materialise_bulk(
    input_df,
    conf: Dict[str, Any],
    run_id: str,
    interactive_mode: bool = None,
) -> Dict[Tuple[int, int], int]:
    """Single-write bulk materialise for bootstrap/rebuild. Returns {(year, month): row_count}."""
    import time as _t
    from pyspark.sql import functions as _F

    _write_start = _t.time()

    # ── Config extraction ────────────────────────────────────────────────────
    target_database = conf['target_database']
    result_table_name = conf['result_table_name']
    output_table_name = BellerophonUtils.apply_test_suffix(
        result_table_name)
    partition_cols = conf.get('partition_by', ['_data_year', '_data_month'])
    use_managed_table = conf.get('use_managed_table', True)
    subpipeline = conf.get('subpipeline')
    append_mode = conf.get('append', False)

    if interactive_mode is None:
        interactive_mode = BellerophonUtils.is_interactive_notebook()

    # ── Namespace resolution ─────────────────────────────────────────────────
    ns_parts = target_database.split('.')
    if len(ns_parts) >= 2:
        full_table_name = f"{ns_parts[0]}.{ns_parts[1]}.{output_table_name}"
    else:
        full_table_name = f"{target_database}.{output_table_name}"

    # ── Storage location (external tables only) ──────────────────────────────
    if use_managed_table:
        delta_location = None
    else:
        blob_dir = BellerophonUtils.build_blob_target_dir(
            target_database, subpipeline)
        delta_location = (
            f"{blob_dir.rstrip('/')}/{full_table_name.replace('.', '/')}/"
            .replace('//', '/')
        )

    # ── Verify partition columns exist ───────────────────────────────────────
    for _pc in partition_cols:
        if _pc not in input_df.columns:
            raise ValueError(
                f"bellerophon_materialise_bulk: partition column "
                f"'{_pc}' not found in DataFrame. "
                f"Columns: {input_df.columns[:20]}")

    # ── Step 1: Per-partition row counts (single Spark action) ────────────────
    _counts_rows = (
        input_df.groupBy(*partition_cols)
        .count()
        .collect()
    )
    _stats = {}
    for _r in _counts_rows:
        _key = tuple(int(_r[c]) for c in partition_cols)
        _stats[_key] = int(_r["count"])
    _total = sum(_stats.values())

    if _total == 0:
        return {}

    # ── Step 2: Inline encryption (lazy — no action) ─────────────────────────
    if conf.get('encrypt') and BellerophonConfig.FEATURE_ENCRYPTION:
        _enc_key = conf.get('encrypt_key')
        _enc_exclude = conf.get('encrypt_exclude', [])
        if _enc_key:
            if BellerophonConfig.ENCRYPTION_STRATEGY == "per_column":
                _enc_exprs = [_F.col(c) for c in _enc_exclude
                              if c in input_df.columns]
                _payload_cols = [
                    c for c in input_df.columns
                    if c not in _enc_exclude
                ]
                for _c in _payload_cols:
                    _enc_exprs.append(
                        _F.expr(
                            f"aes_encrypt(cast(`{_c}` as string), "
                            f"unbase64('{_enc_key}'), "
                            f"'{BellerophonConfig.ENCRYPTION_MODE}',"
                            f" 'DEFAULT')"
                        ).alias(_c)
                    )
                input_df = input_df.select(*_enc_exprs)
            else:
                _payload_cols = [
                    c for c in input_df.columns
                    if c not in _enc_exclude
                ]
                input_df = (
                    input_df
                    .withColumn("_payload",
                        _F.to_json(_F.struct(*_payload_cols)))
                    .withColumn("encrypted_payload",
                        _F.expr(
                            f"aes_encrypt(_payload, "
                            f"unbase64('{_enc_key}'), "
                            f"'{BellerophonConfig.ENCRYPTION_MODE}',"
                            f" 'DEFAULT')"))
                    .select(
                        *[_F.col(c) for c in _enc_exclude
                          if c in input_df.columns],
                        "encrypted_payload")
                )

    # ── Step 3: Single write (one Spark job, all partitions) ─────────────────
    _tbl_exists = spark.catalog.tableExists(full_table_name)

    if _tbl_exists and append_mode:
        # Multi-batch: append to existing table
        writer = (
            input_df.write.format("delta").mode("append")
        )
        if use_managed_table:
            writer.saveAsTable(full_table_name)
        else:
            writer.save(delta_location)
    elif not _tbl_exists or not append_mode:
        # First write or overwrite: create/replace table
        writer = (
            input_df.write.format("delta").mode("overwrite")
            .partitionBy(*partition_cols)
            .option("overwriteSchema", "true")
        )
        if use_managed_table:
            writer.saveAsTable(full_table_name)
        else:
            writer.save(delta_location)
            if not _tbl_exists:
                spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {full_table_name} "
                    f"USING DELTA LOCATION '{delta_location}'")

    # ── Step 4: Batched logging (one row per partition) ───────────────────────
    _write_dur = _t.time() - _write_start
    _now = datetime.datetime.now(datetime.timezone.utc)
    _start_ts = datetime.datetime.fromtimestamp(
        _now.timestamp() - _write_dur, tz=datetime.timezone.utc)

    for _key, _cnt in sorted(_stats.items()):
        _params = ",".join(
            f"{c}={v}" for c, v in zip(partition_cols, _key))
        _log_row = (
            run_id,                               # run_id
            str(uuid.uuid4())[:8],                # log_id
            full_table_name,                      # target_table_name
            delta_location or "UC_MANAGED",       # target_table_blob_dir
            None,                                 # csv_path
            _start_ts,                            # execution_start_time
            _now,                                 # execution_end_time
            _write_dur / len(_stats),             # execution_duration_seconds
            interactive_mode,                     # ran_in_interactive_mode
            True,                                 # success
            None,                                 # error_message
            int(_cnt),                            # row_count
            None,                                 # file_size_bytes
            _params,                              # parameters
            None,                                 # schema_json
            None,                                 # monitored_id_max_value
            None,                                 # monitored_date_max_value
            None,                                 # dag_stage
            False,                                # is_unity_catalog
            use_managed_table,                    # use_managed_table
            None, None, None, None,               # user/service/cluster/notebook
            None, None,                           # spark/dbr version
            int(_cnt),                            # rows_inserted
            None, None, None,                     # rows_updated/deleted/before
            BellerophonErrorCode.SUCCESS,         # error_code
            0,                                    # retry_count
            None,                                 # parent_run_id
            None,                                 # notebook_path
            "BULK_MATERIALISE",                   # execution_context
        )
        _PartitionLogBuffer.append(_log_row)

    _n_parts = len(_stats)
    belle_print(
        f"  bulk_materialise: {full_table_name} "
        f"({_total:,} rows, {_n_parts} partitions, {_write_dur:.1f}s)",
        level=2)

    return _stats


belle_print(f"  partition_mode=available | bulk_mode=available", level=3)


# COMMAND ----------

# DBTITLE 1,BelleValidator — Static Pipeline Analysis
# ============================================================================
# BELLE VALIDATOR — Static Pipeline Analysis
# Callable as: belle.Validator.validate("/path/to/notebook")
# ============================================================================

class BelleValidator:
    """Static validator for Belle pipeline notebooks. Checks structure, registry,
    config, load modes, anti-patterns, and lifecycle without executing the pipeline."""

    # Framework notebooks to skip during %run parsing
    _FRAMEWORK_NOTEBOOKS = {'bellerophon_core', 'belle_core', 'bellerophon'}
    _MAX_RUN_DEPTH = 3

    class Finding:
        """A single validation finding."""
        def __init__(self, severity, category, message, cell_index=None, line_number=None):
            self.severity = severity
            self.category = category
            self.message = message
            self.cell_index = cell_index
            self.line_number = line_number

        def __repr__(self):
            loc = f"[Cell {self.cell_index}" if self.cell_index else ""
            if loc and self.line_number:
                loc += f":L{self.line_number}"
            if loc:
                loc += "] "
            return f"{self.severity} | {self.category} | {loc}{self.message}"

    class _ParsedCell:
        def __init__(self, index, language, source):
            self.index = index
            self.language = language
            self.source = source
            self.lines = source.split("\n")
            self.is_code = language == "python"

    # ── Scope detection utilities ────────────────────────────────────────────

    @staticmethod
    def _is_inside_def(source, target_line_num):
        """Return True if line is inside a def/method block."""
        lines = source.split("\n")
        if target_line_num < 1 or target_line_num > len(lines):
            return False
        target_idx = target_line_num - 1
        target_line = lines[target_idx]
        if not target_line.strip():
            return False
        target_indent = len(target_line) - len(target_line.lstrip())
        for i in range(target_idx - 1, -1, -1):
            line = lines[i]
            stripped = line.lstrip()
            if not stripped or stripped.startswith("#"):
                continue
            line_indent = len(line) - len(stripped)
            if line_indent < target_indent:
                if re.match(r'def\s+\w+\s*\(', stripped):
                    return True
                target_indent = line_indent
        return False

    @staticmethod
    def _get_enclosing_def_params(source, target_line_num):
        """Return parameter names of enclosing def for a given line."""
        lines = source.split("\n")
        if target_line_num < 1 or target_line_num > len(lines):
            return set()
        target_idx = target_line_num - 1
        target_indent = len(lines[target_idx]) - len(lines[target_idx].lstrip())
        for i in range(target_idx - 1, -1, -1):
            line = lines[i]
            stripped = line.lstrip()
            if not stripped or stripped.startswith("#"):
                continue
            line_indent = len(line) - len(stripped)
            if line_indent < target_indent:
                if re.match(r'def\s+\w+\s*\(', stripped):
                    sig_lines = [lines[i]]
                    j = i + 1
                    while j < len(lines) and ')' not in ''.join(sig_lines):
                        sig_lines.append(lines[j])
                        j += 1
                    sig = ''.join(sig_lines)
                    paren_match = re.search(r'\((.*)\)', sig, re.DOTALL)
                    if paren_match:
                        params = set()
                        for p in paren_match.group(1).split(','):
                            p = p.strip()
                            if not p or p == 'self':
                                continue
                            name = re.match(r'\*{0,2}(\w+)', p)
                            if name:
                                params.add(name.group(1))
                        return params
                target_indent = line_indent
        return set()

    # ── Notebook parsing ─────────────────────────────────────────────────────

    @classmethod
    def _parse_notebook(cls, notebook_path, verbose=False):
        """Export and parse a notebook + its %run sub-notebooks."""
        import requests, base64
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        token = ctx.apiToken().get()
        host = ctx.apiUrl().get()

        def _export(path):
            resp = requests.get(
                f"{host}/api/2.0/workspace/export",
                headers={"Authorization": f"Bearer {token}"},
                params={"path": path, "format": "JUPYTER"},
                timeout=30,
            )
            if resp.status_code != 200:
                return None
            return json.loads(base64.b64decode(resp.json()["content"]))

        def _resolve_run_path(run_line, parent_path):
            run_target = run_line.strip().replace("%run ", "").strip()
            if not run_target:
                return None
            parent_dir = "/".join(parent_path.rstrip("/").split("/")[:-1])
            if run_target.startswith("../"):
                while run_target.startswith("../"):
                    parent_dir = "/".join(parent_dir.split("/")[:-1])
                    run_target = run_target[3:]
                return f"{parent_dir}/{run_target}"
            elif run_target.startswith("./"):
                return f"{parent_dir}/{run_target[2:]}"
            elif not run_target.startswith("/"):
                return f"{parent_dir}/{run_target}"
            return run_target

        def _parse_cells(content, start_index=1):
            cells = []
            if "cells" not in content:
                return cells
            for i, cell in enumerate(content["cells"], start_index):
                src_lines = cell.get("source", [])
                source = "".join(src_lines) if isinstance(src_lines, list) else str(src_lines)
                lang = "python"
                if source.strip().startswith("%sql"): lang = "sql"
                elif source.strip().startswith("%md"): lang = "markdown"
                elif source.strip().startswith("%run"): lang = "run"
                cells.append(cls._ParsedCell(i, lang, source))
            return cells

        # Parse main notebook
        nb_content = _export(notebook_path)
        if not nb_content:
            raise FileNotFoundError(f"Notebook not found: {notebook_path}")
        cells = _parse_cells(nb_content)
        main_count = len(cells)

        # Recursively follow %run sub-notebooks
        parsed_paths = {notebook_path}
        sub_count = 0

        def _follow_runs(parent_cells, parent_path, depth=0):
            nonlocal sub_count
            if depth > cls._MAX_RUN_DEPTH:
                return []
            extra = []
            for c in parent_cells:
                if c.language != "run":
                    continue
                resolved = _resolve_run_path(c.source, parent_path)
                if not resolved or resolved in parsed_paths:
                    continue
                nb_name = resolved.rstrip('/').split('/')[-1]
                if nb_name in cls._FRAMEWORK_NOTEBOOKS:
                    parsed_paths.add(resolved)
                    continue
                parsed_paths.add(resolved)
                sub_content = _export(resolved)
                if not sub_content:
                    continue
                start_idx = main_count + len(extra) + 1
                sub_cells = _parse_cells(sub_content, start_idx)
                extra.extend(sub_cells)
                sub_count += 1
                # Recurse
                nested = _follow_runs(sub_cells, resolved, depth + 1)
                extra.extend(nested)
            return extra

        sub_cells = _follow_runs(cells, notebook_path)
        cells.extend(sub_cells)

        if verbose:
            belle_print(f"Parsed {len(cells)} cells "
                        f"(main: {main_count}, from {sub_count} sub-notebooks)", level=2)

        return cells

    # ── Function-call key resolution ─────────────────────────────────────────

    @classmethod
    def _resolve_func_call_key(cls, func_name, func_args, cell_idx, cells):
        """Resolve function-call registry key like _sk(table_name)."""
        func_return_pattern = None
        for c in cells:
            if c.language != "python":
                continue
            def_match = re.search(
                rf'def\s+{re.escape(func_name)}\s*\(([^)]+)\)', c.source)
            if def_match:
                param_names = [
                    p.strip().split(':')[0].split('=')[0].strip()
                    for p in def_match.group(1).split(',')
                ]
                after_def = c.source[def_match.end():]
                next_def = re.search(r'\ndef\s+\w|\nclass\s+\w', after_def)
                func_body = after_def[:next_def.start()] if next_def else after_def
                ret_match = re.search(r'return\s+f(["\'])(.+?)\1', func_body)
                if ret_match:
                    func_return_pattern = ret_match.group(2)
                    for i, pname in enumerate(param_names):
                        if i < len(func_args):
                            func_return_pattern = func_return_pattern.replace(
                                '{' + pname + '}', '{' + func_args[i] + '}')
                    break
                else:
                    ret_match = re.search(r'return\s+(["\'])(.+?)\1', func_body)
                    if ret_match:
                        func_return_pattern = ret_match.group(2)
                        break
        if not func_return_pattern:
            return None
        # Try to enumerate loop variable values
        if len(func_args) == 1 and re.match(r'^\w+$', func_args[0]):
            arg_var = func_args[0]
            src_cell = next((c for c in cells if c.index == cell_idx), None)
            if src_cell:
                for_match = re.search(
                    rf'for\s+{re.escape(arg_var)}(?:\s*,\s*\w+)*\s+in\s+(\w+)',
                    src_cell.source)
                if for_match:
                    list_var = for_match.group(1)
                    for lc in cells:
                        if lc.language != "python":
                            continue
                        list_match = re.search(
                            rf'{re.escape(list_var)}\s*=\s*\[([^\]]+)\]', lc.source)
                        if list_match:
                            vals = re.findall(r'["\']([^"\']+)["\']', list_match.group(1))
                            if vals:
                                return [
                                    func_return_pattern.replace(
                                        '{' + func_args[0] + '}', v) for v in vals
                                ]
                        break
        return [func_return_pattern]

    # ── Main validation entry point ──────────────────────────────────────────

    # Class-level state (populated after validate/validate_config calls)
    findings = []
    tables = {}

    @classmethod
    def validate(cls, notebook_path, verbose=True):
        """Validate a Belle pipeline notebook.

        Usage:
            belle.Validator.validate("/path/to/pipeline")

        Returns True if no errors. Findings stored in belle.Validator.findings.
        """
        if not notebook_path.startswith("/"):
            notebook_path = "/" + notebook_path

        findings = []
        def _add(sev, cat, msg, cell_index=None, line_number=None):
            findings.append(cls.Finding(sev, cat, msg, cell_index, line_number))

        # ── Parse ────────────────────────────────────────────────────────────
        cells = cls._parse_notebook(notebook_path, verbose=verbose)
        all_python = "\n".join(c.source for c in cells if c.language == "python")

        # ── Check 1: Structure ───────────────────────────────────────────────
        core_run_cell = None
        for c in cells:
            if c.language == "run" and "bellerophon_core" in c.source:
                core_run_cell = c.index
                break
            if c.language == "python" and "%run" in c.source and "bellerophon_core" in c.source:
                core_run_cell = c.index
                break

        if not core_run_cell:
            _add("ERROR", "STRUCTURE", "No '%run bellerophon_core' found.")
        else:
            # Only flag if Belle code appears BEFORE the %run
            _BELLE_MARKERS = ['set_output', 'OutputRegistry', 'Orchestrator(', 'belle.Config']
            for c in cells:
                if c.index >= core_run_cell:
                    break
                if c.language == "python" and any(m in c.source for m in _BELLE_MARKERS):
                    _add("ERROR", "STRUCTURE",
                         f"Belle code found BEFORE %run bellerophon_core at cell {core_run_cell}.",
                         cell_index=core_run_cell)
                    break

        # ── Check 2: Registry keys ───────────────────────────────────────────
        registry_calls = []
        set_output_pats = [
            r'(?:belle\.)?OutputRegistry\.set_output\s*\(\s*([^,]+)\s*,',
        ]
        for c in cells:
            if c.language != "python":
                continue
            for ln, line in enumerate(c.lines, 1):
                if line.lstrip().startswith('#'):
                    continue
                for pat in set_output_pats:
                    m = re.search(pat, line)
                    if m:
                        registry_calls.append((c.index, ln, m.group(1).strip()))

        resolved_keys = []
        for cell_idx, line_num, key_expr in registry_calls:
            # String literal
            sm = re.match(r'^["\']([^"\']+)["\']$', key_expr)
            if sm:
                resolved_keys.append(sm.group(1))
                continue
            # f-string
            fm = re.match(r'^f["\'](.+)["\']$', key_expr)
            if fm:
                resolved_keys.append(fm.group(1))
                continue
            # Function call
            fc = re.match(r'^(\w+)\(([^)]+)\)$', key_expr)
            if fc:
                resolved = cls._resolve_func_call_key(
                    fc.group(1), [a.strip() for a in fc.group(2).split(',')],
                    cell_idx, cells)
                if resolved:
                    resolved_keys.extend(resolved)
                    continue
            # Variable — if inside def and is a param, skip
            if re.match(r'^\w+$', key_expr):
                src = next((c for c in cells if c.index == cell_idx), None)
                if src and cls._is_inside_def(src.source, line_num):
                    params = cls._get_enclosing_def_params(src.source, line_num)
                    if key_expr in params:
                        continue  # Unresolvable function param — OK
            _add("INFO", "REGISTRY",
                 f"Dynamic registry key cannot be validated statically: {key_expr}",
                 cell_index=cell_idx, line_number=line_num)

        # ── Check 3: Config extraction ───────────────────────────────────────
        _var_assignments = {}
        for c in cells:
            if c.language != "python":
                continue
            for m in re.finditer(
                    r'^(\w+)\s*=\s*["\']([^"\']+)["\']\s*(?:#.*)?$',
                    c.source, re.MULTILINE):
                _var_assignments[m.group(1)] = m.group(2)

        table_configs = {}
        key_pat = re.compile(
            r'(?:["\']([\w]+\.[\w]+)["\']|f["\']([^"\']+)["\'])\s*:\s*\{')
        for c in cells:
            if c.language != "python":
                continue
            for match in key_pat.finditer(c.source):
                lit_key = match.group(1)
                fstr_key = match.group(2)
                # Brace-balance to extract block
                brace_pos = match.end() - 1
                depth, i = 0, brace_pos
                while i < len(c.source):
                    ch = c.source[i]
                    if ch == '{': depth += 1
                    elif ch == '}':
                        depth -= 1
                        if depth == 0: break
                    i += 1
                block = c.source[brace_pos + 1:i]
                if 'target_database' not in block:
                    continue
                if fstr_key:
                    table_key = re.sub(
                        r'\{(\w+)\}',
                        lambda x: _var_assignments.get(x.group(1), f"__{x.group(1)}__"),
                        fstr_key)
                else:
                    table_key = lit_key
                # Parse load_mode, merge_keys, partition_by, encrypt
                lm = re.search(r'["\']load_mode["\']\s*:\s*["\']([^"\']+)["\']', block)
                mk = re.search(r'["\']merge_keys["\']\s*:\s*\[([^\]]*)\]', block)
                pb = re.search(r'["\']partition_by["\']\s*:\s*\[([^\]]*)\]', block)
                enc = re.search(r'["\']encrypt["\']\s*:\s*(True|False)', block)
                ek = re.search(r'["\']encrypt_key["\']\s*:\s*([^,\n]+)', block)
                table_configs[table_key] = {
                    'load_mode': lm.group(1) if lm else None,
                    'merge_keys': [k.strip().strip("\"'") for k in mk.group(1).split(",") if k.strip()] if mk else None,
                    'partition_by': [p.strip().strip("\"'") for p in pb.group(1).split(",") if p.strip()] if pb else None,
                    'encrypt': (enc.group(1) == 'True') if enc else False,
                    'encrypt_key': ek.group(1).strip() if ek else None,
                    '_cell_index': c.index,
                }

        # ── Check 4: Load-mode mandatory combos ──────────────────────────────
        for tkey, conf in table_configs.items():
            lm = conf.get('load_mode') or ''
            cidx = conf.get('_cell_index')
            # Validate mode value
            lm_base = lm.split('-')[0] if '-' in lm and 'refresh' in lm else lm
            if lm_base and lm_base not in BellerophonConfig.VALID_LOAD_MODES:
                _add("ERROR", "CONFIG",
                     f"Table '{tkey}': invalid load_mode '{lm}'.", cell_index=cidx)
            # merge/update/delete require merge_keys
            if lm_base in ('merge', 'update', 'delete'):
                if not conf.get('merge_keys'):
                    _add("ERROR", "CONFIG",
                         f"Table '{tkey}': load_mode='{lm}' requires 'merge_keys'.",
                         cell_index=cidx)
            # refresh_n_days requires partition_by
            if 'refresh_n_days' in lm:
                if not conf.get('partition_by'):
                    _add("ERROR", "CONFIG",
                         f"Table '{tkey}': load_mode='{lm}' requires 'partition_by'.",
                         cell_index=cidx)
                # Validate the N suffix
                if '-' in lm:
                    try:
                        n_val = int(lm.split('-')[1])
                        if n_val <= 0:
                            raise ValueError()
                    except (ValueError, IndexError):
                        _add("ERROR", "CONFIG",
                             f"Table '{tkey}': invalid day count in '{lm}'.",
                             cell_index=cidx)
            # encrypt requires encrypt_key
            if conf.get('encrypt') and not conf.get('encrypt_key'):
                _add("ERROR", "CONFIG",
                     f"Table '{tkey}': encrypt=True but no 'encrypt_key'.",
                     cell_index=cidx)

        # ── Check 5: Anti-patterns ───────────────────────────────────────────
        anti_patterns = [
            (r'\.saveAsTable\s*\(', "Direct .saveAsTable() — route through Orchestrator."),
            (r"spark\.sql\s*\(.*CREATE\s+TABLE", "spark.sql('CREATE TABLE') bypasses Belle."),
            (r'\.save\s*\(.*format.*delta', "Direct .save(format='delta') — use Orchestrator."),
        ]
        for c in cells:
            if c.language != "python":
                continue
            for ln, line in enumerate(c.lines, 1):
                if line.lstrip().startswith('#') or '# INTENTIONAL' in line:
                    continue
                if cls._is_inside_def(c.source, ln):
                    continue
                for pat, msg in anti_patterns:
                    if re.search(pat, line):
                        _add("WARNING", "ANTI_PATTERN", msg,
                             cell_index=c.index, line_number=ln)

        # ── Check 6: Orchestrator call ───────────────────────────────────────
        orchestrator_calls = []
        for c in cells:
            if c.language != "python":
                continue
            for ln, line in enumerate(c.lines, 1):
                if ".run(" in line and "Orchestrator" in c.source:
                    if not line.lstrip().startswith('#'):
                        if not cls._is_inside_def(c.source, ln):
                            orchestrator_calls.append((c.index, ln))

        # ── Summary & Full Report ─────────────────────────────────────────────
        errors = [f for f in findings if f.severity == "ERROR"]
        warnings = [f for f in findings if f.severity == "WARNING"]
        infos = [f for f in findings if f.severity == "INFO"]
        passed = len(errors) == 0

        if verbose:
            from collections import defaultdict as _dd
            from pyspark.sql import Row as _Row
            from pyspark.sql.functions import when as _when, col as _col
            from datetime import datetime as _dt

            print("\n" + "\u2550" * 100)
            _icon = "\u2705" if passed else "\u274c"
            print(f"  {_icon}  VALIDATION {'PASSED' if passed else 'FAILED'}")
            print("\u2550" * 100)
            print(f"  Notebook : {notebook_path}")
            print(f"  Validated: {_dt.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  Tables   : {len(table_configs)}")
            print(f"  Registry : {len(registry_calls)} set_output() calls "
                  f"({len(resolved_keys)} resolved)")
            print(f"  Orch     : {len(orchestrator_calls)} Orchestrator call(s)")
            print(f"  Findings : {len(errors)} error(s), "
                  f"{len(warnings)} warning(s), {len(infos)} info(s)")
            print("\u2550" * 100)

            # Fix advice map
            _FIX_ADVICE = {
                "Direct .saveAsTable()": "Route through Orchestrator for logging, schema validation, and CSV export. If intentional, add: # INTENTIONAL",
                "Direct .save(format='delta')": "Use Orchestrator. If intentional, add: # INTENTIONAL",
                "CREATE TABLE": "Tables via spark.sql() bypass Belle. Use Orchestrator or add: # INTENTIONAL",
                "missing required key": "Add the missing key. Required: target_database, result_table_name, load_mode, dependencies.",
                "DOT separator": "Registry keys use UNDERSCORE: set_output(f\"{db}_{table}\", df). Config keys use DOT.",
                "no matching set_output": "Add belle.OutputRegistry.set_output(f\"{db}_{table}\", df) before .run().",
                "merge_keys": "Add 'merge_keys': ['col1', ...] to the table config.",
                "_dev suffix": "Remove '_dev'. Belle routes to _dev automatically in interactive mode.",
                "encrypt_key": "Add 'encrypt_key': your_key_variable when encrypt=True.",
                "No '%run bellerophon_core'": "Add %run ../bellerophon_core as the first code cell.",
                "requires 'partition_by'": "Add 'partition_by': ['col1']. Required for windowed replace.",
                "invalid day count": "Use format 'refresh_n_days-N' where N is a positive integer.",
                "invalid load_mode": f"Valid modes: {BellerophonConfig.VALID_LOAD_MODES}",
                "Dynamic registry key": "Dynamic keys can't be validated statically. Consider f-strings with known variables.",
                "BEFORE %run": "Move %run bellerophon_core before any Belle usage.",
                "registered": "Later registrations overwrite earlier ones. If intentional, no action needed.",
                "Hardcoded /mnt/": "Use belle.Config.BLOB_ROOT for write paths. Hardcoded paths break across environments.",
                "fail_on_validation_errors=False": "Re-enable validation unless you have a specific reason to suppress.",
                "global_force_rebuild=True": "Guard with a widget parameter or conditional check.",
            }

            def _match_advice(message):
                for pattern, advice in _FIX_ADVICE.items():
                    if pattern.lower() in message.lower():
                        return advice
                return ""

            def _normalise(msg):
                msg = re.sub(r"Table '[^']+': ", "", msg)
                sentences = re.split(r'\. (?=[A-Z])', msg)
                return sentences[0] + "." if len(sentences) > 1 else msg

            grouped = _dd(list)
            for f in findings:
                pattern = _normalise(f.message)
                loc = ""
                if f.cell_index is not None:
                    loc = f"Cell {f.cell_index}"
                    if f.line_number is not None:
                        loc += f":L{f.line_number}"
                grouped[(f.severity, f.category, pattern)].append(loc)

            if grouped:
                report_rows = []
                for (sev, cat, pattern), locations in grouped.items():
                    count = len(locations)
                    loc_str = (", ".join(locations) if count <= 5
                               else f"{', '.join(locations[:3])} (+{count - 3} more)")
                    advice = _match_advice(pattern)
                    report_rows.append(_Row(
                        Severity=sev, Category=cat, Count=count,
                        Finding=pattern[:150], Locations=loc_str, Fix=advice[:200]))
                df_report = spark.createDataFrame(report_rows)
                df_report = (
                    df_report
                    .withColumn("_sort",
                        _when(_col("Severity") == "ERROR", 1)
                        .when(_col("Severity") == "WARNING", 2)
                        .otherwise(3))
                    .orderBy("_sort", "Category", _col("Count").desc())
                    .drop("_sort")
                )
                display(df_report)
            else:
                print("\n  No findings. Pipeline notebook is clean. \u2705")

            # Tables summary
            if table_configs:
                table_rows = []
                for key in sorted(table_configs.keys()):
                    attrs = table_configs[key]
                    table_rows.append(_Row(
                        Table=key,
                        Mode=attrs.get('load_mode') or '?',
                        Encrypted=str(attrs.get('encrypt', False)),
                        Partition=', '.join(attrs.get('partition_by') or []) or '-',
                        MergeKeys=', '.join(attrs.get('merge_keys') or []) or '-'))
                if table_rows:
                    print("\n  Pipeline tables:")
                    display(spark.createDataFrame(table_rows))

        # Store findings for programmatic access: belle.Validator.findings
        cls.findings = findings
        cls.tables = table_configs
        return passed

    @classmethod
    def validate_config(cls, tables_config):
        """Validate a TABLES_CONFIG dict directly (no notebook parsing needed).

        Usage:
            belle.Validator.validate_config(TABLES_CONFIG)

        Returns True if no errors. Findings stored in belle.Validator.findings.
        """
        findings = []
        def _add(sev, cat, msg):
            findings.append(cls.Finding(sev, cat, msg))

        for tkey, conf in tables_config.items():
            # Required keys
            for rk in ('target_database', 'result_table_name', 'load_mode'):
                if rk not in conf:
                    _add("ERROR", "CONFIG", f"Table '{tkey}': missing required key '{rk}'.")
            # Load mode validation
            lm = conf.get('load_mode', '')
            lm_base = lm.split('-')[0] if '-' in lm and 'refresh' in lm else lm
            if lm_base and lm_base not in BellerophonConfig.VALID_LOAD_MODES:
                _add("ERROR", "CONFIG", f"Table '{tkey}': invalid load_mode '{lm}'.")
            if lm_base in ('merge', 'update', 'delete'):
                if not conf.get('merge_keys'):
                    _add("ERROR", "CONFIG",
                         f"Table '{tkey}': load_mode='{lm}' requires 'merge_keys'.")
            if 'refresh_n_days' in lm:
                if not conf.get('partition_by'):
                    _add("ERROR", "CONFIG",
                         f"Table '{tkey}': load_mode='{lm}' requires 'partition_by'.")
            if conf.get('encrypt') and not conf.get('encrypt_key'):
                _add("ERROR", "CONFIG",
                     f"Table '{tkey}': encrypt=True but no 'encrypt_key'.")
            # _dev suffix check
            db = conf.get('target_database', '')
            if db.endswith('_dev'):
                _add("ERROR", "CONFIG",
                     f"Table '{tkey}': target_database '{db}' has '_dev'. "
                     f"Belle routes to _dev automatically.")

        passed = not any(f.severity == "ERROR" for f in findings)
        cls.findings = findings
        return passed

# COMMAND ----------

# DBTITLE 1,BelleNamespace & Module Init
# ============================================================================
# SIMPLIFIED NAMESPACE - Clean API for Users
# ============================================================================

class BelleNamespace:
    """Unified namespace. Access all Belle classes/functions via belle.X"""
    
    # Core classes
    Orchestrator = BellerophonOrchestrator
    Config = BellerophonConfig
    Utils = BellerophonUtils
    Logger = BellerophonLogger
    
    # Maintenance & Scheduling
    MaintenanceScheduler = BellerophonMaintenanceScheduler
    
    # Visualization & Analysis
    DAGVisualizer = BellerophonDAGVisualizer
    OutputRegistry = BellerophonOutputRegistry
    
    # Advanced Features
    ProgressTracker = BellerophonProgressTracker
    RetryHandler = BellerophonRetryHandler
    DryRunValidator = BellerophonDryRunValidator
    DataQualityChecker = BellerophonDataQualityChecker
    ConfigValidator = BellerophonConfigValidator
    Validator = BelleValidator
    
    # Functions (staticmethod prevents bound-method self injection)
    materialise_table = staticmethod(resilient_materialise_table)
    materialise_dataframe = staticmethod(bellerophon_materialise_dataframe)
    materialise_dataframe_fast = staticmethod(bellerophon_materialise_dataframe_fast)
    materialise_partition = staticmethod(bellerophon_materialise_partition)
    materialise_bulk = staticmethod(bellerophon_materialise_bulk)
    flush_partition_logs = staticmethod(bellerophon_flush_partition_logs)
    
    # Log management
    purge_logs = staticmethod(BellerophonLogger.purge_logs)

    # Partition log buffer (direct access for advanced use)
    PartitionLogBuffer = _PartitionLogBuffer
    
    # Constants
    VERSION = BELLEROPHON_VERSION

    @property
    def VALID_LOAD_MODES(self):
        return BellerophonConfig.VALID_LOAD_MODES

    @property
    def SUPPORTED_DBR_VERSIONS(self):
        return BellerophonConfig.SUPPORTED_DBR_VERSIONS


# ============================================================================
# MODULE-LEVEL INSTANCE - Ready to use immediately
# ============================================================================

# Create belle instance at module level so users can import it directly
belle = BelleNamespace()


# No convenience aliases at module level — use belle.Orchestrator, belle.Config, etc.
# This prevents namespace pollution when users %run this notebook.


# ============================================================================
# MODULE INITIALIZATION
# ============================================================================

# Module load message
if BellerophonConfig.VERBOSITY >= BellerophonConfig.NORMAL:
    _mode = 'Interactive' if BellerophonUtils.is_interactive_notebook() else 'Production'
    print(f"Belle v{BELLEROPHON_VERSION} | {_mode} | "
          f"Encryption={'on' if BellerophonConfig.FEATURE_ENCRYPTION else 'off'} | "
          f"fast_mode=available | partition_mode=available | bulk_mode=available")