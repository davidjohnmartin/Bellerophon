# Databricks notebook source
# DBTITLE 1,Belle Log Dashboard — Discovery & Configuration
# =============================================================================
# BELLE LOG DASHBOARD — Pipeline Health, Data Quality & FinOps
# =============================================================================
# Scans all accessible databases/mounts for Belle log tables, unions them,
# and produces comprehensive monitoring metrics.
#
# Usage:
#   1. Run this notebook on any cluster with access to your pipelines
#   2. It auto-discovers all bellerophon_log_table instances
#   3. Dashboard views are created as temp views for BI tool consumption
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import hashlib

# --- Configuration ---
_LOG_TABLE_NAME = "bellerophon_log_table"
_LOOKBACK_DAYS = 90
_COST_PER_DBU_HOUR = 0.50  # Approximate €/DBU-hour (adjust per SKU)

# --- Anonymisation mode (for documentation/screenshots) ---
# Set to True to replace real pipeline/database names with synthetic ones.
# Useful when generating screenshots for public docs or presentations.
# NO real names are stored in this code — all replacements are hash-derived.
_ANONYMISE_OUTPUT = False

# --- Hash-based anonymisation (no hardcoded real names) ---
_GREEK = [
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
    "iota", "kappa", "lambda_", "mu", "nu", "xi", "omicron", "pi",
    "rho", "sigma", "tau", "upsilon", "phi", "chi", "psi", "omega",
]
_LAYERS = ["Raw", "Curated", "Processed", "Refined", "Landing"]
_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _hash_idx(name):
    """Deterministic index from a name (consistent across runs)."""
    return int(hashlib.md5(name.encode()).hexdigest()[:4], 16)


def _anon_pipeline(name):
    """Pipeline names → 'Pipeline A' .. 'Pipeline Z'."""
    if not _ANONYMISE_OUTPUT or name is None:
        return name
    return f"Pipeline {_LETTERS[_hash_idx(name) % 26]}"


def _anon_database(name):
    """Database/solution names → 'solution_alpha' .. 'solution_omega'."""
    if not _ANONYMISE_OUTPUT or name is None:
        return name
    return f"solution_{_GREEK[_hash_idx(name) % len(_GREEK)]}"


def _anon_fqn(name):
    """Fully qualified names → 'solution_X.table_HASH'."""
    if not _ANONYMISE_OUTPUT or name is None:
        return name
    if "." in name and "@" not in name:
        parts = name.split(".", 1)
        db = f"solution_{_GREEK[_hash_idx(parts[0]) % len(_GREEK)]}"
        tbl = hashlib.md5(parts[1].encode()).hexdigest()[:6]
        return f"{db}.table_{tbl}"
    # Bare table name (no dot)
    db = f"solution_{_GREEK[_hash_idx(name) % len(_GREEK)]}"
    tbl = hashlib.md5(name.encode()).hexdigest()[:6]
    return f"{db}.table_{tbl}"


def _anon_domain(name):
    """Tag domains → 'Domain 1' .. 'Domain 9'."""
    if not _ANONYMISE_OUTPUT or name is None:
        return name
    return f"Domain {(_hash_idx(name) % 9) + 1}"


def _anon_layer(name):
    """Tag layers → generic tier names."""
    if not _ANONYMISE_OUTPUT or name is None:
        return name
    return _LAYERS[_hash_idx(name) % len(_LAYERS)]


def _anon_user(name):
    """User identities → 'user_XXXX@example.com'."""
    if not _ANONYMISE_OUTPUT or name is None:
        return name
    h = hashlib.md5(name.encode()).hexdigest()[:4]
    return f"user_{h}@example.com"


# Register UDFs
_anon_pipeline_udf = F.udf(_anon_pipeline, StringType())
_anon_database_udf = F.udf(_anon_database, StringType())
_anon_fqn_udf = F.udf(_anon_fqn, StringType())
_anon_domain_udf = F.udf(_anon_domain, StringType())
_anon_layer_udf = F.udf(_anon_layer, StringType())
_anon_user_udf = F.udf(_anon_user, StringType())

print("Belle Log Dashboard v1.0")
print(f"Analysis window: {_LOOKBACK_DAYS} days")
print(f"Cost assumption: \u20ac{_COST_PER_DBU_HOUR}/DBU-hour (adjust _COST_PER_DBU_HOUR)")
if _ANONYMISE_OUTPUT:
    print("\n\u26a0\ufe0f  ANONYMISATION ENABLED: All names replaced via hash-based generation.")
    print("   No real names stored in code. Output is deterministic (same input = same alias).")

# COMMAND ----------

# DBTITLE 1,Auto-Discover Belle Log Tables
# =============================================================================
# AUTO-DISCOVERY: Find all Belle log tables across databases and catalogs
# =============================================================================

def discover_belle_logs():
    """Scan all accessible databases/catalogs for bellerophon_log_table."""
    found = []

    # --- Strategy 1 (PRIMARY): Scan mount points for Delta tables ---
    # Blob storage is the most reliable source — catalog tables may have
    # empty schemas or be stale mirrors of the same physical data.
    # The log table lives directly at mount root: /mnt/.../database/bellerophon_log_table
    seen_paths = set()
    try:
        mounts = dbutils.fs.mounts()
        for mount in mounts:
            if mount.mountPoint.startswith("/mnt/"):
                try:
                    check_path = f"{mount.mountPoint}/{_LOG_TABLE_NAME}"
                    files = dbutils.fs.ls(check_path)
                    if any(f.name == "_delta_log/" for f in files):
                        db_name = mount.mountPoint.rstrip("/").split("/")[-1]
                        if db_name not in seen_paths:
                            seen_paths.add(db_name)
                            found.append({
                                "fqn": f"delta.`{check_path}`",
                                "catalog": "mount",
                                "database": db_name,
                                "source": "blob"
                            })
                except Exception:
                    pass
    except Exception:
        pass

    return found

# Run discovery
log_sources = discover_belle_logs()

_SEP = "\u2550" * 79
print(f"\n{_SEP}")
print(f"  Found {len(log_sources)} Belle log table(s)")
print(_SEP)
for src in log_sources:
    print(f"  \u2022 {src['fqn']}")
    print(f"    Source: {src['source']} | Database: {src['database']}")

if not log_sources:
    print("  \u26a0\ufe0f  No Belle log tables found. Check mount/catalog access.")
    print("  Hint: Ensure your cluster has access to the databases where pipelines write.")

# COMMAND ----------

# DBTITLE 1,Combine All Log Sources into Unified View
# =============================================================================
# COMBINE: SQL view approach — no Python UDF, native Spark SQL only
# =============================================================================

def build_combined_view(sources, lookback_days=_LOOKBACK_DAYS):
    """Build SQL UNION ALL view from discovered sources. No UDF — pure SQL."""
    cutoff_str = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    # Canonical columns with alias mapping (old_name -> canonical)
    _ALIASES = {
        "result_table_name": "target_table_name",
        "execution_duration_sec": "execution_duration_seconds",
        "result_row_count": "row_count",
        "user_name": "user",
        "parameters": "parameters_json",
    }
    _OUTPUT_COLS = [
        "run_id", "log_id", "target_table_name", "success",
        "error_code", "error_message",
        "execution_start_time", "execution_end_time",
        "execution_duration_seconds", "row_count", "load_mode", "dag_stage",
        "rows_inserted", "rows_updated", "rows_deleted", "rows_before",
        "ran_in_interactive_mode", "parent_run_id",
        "user", "cluster_id", "notebook_path",
        "spark_version", "dbr_version",
        "is_unity_catalog", "use_managed_table",
        "file_size_bytes", "schema_json", "parameters_json",
        "subpipeline", "target_table_blob_dir", "tag"
    ]

    selects = []
    for src in sources:
        try:
            path = src["fqn"].replace("delta.`", "").rstrip("`")
            # Probe schema (metadata only — no data scan)
            schema = spark.read.format("delta").load(path).schema
            available = {f.name for f in schema.fields}

            # Build SELECT expressions per canonical column
            col_exprs = []
            for canon in _OUTPUT_COLS:
                # Check direct name or alias
                if canon in available:
                    col_exprs.append(f"`{canon}`")
                else:
                    # Check if a legacy alias maps to this canonical name
                    alias_found = None
                    for old, new in _ALIASES.items():
                        if new == canon and old in available:
                            alias_found = old
                            break
                    if alias_found:
                        col_exprs.append(f"`{alias_found}` AS `{canon}`")
                    else:
                        col_exprs.append(f"NULL AS `{canon}`")

            # Add source metadata literals
            db = src["database"].replace("'", "''")
            cat = src["catalog"].replace("'", "''")
            col_exprs.append(f"'{db}' AS _source_database")
            col_exprs.append(f"'{cat}' AS _source_catalog")

            cols_sql = ",\n        ".join(col_exprs)
            select = (
                f"SELECT\n        {cols_sql}\n"
                f"    FROM delta.`{path}`\n"
                f"    WHERE execution_start_time >= '{cutoff_str}'"
            )
            selects.append(select)
            print(f"  \u2713 {src['fqn']}: schema probed")
        except Exception as e:
            print(f"  \u2717 {src['fqn']}: {e}")

    if not selects:
        print("\n  \u274c No sources available.")
        return None

    union_sql = "\n    UNION ALL\n    ".join(selects)

    # Wrap with derived columns + tag parsing (all native SQL)
    view_sql = f"""
    CREATE OR REPLACE TEMP VIEW belle_logs_raw AS
    WITH src AS (
        {union_sql}
    )
    SELECT *,
        -- Derived columns
        COALESCE(subpipeline, tag, _source_database) AS _pipeline_name,
        TO_DATE(execution_start_time) AS run_date,
        HOUR(execution_start_time) AS run_hour,
        COALESCE(target_table_name, 'unknown') AS fqn,
        execution_duration_seconds / 60 AS duration_minutes,
        execution_duration_seconds AS execution_duration_sec,
        row_count AS result_row_count,
        NOT ran_in_interactive_mode AS is_production,
        COALESCE(
            TIMESTAMPADD(SECOND, CAST(execution_duration_seconds AS INT), execution_start_time),
            execution_start_time
        ) AS execution_end_time_calc,
        ROUND(execution_duration_seconds / 3600 * {_COST_PER_DBU_HOUR}, 4) AS estimated_cost_eur,
        -- Tag parsing (native SQL, no UDF)
        CASE
            WHEN tag LIKE '%=%' AND tag LIKE '%domain=%' THEN
                regexp_extract(tag, '(?i)domain=([^,;]+)', 1)
            ELSE split(regexp_replace(tag, '[, ]+', ','), ',')[0]
        END AS tag_domain,
        CASE
            WHEN tag LIKE '%=%' AND tag LIKE '%source=%' THEN
                regexp_extract(tag, '(?i)source=([^,;]+)', 1)
            ELSE NULL
        END AS tag_source,
        CASE
            WHEN tag LIKE '%=%' AND tag LIKE '%layer=%' THEN
                regexp_extract(tag, '(?i)layer=([^,;]+)', 1)
            WHEN lower(tag) LIKE '%staging%' THEN 'Staging'
            WHEN lower(tag) LIKE '%semantic%' THEN 'Semantic'
            WHEN lower(tag) LIKE '%flattened%' THEN 'Flattened'
            ELSE NULL
        END AS tag_layer,
        CASE
            WHEN tag LIKE '%=%' AND tag LIKE '%grain=%' THEN
                regexp_extract(tag, '(?i)grain=([^,;]+)', 1)
            WHEN lower(tag) LIKE '%dimensions%' THEN 'Dimensions'
            WHEN lower(tag) LIKE '%facts%' THEN 'Facts'
            ELSE NULL
        END AS tag_grain
    FROM src
    WHERE execution_start_time IS NOT NULL
    """
    spark.sql(view_sql)
    print(f"\n  \u2705 belle_logs_raw view created ({len(selects)} source(s))")

    # Lightweight DataFrame reference (lazy — no execution yet)
    df = spark.table("belle_logs_raw")

    # Override execution_end_time with the calculated version
    df = (
        df.drop("execution_end_time")
        .withColumnRenamed("execution_end_time_calc", "execution_end_time")
    )

    # Pipeline display name cleanup
    df = df.withColumn("pipeline", F.regexp_replace(
        F.regexp_replace(F.col("_pipeline_name"),
            "/mnt/internal/enhanced/", ""),
        "ci_transversal_telephony_", ""))

    df.createOrReplaceTempView("belle_logs_combined")
    return df


df_logs = build_combined_view(log_sources)
if df_logs is None:
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# DBTITLE 1,Persist Combined Logs & Build Summary Tables
# =============================================================================
# PERSIST: Write combined logs + summary tables as managed Delta tables
# These tables power the dashboard and survive session restarts.
# =============================================================================

_DASHBOARD_DB = "belle_dashboard"

if df_logs is None:
    print("  ⚠️  df_logs is None — no log data was loaded (check cell 2 discovery).")
    print("  Skipping persist step. Ensure mount points are accessible.")
    dbutils.notebook.exit("NO_DATA")

# Create database if needed
spark.sql(f"CREATE DATABASE IF NOT EXISTS {_DASHBOARD_DB}")

# --- Deduplicate (blob + catalog may overlap) & cache for 10 downstream writes ---
df_deduped = df_logs.dropDuplicates(["log_id"])
print("  \u2713 Deduplicated")

# --- Apply anonymisation if enabled (context-specific UDFs, no hardcoded names) ---
if _ANONYMISE_OUTPUT:
    print("  \u26a0\ufe0f  ANONYMISING: Hash-based name replacement...")
    df_deduped = (
        df_deduped
        .withColumn("_pipeline_name", _anon_pipeline_udf(F.col("_pipeline_name")))
        .withColumn("pipeline", _anon_pipeline_udf(F.col("pipeline")))
        .withColumn("fqn", _anon_fqn_udf(F.col("fqn")))
        .withColumn("target_table_name", _anon_fqn_udf(F.col("target_table_name")))
        .withColumn("_source_database", _anon_database_udf(F.col("_source_database")))
        .withColumn("tag_domain", _anon_domain_udf(F.col("tag_domain")))
        .withColumn("tag_layer", _anon_layer_udf(F.col("tag_layer")))
        .withColumn("tag_source", _anon_database_udf(F.col("tag_source")))
        .withColumn("subpipeline", _anon_pipeline_udf(F.col("subpipeline")))
        .withColumn("user", _anon_user_udf(F.col("user")))
        .withColumn("notebook_path", F.lit("/notebooks/pipeline_runner"))
        .withColumn("cluster_id", F.lit("cluster-0001"))
    )
    print("  \u2713 All identifying columns anonymised (hash-based)")

df_deduped = df_deduped.cache()
print("  \u2713 Cached for downstream writes")

# --- 1. Persist raw combined log ---
(
    df_deduped.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{_DASHBOARD_DB}.belle_log_combined")
)
print(f"  \u2713 {_DASHBOARD_DB}.belle_log_combined")

# --- 2. Pipeline runs summary ---
df_runs = df_deduped.filter("is_production = true").groupBy(
    "run_date", "_pipeline_name", "run_id"
).agg(
    F.count("*").alias("total_tables"),
    F.sum(F.when(F.col("success"), 1).otherwise(0)).alias("succeeded"),
    F.sum(F.when(~F.col("success"), 1).otherwise(0)).alias("failed"),
    F.round(F.sum("execution_duration_sec") / 60, 1).alias("total_minutes"),
    F.round(F.max("execution_duration_sec"), 1).alias("slowest_table_sec"),
    F.min("execution_start_time").alias("run_started"),
    F.max("execution_start_time").alias("run_ended"),
).withColumn("success_rate",
    F.round(100.0 * F.col("succeeded") / F.col("total_tables"), 1)
)
df_runs.write.format("delta").mode("overwrite").saveAsTable(f"{_DASHBOARD_DB}.pipeline_runs")
print(f"  \u2713 {_DASHBOARD_DB}.pipeline_runs")

# --- 3. Daily success rate ---
df_daily = df_deduped.filter("is_production = true").groupBy(
    "run_date", "_pipeline_name"
).agg(
    F.count("*").alias("total_writes"),
    F.sum(F.when(F.col("success"), 1).otherwise(0)).alias("succeeded"),
    F.sum(F.when(~F.col("success"), 1).otherwise(0)).alias("failed"),
).withColumn("success_rate_pct",
    F.round(100.0 * F.col("succeeded") / F.col("total_writes"), 1)
)
df_daily.write.format("delta").mode("overwrite").saveAsTable(f"{_DASHBOARD_DB}.daily_success_rate")
print(f"  \u2713 {_DASHBOARD_DB}.daily_success_rate")

# --- 4. Data growth (row count over time per table) ---
df_growth = (
    df_deduped
    .filter("is_production = true AND success = true AND result_row_count IS NOT NULL")
    .groupBy("run_date", "fqn", "_pipeline_name")
    .agg(
        F.max("result_row_count").alias("row_count"),
        F.max("file_size_bytes").alias("size_bytes"),
    )
    .withColumn("size_mb", F.round(F.col("size_bytes") / 1024 / 1024, 2))
    .withColumn("prev_row_count", F.lag("row_count").over(
        Window.partitionBy("fqn").orderBy("run_date")
    ))
    .withColumn("row_delta", F.col("row_count") - F.col("prev_row_count"))
    .withColumn("growth_pct", F.round(
        100.0 * F.col("row_delta") / F.when(F.col("prev_row_count") > 0, F.col("prev_row_count")),
        2
    ))
)
df_growth.write.format("delta").mode("overwrite").saveAsTable(f"{_DASHBOARD_DB}.data_growth")
print(f"  \u2713 {_DASHBOARD_DB}.data_growth")

# --- 5. Cost attribution ---
df_cost = (
    df_deduped
    .filter("is_production = true AND success = true")
    .groupBy("fqn", "_pipeline_name", "load_mode")
    .agg(
        F.count("*").alias("total_runs"),
        F.round(F.sum("execution_duration_sec") / 3600, 3).alias("compute_hours"),
        F.round(F.sum("execution_duration_sec") / 3600 * _COST_PER_DBU_HOUR, 2).alias("estimated_cost_eur"),
        F.round(F.avg("execution_duration_sec"), 1).alias("avg_duration_sec"),
        F.round(F.expr("percentile(execution_duration_sec, 0.95)"), 1).alias("p95_duration_sec"),
        F.sum("result_row_count").alias("total_rows"),
    )
)
df_cost.write.format("delta").mode("overwrite").saveAsTable(f"{_DASHBOARD_DB}.cost_attribution")
print(f"  \u2713 {_DASHBOARD_DB}.cost_attribution")

# --- 6. Performance trend (per table, per day) ---
df_perf = (
    df_deduped
    .filter("is_production = true AND success = true")
    .groupBy("run_date", "fqn", "_pipeline_name")
    .agg(
        F.round(F.avg("execution_duration_sec"), 1).alias("avg_duration_sec"),
        F.round(F.max("execution_duration_sec"), 1).alias("max_duration_sec"),
        F.count("*").alias("writes"),
    )
)
df_perf.write.format("delta").mode("overwrite").saveAsTable(f"{_DASHBOARD_DB}.performance_trend")
print(f"  \u2713 {_DASHBOARD_DB}.performance_trend")

# --- 7. Error breakdown ---
df_errors = (
    df_deduped
    .filter("success = false")
    .groupBy("run_date", "fqn", "_pipeline_name", "error_code")
    .agg(
        F.count("*").alias("occurrences"),
        F.first("error_message").alias("sample_message"),
    )
)
df_errors.write.format("delta").mode("overwrite").saveAsTable(f"{_DASHBOARD_DB}.error_breakdown")
print(f"  \u2713 {_DASHBOARD_DB}.error_breakdown")

# --- 8. Freshness ---
df_fresh = (
    df_deduped
    .filter("is_production = true AND success = true")
    .groupBy("fqn", "_pipeline_name")
    .agg(
        F.max("execution_start_time").alias("last_success"),
        F.count("*").alias("total_writes"),
        F.round(F.avg("execution_duration_sec"), 1).alias("avg_duration_sec"),
    )
    .withColumn("hours_stale", F.round(
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("last_success")) / 3600, 1
    ))
    .withColumn("last_success_date", F.to_date("last_success"))
    .withColumn("freshness_status",
        F.when(
            F.col("last_success_date")
            >= F.date_sub(F.current_date(), 1), "FRESH")
        .otherwise("STALE")
    )
)
df_fresh.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{_DASHBOARD_DB}.freshness")
print(f"  \u2713 {_DASHBOARD_DB}.freshness")

# --- 9. Mode efficiency comparison ---
df_mode = (
    df_deduped
    .filter("is_production = true AND success = true AND result_row_count > 0")
    .groupBy("load_mode")
    .agg(
        F.count("*").alias("total_writes"),
        F.round(F.avg("execution_duration_sec"), 1).alias("avg_sec"),
        F.round(F.avg("result_row_count"), 0).alias("avg_rows"),
        F.round(F.sum("execution_duration_sec") / 3600 * _COST_PER_DBU_HOUR, 2).alias("total_cost_eur"),
        F.round(
            F.sum("execution_duration_sec") / F.sum("result_row_count") * 1e6, 3
        ).alias("sec_per_million_rows"),
    )
)
df_mode.write.format("delta").mode("overwrite").saveAsTable(f"{_DASHBOARD_DB}.mode_efficiency")
print(f"  \u2713 {_DASHBOARD_DB}.mode_efficiency")

# --- 10. Hourly distribution (when do pipelines run?) ---
df_hourly = (
    df_deduped
    .filter("is_production = true")
    .groupBy("run_hour", "_pipeline_name")
    .agg(F.count("*").alias("table_writes"))
)
df_hourly.write.format("delta").mode("overwrite").saveAsTable(f"{_DASHBOARD_DB}.hourly_distribution")
print(f"  \u2713 {_DASHBOARD_DB}.hourly_distribution")

print(f"\n  \u2705 All 10 summary tables written to '{_DASHBOARD_DB}' database.")
print(f"  df_deduped remains cached for advanced analytics (next cell).")

# COMMAND ----------

# DBTITLE 1,Advanced Analytics — Drill-downs, Trends & Forecasting
# =============================================================================
# ADVANCED ANALYTICS: Drill-down, trend, forecasting & reliability tables
# Extends the core 10 tables with deeper insights for all user groups:
#   - Data Engineers: reliability, percentiles, DAG bottlenecks
#   - SRE / Platform: concurrency, SLA breaches, failure blast radius
#   - FinOps / Management: daily cost trends, forecasting inputs
#   - Governance: schema drift, row count anomalies, user activity
#   - Capacity Planning: day-of-week patterns, weekly comparison
# =============================================================================

_prod = df_deduped.filter("is_production = true")
_prod_success = _prod.filter("success = true")

# --- 11. Table Reliability Metrics ---
# Answers: "Which tables fail most?", "What's the MTBF?", "Streaks?"
_rel_base = _prod.groupBy("fqn", "_pipeline_name").agg(
    F.count("*").alias("total_runs"),
    F.sum(F.when(F.col("success"), 1).otherwise(0)).alias("successes"),
    F.sum(F.when(~F.col("success"), 1).otherwise(0)).alias("failures"),
    F.min("run_date").alias("first_seen"),
    F.max("run_date").alias("last_seen"),
    F.max(F.when(~F.col("success"), F.col("run_date"))).alias("last_failure_date"),
    F.max(F.when(F.col("success"), F.col("run_date"))).alias("last_success_date"),
    F.countDistinct("run_date").alias("active_days"),
)
_reliability = (
    _rel_base
    .withColumn("success_rate_pct",
        F.round(100.0 * F.col("successes") / F.col("total_runs"), 2))
    .withColumn("failure_rate_pct",
        F.round(100.0 * F.col("failures") / F.col("total_runs"), 2))
    .withColumn("mtbf_days",
        F.when(F.col("failures") > 0,
            F.round(F.col("active_days") / F.col("failures"), 1))
        .otherwise(F.col("active_days")))
    .withColumn("days_since_last_failure",
        F.when(F.col("last_failure_date").isNotNull(),
            F.datediff(F.current_date(), F.col("last_failure_date")))
        .otherwise(F.col("active_days")))
)
_reliability.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{_DASHBOARD_DB}.table_reliability")
print(f"  \u2713 {_DASHBOARD_DB}.table_reliability")

# --- 12. Performance Percentiles ---
# Answers: "What's normal?", "What's an SLA breach?", "Variability?"
_perf_pct = (
    _prod_success
    .groupBy("fqn", "_pipeline_name")
    .agg(
        F.count("*").alias("sample_count"),
        F.round(F.expr("percentile(execution_duration_sec, 0.50)"), 1)
            .alias("p50_sec"),
        F.round(F.expr("percentile(execution_duration_sec, 0.75)"), 1)
            .alias("p75_sec"),
        F.round(F.expr("percentile(execution_duration_sec, 0.90)"), 1)
            .alias("p90_sec"),
        F.round(F.expr("percentile(execution_duration_sec, 0.95)"), 1)
            .alias("p95_sec"),
        F.round(F.expr("percentile(execution_duration_sec, 0.99)"), 1)
            .alias("p99_sec"),
        F.round(F.avg("execution_duration_sec"), 1).alias("mean_sec"),
        F.round(F.stddev("execution_duration_sec"), 2).alias("stddev_sec"),
        F.round(F.min("execution_duration_sec"), 1).alias("min_sec"),
        F.round(F.max("execution_duration_sec"), 1).alias("max_sec"),
    )
    .withColumn("cv_pct",
        F.round(100.0 * F.col("stddev_sec") / F.col("mean_sec"), 1))
)
_perf_pct.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{_DASHBOARD_DB}.performance_percentiles")
print(f"  \u2713 {_DASHBOARD_DB}.performance_percentiles")

# --- 13. DAG Stage Bottleneck Analysis ---
# Answers: "Where's the bottleneck?", "Which stages are slowest?"
_dag = (
    _prod_success
    .filter("dag_stage IS NOT NULL")
    .groupBy("run_date", "_pipeline_name", "dag_stage")
    .agg(
        F.count("*").alias("table_count"),
        F.round(F.sum("execution_duration_sec"), 1)
            .alias("total_duration_sec"),
        F.round(F.avg("execution_duration_sec"), 1)
            .alias("avg_duration_sec"),
        F.round(F.max("execution_duration_sec"), 1)
            .alias("max_duration_sec"),
    )
)
# Add % of pipeline time
_dag_total = (
    _dag.groupBy("run_date", "_pipeline_name")
    .agg(F.sum("total_duration_sec").alias("pipeline_total_sec"))
)
_dag_final = (
    _dag.join(_dag_total, on=["run_date", "_pipeline_name"], how="left")
    .withColumn("pct_of_pipeline",
        F.round(
            100.0 * F.col("total_duration_sec")
            / F.col("pipeline_total_sec"), 1))
    .drop("pipeline_total_sec")
)
_dag_final.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{_DASHBOARD_DB}.dag_stage_analysis")
print(f"  \u2713 {_DASHBOARD_DB}.dag_stage_analysis")

# --- 14. Row Count Anomaly Detection ---
# Answers: "Is data quality degrading?", "Sudden drops/spikes?"
_w_rc = Window.partitionBy("fqn").orderBy("run_date").rowsBetween(-7, -1)
_rc = (
    _prod_success
    .filter("result_row_count IS NOT NULL AND result_row_count > 0")
    .groupBy("run_date", "fqn", "_pipeline_name")
    .agg(F.max("result_row_count").alias("row_count"))
    .withColumn("rolling_7d_avg",
        F.round(F.avg("row_count").over(_w_rc), 0))
    .withColumn("rolling_7d_stddev",
        F.stddev("row_count").over(_w_rc))
    .withColumn("z_score",
        F.when(
            (F.col("rolling_7d_stddev").isNotNull())
            & (F.col("rolling_7d_stddev") > 0),
            F.round(
                (F.col("row_count") - F.col("rolling_7d_avg"))
                / F.col("rolling_7d_stddev"), 2))
        .otherwise(F.lit(0.0)))
    .withColumn("anomaly_flag",
        F.when(F.abs(F.col("z_score")) > 2.0, F.lit(True))
        .otherwise(F.lit(False)))
    .withColumn("anomaly_type",
        F.when(F.col("z_score") > 2.0, F.lit("SPIKE"))
        .when(F.col("z_score") < -2.0, F.lit("DROP"))
        .otherwise(F.lit("NORMAL")))
    .withColumn("pct_change_from_avg",
        F.when(F.col("rolling_7d_avg") > 0,
            F.round(
                100.0 * (F.col("row_count") - F.col("rolling_7d_avg"))
                / F.col("rolling_7d_avg"), 1))
        .otherwise(F.lit(0.0)))
)
_rc.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{_DASHBOARD_DB}.row_count_anomalies")
print(f"  \u2713 {_DASHBOARD_DB}.row_count_anomalies")

# --- 15. Cost Daily Trend (for forecasting) ---
# Answers: "Is cost growing?", "What's the forecast?"
_w_cost7 = Window.partitionBy("_pipeline_name").orderBy("run_date").rowsBetween(-6, 0)
_w_cost30 = Window.partitionBy("_pipeline_name").orderBy("run_date").rowsBetween(-29, 0)
_cost_daily = (
    _prod_success
    .groupBy("run_date", "_pipeline_name")
    .agg(
        F.count("*").alias("table_writes"),
        F.round(F.sum("execution_duration_sec") / 3600, 3)
            .alias("compute_hours"),
        F.round(
            F.sum("execution_duration_sec") / 3600
            * _COST_PER_DBU_HOUR, 2).alias("daily_cost_eur"),
        F.sum("result_row_count").alias("total_rows_written"),
    )
    .withColumn("rolling_7d_avg_cost",
        F.round(F.avg("daily_cost_eur").over(_w_cost7), 2))
    .withColumn("rolling_30d_avg_cost",
        F.round(F.avg("daily_cost_eur").over(_w_cost30), 2))
    .withColumn("cumulative_cost",
        F.round(
            F.sum("daily_cost_eur").over(
                Window.partitionBy("_pipeline_name")
                .orderBy("run_date")
                .rowsBetween(Window.unboundedPreceding, 0)), 2))
)
_cost_daily.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{_DASHBOARD_DB}.cost_daily_trend")
print(f"  \u2713 {_DASHBOARD_DB}.cost_daily_trend")

# --- 16. Schema Change Detection ---
# Answers: "When did the schema change?", "Which tables evolved?"
_w_schema = Window.partitionBy("fqn").orderBy("execution_start_time")
_schema = (
    _prod_success
    .filter("schema_json IS NOT NULL")
    .select(
        "fqn", "_pipeline_name", "run_date",
        "execution_start_time", "schema_json")
    .withColumn("schema_hash", F.md5(F.col("schema_json")))
    .withColumn("prev_schema_hash",
        F.lag("schema_hash").over(_w_schema))
    .withColumn("schema_changed",
        (F.col("prev_schema_hash").isNotNull())
        & (F.col("schema_hash") != F.col("prev_schema_hash")))
    .filter("schema_changed = true OR prev_schema_hash IS NULL")
    .withColumn("change_sequence",
        F.row_number().over(_w_schema))
    .select(
        "fqn", "_pipeline_name", "run_date",
        "execution_start_time", "schema_hash",
        "prev_schema_hash", "schema_changed", "change_sequence")
)
_schema.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{_DASHBOARD_DB}.schema_changes")
print(f"  \u2713 {_DASHBOARD_DB}.schema_changes")

# --- 17. Run Concurrency & Overlap ---
# Answers: "Are runs stepping on each other?", "Max parallelism?"
_concurrency = (
    _prod
    .groupBy("run_date", "_pipeline_name", "run_id")
    .agg(
        F.count("*").alias("tables_in_run"),
        F.min("execution_start_time").alias("run_start"),
        F.max("execution_end_time").alias("run_end"),
        F.round(F.sum("execution_duration_sec"), 1)
            .alias("sum_individual_sec"),
        F.sum(F.when(F.col("success"), 1).otherwise(0))
            .alias("succeeded"),
        F.sum(F.when(~F.col("success"), 1).otherwise(0))
            .alias("failed"),
    )
    .withColumn("wall_clock_sec",
        F.round(
            (F.unix_timestamp("run_end")
             - F.unix_timestamp("run_start")).cast("double"), 1))
    .withColumn("parallelism_factor",
        F.when(F.col("wall_clock_sec") > 0,
            F.round(
                F.col("sum_individual_sec")
                / F.col("wall_clock_sec"), 2))
        .otherwise(F.lit(1.0)))
    .withColumn("efficiency_pct",
        F.when(F.col("sum_individual_sec") > 0,
            F.round(
                100.0 * F.col("wall_clock_sec")
                / F.col("sum_individual_sec"), 1))
        .otherwise(F.lit(100.0)))
)
_concurrency.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{_DASHBOARD_DB}.run_concurrency")
print(f"  \u2713 {_DASHBOARD_DB}.run_concurrency")

# --- 18. User Activity Summary ---
# Answers: "Who runs what?", "Interactive vs prod split?"
_users = (
    df_deduped
    .groupBy("user", "_pipeline_name")
    .agg(
        F.count("*").alias("total_executions"),
        F.sum(F.when(F.col("success"), 1).otherwise(0))
            .alias("successes"),
        F.sum(F.when(F.col("is_production"), 1).otherwise(0))
            .alias("production_runs"),
        F.sum(F.when(~F.col("is_production"), 1).otherwise(0))
            .alias("interactive_runs"),
        F.countDistinct("fqn").alias("distinct_tables"),
        F.countDistinct("run_date").alias("active_days"),
        F.min("run_date").alias("first_activity"),
        F.max("run_date").alias("last_activity"),
        F.round(F.sum("execution_duration_sec") / 3600, 2)
            .alias("total_compute_hours"),
    )
    .withColumn("success_rate_pct",
        F.round(
            100.0 * F.col("successes")
            / F.col("total_executions"), 1))
    .withColumn("interactive_pct",
        F.round(
            100.0 * F.col("interactive_runs")
            / F.col("total_executions"), 1))
)
_users.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{_DASHBOARD_DB}.user_activity")
print(f"  \u2713 {_DASHBOARD_DB}.user_activity")

# --- 19. Duration Forecast Input ---
# Daily time-series per pipeline, shaped for ai_forecast() / Prophet
_w_dur7 = Window.partitionBy("_pipeline_name").orderBy("run_date").rowsBetween(-6, 0)
_forecast = (
    _prod_success
    .groupBy("run_date", "_pipeline_name")
    .agg(
        F.count("*").alias("table_count"),
        F.round(F.avg("execution_duration_sec"), 1)
            .alias("avg_duration_sec"),
        F.round(F.sum("execution_duration_sec"), 1)
            .alias("total_duration_sec"),
        F.round(F.max("execution_duration_sec"), 1)
            .alias("max_duration_sec"),
        F.sum("result_row_count").alias("total_rows"),
        F.round(
            F.sum("execution_duration_sec") / 3600
            * _COST_PER_DBU_HOUR, 2).alias("daily_cost_eur"),
    )
    .withColumn("rolling_7d_avg_duration",
        F.round(F.avg("avg_duration_sec").over(_w_dur7), 1))
    .withColumn("rolling_7d_total_duration",
        F.round(F.avg("total_duration_sec").over(_w_dur7), 1))
    .withColumn("day_of_week", F.dayofweek("run_date"))
    .withColumn("is_weekend",
        F.col("day_of_week").isin(1, 7))
)
_forecast.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{_DASHBOARD_DB}.duration_forecast_input")
print(f"  \u2713 {_DASHBOARD_DB}.duration_forecast_input")

# --- 20. Weekly Comparison ---
# Answers: "Better or worse than last week?", "WoW trends?"
_weekly = (
    _prod
    .withColumn("iso_week", F.weekofyear("run_date"))
    .withColumn("iso_year", F.year("run_date"))
    .groupBy("iso_year", "iso_week", "_pipeline_name")
    .agg(
        F.count("*").alias("total_writes"),
        F.sum(F.when(F.col("success"), 1).otherwise(0))
            .alias("successes"),
        F.sum(F.when(~F.col("success"), 1).otherwise(0))
            .alias("failures"),
        F.round(F.avg("execution_duration_sec"), 1)
            .alias("avg_duration_sec"),
        F.round(F.sum("execution_duration_sec") / 3600, 2)
            .alias("compute_hours"),
        F.round(
            F.sum("execution_duration_sec") / 3600
            * _COST_PER_DBU_HOUR, 2).alias("weekly_cost_eur"),
        F.sum("result_row_count").alias("total_rows"),
        F.min("run_date").alias("week_start"),
        F.max("run_date").alias("week_end"),
    )
    .withColumn("success_rate_pct",
        F.round(
            100.0 * F.col("successes")
            / F.col("total_writes"), 1))
)
# Add WoW deltas
_w_wow = Window.partitionBy("_pipeline_name").orderBy("iso_year", "iso_week")
_weekly = (
    _weekly
    .withColumn("prev_week_cost",
        F.lag("weekly_cost_eur").over(_w_wow))
    .withColumn("prev_week_duration",
        F.lag("avg_duration_sec").over(_w_wow))
    .withColumn("prev_week_success_rate",
        F.lag("success_rate_pct").over(_w_wow))
    .withColumn("cost_wow_pct",
        F.when(F.col("prev_week_cost") > 0,
            F.round(
                100.0 * (F.col("weekly_cost_eur") - F.col("prev_week_cost"))
                / F.col("prev_week_cost"), 1))
        .otherwise(F.lit(None)))
    .withColumn("duration_wow_pct",
        F.when(F.col("prev_week_duration") > 0,
            F.round(
                100.0
                * (F.col("avg_duration_sec") - F.col("prev_week_duration"))
                / F.col("prev_week_duration"), 1))
        .otherwise(F.lit(None)))
)
_weekly.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{_DASHBOARD_DB}.weekly_comparison")
print(f"  \u2713 {_DASHBOARD_DB}.weekly_comparison")

# --- 21. Day-of-Week Execution Patterns ---
# Answers: "When should we schedule?", "Peak capacity days?"
_dow = (
    _prod
    .withColumn("day_of_week", F.dayofweek("run_date"))
    .withColumn("day_name", F.date_format("run_date", "EEEE"))
    .groupBy("day_of_week", "day_name", "_pipeline_name")
    .agg(
        F.count("*").alias("total_writes"),
        F.round(F.avg("execution_duration_sec"), 1)
            .alias("avg_duration_sec"),
        F.round(F.sum("execution_duration_sec") / 3600, 2)
            .alias("total_compute_hours"),
        F.round(
            100.0
            * F.sum(F.when(F.col("success"), 1).otherwise(0))
            / F.count("*"), 1).alias("success_rate_pct"),
        F.countDistinct("run_date").alias("sample_days"),
        F.round(F.count("*") / F.countDistinct("run_date"), 1)
            .alias("avg_writes_per_day"),
    )
)
_dow.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{_DASHBOARD_DB}.execution_day_patterns")
print(f"  \u2713 {_DASHBOARD_DB}.execution_day_patterns")

# --- 22. Failure Blast Radius ---
# Answers: "When one table fails, what else fails?", "Correlated?"
_blast = (
    _prod
    .filter("success = false")
    .groupBy("run_date", "_pipeline_name", "run_id")
    .agg(
        F.count("*").alias("failed_tables_count"),
        F.collect_set("fqn").alias("failed_tables"),
        F.collect_set("error_code").alias("error_codes"),
        F.min("execution_start_time").alias("first_failure_time"),
    )
    .withColumn("blast_radius",
        F.when(F.col("failed_tables_count") == 1, F.lit("ISOLATED"))
        .when(F.col("failed_tables_count") <= 3, F.lit("LIMITED"))
        .when(F.col("failed_tables_count") <= 10, F.lit("MODERATE"))
        .otherwise(F.lit("WIDESPREAD")))
)
_blast.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{_DASHBOARD_DB}.failure_blast_radius")
print(f"  \u2713 {_DASHBOARD_DB}.failure_blast_radius")

# --- 23. Tag Performance Summary ---
# Answers: "Cost by domain/layer?", "Which layers are slowest?"
_tag_base = (
    _prod
    .filter("tag IS NOT NULL AND tag != ''")
    .groupBy("tag_domain", "tag_source", "tag_layer", "tag_grain",
             "_pipeline_name")
    .agg(
        F.count("*").alias("total_executions"),
        F.sum(F.when(F.col("success"), 1).otherwise(0))
            .alias("successes"),
        F.sum(F.when(~F.col("success"), 1).otherwise(0))
            .alias("failures"),
        F.countDistinct("fqn").alias("distinct_tables"),
        F.round(F.avg("execution_duration_sec"), 1)
            .alias("avg_duration_sec"),
        F.round(F.sum("execution_duration_sec") / 3600, 2)
            .alias("total_compute_hours"),
        F.round(
            F.sum("execution_duration_sec") / 3600
            * _COST_PER_DBU_HOUR, 2).alias("total_cost_eur"),
        F.sum("result_row_count").alias("total_rows_written"),
        F.min("run_date").alias("first_seen"),
        F.max("run_date").alias("last_seen"),
    )
    .withColumn("success_rate_pct",
        F.round(
            100.0 * F.col("successes")
            / F.col("total_executions"), 1))
    .withColumn("cost_per_table_eur",
        F.when(F.col("distinct_tables") > 0,
            F.round(
                F.col("total_cost_eur")
                / F.col("distinct_tables"), 3))
        .otherwise(F.lit(0.0)))
)
_tag_base.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{_DASHBOARD_DB}.tag_performance")
print(f"  \u2713 {_DASHBOARD_DB}.tag_performance")

# --- 24. Tag Daily Trend (for drill-down & forecasting by domain/layer) ---
_tag_daily = (
    _prod
    .filter("tag IS NOT NULL AND tag != ''")
    .groupBy("run_date", "tag_domain", "tag_layer", "_pipeline_name")
    .agg(
        F.count("*").alias("table_writes"),
        F.sum(F.when(F.col("success"), 1).otherwise(0))
            .alias("successes"),
        F.round(F.sum("execution_duration_sec") / 3600, 3)
            .alias("compute_hours"),
        F.round(
            F.sum("execution_duration_sec") / 3600
            * _COST_PER_DBU_HOUR, 2).alias("daily_cost_eur"),
        F.round(F.avg("execution_duration_sec"), 1)
            .alias("avg_duration_sec"),
        F.sum("result_row_count").alias("total_rows"),
    )
    .withColumn("success_rate_pct",
        F.round(
            100.0 * F.col("successes")
            / F.col("table_writes"), 1))
)
_tag_daily.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{_DASHBOARD_DB}.tag_daily_trend")
print(f"  \u2713 {_DASHBOARD_DB}.tag_daily_trend")

# --- Release the cache ---
df_deduped.unpersist()

print(f"\n  \u2705 All 14 advanced analytics tables written.")
print(f"  Total: 24 tables in '{_DASHBOARD_DB}' database.")
print(f"  Cache released.")