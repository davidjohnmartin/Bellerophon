# Log Table Deep Dive — Your Pipeline's Black Box Recorder

**Audience:** Everyone. This document shows why the log table is Belle's most underappreciated feature.

---

## The Power of Automatic Audit

Every time Belle writes a table, it records **exactly what happened** — success or failure, duration, row counts, schema snapshots, error codes, and correlation IDs. This happens automatically. No instrumentation needed. No dashboards to build. No manual logging to forget.

The result: a **complete operational history** of every table write across every run, queryable with standard SQL.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    📊  BELLE LOG TABLE  📊                                 │
│                                                                            │
│  Every table write → One log row                                            │
│  Every run         → One run_id (correlates all tables in that execution)   │
│  Every failure     → Error code + message + stack trace                      │
│  Every success     → Duration + row count + schema snapshot                  │
│                                                                            │
│  ⏱️  HOW LONG did each table take?                                          │
│  📈  HOW BIG is each table growing?                                          │
│  ❌  WHAT FAILED and why?                                                    │
│  🔄  HOW MANY retries before success?                                       │
│  📅  WHEN did the last successful run happen?                                │
│                                                                            │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Log Table Schema (Visual)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  bellerophon_log_table                                                      │
├────────────────────────┬─────────────┬───────────────────────────────────────┤
│  COLUMN                  │  TYPE       │  PURPOSE                              │
├────────────────────────┼─────────────┼───────────────────────────────────────┤
│  run_id                  │  STRING    │  Correlates all tables in one run     │
│  log_id                  │  STRING    │  Unique per table-write                │
│  target_database         │  STRING    │  Which database was written to          │
│  result_table_name       │  STRING    │  Which table was written                │
│  success                 │  BOOLEAN   │  Did the write succeed?                 │
│  error_code              │  STRING    │  BELLE-XXX structured error code        │
│  error_message           │  STRING    │  Human-readable error detail            │
│  execution_duration_sec  │  DOUBLE    │  Seconds taken for this table write     │
│  execution_start_time    │  TIMESTAMP │  When this write started                │
│  result_row_count        │  LONG      │  Rows in target after write             │
│  dag_stage               │  INT       │  Which parallel stage this ran in       │
│  load_mode               │  STRING    │  Write mode (full/merge/insert/...)     │
│  parent_run_id           │  STRING    │  ADF/external run correlation           │
│  schema_json             │  STRING    │  Table schema snapshot (truncated)      │
└────────────────────────┴─────────────┴───────────────────────────────────────┘
```

---

## 1b. What Logs Capture — and What They Don't

Belle's log table is a **write-level audit trail**: one row per table materialisation attempt. Understanding its scope prevents both under-use and unrealistic expectations.

### What the logs DO capture

| Dimension | Detail |
|-----------|--------|
| **Execution timing** | Start timestamp, end timestamp, duration in seconds — per table, per run |
| **Row counts** | Post-write row count, plus rows inserted/updated/deleted (merge modes) |
| **Success/failure** | Boolean outcome + structured error code (BELLE-001 through BELLE-999) + message |
| **Schema snapshot** | JSON representation of the DataFrame schema at write time |
| **DAG position** | Which parallel stage this table was in |
| **Write mode** | Which load_mode was used (full, merge, insert, refresh_n_days, etc.) |
| **Correlation** | `run_id` ties all tables in a single Orchestrator execution together |
| **Environment** | Interactive vs production, cluster ID, notebook path, Spark/DBR version, user |
| **Storage** | Target database, table name, blob path or UC MANAGED flag |
| **Retry context** | Each retry attempt gets its own log row with the same run_id |

### What the logs do NOT capture

| Not captured | Why | Alternative |
|---|---|---|
| **Source query/transformation logic** | Belle operates at the write boundary; it receives a DataFrame | Version control + notebook lineage |
| **Upstream data quality** | Belle validates structure (schema drift, row counts), not business rules | DataQualityChecker / Great Expectations / UC monitors |
| **Read performance** | Time to build DataFrames before they reach Belle | Spark UI / query history |
| **Cost/DBU consumption** | Belle measures wall-clock time, not compute cost | Databricks Account Console / billing APIs |
| **Data content/values** | No row-level data in logs (privacy by design) | Delta time travel on the target table |
| **Cross-pipeline dependencies** | Belle logs one Orchestrator run; doesn't know about other pipelines | ADF/Workflows dependency graphs |

### Extensibility: Passing Custom Context

Belle is designed to accept **additional context values** that flow through to the log table. This makes it a natural integration point with external orchestrators:

```python
# Pass ADF pipeline run ID for cross-system correlation
orte = belle.Orchestrator(TABLES_CONFIG)
orte.run(
    parent_run_id="ADF_RUN_abc123",      # Correlates with Azure Data Factory
    parameters="country=france,mode=incremental",  # Free-text parameter capture
)
```

The `parent_run_id` column enables joining Belle's per-table logs back to external orchestrator run history. Combined with `parameters`, this gives:

- **ADF → Belle correlation**: Which ADF pipeline triggered which Belle run, with what parameters
- **Parameterised runs**: Record what widget values or notebook parameters were active
- **Multi-layer traceability**: ADF run → Belle run_id → individual table writes → Delta table versions

#### Planned extensions

The log schema is forward-compatible. Future versions may capture:
- `custom_tags` (dict/JSON) — arbitrary key-value pairs for team-specific metadata
- `data_lineage_hash` — fingerprint of the source query for change detection
- `cost_dbu_estimate` — estimated DBU based on duration × cluster type

The philosophy: **Belle logs what it controls (the write), and provides hooks for everything else.**

---

## 2. What the Logs Enable

The Belle Pipeline Monitoring dashboard transforms raw log data into actionable visuals. For full documentation, see [17_Dashboard_Guide.md](./17_Dashboard_Guide.md).

![Executive Overview — at-a-glance pipeline health](../images/page1.png)

![Pipeline Deep-Dive — drill into any table's execution history](../images/page13.png)

### 2.1 🎯 Instant Pipeline Health Check

```sql
-- ┌──────────────────────────────────────────────────────────────────────────────┐
-- │  QUERY: "Is my pipeline healthy?"                                          │
-- │  Answer in 2 seconds, not 20 minutes of manual checking.                   │
-- └──────────────────────────────────────────────────────────────────────────────┘

SELECT
    run_id,
    MIN(execution_start_time)                      AS run_started,
    MAX(execution_start_time)                      AS run_ended,
    COUNT(*)                                       AS total_tables,
    SUM(CASE WHEN success THEN 1 ELSE 0 END)       AS ✅_succeeded,
    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END)   AS ❌_failed,
    ROUND(SUM(execution_duration_sec), 1)           AS total_seconds,
    ROUND(SUM(execution_duration_sec) / 60, 1)     AS total_minutes
FROM my_database.bellerophon_log_table
WHERE execution_start_time >= current_date() - INTERVAL 7 DAYS
GROUP BY run_id
ORDER BY run_started DESC;
```

**Output visualisation:**
```
┌──────────────────────────────────────────────────────────────────────────────┐
│  PIPELINE HEALTH  —  Last 7 Days                                            │
├────────────┬─────────┬────────┬──────────┬──────────┬─────────┬───────────────┤
│ run_id     │ started │ tables │ ✅ passed │ ❌ failed │ minutes │ status        │
├────────────┼─────────┼────────┼──────────┼──────────┼─────────┼───────────────┤
│ a3f2...    │ Jun 23  │    29  │       29 │        0 │    8.3  │ █████ PERFECT │
│ b7e1...    │ Jun 22  │    29  │       29 │        0 │    7.9  │ █████ PERFECT │
│ c9d4...    │ Jun 21  │    29  │       27 │        2 │   12.1  │ ███░░ WARNING │
│ d2a8...    │ Jun 20  │    29  │       29 │        0 │    8.5  │ █████ PERFECT │
└────────────┴─────────┴────────┴──────────┴──────────┴─────────┴───────────────┘
```

---

### 2.2 📈 Table Growth Over Time

```sql
-- Track how your tables grow (or shrink) across runs
SELECT
    result_table_name,
    DATE(execution_start_time) AS run_date,
    result_row_count,
    LAG(result_row_count) OVER (
        PARTITION BY result_table_name ORDER BY execution_start_time
    ) AS prev_row_count,
    result_row_count - LAG(result_row_count) OVER (
        PARTITION BY result_table_name ORDER BY execution_start_time
    ) AS row_delta,
    ROUND(100.0 * (result_row_count - LAG(result_row_count) OVER (
        PARTITION BY result_table_name ORDER BY execution_start_time
    )) / NULLIF(LAG(result_row_count) OVER (
        PARTITION BY result_table_name ORDER BY execution_start_time
    ), 0), 2) AS pct_change
FROM my_database.bellerophon_log_table
WHERE success = true
ORDER BY result_table_name, run_date;
```

**Visualisation: Spot anomalies instantly**
```
  Row Count Trend: factorder
  │
  │                                                        ⨯ ← ANOMALY: +340%
  │                                                       ╱
  │  ────────────────────────────────────────────────╱
  │  Steady state (~1.2M rows)                            ╱
  │                                                      ╱
  │                                                     ╱ ← Investigate!
  └─────────────────────────────────────────────────────────
    Jun 16   Jun 17   Jun 18   Jun 19   Jun 20   Jun 21   Jun 22
```

---

### 2.3 ⏱️ Performance Regression Detection

```sql
-- Find tables that are slowing down over time
WITH recent AS (
    SELECT result_table_name,
           AVG(execution_duration_sec) AS avg_duration,
           STDDEV(execution_duration_sec) AS std_duration
    FROM my_database.bellerophon_log_table
    WHERE success = true
      AND execution_start_time >= current_date() - INTERVAL 30 DAYS
    GROUP BY result_table_name
),
latest AS (
    SELECT result_table_name, execution_duration_sec AS last_duration
    FROM my_database.bellerophon_log_table
    WHERE run_id = (SELECT run_id FROM my_database.bellerophon_log_table
                    ORDER BY execution_start_time DESC LIMIT 1)
      AND success = true
)
SELECT
    r.result_table_name,
    ROUND(r.avg_duration, 1)        AS avg_30d_sec,
    ROUND(l.last_duration, 1)       AS last_run_sec,
    ROUND((l.last_duration - r.avg_duration) / NULLIF(r.std_duration, 0), 1)
                                    AS z_score,
    CASE
        WHEN (l.last_duration - r.avg_duration) / NULLIF(r.std_duration, 0) > 2
            THEN '🚨 REGRESSION'
        WHEN (l.last_duration - r.avg_duration) / NULLIF(r.std_duration, 0) > 1
            THEN '⚠️  SLOWER'
        ELSE '✅ NORMAL'
    END AS status
FROM recent r
JOIN latest l ON r.result_table_name = l.result_table_name
ORDER BY z_score DESC;
```

**Visualisation:**
```
  ┌──────────────────────────────────────────────────────────────────────┐
  │  PERFORMANCE Z-SCORE (last run vs 30-day baseline)                    │
  ├───────────────────────┬──────────────────────────────────────────────┤
  │  factorder             │  ████████████████████  z=3.2 🚨 REGRESSION   │
  │  factpayment           │  ████████████         z=1.4 ⚠️  SLOWER       │
  │  dimcustomer           │  ████                  z=0.3 ✅ NORMAL       │
  │  factorder             │  ███                   z=0.1 ✅ NORMAL       │
  │  dimproduct            │  ██                    z=-0.2 ✅ NORMAL       │
  └───────────────────────┴──────────────────────────────────────────────┘
```

---

### 2.4 🔍 Failure Forensics

```sql
-- Complete failure history with patterns
SELECT
    error_code,
    result_table_name,
    error_message,
    execution_start_time,
    run_id
FROM my_database.bellerophon_log_table
WHERE success = false
  AND execution_start_time >= current_date() - INTERVAL 30 DAYS
ORDER BY execution_start_time DESC;
```

**Error pattern heatmap (which tables fail, and why):**
```
                    │ BELLE │ BELLE │ BELLE │ BELLE │ BELLE │ BELLE │
  TABLE             │  001  │  010  │  020  │  030  │  040  │  999  │
  ───────────────────┼───────┼───────┼───────┼───────┼───────┼───────┤
  factorder         │       │       │  ██   │  █    │       │       │
  factpayment       │       │       │  █    │  ███  │       │       │
  dimcontract       │       │  █    │       │       │       │       │
  factcall     │       │       │       │       │  █    │       │
  ───────────────────┴───────┴───────┴───────┴───────┴───────┴───────┘
         █ = 1 occurrence       ██ = 2-3       ███ = 4+

  INSIGHT: factpayment has recurring OOM (BELLE-030) → increase cluster or reduce parallelism
```

---

### 2.5 📊 Run Duration Waterfall (DAG Stage Timing)

```sql
-- See how long each DAG stage takes (parallelism effectiveness)
SELECT
    dag_stage,
    COUNT(*) AS tables_in_stage,
    ROUND(MAX(execution_duration_sec), 1) AS stage_wall_clock,
    ROUND(AVG(execution_duration_sec), 1) AS avg_per_table,
    ROUND(SUM(execution_duration_sec), 1) AS serial_equivalent,
    ROUND(SUM(execution_duration_sec) / MAX(execution_duration_sec), 1)
        AS parallelism_factor
FROM my_database.bellerophon_log_table
WHERE run_id = '<latest_run_id>' AND success = true
GROUP BY dag_stage
ORDER BY dag_stage;
```

**Waterfall visualisation:**
```
  DAG EXECUTION WATERFALL  (wall-clock: 8.3 min | serial equivalent: 31.2 min)
  ─────────────────────────────────────────────────────────────────

  Stage 1 │████████████████│  2.1 min  (9 dims, parallel)
          │                │
  Stage 2 │                ██████████████████████████████│  4.8 min  (7 facts, parallel)
          │                                              │
  Stage 3 │                                              ████████│  1.4 min  (7 views, parallel)
          │                                                      │
          └──────────────────────────────────────────────────────┘
          0 min                                                    8.3 min

  💡 Parallelism saved 22.9 minutes (3.8x speedup)
```

---

### 2.6 📅 SLA & Freshness Monitoring

```sql
-- When was each table last successfully written?
SELECT
    result_table_name,
    MAX(execution_start_time) AS last_success,
    DATEDIFF(current_timestamp(), MAX(execution_start_time)) AS days_stale,
    CASE
        WHEN DATEDIFF(current_timestamp(), MAX(execution_start_time)) > 2
            THEN '🚨 STALE'
        WHEN DATEDIFF(current_timestamp(), MAX(execution_start_time)) > 1
            THEN '⚠️  AGING'
        ELSE '✅ FRESH'
    END AS freshness
FROM my_database.bellerophon_log_table
WHERE success = true
GROUP BY result_table_name
ORDER BY days_stale DESC;
```

---

### 2.7 🔗 Cross-System Tracing (ADF Correlation)

```sql
-- Link Belle runs to ADF pipeline runs
SELECT
    parent_run_id AS adf_run_id,
    run_id AS belle_run_id,
    COUNT(*) AS tables_written,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) AS succeeded,
    MIN(execution_start_time) AS started,
    ROUND(SUM(execution_duration_sec) / 60, 1) AS total_min
FROM my_database.bellerophon_log_table
WHERE parent_run_id IS NOT NULL
GROUP BY parent_run_id, run_id
ORDER BY started DESC;
```

---

## 3. Dashboard Patterns

### 3.1 Executive Summary (Daily Email / Slack Alert)

```
╔══════════════════════════════════════════════════════════════════════════════╗
║   🐎 BELLE DAILY REPORT  —  23 June 2026                                    ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║   Pipelines run:     4                                                     ║
║   Tables written:    87 / 87  (✅ 100% success)                             ║
║   Total duration:    23.4 min (serial equiv: 94.1 min → 4.0x parallel)     ║
║   Rows written:      14.2M (across all tables)                             ║
║                                                                            ║
║   ┌────────────────────────┬──────────┬──────────┬──────────┬───────────┐   ║
║   │ Pipeline               │ Tables   │ Success  │ Duration │ Status    │   ║
║   ├────────────────────────┼──────────┼──────────┼──────────┼───────────┤   ║
║   │ Sales Semantic         │    29    │   29/29  │  8.3 min │ ✅ PASS   │   ║
║   │ CRM Staging            │    18    │   18/18  │  4.2 min │ ✅ PASS   │   ║
║   │ Telephony Semantic     │    31    │   31/31  │  7.8 min │ ✅ PASS   │   ║
║   │ Ops Reporting      │     9    │    9/9   │  3.1 min │ ✅ PASS   │   ║
║   └────────────────────────┴──────────┴──────────┴──────────┴───────────┘   ║
║                                                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

### 3.2 Recommended Databricks Dashboard Widgets

| Widget | Query | Chart type | Purpose |
| --- | --- | --- | --- |
| Success rate (7d) | % success by day | Line chart | Trend |
| Run duration | Total minutes per run | Bar chart | Performance tracking |
| Table heatmap | Success/fail by table×day | Heatmap | Failure hotspots |
| Growth anomalies | Row count % change > 50% | Counter/Alert | Data drift |
| Slowest tables | Top 5 by duration (last run) | Horizontal bar | Bottlenecks |
| Error distribution | Count by error_code | Pie/donut | Root cause categories |

---

## 4. Advanced: Multi-Database Log Union

Since each target database has its own log table, create a cross-pipeline view:

```sql
CREATE OR REPLACE VIEW monitoring.all_belle_logs AS
SELECT 'sales_semantic' AS pipeline, * FROM sales_semantic.bellerophon_log_table
UNION ALL
SELECT 'crm_staging' AS pipeline, * FROM crm_staging.bellerophon_log_table
UNION ALL
SELECT 'telephony_semantic' AS pipeline, *
    FROM telephony_semantic.bellerophon_log_table
UNION ALL
SELECT 'travel_agent' AS pipeline, *
    FROM operations_reporting.bellerophon_log_table;
```

Now all queries above work across ALL pipelines in one shot.

---

## 5. The Log Table as a Time Machine

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  🕰️  QUESTIONS THE LOG TABLE ANSWERS INSTANTLY                               │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  “Did the pipeline run last night?”              → Last run_id timestamp     │
│  “Which table failed?”                           → WHERE success = false     │
│  “Why did it fail?”                              → error_code + message      │
│  “How long does factorder usually take?”         → AVG(duration) over 30d    │
│  “Is it getting slower?”                         → Z-score vs baseline       │
│  “When did row count jump?”                      → LAG() window function     │
│  “Which ADF run triggered this?”                 → parent_run_id             │
│  “How effective is our parallelism?”             → serial_equiv / wall_clock  │
│  “When was the last VACUUM?”                     → load_mode = 'maintenance' │
│  “How many rows total across all tables?”        → SUM(result_row_count)     │
│  “Which tables never fail?”                      → GROUP BY HAVING all TRUE  │
│  “What changed in the schema last week?”         → schema_json comparison    │
│                                                                            │
│  All answers: < 2 seconds. No dashboarding tool required. Just SQL.         │
│                                                                            │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Without Belle Logs vs With Belle Logs

```
  ┌──────────────────────────────────┐  ┌────────────────────────────────────┐
  │  WITHOUT LOGS                       │  │  WITH BELLE LOGS                      │
  ├──────────────────────────────────┤  ├────────────────────────────────────┤
  │                                    │  │                                      │
  │  “Did it run?”                      │  │  SELECT * WHERE run_id = ...          │
  │  → Check ADF manually               │  │  → Instant: yes, 29/29 tables, 8 min │
  │  → Check cluster logs                │  │                                      │
  │  → SSH into driver, grep stdout      │  │  “What failed?”                       │
  │  → 30 min to answer “did it work?”   │  │  → WHERE success=false → 2 sec        │
  │                                    │  │                                      │
  │  “Why did it fail?”                  │  │  “Why did it fail?”                    │
  │  → Find the ADF run                 │  │  → BELLE-030: OOM on factorder        │
  │  → Navigate to activity              │  │  → Duration: 47s before crash          │
  │  → Read truncated error              │  │  → Last success: yesterday, 12.3s     │
  │  → Maybe find the real error         │  │  → Regression clear: +280%             │
  │  → 1-2 hours to diagnose             │  │  → 5 min to diagnose + fix             │
  │                                    │  │                                      │
  └──────────────────────────────────┘  └────────────────────────────────────┘
```

---

## 7. Building a Log-Based Alert System

```python
# Minimal alerting notebook — schedule daily after pipeline
from datetime import datetime, timedelta

today = datetime.now().date()
log_df = spark.sql(f"""
    SELECT *
    FROM my_database.bellerophon_log_table
    WHERE DATE(execution_start_time) = '{today}'
""")

failures = log_df.filter("success = false").collect()
if failures:
    msg = f"🚨 Belle Alert: {len(failures)} table(s) failed today\n"
    for row in failures:
        msg += f"  - {row.result_table_name}: {row.error_code} ({row.error_message[:80]})\n"
    # Send via Teams webhook, email, or Slack
    send_alert(msg)
```

---

*Last updated: June 2026*

---

## Dashboard: belle_log_dashboard

Belle ships a ready-made dashboard notebook (`belle_log_dashboard`) that auto-discovers all log tables across your workspace and produces 11 monitoring views:

### Pipeline Health
| View | Purpose |
|------|---------|
| `belle_pipeline_runs` | Per-run summary: tables, successes, failures, duration |
| `belle_daily_success_rate` | Daily success percentage trend per pipeline |
| `belle_error_patterns` | Top failure patterns grouped by error code and table |

### Data Quality
| View | Purpose |
|------|---------|
| `belle_row_anomalies` | Z-score anomaly detection on row counts (7-day baseline) |
| `belle_schema_changes` | Tables whose schema changed between runs |
| `belle_freshness` | Hours since last successful write per table (SLA tracking) |

### FinOps
| View | Purpose |
|------|---------|
| `belle_cost_attribution` | Estimated cost per table (duration × hourly rate) |
| `belle_cost_trend` | Daily cost trend per pipeline |
| `belle_mode_efficiency` | Cost-per-million-rows comparison across write modes |
| `belle_parallelism` | DAG parallelism effectiveness (time saved) |
| `belle_performance_regression` | Z-score regression detection vs 30-day baseline |

### Usage

```python
# Run the dashboard notebook to populate views, then query:
SELECT * FROM belle_cost_attribution ORDER BY estimated_cost_eur DESC
```

The dashboard notebook can be scheduled as a downstream Job task to produce fresh metrics after each pipeline run.
