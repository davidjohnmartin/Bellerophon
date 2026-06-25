# Bellerophon (Belle)

**Config-driven, DAG-aware batch orchestration for Azure Databricks.**

[![Version](https://img.shields.io/badge/version-1.2.19-blue)](#changelog) [![Platform](https://img.shields.io/badge/platform-Azure%20Databricks-orange)](#) [![Runtime](https://img.shields.io/badge/DBR-13.3.x%E2%80%9417.x-green)](#) [![License](https://img.shields.io/badge/license-MIT-lightgrey)](#license)

![Executive Overview](images/page1.png)

---

## Table of Contents

- [What is Belle?](#what-is-belle)
- [When to Use Belle](#when-to-use-belle)
  - [The Strategic Case: Why Centralise](#the-strategic-case-why-centralise-on-belle)
  - ["Why Can't I Just Use Native Tools?"](#why-cant-i-just-use-native-tools)
  - [Why Belle Over Native Tools Alone](#why-belle-over-native-tools-alone)
  - [All Reasons to Adopt Belle](#all-reasons-to-adopt-belle)
- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Load Modes](#load-modes)
- [Documentation Index](#documentation-index)
- [Monitoring Dashboard](#monitoring-dashboard)
- [Tests & Demos](#tests--demos)
- [Deployment](#deployment)
- [Repository Structure](#repository-structure)
- [Contributing](#contributing)
- [Changelog](#changelog)
- [License](#license)

---

## What is Belle?

Bellerophon ("Belle") is an **open-source**, config-driven batch orchestration framework that materialises PySpark DataFrames into Delta Lake tables on Azure Databricks. It replaces ad-hoc write logic with a declarative pipeline model: you define *what* tables you want and *how* they relate — Belle handles the execution order, parallelism, storage lifecycle, encryption, logging, and maintenance.

Belle is open-source because it is a product for all — any team on Azure Databricks can adopt it. There is no proprietary lock-in, no licensing, and no vendor dependency.

Belle is **not** a scheduling tool. It runs *inside* a Databricks notebook (invoked by a scheduled Job or ADF pipeline) and orchestrates the write operations for a set of tables within a single execution run.

---

## When to Use Belle

### The Problem Belle Solves

Native Azure Databricks gives you excellent components — ADF for scheduling, Jobs for orchestration, Delta Lake for storage, Spark for compute. But between "my DataFrame is ready" and "my table is written, validated, logged, and maintained" there is a **gap that every team fills differently**:

- Team A writes bespoke merge logic per table with inconsistent error handling
- Team B daisy-chains `saveAsTable()` calls with no retry, no logging, no ordering
- Team C has 200 lines of boilerplate per notebook (logging, VACUUM, schema checks)
- Team D has one person who knows how the pipeline works — and they're on holiday

Belle eliminates this gap with a single, config-driven framework. You declare your tables. Belle handles everything else.

### The Break-Even Point

For a **single isolated query** writing a small dataset, you don't need Belle — `df.write.saveAsTable()` is fine. The break-even is roughly **2-3 tables with any dependency or incremental requirement**. Beyond that, Belle pays for itself on the first failed run it recovers from automatically.

### The Strategic Case: Why Centralise on Belle

The real value of Belle is not any single feature — it's that **every pipeline, every team, every solution adopts the same write pattern**. This creates compounding returns that no collection of bespoke scripts can match.

#### One Log Schema → One Dashboard → Total Visibility

Every Belle pipeline writes to the same `bellerophon_log_table` schema. This means:

- **A single monitoring dashboard covers every pipeline in the organisation** — no per-team dashboards to build, maintain, or interpret
- **Cross-team incidents are instantly visible** — the SRE on morning duty sees all pipelines in one RAG matrix, not "check Team A's notebook, then Team B's alerts, then Team C's Slack channel"
- **FinOps works at scale** — because every table write logs its duration, you get per-table cost attribution across the entire estate without per-team instrumentation
- **Anomaly detection spans everything** — row count spikes, schema drift, and performance regression are detected uniformly, not wherever someone remembered to add monitoring
- **ai_forecast projections** cover all pipelines — 30-day cost and growth projections require no per-team setup

Without centralised logging, you get: Team A has a Slack alert, Team B checks a spreadsheet, Team C has "Dave knows if it's broken." With Belle, you get: one dashboard, one alert framework, one source of truth.

#### Consistent Operations → Reduced Bus-Factor

When every team writes their own retry logic, maintenance scripts, and error handling:
- Knowledge is siloed — only the author understands the failure modes
- On-call is impossible — the responder must understand N different pipeline designs
- New starters take weeks to understand each team's bespoke patterns
- Bugs in shared concerns (retry, VACUUM, logging) are fixed N times independently

When every team uses Belle:
- **Any engineer can read any pipeline** — the pattern is identical (config dict + orchestrator call)
- **On-call works** — the responder knows exactly where to look (log table, error codes, dashboard)
- **New starters are productive in hours** — learn Belle once, understand every pipeline
- **Fixes propagate everywhere** — update `bellerophon_core`, every pipeline benefits on next run

#### Standardised Incremental Patterns → Fewer Production Incidents

Complex write patterns (merge, partition overwrite, rolling window refresh) are where most production bugs live. Teams that code these ad-hoc will:
- Forget edge cases (first-ever run, empty partition, schema evolution mid-merge)
- Handle retries differently (or not at all)
- Have inconsistent behaviour when the same pattern appears in 5 pipelines

Belle encodes these patterns **once, correctly, with edge cases handled**. A team adopting `load_mode: "merge"` gets the same battle-tested merge logic that has processed millions of rows across other pipelines. They inherit:
- Correct handling of schema evolution during merge
- Retry semantics on lock contention
- Automatic schema capture pre/post merge
- Row count validation
- Structured error codes for every failure mode

#### Global Observability Enables Global Strategy

With all pipelines on Belle, leadership and platform teams gain:

| Capability | What It Enables |
| --- | --- |
| **Cross-pipeline SLA tracking** | "Are ALL pipelines delivering data before 07:00?" — answered in one table |
| **Estate-wide cost attribution** | "Which domain costs the most compute?" — answered per-table, per-domain, per-layer |
| **Capacity planning** | "At current growth rates, when do we need more compute?" — ai_forecast across all pipelines |
| **Quality governance** | "Which tables have the most schema drift or row count anomalies?" — z-score detection across everything |
| **Audit compliance** | "Who wrote to this table, when, from which notebook, on which cluster?" — every write logged |
| **Incident correlation** | "Did the upstream failure cascade to downstream pipelines?" — correlation via run_id and timing |
| **Platform health reporting** | Monthly executive summary from one data source, not N team surveys |

#### The Network Effect

Each pipeline that adopts Belle makes the dashboard **more valuable** (more coverage), makes on-call **easier** (fewer bespoke systems to learn), and makes FinOps **more accurate** (more tables attributed). This is a positive network effect — the Nth adopter benefits from the N-1 pipelines already on the platform.

#### What This Looks Like in Practice

```
Without Belle (typical):
  Team A: custom retry + Slack alerts + manual VACUUM + no logging
  Team B: no retry + email alerts + OPTIMIZE script + CSV audit log
  Team C: partial retry + no alerts + no maintenance + Spark UI only
  Team D: ???

  → 4 different monitoring approaches
  → 4 different error taxonomies
  → 4 different on-call runbooks
  → No cross-team visibility
  → No estate-wide cost attribution
  → No capacity forecasting

With Belle (standardised):
  Team A: belle.Orchestrator(config).run()
  Team B: belle.Orchestrator(config).run()
  Team C: belle.Orchestrator(config).run()
  Team D: belle.Orchestrator(config).run()

  → 1 monitoring dashboard (13 pages, auto-discovers all pipelines)
  → 1 error code taxonomy (BELLE-000 through BELLE-0XX)
  → 1 on-call runbook (docs/08_Operations_Runbook.md)
  → Full cross-team visibility
  → Per-table cost attribution
  → 30-day ai_forecast projections
```

---

### "Why Can't I Just Use Native Tools?"

You can. And you will — Belle *uses* native tools (Delta Lake, Spark, ADF, Jobs). The question is: **what happens in the space between your DataFrame and your Delta table?**

Here's what "just using native tools" actually looks like at scale:

#### The Native-Only Pipeline (What You'll Build)

```python
# "Simple" pipeline — 5 tables, no framework
# Written by Engineer A in March. Engineer A leaves in July.

df_customers.write.format("delta").mode("overwrite").saveAsTable("semantic.dim_customer")
df_orders.write.format("delta").mode("overwrite").saveAsTable("semantic.fact_order")

# Wait — fact_order depends on dim_customer. Better add ordering:
df_customers.write.format("delta").mode("overwrite").saveAsTable("semantic.dim_customer")
# Now fact_order can run...
df_orders.write.format("delta").mode("overwrite").saveAsTable("semantic.fact_order")

# But what if dim_customer fails? fact_order runs against stale data.
# Add try/except:
try:
    df_customers.write.format("delta").mode("overwrite").saveAsTable(...)
except Exception as e:
    # What now? Log to... where? Retry? How many times? Abort everything?
    print(f"Failed: {e}")  # This is your "logging"

# 6 months later: 25 tables, 3 engineers, merge logic everywhere,
# no one remembers why table 14 has that weird retry loop,
# VACUUM hasn't run in 4 months, and the morning check is
# "ask Dave if he's seen any errors in his notebook output"
```

Now multiply this by 10 pipelines across 4 teams. Each team solves these problems differently (or doesn't). That's your estate without a framework.

#### What You'll End Up Building Anyway

Every team that starts with "just native tools" eventually builds:

| You'll Need | What You'll Build | Time to Build | Belle Gives You |
| --- | --- | --- | --- |
| Execution ordering | Manual sequencing or nested try/except | 1-2 days | DAG from config (0 code) |
| Retry logic | Custom try/except with sleep() | 1 day per pattern | Per-table exponential backoff + OOM detection |
| Logging | print() or custom log table | 2-3 days (then never maintained) | Structured Delta log, 30+ fields, automatic |
| Monitoring | Manual Slack/email alerts or nothing | 1-2 weeks (if at all) | 13-page dashboard, auto-discovers all pipelines |
| VACUUM/OPTIMIZE | Manual scripts, often forgotten | 1 day (then drifts) | Automatic, scheduled, per-table |
| Schema tracking | Nothing (until something breaks) | 1-2 days | MD5 hash per write, drift alerts |
| Merge logic | Copy-paste from Stack Overflow | 2-3 days (per table with edge cases) | `"load_mode": "merge"` (1 line) |
| Partition refresh | Custom replaceWhere per table | 1 day per table | `"load_mode": "partition"` (1 line) |
| Rolling window | Date arithmetic + replaceWhere | 1-2 days | `"load_mode": "refresh_n_days-30"` (1 line) |
| Cost tracking | Nothing (cluster-level billing only) | Never built | Per-table duration → cost attribution |
| Memory management | OOM → increase cluster → OOM again | Ongoing firefighting | Automatic persist/unpersist lifecycle |
| Dev/prod isolation | Separate notebooks or IF statements | 1 day + ongoing maintenance | Automatic `_dev` routing (0 code) |

**Total: 3-6 weeks of engineering time** — per team — to get a fraction of what Belle provides on day one. And that custom solution will be:
- Undocumented (the author knows how it works, no one else does)
- Unmaintained (logging breaks, VACUUM drifts, retry logic goes stale)
- Inconsistent (each pipeline does it slightly differently)
- Unmonitored (no cross-pipeline visibility)

Belle is **one notebook** (`%run ./bellerophon_core`) and **one config dict**. That's the adoption cost.

#### Addressing Specific Objections

**"I'll just write MERGE INTO SQL directly"**

You can. But for every table you'll need to:
- Handle the first-ever run (table doesn't exist yet → MERGE fails)
- Handle schema evolution mid-merge (new columns appear upstream)
- Handle retry when merge hits lock contention
- Log what happened (rows matched, inserted, updated)
- Track schema before and after
- Handle OOM during large merges

Belle's `"load_mode": "merge"` handles all of these. You write: `"merge_keys": ["id"]`. Done.

**"ADF handles retries"**

ADF retries at the **activity level** — meaning it re-runs your entire notebook from scratch. If table 24 of 30 failed, ADF re-runs all 30. Belle retries at the **table level** — only the failed table is retried, with exponential backoff, while successful tables are already done.

**"Databricks Jobs has a task DAG"**

Jobs DAGs orchestrate between **notebooks** (inter-notebook). Belle orchestrates between **tables within a notebook** (intra-notebook). A pipeline with 30 tables doesn't need 30 notebooks — it needs one notebook with a config dict. Belle is the layer Jobs doesn't provide.

**"I can just add a try/except"**

You can. But:
- How many retries? With what backoff? 
- Do you reduce parallelism on OOM?
- Do you log the failure (structured, not print())?
- Does the error code taxonomy match other pipelines?
- Can the on-call engineer read your error the same way they read Team B's?

Belle standardises error handling so `BELLE-007` means the same thing regardless of which pipeline, which team, or which on-call engineer is looking at it.

**"Spark UI shows me what happened"**

Spark UI is:
- Ephemeral (gone when the cluster terminates)
- Cluster-scoped (can't see other pipelines)
- Not queryable (can't trend, alert, or forecast)
- Not shareable (can't build a morning-check dashboard for the team)

Belle's log table is:
- Persistent (Delta Lake, retained for months)
- Cross-pipeline (all pipelines in one table)
- Fully queryable (SQL, dashboard, Genie AI)
- Auto-discovered (the dashboard finds all Belle log tables across all databases)

**"Delta Lake handles schema enforcement"**

Delta enforces schema on write (rejects incompatible writes). Belle goes further:
- Captures a hash of the schema **per write** (so you know WHEN it changed)
- Detects **drift** (schema changed vs last successful write)
- Logs it to a searchable table (so you can query "which tables had schema changes this week?")
- The dashboard flags it automatically (Data Quality page, z-score detection)

Delta tells you "this write failed because schema doesn't match." Belle tells you "3 tables changed schema this week, here's what changed, here's when, and here's who wrote them."

**"We don't need monitoring — we'll know if something breaks"**

Will you? When:
- A table silently writes 0 rows (success = true, row_count = 0)?
- A table's duration creeps from 2 minutes to 20 minutes over 3 months?
- A table that normally gets 50K rows suddenly gets 5M (upstream explosion)?
- A merge that was idempotent starts duplicating rows due to a key change?
- Your VACUUM hasn't run in 6 months and storage costs are climbing?

You won't know. Not until someone calls you. Belle's dashboard shows all of this proactively — anomaly detection, cost regression, SLA breach tracking, and ai_forecast projections.

**"It's another dependency"**

Belle is:
- One notebook (`%run ./bellerophon_core`)
- Zero packages to install
- Zero cluster configuration
- Zero infrastructure to manage
- Works on any DBR 13.3+
- MIT licensed, no vendor lock-in

The alternative "no dependency" approach requires you to build and maintain: retry logic + logging + monitoring + maintenance + schema tracking + memory management + cost attribution. That's not "no dependencies" — that's **undocumented dependencies on bespoke code that only you understand**.

**"Our pipeline is simple — we only have 5 tables"**

Today. In 6 months:
- Stakeholders add 3 more tables
- One table needs incremental logic (merge)
- Another needs encryption (PII)
- A third starts timing out (needs retry)
- Someone asks "how long has this been broken?"
- Finance asks "what does this pipeline cost?"

Starting with Belle costs you **one `%run` and one config dict**. Retrofitting Belle into a 25-table pipeline that was built ad-hoc costs weeks of refactoring.

The cheapest time to adopt Belle is **before you need it**.

---

### Why Belle Over Native Tools Alone

| Concern | Native ADF / Jobs / Delta | What Belle Adds |
| --- | --- | --- |
| **Scheduling** | ADF triggers, Jobs cron, event-driven | — (Belle uses native scheduling) |
| **Cluster management** | Job clusters, serverless, pools | — (Belle runs on whatever compute is attached) |
| **Inter-notebook ordering** | Jobs multi-task DAG | — (Belle uses native task graphs) |
| **Intra-notebook table ordering** | Manual / ad-hoc / hope | Automatic DAG from declared dependencies |
| **Parallel table writes** | Not built-in (sequential by default) | ThreadPoolExecutor with configurable workers per stage |
| **Per-table retry** | Retry whole notebook or nothing | Per-table exponential backoff with OOM detection |
| **Incremental writes** | Manual coding per table (merge logic, partition detection, window calculations) | Config-driven: `merge`, `refresh_n_days-30`, `partition`, `full_if_not_exists` |
| **Write validation** | Basic Delta schema enforcement | Row count validation, schema drift detection (MD5 hash per write) |
| **Execution logging** | Spark UI (ephemeral, cluster-scoped) | Persistent Delta log table: duration, rows, errors, schema, correlation IDs |
| **Cost visibility** | Billing console (cluster-level only) | Per-table duration tracking → table-level cost attribution |
| **Maintenance** | Manual VACUUM/OPTIMIZE scripts | Automatic scheduled VACUUM + OPTIMIZE + compaction post-write |
| **Dev/prod isolation** | Separate workspaces or manual naming | Automatic `_dev` database routing — zero code changes between environments |
| **Encryption** | Workspace-level or manual | Per-column AES-256 encryption with key rotation, transparent to consumers |
| **Memory management** | Manual persist/unpersist (or forget and OOM) | Automatic lifecycle: cache only during dependency window, then release |
| **Observability** | Build your own dashboards | 13-page operational dashboard + Genie AI space — zero build effort |

### All Reasons to Adopt Belle

#### 1. Complex Pipeline Design Made Simple

| Pattern | Without Belle | With Belle |
| --- | --- | --- |
| **Incremental merge (upsert)** | Write MERGE INTO SQL per table, handle schema evolution, manage merge keys | `"load_mode": "merge", "merge_keys": ["id"]` |
| **Rolling window refresh** | Calculate date range, build replaceWhere, handle edge cases | `"load_mode": "refresh_n_days-30"` |
| **Partition overwrite** | Manual replaceWhere with partition detection | `"load_mode": "partition"` with partition config |
| **Conditional full load** | Check if table exists, branch logic | `"load_mode": "full_if_not_exists"` |
| **Mixed-mode pipeline** | Different write patterns across 20+ tables | One config dict — each table declares its own mode |
| **Multi-table DAG** | Manual ordering, sequential execution, hope nothing fails mid-way | Declare dependencies → automatic parallel stages |
| **Cross-table retry** | If table 15 of 30 fails, re-run everything? | Only failed tables retry (with backoff). Succeeded tables are done. |

#### 2. Resilience & Recovery

- **Per-table retry with exponential backoff** — transient failures (network, throttle, lock contention) resolved automatically without human intervention
- **OOM recovery** — detects OutOfMemoryError, reduces thread pool parallelism, retries the failed table with more headroom
- **Crash containment** — a failure in one table does not abort the entire pipeline; other independent tables continue
- **Correlation IDs** — every run gets a `run_id`, every ADF trigger passes `parent_run_id` — full cross-system traceability
- **Deterministic restart** — re-run the notebook and only failed/missing tables are reprocessed (when using incremental modes)

#### 3. Operational Visibility (Zero Build Effort)

- **Structured log table** — one row per table write: duration, row count, error code, error message, schema hash, cluster ID, user, notebook path, run mode, load mode
- **13-page monitoring dashboard** — Executive Overview, Daily Ops, SLAs, Data Quality, Issues, Performance, Growth, FinOps, MoM Trends, Tags, User Activity, Actions, Deep-Dive
- **Per-table cost attribution** — something no native billing console provides (they show cluster cost, not which TABLE consumed the compute)
- **ai_forecast projections** — 30-day cost and growth forecasts built into the dashboard
- **Schema drift alerts** — know instantly when an upstream schema change affects your tables
- **Row count anomaly detection** — z-score based spike/drop flagging on every table

#### 4. Production/Development Safety

- **Automatic mode detection** — Job context → production databases; Interactive → `_dev` databases
- **Zero code changes** between environments — same notebook, same config, different targets
- **No accidental production writes** — interactive users physically cannot write to production tables
- **Test mode isolation** — run the full pipeline with table suffixes for safe parallel testing

#### 5. Maintenance & Hygiene (Automated)

- **Scheduled VACUUM** — configurable retention per table, runs post-write on schedule
- **Scheduled OPTIMIZE** — Z-ORDER support, file compaction, runs when due
- **Log retention** — auto-cleanup of old log entries (configurable days)
- **No human intervention** — maintenance runs silently as part of normal pipeline execution

#### 6. Security & Compliance

- **AES-256 column-level encryption** — encrypt PII/financial columns at write time
- **Key rotation** — rotate encryption keys without rewriting all data
- **Transparent decryption** — downstream views decrypt inline; consumers see plaintext
- **Audit trail** — every write logged with user, timestamp, cluster, notebook path

#### 7. Developer Experience

- **Config-driven** — add a table by adding 5 lines to a dict, not writing a new module
- **Pre-flight validation** — catches config errors (missing merge keys, circular deps, invalid modes) before execution starts
- **Visual DAG** — ASCII/SVG execution plan displayed before any writes happen
- **Progress tracking** — real-time progress bar with ETA during execution
- **One framework, all teams** — consistent patterns across every pipeline, every team, every solution

#### 8. FinOps & Cost Control

- **Per-table cost attribution** — duration × DBU rate = table-level cost estimate
- **Optimization candidates** — dashboard flags REGRESSING, UNSTABLE, HIGH COST, INEFFICIENT tables
- **Cost forecasting** — ai_forecast 30-day projections for budget planning
- **Load mode efficiency** — compare throughput across modes to identify waste
- **Cost regression detection** — automatic alerting when table costs trend upward

#### 9. Scale & Governance

- **Multi-pipeline discovery** — dashboard auto-discovers all Belle log tables across all databases/mounts
- **Tag-based grouping** — organise tables by domain, layer, source for governance reporting
- **Cross-system correlation** — `parent_run_id` links Belle runs to ADF/external orchestrators
- **Unity Catalog + Hive** — dual-namespace support (auto-detected from database format)
- **No vendor lock-in** — MIT licensed, no proprietary dependencies, fork freely

### Where Belle Sits in the Stack

```
┌─────────────────────────────────────────────────────┐
│  SCHEDULING LAYER (external — not Belle)            │
│  Azure Data Factory │ Databricks Jobs │ Cron        │
└───────────────────────────┬─────────────────────────┘
                            │ triggers
                            ▼
┌─────────────────────────────────────────────────────┐
│  YOUR NOTEBOOK                                      │
│                                                     │
│  ┌───────────────────────────────────────────────┐  │
│  │  YOUR ETL LOGIC (PySpark transformations)     │  │
│  │  • Read sources                               │  │
│  │  • Transform DataFrames                       │  │
│  │  • Business logic                             │  │
│  └───────────────────────────┬───────────────────┘  │
│                              │ DataFrames ready     │
│                              ▼                      │
│  ┌───────────────────────────────────────────────┐  │
│  │  BELLE (write orchestration layer)            │  │
│  │  • DAG resolution + parallel execution        │  │
│  │  • Load mode routing (merge/full/partition)   │  │
│  │  • Retry + OOM recovery                       │  │
│  │  • Encryption + schema capture                │  │
│  │  • Logging + maintenance                      │  │
│  └───────────────────────────┬───────────────────┘  │
│                              │                      │
└──────────────────────────────┼──────────────────────┘
                               ▼
                    ┌────────────────────┐
                    │  DELTA LAKE TABLES │
                    └────────────────────┘
```

Belle does **not** replace ADF or Jobs. It fills the space *between* your transformation logic and the Delta Lake write — the part that every team currently builds ad-hoc, inconsistently, and without observability.

### Belle is NOT the right choice for:

- **Streaming/real-time** — use Lakeflow Spark Declarative Pipelines (structured streaming)
- **SQL-only transformations** — use dbt or Databricks SQL Warehouses
- **Single-table, no-dependency writes** — use `df.write.saveAsTable()` directly
- **Cross-notebook orchestration** — use Databricks Jobs multi-task DAGs (Belle orchestrates *within* a notebook)

---

## Key Features

| Feature | Description |
| --- | --- |
| **DAG Orchestration** | Builds a dependency graph from your config, sorts into parallel stages, and executes via ThreadPoolExecutor. Visual ASCII/SVG DAG output before execution. |
| **5 Load Modes** | `full` (overwrite), `merge` (upsert via merge keys), `insert` (append-only), `fast` (optimised append), `partition` (partition-level overwrite) |
| **Column Encryption** | AES-256 column-level encryption. Encrypt on write, decrypt on read. Key rotation support. Per-table or per-column granularity. |
| **Schema Drift Detection** | MD5 hash of schema captured per write. Changes logged and flagged automatically. |
| **Retry Logic** | Exponential backoff (configurable base/max). Automatic retry on transient Spark errors (OOM, shuffle failures). |
| **Maintenance Scheduler** | Automated VACUUM (configurable retention) + OPTIMIZE (Z-ORDER support) + file compaction. Runs post-write on schedule. |
| **Structured Logging** | Every table write produces one log row: duration, row count, error code, schema snapshot, cluster ID, user, run correlation ID. |
| **Memory Management** | Automatic persist/unpersist lifecycle. DataFrames cached only for their dependency window, then released. |
| **Pre-flight Validation** | Config validator catches errors (missing merge keys, invalid load modes, circular dependencies) before execution begins. |
| **Progress Tracking** | Visual progress bar with ETA during execution. Stage-by-stage completion reporting. |
| **Production Auto-detect** | Automatically detects Job vs interactive context. Adjusts logging, paths, and behaviour accordingly. |
| **Monitoring Dashboard** | 13-page operational dashboard with ai_forecast projections, RAG status, SLA tracking, FinOps, and anomaly detection. |
| **Genie AI Space** | Natural-language query interface over Belle log data via Databricks Genie. |

---

## Quick Start

### 1. Copy `bellerophon_core` into your workspace

Place the notebook alongside your pipeline notebook (or in a shared location):
```
/Users/you@company.com/my_project/
├── bellerophon_core        ← Copy from core/
└── my_pipeline             ← Your notebook
```

### 2. Load Belle

```python
%run ./bellerophon_core
```

### 3. Build your DataFrames (your ETL logic)

```python
df_customers = (
    spark.table("raw.customers")
    .filter("active = true")
    .withColumn("updated_at", F.current_timestamp())
)

df_orders = (
    spark.table("raw.orders")
    .join(df_customers.select("customer_id"), "customer_id")
)
```

### 4. Register outputs

```python
belle.OutputRegistry.set_output("dim_customer", df_customers)
belle.OutputRegistry.set_output("fact_order", df_orders)
```

### 5. Define table configuration

```python
TABLES_CONFIG = {
    "dim_customer": {
        "database": "my_semantic",
        "load_mode": "merge",
        "merge_keys": ["customer_id"],
        "dependencies": [],
        "tags": {"domain": "CRM", "layer": "semantic"},
    },
    "fact_order": {
        "database": "my_semantic",
        "load_mode": "merge",
        "merge_keys": ["order_id"],
        "dependencies": ["dim_customer"],  # Waits for dim_customer
        "tags": {"domain": "Sales", "layer": "semantic"},
    },
}
```

### 6. Orchestrate

```python
belle.Orchestrator(TABLES_CONFIG).run()
```

Belle will:
1. Validate your config (pre-flight checks)
2. Build the DAG and display the execution plan
3. Execute `dim_customer` first (Stage 1)
4. Execute `fact_order` after (Stage 2, depends on dim_customer)
5. Log every write to `bellerophon_log_table`
6. Run scheduled maintenance (VACUUM/OPTIMIZE) if due

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  YOUR CONSUMING NOTEBOOK                                            │
│                                                                     │
│  1. %run ./bellerophon_core          ← Load framework               │
│  2. Build DataFrames (PySpark)       ← Your ETL logic               │
│  3. Register outputs                 ← OutputRegistry.set_output()  │
│  4. Define TABLES_CONFIG             ← Declarative table manifest   │
│  5. Orchestrate                      ← belle.Orchestrator().run()   │
└───────────────────────────────────────┬─────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BELLEROPHON CORE                                                   │
│                                                                     │
│  ┌────────────┐  ┌────────────┐  ┌──────────────┐  ┌────────────┐   │
│  │  Config    │  │ Pre-flight │  │ DAG          │  │Maintenance │   │
│  │  Validator │  │ Validator  │  │ Visualizer   │  │ Scheduler  │   │
│  └────────────┘  └────────────┘  └──────────────┘  └────────────┘   │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  ORCHESTRATOR                                                │   │
│  │  • Build DAG from dependencies                               │   │
│  │  • Sort into parallel stages                                 │   │
│  │  • ThreadPoolExecutor per stage                              │   │
│  │  • Persist/unpersist lifecycle                               │   │
│  │  • Retry handler (exponential backoff)                       │   │
│  │  • Progress tracker (visual ETA)                             │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                         │                                           │
│                         ▼                                           │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  MATERIALISE DATAFRAME                                       │   │
│  │  • Load mode router (full/merge/insert/fast/partition)       │   │
│  │  • Schema capture + drift detection                          │   │
│  │  • Encryption (AES-256, per-column)                          │   │
│  │  • Delta write (saveAsTable / merge / insertInto)            │   │
│  │  • OOM recovery wrapper (resilient_materialise_table)        │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                         │                                           │
│                         ▼                                           │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  LOGGER                                                      │   │
│  │  • One row per table write (success or failure)              │   │
│  │  • Duration, row count, schema hash, error code/message      │   │
│  │  • Correlation: run_id + parent_run_id (ADF linkage)         │   │
│  │  • Auto-cleanup (configurable retention)                     │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                         │
                         ▼
              ┌────────────────────┐
              │  DELTA LAKE TABLE  │  (+ bellerophon_log_table)
              └────────────────────┘
```

---

## Load Modes

| Mode | Behaviour | Use Case |
| --- | --- | --- |
| `full` | DROP + recreate (or overwrite) | Small dimensions, lookup tables |
| `merge` | MERGE INTO on `merge_keys` (upsert) | SCD1 dimensions, fact tables with updates |
| `insert` | INSERT INTO (append-only) | Immutable event streams |
| `fast` | Optimised append with partition pruning | High-volume append tables |
| `partition` | Partition-level overwrite (replaceWhere) | Large partitioned facts (reprocess one partition) |

See [05_User_Guide_Standard_Materialisation.md](docs/05_User_Guide_Standard_Materialisation.md) and [06_User_Guide_Fast_and_Partition_Modes.md](docs/06_User_Guide_Fast_and_Partition_Modes.md) for full details.

---

## Documentation Index

### Getting Started

| # | Document | Description | Audience |
| --- | --- | --- | --- |
| 01 | [Full Technical README](docs/01_README.md) | Complete feature overview, architecture, and reference | Everyone |
| 02 | [New Starter Guide](docs/02_New_Starter_Guide.md) | Step-by-step onboarding tutorial | New users |

### Architecture & Configuration

| # | Document | Description | Audience |
| --- | --- | --- | --- |
| 03 | [Architecture & Design](docs/03_Architecture_and_Design.md) | Internal design, class structure, execution model | Engineers |
| 04 | [Configuration Reference](docs/04_Configuration_Reference.md) | Complete TABLES_CONFIG schema with all options | Everyone |
| A1 | [Appendix: Config Settings](docs/A1_Appendix_Config_Settings.md) | Global settings, environment variables, overrides | Platform team |

### User Guides

| # | Document | Description | Audience |
| --- | --- | --- | --- |
| 05 | [Standard Materialisation](docs/05_User_Guide_Standard_Materialisation.md) | `full`, `merge`, `insert` modes with examples | Data engineers |
| 06 | [Fast & Partition Modes](docs/06_User_Guide_Fast_and_Partition_Modes.md) | High-volume append + partition overwrite patterns | Data engineers |
| 07 | [Encryption Guide](docs/07_User_Guide_Encryption.md) | AES-256 column encryption, key rotation, read patterns | Data engineers, security |

### Operations & Production

| # | Document | Description | Audience |
| --- | --- | --- | --- |
| 08 | [Operations Runbook](docs/08_Operations_Runbook.md) | Deployment, monitoring, maintenance, error recovery | SRE, platform ops |
| 09 | [Troubleshooting Guide](docs/09_Troubleshooting_Guide.md) | Common errors, diagnostics, resolution steps | On-call engineers |
| 12 | [Migration & Upgrade Guide](docs/12_Migration_and_Upgrade_Guide.md) | Version upgrades, breaking changes, migration scripts | Platform team |

### Testing & Quality

| # | Document | Description | Audience |
| --- | --- | --- | --- |
| 10 | [Testing Guide](docs/10_Testing_Guide.md) | How to write and run tests for Belle pipelines | Data engineers |
| 15 | [Feature Testing Checklist](docs/15_Feature_Testing_Checklist.md) | Pre-release verification checklist | Maintainers |
| 16 | [Validator Guide](docs/16_Validator_Guide.md) | Pre-flight validation rules and custom validators | Data engineers |

### Reference

| # | Document | Description | Audience |
| --- | --- | --- | --- |
| 11 | [API Reference](docs/11_API_Reference.md) | All classes, methods, and function signatures | Engineers |
| 14 | [Log Table Deep Dive](docs/14_Log_Deep_Dive.md) | Log schema, query patterns, dashboard integration | Everyone |

### Community

| # | Document | Description | Audience |
| --- | --- | --- | --- |
| 13 | [Contributing Guide](docs/13_Contributing_Guide.md) | How to contribute, PR process, coding standards | Contributors |
| 17 | [Dashboard Guide](docs/17_Dashboard_Guide.md) | All 13 dashboard pages, widgets, data refresh | Everyone |

---

## Monitoring Dashboard

Belle automatically logs every table write. The companion **Belle Pipeline Monitoring** dashboard (located in `dashboard/`) transforms this raw log data into 13 purpose-built operational pages.

![Daily Ops — morning health check](images/page2.png)

### Dashboard Pages

| # | Page | Key Widgets |
| --- | --- | --- |
| 1 | Executive Overview | KPI counters, Gantt timeline, RAG status table |
| 2 | Daily Ops | RAG matrix, Go/No-Go table, freshness, anomalies |
| 3 | Delivery SLAs | P50/P90 completion, SLA pivot with RAG formatting |
| 4 | Data Quality | Row count anomalies (z-score), schema drift tracking |
| 5 | Issues & Incidents | Error code analysis, failure rate trends, blast radius |
| 6 | Performance | P95 duration bars, instability scatter, cost per table |
| 7 | Growth & Capacity | Volume trends, 30-day ai_forecast row projection |
| 8 | FinOps & Optimization | Cost attribution, 30-day ai_forecast cost projection |
| 9 | MoM Trends | Month-over-month RAG comparison |
| 10 | Tags & Domains | Business domain/layer breakdown |
| 11 | User Activity | Production vs interactive, compute hours by user |
| 12 | Actions & Next Steps | Prioritised engineering recommendations |
| 13 | Pipeline Deep-Dive | Exploratory drill-down (Gantt + scatter + detail) |

### Dashboard Setup

1. Copy `dashboard/belle_log_dashboard` notebook alongside your pipelines
2. Run all 5 cells — auto-discovers Belle log tables, unions, and produces 24 summary tables in `belle_dashboard` database
3. Open `dashboard/Belle Pipeline Monitoring.lvdash.json` — it queries those tables directly
4. Optional: schedule the notebook as a daily Job for automatic refresh

See [17_Dashboard_Guide.md](docs/17_Dashboard_Guide.md) for full documentation.

---

## Tests & Demos

### Test Suite (`tests/`)

| Notebook | What it validates |
| --- | --- |
| `test_backwards_compatibility` | Config from older versions still works |
| `test_encryption_roundtrip` | Encrypt → write → read → decrypt integrity |
| `test_fast_mode` | Fast-mode append correctness and idempotency |
| `test_maintenance_scheduler` | VACUUM/OPTIMIZE scheduling and execution |
| `test_partition_mode` | Partition-level overwrite with replaceWhere |

Run the full suite before any release — see [15_Feature_Testing_Checklist.md](docs/15_Feature_Testing_Checklist.md).

### Demos (`demos/`)

| Notebook | What it demonstrates |
| --- | --- |
| `01_Demo_All_Write_Modes` | End-to-end example of all 5 load modes with sample data |

---

## Deployment

### Option A: Databricks Jobs (Recommended)

1. Copy `core/bellerophon_core` into your project folder
2. Create your pipeline notebook with `%run ./bellerophon_core`
3. Create a Databricks Job pointing to your notebook
4. Schedule as required (cron expression)

Belle auto-detects production mode via the job context — no code changes needed.

### Option B: Azure Data Factory

Use a Databricks **Notebook** activity in ADF:
- **Notebook path:** `/Users/.../my_pipeline_notebook`
- Belle's `%run ./bellerophon_core` works normally inside the notebook
- Pass `parent_run_id` from ADF for cross-system correlation

### Option C: Interactive Development

Run the notebook manually on any cluster. Belle auto-detects interactive mode and adjusts:
- Database routing (uses `_dev` suffix databases)
- Logging level (verbose)
- Maintenance (skipped in interactive)

See [08_Operations_Runbook.md](docs/08_Operations_Runbook.md) for full deployment details.

---

## Repository Structure

```
bellerophon/
├── README.md                       ← You are here
├── CHANGELOG.md                    ← Version history
├── LICENSE                         ← MIT
├── .gitignore
├── core/
│   └── bellerophon_core            ← The framework (Databricks notebook)
├── dashboard/
│   ├── belle_log_dashboard         ← Data pipeline for dashboard tables
│   ├── Belle Pipeline Monitoring   ← Operational dashboard (.lvdash.json)
│   └── belle_genie_assistant.md    ← Genie AI space instructions
├── docs/                           ← 18 documentation files (see index above)
├── images/                         ← Dashboard screenshots (anonymised)
│   └── page1.png — page13.png
├── tests/                          ← 5 test notebooks
└── demos/                          ← Example notebooks
```

---

## Contributing

Contributions welcome. See [13_Contributing_Guide.md](docs/13_Contributing_Guide.md) for:
- Coding standards and conventions
- PR process and review requirements
- How to add new load modes or features
- Testing requirements before merge

---

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for full version history.

**Recent highlights (v1.2.19):**
- 13-page operational monitoring dashboard with ai_forecast projections
- Hash-based anonymisation mode for documentation screenshots
- Repository restructure: `core/` + `dashboard/` subfolders
- 18-file documentation suite

---

## License

MIT License — see [LICENSE](LICENSE) for full text.

Copyright (c) 2025-2026 AXA Partners — David Martin
