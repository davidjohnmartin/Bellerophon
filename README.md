# Bellerophon (Belle)

**Config-driven, DAG-aware batch orchestration for Azure Databricks.**

[![Version](https://img.shields.io/badge/version-1.2.19-blue)](#changelog) [![Platform](https://img.shields.io/badge/platform-Azure%20Databricks-orange)](#) [![Runtime](https://img.shields.io/badge/DBR-13.3.x%E2%80%9417.x-green)](#)

![Executive Overview](images/page1.png)

---

## What is Belle?

Bellerophon ("Belle") is an open-source, config-driven batch orchestration framework that materialises PySpark DataFrames into Delta Lake tables. You define *what* tables you want and *how* they relate — Belle handles execution order, parallelism, storage lifecycle, encryption, logging, and maintenance.

Belle is **not** a scheduling tool. It runs *inside* a Databricks notebook (invoked by a Job or ADF pipeline) and orchestrates write operations for a set of tables within a single execution run.

---

## Quick Start

```python
# 1. Load Belle into session
%run ./bellerophon_core

# 2. Register your DataFrames
belle.OutputRegistry.set_output("my_table", my_dataframe)

# 3. Define table config
TABLES_CONFIG = {
    "my_table": {
        "database": "my_database",
        "load_mode": "merge",
        "merge_keys": ["id"],
        "dependencies": [],
    }
}

# 4. Orchestrate
belle.Orchestrator(TABLES_CONFIG).run()
```

---

## Repository Structure

```
bellerophon/
├── README.md                       ← You are here
├── CHANGELOG.md                    ← Version history
├── LICENSE
├── .gitignore
├── core/
│   └── bellerophon_core            ← The framework (Databricks notebook)
├── dashboard/
│   ├── belle_log_dashboard         ← Data pipeline for dashboard tables
│   ├── Belle Pipeline Monitoring   ← Operational dashboard (.lvdash.json)
│   └── belle_genie_assistant.md     ← Genie AI space instructions
├── docs/
│   ├── 01_README.md              Full technical README
│   ├── 02_New_Starter_Guide.md   Onboarding guide
│   ├── 03_Architecture_and_Design.md
│   ├── 04_Configuration_Reference.md
│   ├── 05_User_Guide_Standard_Materialisation.md
│   ├── 06_User_Guide_Fast_and_Partition_Modes.md
│   ├── 07_User_Guide_Encryption.md
│   ├── 08_Operations_Runbook.md
│   ├── 09_Troubleshooting_Guide.md
│   ├── 10_Testing_Guide.md
│   ├── 11_API_Reference.md
│   ├── 12_Migration_and_Upgrade_Guide.md
│   ├── 13_Contributing_Guide.md
│   ├── 14_Log_Deep_Dive.md
│   ├── 15_Feature_Testing_Checklist.md
│   ├── 16_Validator_Guide.md
│   ├── 17_Dashboard_Guide.md
│   └── A1_Appendix_Config_Settings.md
├── images/                     ← Dashboard screenshots (anonymised)
├── tests/                      ← Test suite
└── demos/                      ← Example notebooks
```

---

## Key Features

| Feature | Description |
| --- | --- |
| DAG orchestration | Dependency-ordered parallel execution via ThreadPoolExecutor |
| Load modes | `full`, `merge`, `insert`, `fast`, `partition` |
| Encryption | AES-256 column-level encryption with key rotation |
| Schema drift | Automatic detection + structured logging |
| Retry logic | Exponential backoff with configurable max attempts |
| Maintenance | Scheduled VACUUM + OPTIMIZE with compaction |
| Logging | Every table write logged (duration, row count, errors, schema) |
| Dashboard | 13-page operational dashboard with ai_forecast projections |

---

## Documentation

Start with [02_New_Starter_Guide.md](docs/02_New_Starter_Guide.md) for onboarding, or jump to:

| Doc | Audience |
| --- | --- |
| [04_Configuration_Reference](docs/04_Configuration_Reference.md) | Table config schema |
| [08_Operations_Runbook](docs/08_Operations_Runbook.md) | Production ops |
| [11_API_Reference](docs/11_API_Reference.md) | Function signatures |
| [17_Dashboard_Guide](docs/17_Dashboard_Guide.md) | Monitoring dashboard |

---

## Monitoring Dashboard

The companion **Belle Pipeline Monitoring** dashboard provides real-time visibility across 13 pages:

| Page | Purpose |
| --- | --- |
| Executive Overview | At-a-glance KPIs and Gantt timeline |
| Daily Ops | Morning health check (RAG matrix, Go/No-Go) |
| Delivery SLAs | Completion time trends and SLA analysis |
| Data Quality | Row count anomalies and schema drift |
| Issues & Incidents | Failure investigation |
| Performance | Table-level duration and cost analysis |
| Growth & Capacity | Volume trends with ai_forecast projection |
| FinOps | Cost attribution with 30-day forecast |
| MoM Trends | Month-over-month health comparison |
| Tags & Domains | Business domain breakdown |
| User Activity | Who runs what |
| Actions & Next Steps | Prioritised engineering recommendations |
| Pipeline Deep-Dive | Exploratory drill-down |

See [17_Dashboard_Guide.md](docs/17_Dashboard_Guide.md) for full details.

---

## Contributing

See [13_Contributing_Guide.md](docs/13_Contributing_Guide.md).

---

## License

MIT License. See [LICENSE](LICENSE) for details.
