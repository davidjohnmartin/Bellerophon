# Changelog

All notable changes to Bellerophon (Belle) are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.2.19] — 2026-06-25

### Added
- **Belle Pipeline Monitoring Dashboard** (13 pages, 44 datasets)
  - Executive Overview with MTD/YTD cost KPIs
  - Daily Ops with RAG matrix, Go/No-Go, solution currency
  - Delivery SLAs with completion pivot and P50/P90 stats
  - Data Quality with anomaly detection and schema drift tracking
  - Issues & Incidents with error code analysis
  - Performance with instability scatter and P95 bars
  - Growth & Capacity with ai_forecast 30-day row projection
  - FinOps with ai_forecast 30-day cost projection
  - MoM Trends with month-over-month RAG comparison
  - Tags & Domains breakdown by business domain/layer
  - User Activity with production vs interactive split
  - Actions & Next Steps with prioritised recommendations
  - Pipeline Deep-Dive exploratory drill-down
- `belle_log_dashboard` notebook — auto-discovers all Belle log tables, unions them, and produces 24 summary tables
- Hash-based anonymisation mode (no hardcoded names) for documentation screenshots
- Dashboard documentation (`docs/17_Dashboard_Guide.md`)
- Screenshots for all 13 dashboard pages (`images/`)
- Dashboard screenshots integrated into Operations Runbook, Log Deep Dive, and README
- Repository restructure: `core/` (framework) and `dashboard/` (monitoring) subfolders
- Root `README.md`, `CHANGELOG.md`, `LICENSE` (MIT), `.gitignore`

### Changed
- Docstring format trimmed to single-line (from verbose multi-section)
- Removed all inline `# FIX #N` and `# v1.2.x` comments — semantic section headers only

---

## [1.2.18] — 2026-05-15

### Added
- Validator Guide (`docs/16_Validator_Guide.md`)
- `belle.Validator` pre-flight checks (schema, merge keys, dependencies, partition columns)
- Feature Testing Checklist (`docs/15_Feature_Testing_Checklist.md`)

### Fixed
- OOM recovery in `resilient_materialise_table` — now properly unpersists before retry
- DAGVisualizer ASCII rendering for deeply nested dependency chains

---

## [1.2.17] — 2026-04-20

### Added
- `bellerophon_materialise_partition` — partition-level writes for large tables
- `bellerophon_materialise_bulk` — bulk overwrite mode
- Partition materialisation documentation in User Guide

### Changed
- MaintenanceScheduler: VACUUM retention now configurable per table
- Logger: added `parameters_json` column for runtime parameter capture

---

## [1.2.16] — 2026-03-10

### Added
- `parent_run_id` support for ADF/external orchestrator correlation
- Schema change detection (MD5 hash comparison, logged per write)
- Row count anomaly detection (z-score based, 7-day rolling window)

### Fixed
- ThreadPoolExecutor thread leak on stage failure (added proper cleanup)

---

## [1.2.15] — 2026-02-01

### Added
- Fast mode (`bellerophon_materialise_dataframe_fast`) for append-only tables
- DAGVisualizer SVG output option
- Encryption key rotation support

### Changed
- Default retry backoff: 30s → 60s (reduced cluster contention)

---

## [1.2.14] — 2025-12-15

### Added
- Tag-based pipeline grouping (`tag` column in TABLES_CONFIG)
- Cost attribution estimation (based on duration × DBU rate)
- `belle_genie_assistant.md` for AI-powered log queries

---

## [1.1.0] — 2025-09-01

### Added
- Multi-database support (scan multiple catalogs/mounts)
- Unity Catalog compatibility (`is_unity_catalog` flag)
- Managed table mode (`use_managed_table` flag)

### Changed
- Logger schema: expanded from 12 to 30+ columns

---

## [1.0.0] — 2025-03-01

### Added
- Initial release
- Core orchestrator with DAG resolution
- Full and merge load modes
- AES-256 column encryption
- Structured logging to `bellerophon_log_table`
- VACUUM/OPTIMIZE maintenance scheduler
- Auto-detection of production vs interactive mode
