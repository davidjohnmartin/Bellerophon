# Migration & Upgrade Guide

**Audience:** Platform engineers handling storage migrations, DBR upgrades, or service account changes.

---

## 1. Upgrading Belle Versions

### 1.1 Belle Is Loaded via %run

Consuming notebooks reference Belle by path:
```python
%run ../Belle_Versions/bellerophon_core
```

To upgrade: update the `bellerophon_core` notebook in-place. All consumers pick up the change on their next run.

### 1.2 Version Checking

```python
print(belle.VERSION)  # e.g., "1.2.18"
```

### 1.3 Breaking Changes (When to Be Careful)

| Change type | Risk | Mitigation |
| --- | --- | --- |
| New config attribute | None | Defaults maintain backward compatibility |
| Changed default value | Medium | Check changelog; test before production deploy |
| New required TABLES_CONFIG key | High | Update all consuming notebooks |
| Removed function | High | Search consumers for usage before removing |
| Changed function signature | High | Verify all callers |

---

## 2. Blob Storage Migration

### 2.1 What to Change

Update `BellerophonConfig` in the `bellerophon_core` notebook:

```python
# Old
BLOB_ROOT = "/mnt/internal/enhanced"

# New (example: new ADLS Gen2 mount)
BLOB_ROOT = "/mnt/datalake/gold"
```

### 2.2 Migration Steps

1. Mount new storage in Databricks
2. Update `BLOB_ROOT` in BellerophonConfig
3. Run all pipelines with `global_force_rebuild=True` to write to new location
4. Verify data at new location
5. Update ADF pipelines if they reference blob paths directly
6. Remove old mount after validation period

### 2.3 Considerations

* Log tables will be recreated at new path (historical logs at old path are orphaned)
* CSV exports land at new path
* External tables will point to new LOCATION
* Managed tables (Unity Catalog) are unaffected (UC manages location)

---

## 3. Service Account Changes

### 3.1 What to Change

```python
# Update the prefix pattern
belle.Config.SERVICE_ACCOUNT_PREFIX = "svc_new_pattern"
```

### 3.2 Why This Matters

Belle uses the service account prefix to detect production mode. If the prefix doesn't match:
* Interactive mode is assumed
* CSV exports are suppressed
* Logs are not persisted

### 3.3 Migration Steps

1. Identify new service principal naming pattern
2. Update `SERVICE_ACCOUNT_PREFIX` in BellerophonConfig
3. Verify production detection: run pipeline, check that logs are persisted

---

## 4. Unity Catalog Migration (Hive → UC)

### 4.1 The Key Change

Belle auto-detects UC vs Hive from `target_database` format:

```python
# Hive (1-level namespace)
"target_database": "sales_semantic"

# Unity Catalog (2-level namespace: catalog.schema)
"target_database": "my_catalog.my_schema"
```

### 4.2 Behaviour Differences

| Aspect | Hive | Unity Catalog |
| --- | --- | --- |
| Table type | External (explicit LOCATION) | Managed (UC handles location) |
| Path | `{BLOB_ROOT}/{database}/data/{table}/` | UC-managed |
| Permissions | Hive grants | UC grants |
| CSV export path | Blob-based | Blob-based (unchanged) |

### 4.3 Migration Steps

1. Create UC catalog and schema
2. Update `target_database` in all TABLES_CONFIG entries
3. Update OutputRegistry key format if needed
4. Run with `global_force_rebuild=True` to create tables in UC
5. Update downstream queries to use 3-level names

---

## 5. DBR Version Upgrades

### 5.1 Supported Versions

Belle supports: DBR 13.3.x, 14.x, 15.x, 16.x, 17.x

### 5.2 Known Considerations

| DBR Version | Notes |
| --- | --- |
| 13.3.x | Minimum supported. Some Delta features unavailable. |
| 14.x | Liquid clustering available (Belle uses traditional partitioning) |
| 15.x | Spark Connect changes. Belle uses classic driver mode. |
| 16.x | Full compatibility |
| 17.x | Full compatibility (latest tested) |

### 5.3 Upgrade Testing

1. Run Belle with `test_mode=True` on new DBR version
2. Verify all write modes work (full, merge, insert, refresh)
3. Verify encryption round-trip
4. Check log table writes
5. Run performance comparison on key tables

---

## 6. ADF Deployment Changes

### 6.1 Notebook Activity (No Change Needed)

If using Databricks Notebook activity in ADF, no changes when upgrading Belle.

### 6.2 Python Activity (".py" Reference)

If ADF uses a Python activity:
* The wrapper `.py` file references the consuming notebook via `dbutils.notebook.run()`
* Belle itself (`bellerophon_core`) is never referenced directly by ADF
* Update the wrapper only if the consuming notebook path changes

### 6.3 When ADF Timeout Needs Adjusting

If Belle version adds overhead (e.g., new validation step), increase ADF activity timeout. Current recommendation: 2x the longest observed run duration.

---

## 7. Multi-Environment Deployment Pattern

```
/Users/.../Belle_Versions/
    bellerophon_core            ← Single source of truth (all environments)

/Users/.../my_pipeline/
    sales_pipeline              ← Uses %run ../Belle_Versions/bellerophon_core
    sales_pipeline_config       ← Environment-specific settings
```

Belle is environment-agnostic. The consuming pipeline handles environment routing:
```python
import os
env = os.getenv("ENVIRONMENT", "dev")
TARGET_DB = f"sales_semantic_{env}" if env != "prod" else "sales_semantic"
```

---

*Last updated: June 2026*
