# Contributing Guide

**Audience:** Developers modifying `bellerophon_core` itself.

---

## 1. Code Conventions

### 1.1 Docstrings

* Single-line docstrings only
* Format: `"""Brief description."""`
* No multi-section docstrings (Args, Returns, etc.) — keep compact

### 1.2 Comments

* No `# FIX #N` or `# v1.2.x` inline comments
* Use semantic section headers only (e.g., `# ── Write ───...`)
* One blank line before section comments
* No comments inside chain blocks

### 1.3 Imports

* `pyspark.sql.functions`, `pyspark.sql.types`, `delta.tables`, and `pandas` are imported LOCALLY inside each function
* This prevents namespace pollution for `%run` consumers
* Module-level imports: only `datetime`, `uuid`, `json`, `time`, `sys`, `re`, `traceback`, `typing`, `concurrent.futures`, `functools.reduce`, `py4j`, `pyspark.StorageLevel`, `pyspark.sql.DataFrame`

### 1.4 Naming

* Classes: `BellerophonX` (e.g., `BellerophonConfig`, `BellerophonOrchestrator`)
* Functions: `bellerophon_materialise_*` for public, `_helper_name` for private
* Constants: `UPPER_SNAKE_CASE`
* Internal/temp variables: `_prefixed` with underscore

### 1.5 Line Length

* 100 characters max (following user/team conventions)
* Chain indent: 4 + 8 pattern for PySpark DataFrames

---

## 2. Cell Structure & Organisation

The notebook has 16 cells. Each cell is a logical module:

| Cell | Purpose | Approximate size |
| --- | --- | --- |
| 1 | Header/changelog | Small |
| 2 | Imports + Tracer | ~250 lines |
| 3 | BellerophonConfig | ~400 lines |
| 4 | ErrorCode + Table Readiness | ~200 lines |
| 5 | OutputRegistry | ~100 lines |
| 6 | BellerophonUtils | ~250 lines |
| 7 | BellerophonLogger | ~200 lines |
| 8 | Validators + Production Features | ~500 lines |
| 9 | DAGVisualizer | ~300 lines |
| 10 | MaintenanceScheduler | ~600 lines |
| 11 | Orchestrator | ~1200 lines |
| 12 | Materialise DataFrame | ~1500 lines |
| 13 | OOM Retry Wrapper | ~80 lines |
| 14 | Fast Mode | ~500 lines |
| 15 | Partition Materialisation | ~400 lines |
| 16 | BelleNamespace + Init | ~80 lines |

### 2.1 Rules for Cell Changes

* Do NOT split cells without discussing with the team (consumers `%run` the whole notebook)
* New functionality should go in the most logical existing cell
* If a cell exceeds ~2000 lines, consider splitting (candidates: Cell 11, Cell 12)
* Cell order matters: classes/functions must be defined before they are referenced

---

## 3. Adding a New Load Mode

1. Add the mode name to `BellerophonConfig.VALID_LOAD_MODES`
2. Add validation logic in `BellerophonConfig.validate_and_enrich_table_config()`
3. Add the write logic in `bellerophon_materialise_dataframe()` (Cell 12)
4. Add a test case in `10_Testing_Guide.md`
5. Update `04_Configuration_Reference.md` and `05_User_Guide`
6. Increment version (patch: `1.2.x` → `1.2.x+1`)

---

## 4. Adding a New Error Code

1. Add the code to `BellerophonErrorCode` class (Cell 4)
2. Add description in `get_description()` method
3. Use it in the appropriate error handling path
4. Update `08_Operations_Runbook.md` error codes table
5. Update `11_API_Reference.md` error codes section

---

## 5. Adding a New Config Option

1. Add the attribute to `BellerophonConfig` with a sensible default
2. If it should be overridable via env var: add to `from_env()`
3. If it should reset to default: add to `reset_defaults()`
4. Document in `04_Configuration_Reference.md`
5. Add to `A1_Appendix_Config_Settings.md`

---

## 6. Version Numbering

```
1.MAJOR.MINOR
  │     └── Bug fixes, non-breaking improvements
  └─────── Feature additions (new load modes, new config, new classes)
```

Current: 1.2.18

* Increment MINOR for any code change (bug fix, improvement)
* Increment MAJOR for new features or breaking changes
* Update `BELLEROPHON_VERSION` constant in Cell 2
* Update the header comment in Cell 1

---

## 7. Testing Before Release

1. Run the test patterns in `10_Testing_Guide.md`
2. Run at least one production pipeline interactively (e.g., Operations Reporting — small, fast)
3. Check that test_mode works and tables are correctly suffixed
4. Verify log table writes in a test database
5. Confirm no namespace pollution: in a fresh notebook, `%run bellerophon_core`, then check that `F`, `col`, `lit` are NOT defined

---

## 8. Common Pitfalls

| Pitfall | Why it happens | Prevention |
| --- | --- | --- |
| Adding `from pyspark.sql.functions import *` at module level | Pollutes consumer namespace | Always import locally inside functions |
| Changing a default value | Breaks existing consumers silently | Document in changelog; notify team |
| Modifying `BelleNamespace` without updating `belle.X` | Users call `belle.NewThing` and get AttributeError | Always add new public API to BelleNamespace |
| Breaking the Orchestrator constructor signature | All consumers call `belle.Orchestrator(config)` | Use `**kwargs` or optional params |
| Forgetting to protect log table from force_rebuild | History is lost | Check `ensure_table_ready()` guards |

---

## 9. Release Checklist

- [ ] Version bumped in `BELLEROPHON_VERSION` and Cell 1 header
- [ ] No `# FIX #N` comments remaining
- [ ] No `print()` debugging statements left in
- [ ] All new public API added to `BelleNamespace`
- [ ] Documentation updated (at minimum: API Reference, Config Reference)
- [ ] Test mode verified working
- [ ] At least one consumer notebook tested interactively
- [ ] No module-level imports of pyspark.sql.functions or delta.tables

---

*Last updated: June 2026*
