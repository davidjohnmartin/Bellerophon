# Pipeline Validator Guide

**Since:** Belle v1.2.19  
**Audience:** All Belle pipeline developers.

---

## Overview

Belle v1.2.19 ships an integrated static validator that analyses your pipeline notebook **without executing it**. It parses your code, follows `%run` sub-notebooks recursively, and checks for structural errors, misconfigurations, and anti-patterns — all in under 10 seconds.

---

## Quick Start

From any notebook that has loaded Belle (`%run ../bellerophon_core`):

```python
belle.Validator.validate("/Users/you@company.com/my_pipeline/main_notebook")
```

That’s it. One line. Full report.

---

## What It Checks

| Phase | Check | Severity |
|-------|-------|----------|
| Structure | `%run bellerophon_core` present | ERROR |
| Structure | Belle code used before `%run` loads it | ERROR |
| Structure | `%run` targets exist in workspace | ERROR |
| Registry | `set_output()` keys resolve to valid patterns | ERROR/INFO |
| Registry | Duplicate key registrations | WARNING |
| Registry | DOT vs UNDERSCORE separator | ERROR |
| Config | Required keys present (target_database, result_table_name, load_mode) | ERROR |
| Config | Load mode is a valid Belle mode | ERROR |
| Config | `merge`/`update`/`delete` have `merge_keys` | ERROR |
| Config | `refresh_n_days` has `partition_by` | ERROR |
| Config | `encrypt=True` has `encrypt_key` | ERROR |
| Config | `_dev` suffix in target_database (Belle routes automatically) | ERROR |
| Anti-patterns | Top-level `.saveAsTable()` outside Belle | WARNING |
| Anti-patterns | `spark.sql('CREATE TABLE')` bypassing Belle | WARNING |
| Anti-patterns | `.save(format='delta')` bypassing Belle | WARNING |
| Orchestrator | `.run()` called without prior `set_output()` | WARNING |

---

## Output

The validator prints:

1. **Banner** — PASSED/FAILED, timestamp, table count, registry count
2. **Report DataFrame** — grouped findings with Severity, Category, Count, Finding, Locations, and Fix advice (displayed via `display()`)
3. **Tables DataFrame** — summary of all discovered TABLES_CONFIG entries with mode, encryption, partition, and merge key columns

Return value is a **boolean**: `True` if no errors (warnings are acceptable).

---

## Accessing Findings Programmatically

After calling `validate()`, findings are stored on the class:

```python
belle.Validator.validate("/path/to/pipeline")

# Access the findings list
for f in belle.Validator.findings:
    print(f"{f.severity} | {f.category} | {f.message}")

# Access extracted table configs
for table_key, attrs in belle.Validator.tables.items():
    print(f"{table_key}: mode={attrs.get('load_mode')}")
```

Each finding has: `.severity`, `.category`, `.message`, `.cell_index`, `.line_number`.

---

## Config-Only Validation

If you already have a `TABLES_CONFIG` dict in memory, validate it directly without notebook parsing:

```python
belle.Validator.validate_config(TABLES_CONFIG)
```

Same return semantics: `True` if no errors.

---

## How It Handles Complex Patterns

### Recursive %run Following

The validator follows `%run` directives up to 3 levels deep, parsing all sub-notebooks as if they were part of the main notebook. It **skips `bellerophon_core` itself** (the framework is not pipeline code).

### Function-Scoped Code

Code inside `def` blocks (including class methods) is **not flagged** for anti-patterns. The reasoning: `.saveAsTable()` inside a function is a design choice the developer made deliberately (e.g., writing pipeline control tables). Only top-level calls are flagged.

### Dynamic Registry Keys

When `set_output()` is called with a function like `_sk(table_name)`, the validator:
1. Finds the function definition across all cells
2. Extracts the return f-string pattern
3. Maps formal parameters to calling arguments
4. Enumerates loop variable values where possible

This means `_sk(table_name)` inside `for table_name in TABLE_LIST:` resolves to concrete keys.

### Function Parameters as Keys

When the registry key is a function parameter (e.g., `def write_table(key, df): set_output(key, df)`), the validator recognises it cannot resolve statically and skips it without raising a finding.

---

## Suppressing Findings

Add `# INTENTIONAL` on the same line as a flagged anti-pattern to suppress it:

```python
df.write.saveAsTable("pipeline_control")  # INTENTIONAL
```

---

## Standalone Validator Notebook

The integrated `belle.Validator.validate()` covers the core static checks. For **runtime validation** (Belle ConfigValidator, DryRun, DAG staging, write destination maps), use the standalone validator notebook at:

```
Belle_Versions/tools/belle_validator
```

This notebook adds Phase 6 (runtime checks) that require DataFrames to be registered in the OutputRegistry — something static analysis cannot do.

---

## CI Integration

Use in a test notebook or CI job:

```python
assert belle.Validator.validate("/path/to/pipeline"), \
    f"Validation failed: {[f for f in belle.Validator.findings if f.severity == 'ERROR']}"
```

Return code is `True`/`False`, making it trivial to gate deployments on validation passing.
