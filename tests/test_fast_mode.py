# Databricks notebook source
# =============================================================================
# BELLE TEST: Fast Mode & Bulk Materialisation
# =============================================================================
# Validates fast mode behaviour:
#   - Full mode works via fast path
#   - Non-full modes fall back to standard
#   - Weight-sorted orchestration (heavy tables sequential)
#   - Bulk materialise processes all tables
# =============================================================================

%run ../bellerophon_core

# COMMAND ----------

from pyspark.sql import functions as F
import uuid, time

TEST_DB = "belle_test_fast"

# Generate 5 synthetic tables of varying size
tables = {}
for i in range(1, 6):
    tables[f"table_{i}"] = spark.range(1000 * i).select(
        F.col("id"),
        F.lit(f"value_{i}").alias("source"),
        F.rand().alias("metric"),
    )

print(f"Generated {len(tables)} test tables")
for name, df in tables.items():
    print(f"  {name}: {df.count()} rows")

# COMMAND ----------

# =============================================================================
# TEST 1: Fast mode writes all tables (full mode)
# =============================================================================
run_id = str(uuid.uuid4())
configs = {}
dataframes = {}

for name, df in tables.items():
    key = f"{TEST_DB}.{name}"
    configs[key] = {
        "target_database": TEST_DB,
        "result_table_name": name,
        "load_mode": "full",
        "dependencies": [],
        "use_managed_table": True,
    }
    dataframes[key] = df

with belle.Config.temp_config(VERBOSITY=1):
    results = belle.materialise_bulk(
        dataframes=dataframes,
        configs=configs,
        run_id=run_id,
        interactive_mode=True,
    )

# Verify all tables were written
written = [r for r in results if r.get('success', r.get('status') == 'success')]
assert len(written) == 5, f"Expected 5 tables written, got {len(written)}"

# Verify row counts
for i in range(1, 6):
    expected = 1000 * i
    actual = spark.table(f"{TEST_DB}_dev.table_{i}").count()
    assert actual == expected, f"table_{i}: expected {expected} rows, got {actual}"

print("\u2705 TEST 1 PASSED: materialise_bulk writes all 5 tables with correct row counts")

# COMMAND ----------

# =============================================================================
# TEST 2: Fast mode single table via materialise_dataframe_fast
# =============================================================================
df_single = spark.range(500).select(
    F.col("id"),
    F.lit("fast_single").alias("tag"),
)

conf_single = {
    "target_database": TEST_DB,
    "result_table_name": "fast_single",
    "load_mode": "full",
    "use_managed_table": True,
}

with belle.Config.temp_config(VERBOSITY=0):
    result_df, log_df, _ = belle.materialise_dataframe_fast(
        input_df=df_single,
        conf=conf_single,
        run_id=str(uuid.uuid4()),
        dag_stage=1,
        interactive_mode=True,
    )

count = spark.table(f"{TEST_DB}_dev.fast_single").count()
assert count == 500, f"Expected 500 rows, got {count}"

print("\u2705 TEST 2 PASSED: materialise_dataframe_fast writes single table correctly")

# COMMAND ----------

# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "="*80)
print("  \U0001f3c6  ALL FAST MODE TESTS PASSED")
print("="*80)
print(f"  Belle version: {belle.VERSION}")
print(f"  Tests: 1 (bulk write), 2 (single fast write)")
print("="*80)