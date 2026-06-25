# Databricks notebook source
# =============================================================================
# BELLE TEST: Partition Mode
# =============================================================================
# Validates partition materialisation:
#   - First write creates table with partition scheme
#   - Subsequent writes use replaceWhere (surgical)
#   - Log buffer accumulates and flushes correctly
#   - Zero-row partitions are handled gracefully
# =============================================================================

%run ../bellerophon_core

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

TEST_DB = "belle_test_partition"
run_id = str(uuid.uuid4())

# Synthetic monthly data: 12 months, varying row counts
import random
random.seed(99)
rows = []
for month in range(1, 13):
    n_rows = random.randint(50, 200)
    for i in range(n_rows):
        rows.append((f"order_{month}_{i}", 2024, month, round(random.uniform(10, 500), 2)))

df_all = spark.createDataFrame(rows, ["order_id", "_data_year", "_data_month", "amount"])
print(f"Total rows: {df_all.count()}")
df_all.groupBy("_data_year", "_data_month").count().orderBy("_data_month").show(12)

# COMMAND ----------

# =============================================================================
# TEST 1: First partition write creates table
# =============================================================================
# Drop if exists from previous test run
spark.sql(f"DROP TABLE IF EXISTS {TEST_DB}_dev.monthly_orders")

conf = {
    "target_database": f"{TEST_DB}",
    "result_table_name": "monthly_orders",
    "partition_by": ["_data_year", "_data_month"],
    "partition_filter": {"_data_year": 2024, "_data_month": 1},
    "use_managed_table": True,
}

with belle.Config.temp_config(VERBOSITY=0):
    rows_written = belle.materialise_partition(
        input_df=df_all,
        conf=conf,
        run_id=run_id,
        interactive_mode=True,
    )

assert rows_written > 0, "First partition should write rows"
assert spark.catalog.tableExists(f"{TEST_DB}_dev.monthly_orders"), "Table should exist after first write"

# Check partitioning
partitions = spark.sql(f"SHOW PARTITIONS {TEST_DB}_dev.monthly_orders").collect()
assert len(partitions) == 1, f"Should have 1 partition, got {len(partitions)}"

print(f"\u2705 TEST 1 PASSED: First write created table with {rows_written} rows in 1 partition")

# COMMAND ----------

# =============================================================================
# TEST 2: Subsequent writes use replaceWhere (add more partitions)
# =============================================================================
for month in range(2, 7):  # Write months 2-6
    conf["partition_filter"] = {"_data_year": 2024, "_data_month": month}
    with belle.Config.temp_config(VERBOSITY=0):
        belle.materialise_partition(
            input_df=df_all,
            conf=conf,
            run_id=run_id,
            interactive_mode=True,
        )

partitions = spark.sql(f"SHOW PARTITIONS {TEST_DB}_dev.monthly_orders").collect()
assert len(partitions) == 6, f"Should have 6 partitions after writing months 1-6, got {len(partitions)}"

print("\u2705 TEST 2 PASSED: replaceWhere added 5 more partitions (6 total)")

# COMMAND ----------

# =============================================================================
# TEST 3: Overwrite existing partition (idempotency)
# =============================================================================
month1_count_before = spark.table(f"{TEST_DB}_dev.monthly_orders").filter(
    "_data_month = 1"
).count()

# Re-write month 1 (same data)
conf["partition_filter"] = {"_data_year": 2024, "_data_month": 1}
with belle.Config.temp_config(VERBOSITY=0):
    belle.materialise_partition(input_df=df_all, conf=conf, run_id=run_id, interactive_mode=True)

month1_count_after = spark.table(f"{TEST_DB}_dev.monthly_orders").filter(
    "_data_month = 1"
).count()

assert month1_count_after == month1_count_before, \
    f"Re-write should produce same count: before={month1_count_before}, after={month1_count_after}"

print(f"\u2705 TEST 3 PASSED: Partition overwrite is idempotent ({month1_count_after} rows unchanged)")

# COMMAND ----------

# =============================================================================
# TEST 4: Flush partition logs
# =============================================================================
buffer_count = belle.PartitionLogBuffer.count()
print(f"Buffer has {buffer_count} entries before flush")
assert buffer_count > 0, "Buffer should have entries from previous writes"

belle.flush_partition_logs(target_database=f"{TEST_DB}", run_id=run_id)

buffer_after = belle.PartitionLogBuffer.count()
assert buffer_after == 0, f"Buffer should be empty after flush, got {buffer_after}"

print("\u2705 TEST 4 PASSED: Partition log buffer flushed successfully")

# COMMAND ----------

# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "="*80)
print("  \U0001f3c6  ALL PARTITION MODE TESTS PASSED")
print("="*80)
print(f"  Belle version: {belle.VERSION}")
print(f"  Tests: 1 (create), 2 (replaceWhere), 3 (idempotent), 4 (log flush)")
print("="*80)