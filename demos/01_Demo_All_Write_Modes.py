# Databricks notebook source
# DBTITLE 1,Demo: All Write Modes with Synthetic Data
# =============================================================================
# BELLE DEMO: All 7 Write Modes
# =============================================================================
# This notebook demonstrates every Belle write mode using synthetic data.
# Run interactively — writes to _dev databases (safe, isolated).
#
# Modes demonstrated:
#   1. full           — Drop and recreate
#   2. insert         — Append new rows
#   3. merge          — Upsert (match on keys)
#   4. update         — Update matched rows only
#   5. delete         — Delete matched rows
#   6. refresh_n_days — Rolling window replace
#   7. full_if_not_exists — Write once, skip on re-run
# =============================================================================

%run ../bellerophon_core

# COMMAND ----------

# DBTITLE 1,Generate Synthetic Data
# =============================================================================
# SYNTHETIC DATA: Customers (dimension) + Orders (fact)
# =============================================================================
import uuid
from datetime import date, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# --- Dimension: 500 synthetic customers ---
customer_data = [
    (i, f"Customer_{i:04d}", f"customer{i}@example.com",
     ["UK", "DE", "FR", "BE", "CZ"][i % 5],
     date(2020, 1, 1) + timedelta(days=i % 365))
    for i in range(1, 501)
]
df_customers = spark.createDataFrame(
    customer_data,
    ["customer_id", "name", "email", "country", "registered_date"]
)

# --- Fact: 10,000 synthetic orders ---
import random
random.seed(42)
order_data = [
    (i, random.randint(1, 500), round(random.uniform(10, 5000), 2),
     ["pending", "shipped", "delivered", "returned"][random.randint(0, 3)],
     date(2024, 1, 1) + timedelta(days=random.randint(0, 540)),
     (date(2024, 1, 1) + timedelta(days=random.randint(0, 540))).year,
     (date(2024, 1, 1) + timedelta(days=random.randint(0, 540))).month)
    for i in range(1, 10001)
]
df_orders = spark.createDataFrame(
    order_data,
    ["order_id", "customer_id", "amount", "status",
     "order_date", "_data_year", "_data_month"]
)

# --- Incremental: New orders arriving (for insert/merge demos) ---
new_order_data = [
    (10001 + i, random.randint(1, 500), round(random.uniform(10, 5000), 2),
     "pending", date(2025, 7, 1) + timedelta(days=i), 2025, 7)
    for i in range(100)
]
df_new_orders = spark.createDataFrame(
    new_order_data,
    ["order_id", "customer_id", "amount", "status",
     "order_date", "_data_year", "_data_month"]
)

# --- Updates: Status changes (for merge/update demos) ---
df_status_updates = spark.createDataFrame(
    [(i, "delivered") for i in range(1, 201)],
    ["order_id", "status"]
)

# --- Deletes: Cancelled orders (for delete demo) ---
df_deletes = spark.createDataFrame(
    [(i,) for i in range(9900, 10001)],
    ["order_id"]
)

print(f"Customers: {df_customers.count()} rows")
print(f"Orders:    {df_orders.count()} rows")
print(f"New:       {df_new_orders.count()} rows")
print(f"Updates:   {df_status_updates.count()} rows")
print(f"Deletes:   {df_deletes.count()} rows")

# COMMAND ----------

# DBTITLE 1,Register & Configure All 7 Modes
# =============================================================================
# REGISTER ALL DataFrames
# =============================================================================
TARGET_DB = "belle_demo"  # Will become belle_demo_dev in interactive mode

belle.OutputRegistry.set_output(f"{TARGET_DB}_dim_customer", df_customers)
belle.OutputRegistry.set_output(f"{TARGET_DB}_fact_order", df_orders)
belle.OutputRegistry.set_output(f"{TARGET_DB}_fact_order_incremental", df_new_orders)
belle.OutputRegistry.set_output(f"{TARGET_DB}_fact_order_merge", df_orders.join(df_status_updates, "order_id", "left").select(
    df_orders["order_id"], df_orders["customer_id"], df_orders["amount"],
    F.coalesce(df_status_updates["status"], df_orders["status"]).alias("status"),
    df_orders["order_date"], df_orders["_data_year"], df_orders["_data_month"]
))
belle.OutputRegistry.set_output(f"{TARGET_DB}_fact_order_updates", df_status_updates)
belle.OutputRegistry.set_output(f"{TARGET_DB}_fact_order_deletes", df_deletes)
belle.OutputRegistry.set_output(f"{TARGET_DB}_dim_customer_once", df_customers)

# =============================================================================
# TABLES_CONFIG: One table per write mode
# =============================================================================
TABLES_CONFIG = {
    # MODE 1: full — Drop and recreate every run
    f"{TARGET_DB}.dim_customer": {
        "target_database": TARGET_DB,
        "result_table_name": "dim_customer",
        "load_mode": "full",
        "dependencies": [],
    },

    # MODE 2: insert — Append new rows
    f"{TARGET_DB}.fact_order_incremental": {
        "target_database": TARGET_DB,
        "result_table_name": "fact_order_incremental",
        "load_mode": "insert",
        "dependencies": [f"{TARGET_DB}.dim_customer"],
    },

    # MODE 3: merge — Upsert (insert new + update existing)
    f"{TARGET_DB}.fact_order_merge": {
        "target_database": TARGET_DB,
        "result_table_name": "fact_order_merge",
        "load_mode": "merge",
        "merge_keys": ["order_id"],
        "dependencies": [f"{TARGET_DB}.dim_customer"],
    },

    # MODE 4: update — Update matched rows only (no inserts)
    f"{TARGET_DB}.fact_order_updates": {
        "target_database": TARGET_DB,
        "result_table_name": "fact_order_updates",
        "load_mode": "update",
        "merge_keys": ["order_id"],
        "dependencies": [f"{TARGET_DB}.fact_order_merge"],
    },

    # MODE 5: delete — Remove matched rows
    f"{TARGET_DB}.fact_order_deletes": {
        "target_database": TARGET_DB,
        "result_table_name": "fact_order_deletes",
        "load_mode": "delete",
        "merge_keys": ["order_id"],
        "dependencies": [f"{TARGET_DB}.fact_order_merge"],
    },

    # MODE 6: refresh_n_days-7 — Rolling window replace
    f"{TARGET_DB}.fact_order": {
        "target_database": TARGET_DB,
        "result_table_name": "fact_order",
        "load_mode": "refresh_n_days-7",
        "dependencies": [f"{TARGET_DB}.dim_customer"],
        "partition_by": ["_data_year", "_data_month"],
    },

    # MODE 7: full_if_not_exists — Write once, skip on re-run
    f"{TARGET_DB}.dim_customer_once": {
        "target_database": TARGET_DB,
        "result_table_name": "dim_customer_once",
        "load_mode": "full_if_not_exists",
        "dependencies": [],
    },
}

print(f"Configured {len(TABLES_CONFIG)} tables across all 7 write modes")

# COMMAND ----------

# DBTITLE 1,Execute Orchestrator
# =============================================================================
# RUN THE ORCHESTRATOR
# =============================================================================
orchestrator = belle.Orchestrator(
    TABLES_CONFIG,
    test_mode=True,  # Table suffix isolation for safety
)

results = orchestrator.run(
    show_dag=True,     # Visualise the execution plan
    sample_rows=5,     # Show 5 rows per table after write
)

print("\n" + "="*80)
print("  DEMO COMPLETE: All 7 write modes executed successfully")
print("="*80)