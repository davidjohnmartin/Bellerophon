# Databricks notebook source
# =============================================================================
# BELLE TEST: Encryption Round-Trip
# =============================================================================
# Validates that per-column and blob encryption produce correct results
# and that decryption recovers original values.
#
# PASS CRITERIA: All assertions pass.
# =============================================================================

%run ../bellerophon_core

# COMMAND ----------

import base64, os
from pyspark.sql import functions as F

# Generate test key (256-bit AES)
TEST_KEY = base64.b64encode(os.urandom(32)).decode()
TEST_DB = "belle_test_encryption"

# Synthetic data: mix of types
df = spark.createDataFrame([
    (1, "Alice Smith", "alice@example.com", 42500.75, "UK"),
    (2, "Bob M\u00fcller", "bob@example.de", 38000.00, "DE"),
    (3, "Claire Dupont", "claire@example.fr", 55000.50, "FR"),
    (4, "\u5f20\u4f1f", "zhang@example.cn", 61000.00, "CN"),
], ["id", "name", "email", "salary", "country"])

print(f"Test key generated: {TEST_KEY[:8]}...")
print(f"Test data: {df.count()} rows, {len(df.columns)} columns")

# COMMAND ----------

# =============================================================================
# TEST A: Per-Column Encryption (default strategy)
# =============================================================================
belle.Config.ENCRYPTION_STRATEGY = "per_column"

belle.OutputRegistry.set_output(f"{TEST_DB}_per_col", df)

config_percol = {
    f"{TEST_DB}.per_col": {
        "target_database": TEST_DB,
        "result_table_name": "per_col",
        "load_mode": "full",
        "dependencies": [],
        "encrypt": True,
        "encrypt_key": TEST_KEY,
        "encrypt_exclude": ["id", "country"],  # These stay plaintext
    }
}

orch = belle.Orchestrator(config_percol, test_mode=True)
results = orch.run(show_dag=False, sample_rows=0)

# Get the test table name
suffix = orch.test_suffix if hasattr(orch, 'test_suffix') else ''
table_name = f"{TEST_DB}.per_col{suffix}"

# Verify schema: id and country are plaintext, others are BINARY
schema = spark.table(table_name).schema
assert schema["id"].dataType.simpleString() in ("int", "bigint", "long"), \
    f"id should be numeric, got {schema['id'].dataType}"
assert schema["country"].dataType.simpleString() == "string", \
    f"country should be string (excluded), got {schema['country'].dataType}"
assert schema["name"].dataType.simpleString() == "binary", \
    f"name should be binary (encrypted), got {schema['name'].dataType}"
assert schema["email"].dataType.simpleString() == "binary", \
    f"email should be binary (encrypted), got {schema['email'].dataType}"
assert schema["salary"].dataType.simpleString() == "binary", \
    f"salary should be binary (encrypted), got {schema['salary'].dataType}"

print("\u2705 TEST A.1 PASSED: Encrypted columns are BINARY, excluded columns are plaintext")

# Verify decryption recovers original
decrypted = spark.table(table_name).select(
    "id", "country",
    F.expr(f"aes_decrypt(name, unbase64('{TEST_KEY}'), 'GCM', 'DEFAULT')").alias("name"),
    F.expr(f"aes_decrypt(email, unbase64('{TEST_KEY}'), 'GCM', 'DEFAULT')").alias("email"),
    F.expr(f"CAST(aes_decrypt(salary, unbase64('{TEST_KEY}'), 'GCM', 'DEFAULT') AS DOUBLE)").alias("salary"),
).orderBy("id").collect()

assert decrypted[0]["name"] == "Alice Smith"
assert decrypted[1]["name"] == "Bob M\u00fcller"  # Unicode
assert decrypted[3]["name"] == "\u5f20\u4f1f"  # CJK characters
assert decrypted[0]["salary"] == 42500.75
assert decrypted[2]["email"] == "claire@example.fr"

print("\u2705 TEST A.2 PASSED: Decryption recovers original values (including Unicode)")

# COMMAND ----------

# =============================================================================
# TEST B: Wrong Key Fails Decryption
# =============================================================================
WRONG_KEY = base64.b64encode(os.urandom(32)).decode()

try:
    spark.table(table_name).select(
        F.expr(f"aes_decrypt(name, unbase64('{WRONG_KEY}'), 'GCM', 'DEFAULT')")
    ).collect()
    assert False, "Should have raised an error with wrong key"
except Exception as e:
    # GCM authentication should fail with wrong key
    assert "GCM" in str(e) or "decrypt" in str(e).lower() or "tag" in str(e).lower(), \
        f"Unexpected error: {e}"

print("\u2705 TEST B PASSED: Wrong key correctly fails decryption (GCM auth)")

# COMMAND ----------

# =============================================================================
# TEST C: Feature Kill Switch
# =============================================================================
with belle.Config.temp_config(FEATURE_ENCRYPTION=False):
    belle.OutputRegistry.set_output(f"{TEST_DB}_no_enc", df)
    config_noenc = {
        f"{TEST_DB}.no_enc": {
            "target_database": TEST_DB,
            "result_table_name": "no_enc",
            "load_mode": "full",
            "dependencies": [],
            "encrypt": True,  # Set but kill switch overrides
            "encrypt_key": TEST_KEY,
            "encrypt_exclude": ["id"],
        }
    }
    orch2 = belle.Orchestrator(config_noenc, test_mode=True)
    orch2.run(show_dag=False, sample_rows=0)
    suffix2 = orch2.test_suffix if hasattr(orch2, 'test_suffix') else ''
    table2 = f"{TEST_DB}.no_enc{suffix2}"

    # All columns should be plaintext (encryption disabled globally)
    schema2 = spark.table(table2).schema
    assert schema2["name"].dataType.simpleString() == "string", \
        f"name should be string when encryption disabled, got {schema2['name'].dataType}"

print("\u2705 TEST C PASSED: FEATURE_ENCRYPTION=False disables encryption globally")

# COMMAND ----------

# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "="*80)
print("  \U0001f3c6  ALL ENCRYPTION TESTS PASSED")
print("="*80)
print(f"  Belle version: {belle.VERSION}")
print(f"  Tests: A.1 (schema), A.2 (decrypt), B (wrong key), C (kill switch)")
print("="*80)