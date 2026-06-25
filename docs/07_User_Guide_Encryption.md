# User Guide — Encryption

**Audience:** Data engineers working with PII-sensitive or regulated data requiring encryption at rest.  
**Prerequisites:** Read `05_User_Guide_Standard_Materialisation.md`.

---

## 1. Overview

Belle provides inline AES encryption applied during the materialisation step — between DataFrame construction and Delta write. The encrypted data is what lands on disk. Consumers must explicitly decrypt using the same key.

**Key facts:**
* Encryption is applied per-table (configured individually in TABLES_CONFIG)
* Global kill switch: `belle.Config.FEATURE_ENCRYPTION = False` disables all encryption
* Algorithm: AES with configurable mode (default: GCM — authenticated encryption)
* Two strategies: per-column (default, v1.2.15+) or blob (legacy)
* Keys are passed as base64-encoded strings in config
* Belle does NOT manage key rotation — that is your responsibility

---

## 2. Encryption Strategies

### 2.1 Per-Column (Default)

**Config setting:** `belle.Config.ENCRYPTION_STRATEGY = "per_column"`

Each column is individually cast to STRING, then encrypted. The result is a BINARY column with the same name.

**Advantages:**
* Preserves column structure (schema is readable, values are encrypted)
* Can decrypt individual columns without decrypting the entire row
* Partition/join key columns excluded via `encrypt_exclude`

**SQL equivalent of what Belle does:**
```sql
aes_encrypt(cast(`column_name` as string), unbase64('<key>'), 'GCM', 'DEFAULT')
```

### 2.2 Blob (Legacy)

**Config setting:** `belle.Config.ENCRYPTION_STRATEGY = "blob"`

All non-excluded columns are serialised to a single JSON string via `to_json(struct(*columns))`, then encrypted as one blob.

**Result:** Table has only the excluded columns + one `encrypted_payload` BINARY column.

**Advantages:**
* Fewer encryption operations (one per row, not one per column)
* Smaller schema footprint

**Disadvantages:**
* Cannot query or decrypt individual columns
* Must decrypt entire payload to access any field
* Schema is lost (payload is opaque)

---

## 3. Configuration

### 3.1 Table-Level Config

```python
"sales_semantic.factorder": {
    "target_database": "sales_semantic",
    "result_table_name": "factorder",
    "load_mode": "full",
    "dependencies": ["sales_semantic.dimclaim"],
    "partition_by": ["_data_year", "_data_month"],

    # Encryption settings
    "encrypt": True,
    "encrypt_key": "dGhpcyBpcyBhIDI1Ni1iaXQga2V5IGV4YW1wbGUh",  # base64-encoded 256-bit key
    "encrypt_exclude": ["_data_year", "_data_month", "claim_key", "policy_key"],

    "export_csv": True,
}
```

### 3.2 What to Exclude

Columns in `encrypt_exclude` remain in plaintext. You MUST exclude:

* **Partition columns** — Delta needs plaintext values for partition pruning
* **Join/foreign key columns** — Downstream queries join on these
* **Sort/filter columns** — Any column used in WHERE clauses downstream

Common pattern:
```python
"encrypt_exclude": [
    "_data_year", "_data_month",   # Partition columns
    "claim_key", "policy_key",      # Foreign keys
    "source_system", "country",     # Filter columns
]
```

### 3.3 Global Settings

```python
# Kill switch (disable all encryption)
belle.Config.FEATURE_ENCRYPTION = False

# Strategy (per_column or blob)
belle.Config.ENCRYPTION_STRATEGY = "per_column"  # Default

# AES mode
belle.Config.ENCRYPTION_MODE = "GCM"  # Authenticated encryption (recommended)
```

---

## 4. Key Management

### 4.1 Key Format

Keys must be base64-encoded. For AES-256-GCM, the decoded key must be exactly 32 bytes (256 bits).

```python
import base64
import os

# Generate a new 256-bit key
raw_key = os.urandom(32)
encoded_key = base64.b64encode(raw_key).decode('utf-8')
print(encoded_key)  # Use this in your config
```

### 4.2 Key Storage

Belle does NOT store keys. You are responsible for:
* Storing keys in Azure Key Vault, Databricks secrets, or equivalent
* Passing keys into your notebook at runtime
* Rotating keys when required by policy

**Recommended pattern:**
```python
# Load key from Databricks secrets
ENCRYPT_KEY = dbutils.secrets.get(scope="pipeline-keys", key="semantic-encrypt-key")

TABLES_CONFIG = {
    "db.my_table": {
        ...
        "encrypt_key": ENCRYPT_KEY,
        ...
    }
}
```

### 4.3 Key Rotation

To rotate keys:
1. Set `force_full_rebuild: True` on all affected tables
2. Update the `encrypt_key` value in config
3. Run the pipeline — tables are dropped and re-encrypted with new key
4. Update any downstream decrypt logic with the new key
5. Remove `force_full_rebuild: True` after successful run

---

## 5. Decrypting Data

### 5.1 Per-Column Decryption (SQL)

```sql
SELECT
    claim_key,                          -- plaintext (excluded from encryption)
    _data_year,                         -- plaintext (partition column)
    aes_decrypt(
        claimant_name,                  -- BINARY encrypted column
        unbase64('<base64-key>'),
        'GCM',
        'DEFAULT'
    ) AS claimant_name,                 -- Decrypted STRING
    CAST(aes_decrypt(
        claim_amount,
        unbase64('<base64-key>'),
        'GCM',
        'DEFAULT'
    ) AS DOUBLE) AS claim_amount        -- Decrypt then cast back to original type
FROM sales_semantic.factorder
```

### 5.2 Per-Column Decryption (PySpark)

```python
from pyspark.sql import functions as F

key = "<base64-key>"
df = spark.table("sales_semantic.factorder")

df_decrypted = df.select(
    "claim_key",
    "_data_year",
    F.expr(f"aes_decrypt(claimant_name, unbase64('{key}'), 'GCM', 'DEFAULT')").alias("claimant_name"),
    F.expr(f"CAST(aes_decrypt(claim_amount, unbase64('{key}'), 'GCM', 'DEFAULT') AS DOUBLE)").alias("claim_amount"),
)
```

### 5.3 Blob Decryption (Legacy)

```sql
SELECT
    claim_key,
    _data_year,
    from_json(
        aes_decrypt(encrypted_payload, unbase64('<key>'), 'GCM', 'DEFAULT'),
        'claimant_name STRING, claim_amount DOUBLE, ...'
    ).*
FROM sales_semantic.factorder
```

### 5.4 Worldwide Views (Sales Pattern)

The Sales Pipeline creates `worldwide_*` views that inline-decrypt from per-country encrypted tables:

```sql
CREATE OR REPLACE VIEW worldwide_factorder AS
SELECT
    claim_key,
    aes_decrypt(claimant_name, unbase64(secret('pipeline-keys', 'region-a-key')), 'GCM', 'DEFAULT') AS claimant_name,
    ...
FROM sales_semantic_germany.factorder
UNION ALL
SELECT
    claim_key,
    aes_decrypt(claimant_name, unbase64(secret('pipeline-keys', 'region-b-key')), 'GCM', 'DEFAULT') AS claimant_name,
    ...
FROM sales_semantic_france.factorder
```

Consumers of worldwide views see plaintext — they never handle encryption directly.

---

## 6. Performance Considerations

| Factor | Impact | Mitigation |
| --- | --- | --- |
| Per-column encryption overhead | ~10-30% write time increase | Exclude non-sensitive columns |
| Heavy tables (20+ encrypted cols) | Significant memory pressure | Fast mode weight-sorting runs them sequentially |
| Blob strategy | Single encryption op per row | Use for tables with 50+ sensitive columns |
| Decryption in queries | CPU overhead per row | Cache decrypted views; use worldwide view pattern |

### 6.1 Fast Mode Integration

Fast mode classifies tables by encrypted column count:
* Tables with >`FAST_MODE_HEAVY_COL_THRESHOLD` (default 20) encrypted columns = "heavy"
* Heavy tables execute sequentially (all cluster cores available)
* Light tables execute in parallel

This prevents memory exhaustion from concurrent encryption of multiple large tables.

---

## 7. Constraints & Requirements

- `encrypt_key` is REQUIRED when `encrypt: True` (Belle raises ValueError if missing)
- Keys must be valid base64-encoded AES keys (16, 24, or 32 bytes decoded for AES-128/192/256)
- `encrypt_exclude` MUST include all partition columns (otherwise partition pruning breaks)
- `encrypt_exclude` SHOULD include join key columns (otherwise downstream queries cannot join)
- Per-column strategy: all encrypted columns become BINARY type in the Delta table
- Blob strategy: all non-excluded columns disappear; replaced by single `encrypted_payload` BINARY
- Encrypted tables cannot be queried without the key (no accidental data exposure)
- Schema evolution on encrypted tables: adding a new column works; it will be encrypted on next write
- Changing `encrypt_exclude` (adding/removing columns from exclusion): requires `force_full_rebuild`
- Switching strategy (per_column ↔ blob): requires `force_full_rebuild` (schema is incompatible)

---

## 8. Security Considerations

* Keys in notebook code are visible to anyone with notebook access. Use Databricks secrets.
* Encrypted data on blob storage is safe at rest. But anyone with the key AND table access can decrypt.
* GCM mode provides authentication — tampered ciphertext is detected on decrypt.
* Belle strips keys from log output (they are never written to the log table).
* In interactive mode, encrypted tables are still written encrypted (no plaintext shortcut).

---

*Last updated: June 2026*
