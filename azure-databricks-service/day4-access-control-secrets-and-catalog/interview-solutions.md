# Day 4 — Interview Solutions: Access Control, Secrets, Catalog & ADF Integration

---

## Secrets & Key Vault

**A1**
A secret scope is a named collection in Databricks that acts as a reference to a secret store. Notebooks read secrets from it using `dbutils.secrets.get(scope, key)` — the actual value never appears in logs or cell output.

Two types:
- **Databricks-managed:** Databricks stores the secrets in its own encrypted internal store. Created via CLI or REST API.
- **AKV-backed (Azure Key Vault-backed):** Databricks reads secrets from Azure Key Vault at runtime. The secrets physically live in Key Vault. Databricks just has read access to the vault.

Recommended for production: **AKV-backed scope**, because:
- Secrets are managed centrally in Key Vault (rotation, expiry, access policies)
- Audit logs of who accessed which secret are in Azure Monitor
- One Key Vault secret rotation immediately takes effect in all Databricks notebooks — no update needed in Databricks

---

**A2**
`print(password)` shows: `[REDACTED]`

Databricks intercepts any `print()`, `display()`, or logging of a secret value and replaces it with `[REDACTED]` in the notebook output, job logs, and Spark UI. This is a safety feature — even if a developer accidentally prints a secret, it is never visible in logs.

The variable `password` **does contain the real value** in memory. The redaction only happens at the output layer. You can pass `password` to a JDBC URL, a Spark config, or an HTTP header — it works correctly. The protection is in the display, not the computation.

---

**A3**
No, the notebook does not need to be updated.

With an AKV-backed scope, Databricks reads the secret value from Key Vault **at the time `dbutils.secrets.get()` is called** — every run, every time. When the secret is rotated in Key Vault, the next notebook run automatically picks up the new value because Databricks queries Key Vault fresh on every call.

This is the key advantage of AKV-backed scopes over Databricks-managed scopes: rotation is transparent. With a Databricks-managed scope, you would have to manually update the stored secret in Databricks as well.

---

**A4**
URL format:
```
https://<workspace-url>/#secrets/createScope
```

This page is not reachable from the normal workspace navigation because Databricks intentionally excludes it from the sidebar menu. Secret scope creation is a privileged admin operation — hiding it from the main UI reduces accidental access. Only a user who knows this specific URL and has admin rights can reach it.

---

**A5**
`Manage Principal: All Users` means any user in the workspace can manage the scope — add secrets, delete secrets, change scope settings.

Risk: a non-admin user could add a fake secret with the same name as a real one, or delete secrets that notebooks depend on.

It should be changed to `Manage Principal: Creators` — only the user who created the scope (or a workspace admin) can manage it. Regular users can still **read** secrets from the scope (if they have `READ` permission), but they cannot change the scope configuration.

---

**A6**
`dbutils.secrets.list(scope="kv-scope")` — returns a list of `SecretMetadata` objects. Each object has a `.key` attribute (the secret name). It does NOT return values — only names. Use this to verify which secrets exist in a scope.

`dbutils.secrets.get(scope="kv-scope", key="sp-client-id")` — returns the **actual string value** of the secret named `sp-client-id`. The value is real in memory but shows as `[REDACTED]` if printed.

---

## Service Principal & Storage Access

**A7**
A Service Principal is an identity in Azure Active Directory (Azure AD) created for an application, service, or automated tool. It has its own Client ID and Client Secret — like a username and password for a service, not a person.

Databricks uses a Service Principal (not a user account) to access ADLS Gen2 because:
1. A user account is tied to a person — if they leave, the pipeline breaks
2. User accounts require interactive login — automated jobs cannot do that
3. Service Principals can have fine-grained RBAC roles (e.g. `Storage Blob Data Reader` on one specific container)
4. Credentials (client secret) can be rotated without affecting the human user's account
5. Audit logs show `service-principal-name` in access logs, not a person's email

---

**A8**
`abfss://` stands for Azure Blob File System Secure. It is the URI scheme for accessing Azure Data Lake Storage Gen2 (and Blob storage with HNS enabled) from Spark/Databricks.

Full URI for the `bronze` container in `stadlsdev001`:
```
abfss://bronze@stadlsdev001.dfs.core.windows.net/
```

Format breakdown:
```
abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
  abfss    = scheme
  bronze   = container name
  stadlsdev001 = storage account name
  dfs.core.windows.net = ADLS Gen2 endpoint
  /        = root of the container (add path after this)
```

---

**A9**
The `spark.conf.set(...)` calls configure the SparkSession for the current cluster session only. When a new job cluster is created, it starts a brand new SparkSession with no inherited configuration.

The job cluster has no idea about the credentials that were set manually on the dev cluster — those config values were set at runtime, not in the cluster definition.

Fix: put the `spark.conf.set(...)` calls inside the notebook itself (Cell 1), reading secrets with `dbutils.secrets.get()`. This way, every time the notebook runs (on any cluster), it configures its own SparkSession with the correct credentials.

Alternatively: add the Spark config entries directly to the job cluster configuration in the cluster definition → Advanced options → Spark config. But using `dbutils.secrets` in the notebook is better because it does not expose secrets in the cluster config UI.

---

**A10**
Two security risks of using the storage account access key:

1. **Full access to the entire storage account:** An access key grants full read/write/delete access to ALL containers in the storage account. If the key is leaked, an attacker can access, modify, or delete all data — not just the container Databricks needs. A Service Principal can be granted access to one specific container only.

2. **Hard to rotate without downtime:** If the key needs to be rotated (due to a leak), all applications using that key must be updated simultaneously. A Service Principal client secret can be rotated and updated in Key Vault without changing code, and the old secret can be kept valid for a short overlap period.

---

## Unity Catalog

**A11**
The 3-level namespace in Unity Catalog is: `catalog.schema.table`

Example:
```sql
SELECT * FROM dev_catalog.bronze.payments
```

- `dev_catalog` = the catalog (top level, like a database server)
- `bronze` = the schema (like a database)
- `payments` = the table (like a table in that database)

This is the same as the 3-part name in traditional SQL: `database.schema.table`, but in Databricks the top level is called a catalog instead of a database.

---

**A12**
- **Storage Credential:** holds the authentication details (Service Principal: client ID, client secret, tenant ID). It is the "identity" Unity Catalog uses to prove who it is to Azure storage. It does NOT specify which storage path is allowed — just the credentials.

- **External Location:** maps a storage URL prefix to a storage credential. It says: "use credential X to access any path under `abfss://bronze@stadlsdev001...`". Unity Catalog checks external locations when you create external tables or volumes — the location's URL must cover the table's path.

Analogy: the credential is the key; the external location is which doors that key opens.

---

**A13**
The external location covers `abfss://bronze@stadlsdev001...` (the `bronze` container). The engineer is trying to create a table at `abfss://silver@stadlsdev001...` (the `silver` container). Unity Catalog checks: does any external location cover this URL prefix? No — the `silver` container is not covered. Access is denied.

Fix: create a second external location for `abfss://silver@stadlsdev001.dfs.core.windows.net/`, using the same or a different storage credential.

---

**A14**
Without `LOCATION`, Databricks creates an **internal (managed) table**. The Delta files are stored in the Unity Catalog managed storage location — a storage account that was configured when the Unity Catalog metastore was set up (not your ADLS).

The engineer does not choose the file path — Databricks assigns it automatically, typically something like:
```
abfss://unity-catalog@<uc-managed-storage>.dfs.core.windows.net/<metastore-id>/<catalog>/<schema>/<table>/
```

If you `DROP TABLE my_table`, the files at that managed location are permanently deleted.

---

## External vs Internal Tables

**A15**
The key difference is **who owns the data files and what happens when the table is dropped**:

- **Internal (managed):** Databricks manages the file location. When you `DROP TABLE`, Databricks deletes the Delta files. You never specify a `LOCATION` when creating an internal table.

- **External:** You specify the file location (`LOCATION 'abfss://...'`). When you `DROP TABLE`, only the metadata is removed from the catalog. The actual files on ADLS are not touched.

---

**A16**
Nothing happens to the files on ADLS. An external table's `DROP TABLE` only removes the metadata entry from the Unity Catalog. The Delta files at the specified ADLS location remain completely intact.

This is intentional — Unity Catalog assumes that files on external storage are owned by the data team, not by Databricks. Dropping the table is like removing a shortcut from your desktop — the actual file it pointed to still exists.

To also delete the files, you would need to explicitly run `dbutils.fs.rm(path, True)` after dropping the table.

---

**A17**
The data is deleted. Without a `LOCATION` clause, the table is internal (managed). When Databricks drops a managed table, it deletes the Delta files from Unity Catalog's managed storage.

The engineer should have used `LOCATION` if they wanted to keep the data after dropping the table. This is one of the most common accidental data loss scenarios in Databricks — forgetting `LOCATION` on a production table.

---

**A18**
Run:
```sql
DESCRIBE EXTENDED <catalog>.<schema>.<table_name>
```

In the output, find the row where `col_name = 'Type'`. The value will be either:
- `MANAGED` (internal)
- `EXTERNAL`

Also check `Location` — for a managed table it points to Unity Catalog's internal storage; for an external table it points to your own ADLS path.

---

**A19**
Yes, the external table will see the new data automatically, **as long as the new files are valid Delta format**.

A Delta table's `_delta_log/` folder contains the transaction log — a list of all files that are part of the table. When ADF writes new files to the same ADLS path using a Delta-aware writer (like the ADF Delta sink), it appends entries to the transaction log. Databricks reads the log on every query, so it automatically discovers the new files.

If ADF writes raw Parquet files (not Delta) to the same path, the external table will NOT see them — the Delta transaction log would not know about files added outside of Delta protocol.

---

## External Volumes

**A20**
An external volume is a Unity Catalog object that makes a storage path (ADLS or Blob) accessible as a file system under `/Volumes/<catalog>/<schema>/<volume>/`.

Access in a notebook:
```python
volume_path = "/Volumes/dev_catalog/bronze/blob_files/"
files = dbutils.fs.ls(volume_path)
```

You can also read files directly with Spark:
```python
df = spark.read.option("header", "true").csv(f"{volume_path}myfile.csv")
```

---

**A21**
| | External Table | External Volume |
|---|---|---|
| Data format | Delta (required) | Any (CSV, JSON, images, etc.) |
| Access method | SQL `SELECT *` | File path `/Volumes/...` |
| Schema | Yes — defined columns | No — raw files |
| SQL queryable | Yes directly | No — read into DataFrame first |

Use case for external table: a Silver payments table that analysts query with SQL in Databricks SQL or Power BI.

Use case for external volume: a landing zone where ADF drops raw CSV files from an API — engineers read them with `spark.read.csv(volume_path)` and process them.

---

**A22**
The query fails because `blob_files` is a volume, not a table. You cannot `SELECT *` from a volume — it has no schema.

To read CSV files from the volume:
```python
# Step 1 — read into a DataFrame
df = spark.read.option("header", "true").csv("/Volumes/dev_catalog/bronze/blob_files/")

# Step 2 — register as a temp view if you want to use SQL
df.createOrReplaceTempView("blob_data")
```

```sql
%sql
SELECT * FROM blob_data
```

---

**A23**
The files in `stblobdev001` are NOT deleted. `DROP VOLUME` only removes the volume metadata from Unity Catalog — exactly the same behaviour as `DROP TABLE` on an external table. External volumes are external — Databricks does not own the files and will not delete them.

The Blob storage container and all its files remain untouched. You can recreate the volume at any time pointing to the same path and the files will be accessible again.

---

**A24**
Yes, you can write Delta format data to a volume path:
```python
df.write.format("delta").save("/Volumes/dev_catalog/bronze/blob_files/my_delta/")
```

However, you cannot query it with a normal `SELECT * FROM volume_name`. To query it as a Delta table, you need to register it as an external table:
```sql
CREATE TABLE dev_catalog.bronze.my_table
USING DELTA
LOCATION '/Volumes/dev_catalog/bronze/blob_files/my_delta/'
```

Or query it directly without registration:
```python
df = spark.read.format("delta").load("/Volumes/dev_catalog/bronze/blob_files/my_delta/")
```

---

## ADF & Databricks Integration

**A25**
The ADF Databricks Notebook Activity is an ADF activity that triggers a Databricks notebook and waits for it to complete. ADF monitors the notebook execution and captures the exit value.

To connect to a Databricks workspace, it requires a **Databricks Linked Service** which contains:
- The workspace URL
- Authentication: either a Personal Access Token (PAT) or a Service Principal (client ID + secret)
- Cluster config: new job cluster settings OR reference to an existing cluster ID

---

**A26**
By default, the Notebook Activity has a dependency on the Copy Activity succeeding. If the Copy Activity fails (red X), the Notebook Activity is **skipped** — it does not run.

In ADF, when you draw the green arrow (success dependency) from Copy to Notebook, it means: "only execute Notebook if Copy succeeded." A failed Copy breaks the chain and the pipeline overall shows as failed.

---

**A27**
The notebook reads it with `dbutils.widgets`:

```python
# Cell 1 — must define the widget before reading it
dbutils.widgets.text("env", "dev", "Environment")

# Cell 2 — read the value injected by ADF
env = dbutils.widgets.get("env")
print(f"Environment: {env}")  # prints: Environment: prod
```

The key in the ADF base parameters (`env`) must match exactly the key in `dbutils.widgets.text("env", ...)`. Case-sensitive. If the keys don't match, the widget uses its default value instead of the ADF-supplied value.

---

**A28**
The extra 5 minutes is **job cluster startup time**. When ADF triggers a new job cluster:
- Azure must provision the VM(s): ~2–3 minutes
- The Databricks runtime must initialise on the VM: ~1–2 minutes
- Only then does the notebook start executing

The dev all-purpose cluster is already running, so the notebook starts immediately.

Fix: use an **instance pool** in the job cluster configuration. The pool keeps VMs pre-warmed so cluster startup drops from ~5 minutes to ~30 seconds. The notebook code itself runs in the same 4 minutes — only the startup is different.

---

**A29**
The exit value appears in the ADF activity **Output** tab in the pipeline monitoring view.

In ADF Monitor:
1. Open the pipeline run
2. Click the Notebook Activity row
3. Click the **Output** tab
4. Find `runOutput`:
   ```json
   {
     "runOutput": "PROCESSED: 5000 rows",
     "effectiveIntegrationRuntime": "...",
     "executionDuration": 245
   }
   ```

This value can also be referenced by downstream ADF activities using the expression:
```
@activity('Run Silver Transform').output.runOutput
```

---

**A30**
Complete access control design for VoltGrid:

**Secrets:**
- Azure Key Vault `key-vault-session-ded` holds: `sp-client-id`, `sp-client-secret`, `sp-tenant-id`
- Databricks AKV-backed secret scope `kv-scope` points to this Key Vault
- All notebooks read credentials with `dbutils.secrets.get(scope="kv-scope", key=...)`

**ADLS Gen2 access (stadlsdev001 — external tables):**
- Unity Catalog storage credential `sp-stadls-credential` holds the Service Principal details
- Unity Catalog external location `ext-loc-stadls` maps `abfss://bronze@stadlsdev001.dfs.core.windows.net/` to this credential
- External tables registered under `dev_catalog.bronze.*` point to paths within this external location
- The Service Principal has `Storage Blob Data Contributor` role on `stadlsdev001`

**Blob Storage access (stblobdev001 — external volumes):**
- Unity Catalog storage credential (same SP or a separate one)
- Unity Catalog external location `ext-loc-stblob` maps `abfss://files@stblobdev001.dfs.core.windows.net/`
- External volume `dev_catalog.bronze.blob_files` registered at this path
- Accessed in notebooks via `/Volumes/dev_catalog/bronze/blob_files/`

**ADF triggers Silver transform notebook:**
- ADF Databricks Linked Service `ls_databricks_dev` uses the Service Principal to authenticate to `dbw-ev-dev`
- The Notebook Activity points to `/Shared/voltgrid/silver_transform`
- Base parameters pass `env` and `adf_pipeline_name` into the notebook via widgets
- The notebook reads credentials from `kv-scope` to access `stadlsdev001`
- ADF authenticates to Databricks using the Service Principal (client ID + secret from ADF Key Vault linked service), NOT a PAT
