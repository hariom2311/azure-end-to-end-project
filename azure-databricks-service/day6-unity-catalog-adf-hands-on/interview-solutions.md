# Day 6 — Interview Solutions: Unity Catalog, Tables, Volumes & ADF

---

## Storage Credentials & External Locations

**A1**
A Storage Credential is a Unity Catalog object that stores the authentication details a Service Principal uses to access Azure storage. It holds:
- **Directory (tenant) ID** — identifies your Azure AD tenant
- **Application (client) ID** — the Service Principal's unique ID
- **Client secret** — the password for the Service Principal

It is needed because Unity Catalog needs to authenticate to Azure storage on your behalf when notebooks access external tables or volumes. Instead of every notebook configuring `spark.conf.set()` with these credentials, you store them once in a Storage Credential and all notebooks benefit automatically.

---

**A2**
- **Storage Credential** = the identity (who you are). Holds the SP credentials.
- **External Location** = the access rule (what you can access and with which identity). Maps a storage path prefix to a Storage Credential.

For **two storage accounts** with **one Service Principal** that has access to both:
- You need **one Storage Credential** (the SP is the same)
- You need **two External Locations** — one for each storage account's container/path prefix

If the SP for each account is different, you would need two Storage Credentials.

---

**A3**
The External Location covers `abfss://bronze@stadlsdev001.dfs.core.windows.net/` — only the `bronze` container. The table is at `abfss://silver@stadlsdev001.dfs.core.windows.net/folder/` — the `silver` container.

Unity Catalog checks external locations at **query time** (not at CREATE TABLE time). At query time, it finds no external location covering the `silver` container → access denied.

Fix: create a second External Location for `abfss://silver@stadlsdev001.dfs.core.windows.net/` using the same or a different Storage Credential.

---

**A4**
No, it will not work. The External Location URL is `abfss://bronze@stadlsdev001.dfs.core.windows.net/raw/` — it only covers paths that START with `.../raw/`. The table is at `.../processed/table1/` — this path does not start with `.../raw/`, so it is not covered.

External Location URL matching is a **prefix match**: any path that begins with the External Location URL is covered. Paths outside that prefix are not.

Fix: create an External Location at the broader prefix `abfss://bronze@stadlsdev001.dfs.core.windows.net/` to cover the entire container.

---

**A5**
The most likely cause is that the Service Principal does not have the correct **RBAC role on the storage account** in Azure.

The Storage Credential is valid (correct credentials), but the SP does not have permission to read the storage. The SP needs `Storage Blob Data Contributor` (or at minimum `Storage Blob Data Reader`) role assigned on the storage account or the specific container.

Fix:
1. Azure Portal → **Storage accounts** → `stadlsdev001`
2. Left menu → **Access Control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. Role: `Storage Blob Data Contributor`
5. Assign access to: `User, group, or service principal`
6. Search for your SP name → select it → **Save**
7. Wait 1–2 minutes, then test the connection again.

---

**A6**
**Option A — Account Console:**
1. Navigate to `https://accounts.azuredatabricks.net`
2. Sign in
3. Left sidebar → **Catalog**
4. Top tab → **External locations**
5. Left sub-tab → **Credentials**
6. Click **+ Add a credential**

**Option B — Workspace UI:**
1. Open Databricks workspace
2. Left sidebar → **Catalog** (grid icon)
3. Left panel → **External Data**
4. Click **Credentials** tab
5. Click **Create credential** (top right)

---

## Unity Catalog Hierarchy

**A7**
```
Metastore              ← account-level, ONE per Azure region
  └── Catalog          ← top-level namespace (like a database server)
        └── Schema     ← like a database (groups tables and volumes)
              ├── Table     ← structured data (internal or external)
              └── Volume    ← file storage (internal or external)
```

The **Metastore** is shared across all workspaces in the Azure Databricks account within the same region. Catalogs, schemas, tables, and volumes created in the metastore are accessible from any workspace that is attached to it.

---

**A8**
They need to ensure all three workspaces are **attached to the same Unity Catalog Metastore**. A metastore is account-level and region-level — if all three workspaces are in the same Azure region and account, they share one metastore, and all Unity Catalog objects (catalogs, schemas, tables, volumes) are automatically visible from all three workspaces.

If the workspaces are in different regions, each region has its own metastore and tables are not automatically shared.

---

**A9**
When a catalog is created with `Storage = Default` (no custom storage location), internal (managed) tables in that catalog store their Delta files in the **Unity Catalog Metastore's managed storage** — a storage account that was configured by the account admin when the metastore was set up. This is typically a dedicated ADLS Gen2 account managed by Databricks.

The path looks like:
`abfss://unitycatalog@<managed-account>.dfs.core.windows.net/<metastore-id>/<catalog>/<schema>/<table>/`

You did not choose this path — Databricks assigned it. Dropping an internal table deletes files at this path.

---

## Internal vs External Tables

**A10**
The `LOCATION` clause determines the table type.

**Internal table (no LOCATION):**
```sql
CREATE TABLE dev_catalog.bronze.sales (
    id     INT,
    amount DOUBLE
)
USING DELTA
```

**External table (with LOCATION):**
```sql
CREATE TABLE dev_catalog.bronze.sales
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/sales/'
```

---

**A11**
The analyst is wrong. Dropping an external table only removes the **metadata entry** from Unity Catalog — it is like removing a shortcut from your desktop. The actual Delta files at the `LOCATION` path on ADLS are completely untouched.

To verify: after `DROP TABLE`, run:
```python
dbutils.fs.ls("abfss://bronze@stadlsdev001.dfs.core.windows.net/orders/")
```
The files are still there. The table can be re-registered with `CREATE TABLE ... LOCATION '...'` and all data is immediately accessible again.

---

**A12**
The senior engineer is correct that the data is permanently gone from Databricks. The internal table's files at the Databricks-managed storage location are deleted when the table is dropped.

The data engineer's point is partially correct — if the data can be regenerated by re-running the upstream pipeline, it is not "lost" in the business sense. But from Databricks' perspective, the files are deleted permanently. There is no recycle bin.

For production tables, this is why you should always use external tables (`LOCATION` clause pointing to your own ADLS) — so that a `DROP TABLE` cannot destroy data.

---

**A13**
**Delta sink:** Yes, the external table will see the new 5000 rows automatically. ADF's Delta sink writes new Parquet files AND updates the `_delta_log/` transaction log at the same ADLS path. Databricks reads the transaction log on every query, so it immediately sees the new files.

**Parquet sink:** No, the external table will NOT see the new rows. ADF writes raw Parquet files to the path but does NOT update the `_delta_log/`. The Delta protocol requires all changes to go through the transaction log — files added outside it are invisible to the Delta table.

---

**A14**
The `LOCATION` clause was missing when the table was created. Without `LOCATION`, Unity Catalog creates an internal (managed) table and assigns its own managed storage path.

Fix without data loss:
1. Read the existing data: `df = spark.read.table("dev_catalog.bronze.orders")`
2. Write to the correct ADLS path: `df.write.format("delta").mode("overwrite").save("abfss://bronze@stadlsdev001.dfs.core.windows.net/orders/")`
3. Drop the managed table: `DROP TABLE dev_catalog.bronze.orders` (this deletes the misplaced managed files)
4. Re-create as external: `CREATE TABLE dev_catalog.bronze.orders USING DELTA LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/orders/'`

---

**A15**
`SELECT *` fails with an error like: `DELTA_MISSING_TRANSACTION_LOG: The schema of your Delta table has changed in an incompatible way` or `FileNotFoundException: Delta log not found`.

The table metadata still exists in Unity Catalog, but the underlying Delta files (including `_delta_log/`) at the ADLS location are gone. Every query fails because Databricks cannot read the table.

**Recovery options:**
1. If the files were soft-deleted (Azure Blob soft delete is enabled on the storage account): restore them through Azure Portal → Storage account → Data protection → Soft deleted blobs.
2. If there is an ADLS backup or snapshot: restore the files from backup.
3. If neither: the data is lost. Drop the table (`DROP TABLE`) to remove the stale catalog entry, then re-populate from the source.

---

## External Volumes

**A16**
SQL to create:
```sql
CREATE EXTERNAL VOLUME IF NOT EXISTS dev_catalog.bronze.raw_files
LOCATION 'abfss://files@stblobdev001.dfs.core.windows.net/'
```

Access in Python:
```python
volume_path = "/Volumes/dev_catalog/bronze/raw_files/"

# List files
for f in dbutils.fs.ls(volume_path):
    print(f.name, f.size)

# Write a file
dbutils.fs.put(f"{volume_path}myfile.txt", "content", overwrite=True)

# Read a CSV
df = spark.read.option("header", "true").csv(f"{volume_path}data.csv")
```

---

**A17**
Yes, they will see the uploaded file. An External Volume is just a Unity Catalog representation of a real storage path. The `/Volumes/dev_catalog/bronze/raw_files/` path maps directly to `abfss://files@stblobdev001.dfs.core.windows.net/`. Any file put in the Blob container by any means — Portal upload, ADF Copy, Azure Storage Explorer, CLI — is immediately visible through the volume path in Databricks, because there is no caching or syncing step. It is the same underlying storage.

---

**A18**
The query fails because `raw_files` is a **volume**, not a table. A volume has no schema — it is a file system abstraction. `SELECT *` requires a relation with defined columns. Unity Catalog does not allow SQL queries directly against a volume name.

Correct way to read CSV data from the volume:
```python
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/dev_catalog/bronze/raw_files/")
df.show()
```

If SQL is needed, register a temp view:
```python
df.createOrReplaceTempView("raw_data")
spark.sql("SELECT * FROM raw_data LIMIT 10").show()
```

---

**A19**
Yes, two External Volumes can point to the same storage path. There is no Unity Catalog restriction against this.

Both notebooks would be reading/writing the same underlying storage simultaneously. File-level access would depend on the storage service — ADLS Gen2 supports concurrent reads and conditional writes. If two notebooks write the same file at the same time, the last write wins (no merge). For Delta data on the same path, concurrent writes would require Delta's ACID transaction support (which only applies to registered Delta tables, not volume paths).

---

**A20**
The operations team is wrong. `DROP VOLUME` only removes the **volume metadata** from Unity Catalog — it does not touch the files on `stblobdev001`. The 500 GB of raw files in the Blob container are completely safe.

The volume is like a bookmark — dropping it removes the bookmark, not the page it pointed to. The files can be accessed again immediately by re-creating the volume:
```sql
CREATE EXTERNAL VOLUME dev_catalog.bronze.raw_files
LOCATION 'abfss://files@stblobdev001.dfs.core.windows.net/'
```

---

## Internal Volumes

**A21**
- `DROP VOLUME dev_catalog.silver.temp_vol` (internal): **Files are deleted** from the Databricks-managed storage location. The files cannot be recovered through Unity Catalog.
- `DROP VOLUME dev_catalog.bronze.raw_vol` (external): **Only the metadata is removed** from Unity Catalog. The files at `abfss://...` remain completely intact on your storage.

The difference is ownership: Databricks owns internal volume storage and cleans it up on drop. External volume storage is owned by you — Databricks only holds a reference.

---

**A22**
Use an **internal volume**. Reasons:
1. The files are not needed after the job — there is no reason to persist them on external storage.
2. An internal volume requires no External Location setup — it works out of the box.
3. When the volume is dropped (cleanup), the files are automatically deleted — no orphan files left on Blob/ADLS.
4. An external volume would leave files on storage indefinitely after the job, creating clutter and unnecessary storage costs.

---

## ADF & Databricks Integration

**A23**
A Databricks Linked Service is an ADF connection object that stores the information needed to connect to a Databricks workspace: the workspace URL, authentication details, and optionally a cluster configuration.

Two authentication methods:
1. **Access Token (PAT — Personal Access Token):** A token generated in Databricks User Settings. Simple but tied to a user account — if the user leaves or the token expires, the Linked Service breaks.
2. **Service Principal:** Uses Client ID and Client Secret from an Azure AD App Registration. Recommended for production because it is not tied to a specific user account.

---

**A24**
```python
# Define widgets — must be called before dbutils.widgets.get()
dbutils.widgets.text("env",        "dev",        "Environment")
dbutils.widgets.text("batch_date", "2024-01-01", "Batch Date")

# Read the values injected by ADF
env        = dbutils.widgets.get("env")
batch_date = dbutils.widgets.get("batch_date")

print(f"Environment: {env}")    # prints: prod
print(f"Batch date:  {batch_date}")  # prints: 2024-01-15
```

The key in `dbutils.widgets.text("key", ...)` must match exactly the key in ADF Base Parameters. Case-sensitive.

---

**A25**
The extra 5 minutes is **job cluster startup time**. When the Notebook Activity is configured with `New job cluster`:
- Azure must provision the VM(s): ~2–3 minutes
- The Databricks runtime initialises on the VM: ~1–2 minutes
- Only then does the notebook begin executing

The all-purpose cluster is already running, so the notebook starts in seconds.

To reduce cluster startup time:
1. **Use an Instance Pool:** Pre-warmed VMs that reduce startup from ~5 minutes to ~30 seconds. Configure in Compute → Pools → Create pool. Then reference the pool in the Linked Service cluster config.
2. **Use an Existing Cluster:** Change the Linked Service to point to an existing all-purpose cluster instead of `New job cluster`. Note: the cluster must be running 24/7, which has a cost.

---

**A26**
The value appears in the ADF pipeline monitoring view:
1. ADF Studio → **Monitor** (clock icon, left sidebar)
2. Click the pipeline run
3. Click the Notebook Activity row
4. Click the **Output** icon (glasses)
5. In the JSON output, find `runOutput`:
   ```json
   { "runOutput": "DONE: 5000 rows" }
   ```

ADF expression to reference it in a downstream activity (e.g. in a Set Variable or another activity's input):
```
@activity('Run Day6 Notebook').output.runOutput
```
Where `Run Day6 Notebook` is the exact `Name` field set in the activity's General tab.

---

**A27**
When the Copy Activity fails:
- The **Notebook Activity is skipped** — because the green (success) dependency arrow means "run only if the upstream activity succeeded". A failed Copy activity does not satisfy the success condition.
- The **pipeline overall shows as Failed** — because a required activity did not succeed.

The Notebook Activity status shows as `Skipped` in the pipeline monitoring output tab.

If you want the Notebook Activity to run regardless of Copy success/failure, use a **blue arrow** (completion dependency) instead of a green arrow.

---

**A28**
The problem is that the ADF Service Principal does not have permission to run the notebook in the engineer's personal `Users/` folder.

Personal folders (`Users/<email>/`) in Databricks are private by default. The Service Principal does not have Can Run permission there.

Fix (two options):
1. **Move the notebook to `Shared/`** — Service Principals have access to the Shared folder by default. Change the ADF Notebook path to `/Shared/adf_notebooks/my_notebook`.
2. **Grant explicit permission** — in Databricks, right-click the notebook in the personal folder → Permissions → Add the Service Principal (search by its display name or client ID) → set `Can Run`.

Option 1 is simpler and is the recommended practice for ADF-triggered notebooks.

---

## Cluster Access Modes

**A29**
Three cluster access modes:

| Mode | Unity Catalog | Multiple users | Languages |
|---|---|---|---|
| Single User | Full support | No — 1 user only | Python, SQL, Scala, R |
| Shared | Full support | Yes — isolated sessions | Python, SQL only |
| No Isolation Shared | Not supported | Yes — no isolation | All languages |

**None** of them support all four languages AND multiple simultaneous users. The tradeoff:
- `Shared` supports multiple users but only Python and SQL (no Scala, no R)
- `Single User` supports all four languages but only one user at a time

For a team cluster that needs Scala/R: each engineer creates their own Single User cluster, or the team uses separate clusters per user.

---

**A30**
Complete design:

**Unity Catalog objects to create:**

```
Storage Credentials:
  sp-landing-credential   → SP with Storage Blob Data Contributor on stblobdev001
  sp-bronze-credential    → SP with Storage Blob Data Contributor on stadlsdev001

External Locations:
  ext-loc-landing  → abfss://landing@stblobdev001.dfs.core.windows.net/  → sp-landing-credential
  ext-loc-bronze   → abfss://bronze@stadlsdev001.dfs.core.windows.net/   → sp-bronze-credential

Catalog:
  prod_catalog  (Standard, default storage)

Schemas:
  prod_catalog.landing  (conceptual — volumes live here)
  prod_catalog.bronze   (Delta tables live here)

External Volume:
  prod_catalog.landing.raw_csv
  LOCATION 'abfss://landing@stblobdev001.dfs.core.windows.net/'
  → ADF Copy Activity drops CSVs here daily

External Tables (after notebook runs):
  prod_catalog.bronze.orders
  LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/orders/'
  → Databricks notebook writes Delta here
```

**ADF Setup:**

```
Linked Service: ls_databricks_prod
  → workspace: dbw-ev-prod
  → New job cluster (or instance pool for faster startup)
  → Auth: Service Principal (client ID + secret)

Pipeline: pl_daily_ingest
  └── Notebook Activity: Run Bronze Transform
        ├── Linked service: ls_databricks_prod
        ├── Notebook path: /Shared/pipelines/bronze_transform
        ├── Base parameters:
        │     env          = prod
        │     input_path   = /Volumes/prod_catalog/landing/raw_csv/
        │     output_table = prod_catalog.bronze.orders
        └── Schedule: daily at 06:00 (Trigger → New/Edit → Recurrence → Daily, 06:00 UTC)

Schedule Trigger: tr_daily_6am
  → Type: Schedule
  → Start date: today
  → Recurrence: Every 1 Day at 06:00 UTC
  → Pipeline: pl_daily_ingest
```

**Notebook design (Shared/pipelines/bronze_transform):**
1. Read parameters via `dbutils.widgets`
2. Read CSVs from `/Volumes/prod_catalog/landing/raw_csv/`
3. Clean and transform with Spark
4. Write as Delta to `abfss://bronze@stadlsdev001.dfs.core.windows.net/orders/`
5. `CREATE TABLE IF NOT EXISTS prod_catalog.bronze.orders USING DELTA LOCATION '...'`
6. `dbutils.notebook.exit(f"SUCCESS: {row_count} rows")`
