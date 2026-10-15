# Day 3 — Databricks Unity Catalog Setup + Control Table

**Duration:** 2 hours | **Goal:** Set up the Unity Catalog infrastructure that gives Databricks credential-free access to ADLS Gen2, create the Delta control table that will drive the ADF pipeline, and build the two notebooks ADF will call to read and update pipeline state.

> **Dependency order:** Parts 1–3 must complete in order before Day 4 (ADF pipeline build) can begin. Do not start ADF until the control table is verified and both notebooks are tested.

---

## What We Are Building Today

```
Day 3 (this session):              Day 4 (next session):
──────────────────────────────     ──────────────────────────────────────────
Access Connector + MI              ADF linked service for Databricks
Storage Credential                 pl_bronze_incremental_load pipeline
External Location                  Until loop pagination
ev_catalog / ctl schema            Source dataset update
pipeline_control Delta table       Tumbling Window trigger
nb_read_control_table              Watermark update via notebook activity
nb_update_watermark
```

### Control Table — What It Drives

```
bronze/ctl/pipeline_control (Delta table)
  ├── endpoint          →  which API path to call
  ├── sink_base_path    →  where in Bronze to land the files
  ├── file_prefix       →  stable file name prefix (e.g. "payments")
  ├── page_size         →  records per API call (1000)
  ├── last_watermark    →  updated_after value for next incremental run
  └── load_type         →  "full" or "incremental"

ADF reads this at pipeline start → uses values for all subsequent activities
```

---

## Architecture: Day 3 Scope

```
[Databricks Unity Catalog — one-time setup]
  Access Connector (MI) → Storage Blob Data Contributor → evdatalakedev2
  Storage Credential: mi-ev-credential
  External Location:  ext-loc-ev-bronze  (abfss://bronze@evdatalakedev2...)
  Catalog: ev_catalog | Schema: ctl
  Delta Table: ev_catalog.ctl.pipeline_control

[Databricks Notebooks — created and tested today]
  nb_read_control_table  →  reads control row, exits with JSON → ADF consumes in Day 4
  nb_update_watermark    →  updates last_watermark after load → ADF calls in Day 4
```

---

## Part 1 — Databricks Account Setup (Unity Catalog Prerequisites)

Before creating the control table, the Unity Catalog infrastructure must exist. This is a one-time setup.

### 1.1 Prerequisites Check

Verify in Azure Portal:
- Databricks workspace exists (`dbw-ev-intelligence-dev` in `rg-ev-intelligence-dev`)
- ADLS Gen2 `evdatalakedev2` has `bronze` container
- `MicrosoftDatabricks` resource provider is registered

```bash
az provider show --namespace Microsoft.Databricks --query registrationState -o tsv
# Expected: Registered
```

### 1.2 Create Access Connector for Azure Databricks

The Access Connector is an Azure resource whose Managed Identity Unity Catalog uses to access storage — no password, no rotation.

**Via Portal:**
1. Portal → search `Access Connector for Azure Databricks` → **+ Create**
2. Fill:

   | Field | Value |
   |---|---|
   | Subscription | yours |
   | Resource group | `rg-ev-intelligence-dev` |
   | Name | `ac-ev-databricks-02` |
   | Region | `Central India` |

3. **Review + Create** → **Create** (~1 minute)
4. Go to resource → **Overview** → copy the **Resource ID**:
   ```
   /subscriptions/<sub-id>/resourceGroups/rg-ev-intelligence-dev/providers/Microsoft.Databricks/accessConnectors/ac-ev-databricks
   ```
   Save this — you paste it into Databricks in Part 1.4.

**Via CLI:**
```bash
az databricks access-connector create \
  --resource-group rg-ev-intelligence-dev \
  --name ac-ev-databricks-02 \
  --location centralindia \
  --identity-type SystemAssigned
```

### 1.3 Grant Access Connector Permission on ADLS Gen2

The Access Connector's Managed Identity must have `Storage Blob Data Contributor` on `evdatalakedev2`.

**Via Portal:**
1. Portal → **Storage accounts** → `evdatalakedev2` → **Access Control (IAM)**
2. **+ Add** → **Add role assignment**
3. Role: `Storage Blob Data Contributor` → **Next**
4. Members: **Managed identity** → **+ Select members**
   - Managed identity type: `Access Connector for Azure Databricks`
   - Select: `ac-ev-databricks-02`
5. **Review + assign**

**Via CLI:**
```bash
STORAGE_ID=$(az storage account show \
  --name evdatalakedev2 \
  --resource-group rg-ev-intelligence-dev \
  --query id -o tsv)

AC_PRINCIPAL=$(az resource show \
  --resource-group rg-ev-intelligence-dev \
  --resource-type "Microsoft.Databricks/accessConnectors" \
  --name ac-ev-databricks-02 \
  --query "identity.principalId" -o tsv)

az role assignment create \
  --assignee-object-id $AC_PRINCIPAL \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ID
```

Wait 2 minutes after assignment before proceeding.

---

## Part 2 — Unity Catalog Setup in Databricks

### 2.1 Create Storage Credential

The Storage Credential tells Unity Catalog how to authenticate to storage — it holds the Access Connector reference.

1. Open Databricks workspace → left sidebar → **Catalog** (grid icon)
2. Left panel → **External Data** → **Credentials** tab
3. **Create credential** (top right)

Fill in:

| Field | Value |
|---|---|
| Radio button | `Storage Credential` (left option) |
| Credential Type | `Azure Managed Identity` |
| Credential name | `mi-ev-credential` |
| Access connector ID | paste the Resource ID from Part 1.2 |
| User assigned managed identity ID | leave blank |
| Comment | `MI credential for evdatalakedev2` |

Click **Create** → `mi-ev-credential` appears in the list.

> If you see `You do not have permission to create credentials` → you need **Metastore Admin** or **Account Admin** role in Databricks. Ask your workspace admin, or assign yourself via `https://accounts.azuredatabricks.net` → **Account** → **Users** → your user → **Roles**.

### 2.2 Create External Location

The External Location maps an `abfss://` path to the Storage Credential.

1. **External Data** → **External Locations** tab → **+ Create location**

Fill in:

| Field | Value |
|---|---|
| External location name | `ext-loc-ev-bronze` |
| Storage type | `Azure Data Lake Storage` (default) |
| URL | `abfss://bronze@evdatalakedev2.dfs.core.windows.net/` |
| Storage credential | `mi-ev-credential` |

Click **Create** → **Test connection**

Expected result:
```
✅ Read    ✅ List    ✅ Write    ✅ Delete    ✅ Hierarchical Namespace Enabled
❌ File Events Resource Provision   ← optional warning, not a blocker
❌ File Events Resource Teardown    ← optional warning, not a blocker
```

If Read/List/Write fail with `Authorization failed` → the Access Connector MI doesn't have `Storage Blob Data Contributor` yet — re-check Part 1.3, wait 2 more minutes.

> **What Unity Catalog provides:** Once this External Location exists, any notebook that reads/writes to `abfss://bronze@evdatalakedev2.dfs.core.windows.net/` using a path covered by this location will have credentials injected automatically. No `spark.conf.set`, no account key, no mount.

### 2.3 Create Catalog and Schema

A **catalog** is the top-level namespace. A **schema** holds the control tables.

```sql
-- Run in a Databricks notebook (SQL cell)
CREATE CATALOG IF NOT EXISTS ev_catalog;
USE CATALOG ev_catalog;

CREATE SCHEMA IF NOT EXISTS ctl
COMMENT 'Control tables — pipeline state, watermarks, config';
```

Or via UI:
1. **Catalog** → **+ Create catalog** → name: `ev_catalog` → **Create**
2. Click `ev_catalog` → **+ Create schema** → name: `ctl` → **Create**

---

## Part 3 — Create the Control Table

The control table is a Delta table in `bronze/ctl/` that drives the entire pipeline. ADF reads it at the start of every run.

### 3.1 Control Table Schema

```
ev_catalog.ctl.pipeline_control
┌──────────────────┬───────────────┬────────────────────────────────────────────────────────┐
│ Column           │ Type          │ Purpose                                                │
├──────────────────┼───────────────┼────────────────────────────────────────────────────────┤
│ source_name      │ STRING        │ Unique key — e.g. "payments", "sessions"               │
│ endpoint         │ STRING        │ API relative path — "/api/db/payments/"                │
│ sink_base_path   │ STRING        │ Bronze directory — "api/payments"                      │
│ file_prefix      │ STRING        │ File name stem — "payments"                            │
│ page_size        │ INT           │ Records per API call — 1000                            │
│ load_type        │ STRING        │ "full" or "incremental"                                │
│ last_watermark   │ STRING        │ ISO 8601 UTC — used as updated_after in next run       │
│ last_run_ts      │ TIMESTAMP     │ When this row was last updated                         │
│ is_active        │ BOOLEAN       │ false = skip this source                               │
└──────────────────┴───────────────┴────────────────────────────────────────────────────────┘
```

### 3.2 Create the Table

Run in a Databricks SQL cell or notebook:

```sql
CREATE TABLE IF NOT EXISTS ev_catalog.ctl.pipeline_control (
    source_name    STRING       NOT NULL,
    endpoint       STRING       NOT NULL,
    sink_base_path STRING       NOT NULL,
    file_prefix    STRING       NOT NULL,
    page_size      INT          NOT NULL,
    load_type      STRING       NOT NULL,
    last_watermark STRING       NOT NULL,
    last_run_ts    TIMESTAMP,
    is_active      BOOLEAN      NOT NULL,
    CONSTRAINT pk_source_name PRIMARY KEY (source_name)
)
USING DELTA
LOCATION 'abfss://bronze@evdatalakedev2.dfs.core.windows.net/ctl/pipeline_control'
COMMENT 'ADF ingestion control table — drives endpoint, path, watermark per source';
```

> `LOCATION` makes this an **External Delta table** — the files live inside the External Location at `bronze/ctl/pipeline_control/`. Unity Catalog manages the metadata; the data lives in your own ADLS.

### 3.3 Seed the Payments Row

```sql
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('payments', '/api/db/payments/', 'api/payments', 'payments', 1000, 'full', '1900-01-01T00:00:00Z', true);
```

Verify:
```sql
SELECT * FROM ev_catalog.ctl.pipeline_control;
```

Expected:
```
source_name | endpoint           | sink_base_path | file_prefix | page_size | load_type | last_watermark          | last_run_ts | is_active
payments    | /api/db/payments/  | api/payments   | payments    | 1000      | full      | 1900-01-01T00:00:00Z    | null        | true
```

### 3.4 Create a Databricks Notebook to Read the Control Table

ADF will call this notebook as an activity to retrieve control values. Create `nb_read_control_table` in Databricks:

```python
# nb_read_control_table
# Parameters passed by ADF via Databricks job parameters:
#   source_name  (string) — which row to read

dbutils.widgets.text("source_name", "payments")
source_name = dbutils.widgets.get("source_name")

row = spark.sql(f"""
    SELECT endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark
    FROM ev_catalog.ctl.pipeline_control
    WHERE source_name = '{source_name}'
      AND is_active = true
""").collect()

if not row:
    raise Exception(f"No active control row for source_name='{source_name}'")

r = row[0]

# Return values to ADF via notebook exit value (JSON string)
import json
result = {
    "endpoint":       r["endpoint"],
    "sink_base_path": r["sink_base_path"],
    "file_prefix":    r["file_prefix"],
    "page_size":      int(r["page_size"]),
    "load_type":      r["load_type"],
    "last_watermark": r["last_watermark"]
}

dbutils.notebook.exit(json.dumps(result))
```

> ADF's Databricks notebook activity returns `dbutils.notebook.exit(value)` as `activity('act_read_ctl').output.runOutput`. The JSON string is then parsed in ADF expressions.

### 3.5 Create a Notebook to Update the Watermark

ADF calls this after all pages are successfully loaded.

```python
# nb_update_watermark
# Parameters passed by ADF:
#   source_name    (string)
#   new_watermark  (string) — ISO 8601 UTC timestamp

dbutils.widgets.text("source_name", "payments")
dbutils.widgets.text("new_watermark", "1900-01-01T00:00:00Z")

source_name   = dbutils.widgets.get("source_name")
new_watermark = dbutils.widgets.get("new_watermark")

spark.sql(f"""
    UPDATE ev_catalog.ctl.pipeline_control
    SET
        last_watermark = '{new_watermark}',
        load_type      = 'incremental',
        last_run_ts    = current_timestamp()
    WHERE source_name = '{source_name}'
""")

print(f"Watermark updated: {source_name} → {new_watermark}")
dbutils.notebook.exit("OK")
```

---

## Verify: Day 3 Checklist

- [ ] Access Connector `ac-ev-databricks-02` exists in `rg-ev-intelligence-dev`
- [ ] Access Connector MI has `Storage Blob Data Contributor` on `evdatalakedev2`
- [ ] Storage Credential `mi-ev-credential` created in Databricks (references `ac-ev-databricks-02`)
- [ ] External Location `ext-loc-ev-bronze` created — Read/Write/Delete test green
- [ ] Catalog `ev_catalog` and schema `ctl` created
- [ ] Table `ev_catalog.ctl.pipeline_control` exists at `abfss://bronze@evdatalakedev2.dfs.core.windows.net/ctl/pipeline_control`
- [ ] Payments row: `last_watermark = '1900-01-01T00:00:00Z'`, `load_type = 'full'`, `is_active = true`
- [ ] `nb_read_control_table` exits with valid JSON when run manually
- [ ] `nb_update_watermark` updates `last_watermark` and `load_type` when run manually
- [ ] Control table reset to `load_type = 'full'`, `last_watermark = '1900-01-01T00:00:00Z'` before Day 4

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `Authorization failed` on External Location test | Access Connector MI role not propagated | Wait 2 more minutes after RBAC assignment |
| `You do not have permission to create credentials` | Missing Metastore Admin or Account Admin role | Assign role at `accounts.azuredatabricks.net` → Users |
| `Credential not found` | Storage credential deleted or renamed | Re-create `mi-ev-credential` in Catalog → External Data → Credentials |
| `Table not found` after `CREATE TABLE` | Catalog or schema not set correctly | Run `USE CATALOG ev_catalog` before the `CREATE TABLE` |
| `delta.io.delta.exceptions.DeltaIllegalArgumentException` on LOCATION | Path already contains non-Delta files | Use a fresh path — e.g. `ctl/pipeline_control_v2` then rename |
| Notebook exits with no output | `dbutils.notebook.exit()` not reached — exception before it | Add try/except in notebook to always exit with an error JSON |

---

## Quick Reference

```
Control table : ev_catalog.ctl.pipeline_control
                Location: abfss://bronze@evdatalakedev2.dfs.core.windows.net/ctl/pipeline_control

Access chain:
  ac-ev-databricks-02 (MI)  →  Storage Blob Data Contributor  →  evdatalakedev2
  mi-ev-credential          →  references ac-ev-databricks-02
  ext-loc-ev-bronze         →  abfss://bronze@evdatalakedev2.dfs.core.windows.net/

Notebooks:
  /Workspace/ev-project/nb_read_control_table   → reads row, exits with JSON
  /Workspace/ev-project/nb_update_watermark     → updates last_watermark + load_type

Next session (Day 4):
  ADF linked service ls_databricks → pipeline pl_bronze_incremental_load
```
