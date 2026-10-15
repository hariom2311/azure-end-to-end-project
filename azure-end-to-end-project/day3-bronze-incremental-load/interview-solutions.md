# Day 3 — Interview Solutions: Unity Catalog Setup + Control Table

---

## Unity Catalog and Access Connector

**A1**
An Access Connector is an Azure resource (`Microsoft.Databricks/accessConnectors`) that has a system-assigned Managed Identity. Unity Catalog uses this MI to access storage — no password, no client secret, no rotation needed. Azure manages the identity.

| | Access Connector | Service Principal |
|---|---|---|
| Credentials | None — Azure handles token issuance | Client ID + Secret (expires, must rotate) |
| Risk of leak | None — no secret exists | Secret can be exposed in logs, code, ARM |
| Setup | Create resource + assign RBAC role | Create SP + create secret + store in KV + assign RBAC |
| Lifecycle | Tied to the Access Connector resource | Independent — must be managed separately |

Preferred because it eliminates credential management entirely. RBAC roles on the MI can be revoked immediately if needed — no secret to hunt down and rotate.

---

**A2**
The three-layer chain:

```
Access Connector (Azure resource)
  └── system-assigned Managed Identity
  └── MI has Storage Blob Data Contributor on evdatalakedev2
        ↓ referenced by
  Storage Credential (Unity Catalog object)
    name: mi-ev-credential
    holds: the Access Connector resource ID
        ↓ referenced by
  External Location (Unity Catalog object)
    name: ext-loc-ev-bronze
    URL: abfss://bronze@evdatalakedev2.dfs.core.windows.net/
    credential: mi-ev-credential
```

- **Access Connector:** provides the MI that has actual Azure RBAC access to storage
- **Storage Credential:** Unity Catalog's reference to that MI — decouples Databricks config from the Azure resource ID
- **External Location:** maps a storage path to a credential — tells Unity Catalog which credential to use for any path under that URL

If the External Location is removed: any notebook accessing `abfss://bronze@evdatalakedev2...` fails with `No storage credential found for path`. The Access Connector and Storage Credential still exist — recreating the External Location restores access immediately.

---

**A3**
Most likely cause: the Access Connector's Managed Identity has `Storage Blob Data Reader` instead of `Storage Blob Data Contributor`, or the role was assigned but hasn't propagated yet (Azure RBAC can take up to 5 minutes).

`Storage Blob Data Reader` grants Read + List only. Write requires `Storage Blob Data Contributor`.

Fix:
1. Portal → `evdatalakedev2` → **Access Control (IAM)** → check role assignments for the AC's MI
2. If `Reader` only → add `Storage Blob Data Contributor` role assignment
3. Wait 2 minutes → **Test connection** again

---

**A4**
Unity Catalog's **credential injection** mechanism:

1. Notebook calls `spark.read.format("delta").load("abfss://bronze@evdatalakedev2...")`
2. Spark passes the path to the Unity Catalog driver
3. Unity Catalog looks up registered External Locations — finds `ext-loc-ev-bronze` covering that path
4. Unity Catalog fetches a short-lived OAuth token using the Access Connector's MI
5. Token is injected into the Spark context for this specific path
6. Read proceeds — the notebook author never touches a credential

This is different from `spark.conf.set()` (session-scoped, author must know the credential) and account key (full storage access, no expiry, dangerous if leaked).

---

**A5**
The engineer should NOT block on File Events. The failures are optional warnings, not blockers.

File Events enable Databricks to receive storage notifications via Azure EventGrid (blob created/deleted events). This powers Auto Loader and Delta streaming table auto-refresh. For this project's batch ADF ingestion pattern there is no streaming — File Events are irrelevant.

The three RBAC roles needed to fix File Events (optional, can be done later):
- `Storage Account Contributor` — to register EventGrid subscriptions
- `EventGrid EventSubscription Contributor` — to manage subscriptions
- `Storage Queue Data Contributor` — to write notification events to a queue

---

## Delta Control Table

**A6**
**Managed table:** Unity Catalog owns the data. `DROP TABLE` deletes both the metadata and the underlying Parquet files from storage.

**External table:** Created with `LOCATION 'abfss://...'`. Unity Catalog owns only the metadata. `DROP TABLE` removes the metadata entry only — the Delta files in ADLS Gen2 remain untouched.

This project uses External because:
- The control table lives inside the Bronze container — part of the data lake, not an internal Databricks store
- If the Databricks workspace is destroyed and rebuilt, a `CREATE TABLE ... LOCATION '...'` re-registers the existing data without any data loss
- The watermark history (in the Delta transaction log) is preserved even if Unity Catalog metadata is wiped

---

**A7**
`1900-01-01T00:00:00Z` is the "beginning of time" watermark — it means fetch all records on the first run.

**Why not null:** The watermark is passed into the API URL as `updated_after=<value>`. If the value is `null`:
```
/api/db/payments/?page=1&page_size=1000&updated_after=null
```
The API receives the literal string `"null"` as the date filter. Depending on the server:
- Returns 0 records (filter is invalid)
- Returns an error
- Treats it as a string with no matching dates

All three outcomes cause a broken first run.

**Why not today's date:** The first run must be a full load — fetching all historical records, not just today's changes.

**Why 1900:** Universally understood as epoch/beginning-of-time in ISO 8601. All real `updated_at` timestamps in the API are 2025–2026, so `updated_after=1900-01-01T00:00:00Z` returns everything.

---

**A8**
Two options:

**Option 1 — Drop and recreate (safe only if the path is truly leftover junk):**
```sql
DROP TABLE IF EXISTS ev_catalog.ctl.pipeline_control;
-- Then rerun the CREATE TABLE statement
```
This removes the metadata. The files remain at the LOCATION path but Delta will reinitialise the transaction log.

**Option 2 — Use a fresh path (safest):**
```sql
LOCATION 'abfss://bronze@evdatalakedev2.dfs.core.windows.net/ctl/pipeline_control_v2'
```
Avoids touching existing files entirely. Once verified working, the old path can be cleaned up manually via the CLI.

**Which is safer:** Option 2. Option 1 risks data loss if the existing files contained valid Delta state from a previous run that should be preserved. Always use a fresh path when in doubt.

---

**A9**
Two reasons soft-delete is better:

1. **Audit trail:** The control table is the history of what was configured for each source. If someone asks "why was payments loading 500 records per page instead of 1000 last month?", a row with `is_active = false` can answer that. Hard delete erases the evidence permanently.

2. **Safe reactivation:** If a source is deactivated and later re-enabled, the engineer checks `is_active = false` rows to understand the previous configuration (endpoint, page size, last watermark) before deciding how to restart. With hard delete, the configuration must be reconstructed from scratch — risking misconfiguration.

---

**A10**
**Operational consequence:** Every INSERT must explicitly supply all NOT NULL columns. There is no safety net — omitting a column causes a `NOT NULL constraint violation`. This makes inserts more verbose but also more intentional.

**When DEFAULT is appropriate:**
- `load_type`: almost always starts as `'full'` → a DEFAULT makes sense
- `last_watermark`: always starts as `'1900-01-01T00:00:00Z'` → a DEFAULT makes sense
- `is_active`: almost always starts as `true` → a DEFAULT makes sense

**When DEFAULT is a risk:**
- `endpoint`, `sink_base_path`, `file_prefix`: source-specific — a wrong default (e.g. empty string) would silently seed bad config; better to force explicit values
- `page_size`: source-specific — a default of 1000 might be wrong for some APIs

This project omits DEFAULTs to match the DDL run in practice. Adding them back for `load_type`, `last_watermark`, and `is_active` would be a reasonable improvement for production.

---

## Databricks Notebooks as ADF Integration Points

**A11**
`dbutils.notebook.exit(value)` terminates the notebook and sets a return value that the caller (ADF) can read.

In ADF, a Databricks Notebook activity exposes the exit value as:
```
activity('act_read_ctl').output.runOutput
```
This is a **string** — exactly whatever was passed to `dbutils.notebook.exit()`. In this notebook, `json.dumps(result)` serialises the Python dict to a JSON string, so ADF receives:
```
{"endpoint": "/api/db/payments/", "sink_base_path": "api/payments", ...}
```
ADF then parses it with `@json(activity('act_read_ctl').output.runOutput)` before accessing individual fields.

---

**A12**
ADF's Databricks Notebook activity has exactly one output channel for the notebook's return value: `runOutput`. It is a single string — there is no mechanism to return multiple named fields. `print()` output goes to the Databricks job logs only; ADF does not read stdout at all.

By serialising a Python dict to a JSON string:
```python
dbutils.notebook.exit(json.dumps(result))
```
All values (endpoint, page_size, last_watermark, etc.) are packed into one string that ADF can receive via `runOutput`, then unpack with `@json(...)`.

---

**A13**
Two possible causes in the notebook:

1. **`dbutils.notebook.exit()` was never called** — the notebook completed normally but the last cell did not call `exit()`. In this case `runOutput` is an empty string `""`. Fix: ensure `dbutils.notebook.exit(json.dumps(result))` is the last statement and not inside a conditional branch that was skipped.

2. **An exception was raised before `dbutils.notebook.exit()`** — but the ADF activity still shows "Succeeded" because the exception did not propagate as a job failure. In this case `runOutput` is also empty. Fix: wrap the notebook body in try/except and always call `dbutils.notebook.exit()` — either with the result JSON or with an error JSON that ADF can detect downstream.

---

**A14**
`load_type` represents the state of the last completed run — it is data, not a pipeline parameter. Keeping it in the control table means:

- Any query on the table instantly shows whether each source has ever completed a full load
- ADF pipelines don't need to pass `load_type` around — they read it from the table at the start of each run
- If the watermark update runs but ADF fails afterward, the table still records the correct state of the last successful load

If ADF managed `load_type` via a parameter instead, it would be stateless between runs — a pipeline re-trigger would have no way to know whether the previous run completed a full or incremental load without checking the table anyway.

---

**A15**
The manual test left `last_watermark = '2026-10-14T00:00:00Z'` and `load_type = 'incremental'` in the table.

If Day 4 runs without resetting:
- The ADF pipeline reads `last_watermark = '2026-10-14T00:00:00Z'` and sends `updated_after=2026-10-14T00:00:00Z` to the API
- The API returns only records updated after that date — likely 0 records (nothing changed in the test window)
- `total_pages = 0` or `1` → the Until loop exits immediately → no data lands in Bronze
- The engineer thinks the pipeline ran successfully but Bronze is empty — a silent data gap

The reset ensures the first ADF run is a true full load that fetches all 320,000+ records as intended.

---
