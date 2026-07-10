# Full Load Guide — full_load_bronze.py
**Run this ONCE locally before switching ADF to incremental mode**

---

## What it does

Fetches all pages from 17 VoltGrid API entities in parallel and uploads them
to Azure ADLS Gen2 Bronze container in the same structure ADF v4 produces.
Also creates per-entity watermark seed files so ADF incremental runs work correctly.

```
bronze/
└── api/
    ├── payments/ingestion_date=2026-07-10/page_1.json
    ├── payments/ingestion_date=2026-07-10/page_2.json
    ├── sessions/ingestion_date=2026-07-10/page_1.json
    ├── customers/...
    └── (one folder per entity)

bronze/audit/
    ├── pipeline_audit.csv              ← history log (one row per entity)
    ├── watermark_payments.csv          ← per-entity watermark for ADF incremental
    ├── watermark_sessions.csv
    └── watermark_<entity>.csv × 17
```

After this script finishes, switch ADF `pl_bronze_api_master_v4` to
`p_load_type = incremental` — it reads the correct per-entity watermark
from `watermark_<entity>.csv` and only fetches new/changed records.

---

## Prerequisites

- Python 3.9 or above
- `.env` file in the project root (already present — do not commit it)
- Azure Service Principal with `Storage Blob Data Contributor` role on `evdatalakedev`

---

## Step 1 — Install dependencies

```bash
pip install -r day_5_metadata_driven_pipeline/requirements.txt
```

| Package | Purpose |
|---|---|
| `requests` | HTTP calls to VoltGrid API |
| `azure-storage-file-datalake` | ADLS Gen2 upload (correct client for dfs endpoints) |
| `azure-identity` | Service Principal authentication to Azure |
| `python-dotenv` | Reads credentials from `.env` file |

---

## Step 2 — Verify your .env file

The script reads these values from `.env` in the project root:

| Variable | What it is |
|---|---|
| `VOLTGRID_USERNAME` | VoltGrid API username |
| `VOLTGRID_PASSWORD` | VoltGrid API password |
| `AZURE_TENANT_ID` | Azure AD tenant ID |
| `AZURE_PROJECT_CLIENT_ID` | Service Principal client ID |
| `AZURE_PROJECT_CLIENT_SECRET` | Service Principal client secret |

---

## Step 3 — Run the script

```bash
cd day_5_metadata_driven_pipeline
python full_load_bronze.py
```

Or from the project root:

```bash
python day_5_metadata_driven_pipeline/full_load_bronze.py
```

---

## What you will see

```
============================================================
  VoltGrid Full Load — 2026-07-10
  Entities : 17
  Max pages: 1000 per entity
  Workers  : 4 parallel entities
============================================================
[auth] Token obtained successfully
[payments]          641 pages | ~320035 records — starting upload
[sessions]          401 pages | ~200003 records — starting upload
[customers]         400 pages | ~200000 records — starting upload
[fleet]              80 pages | ~40000  records — starting upload
[payments]          page 1/641 uploaded (500 records)
[sessions]          page 1/401 uploaded (500 records)
[payments]          page 2/641 uploaded (500 records)
...
[done] payments                   — succeeded (641 pages)
[done] sessions                   — succeeded (401 pages)
...
[audit] pipeline_audit.csv updated with 17 rows

============================================================
  Succeeded: 17/17
============================================================

Full load complete. Switch ADF pl_bronze_api_master_v4 to incremental mode now.
```

4 entities run in parallel so logs from different entities will interleave — that is normal.

---

## Entities loaded

| Entity | API path |
|---|---|
| payments | `/api/db/payments/` |
| sessions | `/api/db/sessions/` |
| customers | `/api/db/customers/` |
| fleet | `/api/db/fleet/` |
| chargers | `/api/db/chargers/` |
| vehicles | `/api/db/vehicles/` |
| stations | `/api/db/stations/` |
| complaints | `/api/db/complaints/` |
| maintenance_events | `/api/db/maintenance-events/` |
| energy_prices | `/api/db/energy-prices/` |
| tariffs | `/api/db/tariffs/` |
| charge_cards | `/api/db/charge-cards/` |
| employees | `/api/db/employees/` |
| partners | `/api/db/partners/` |
| cities | `/api/db/cities/` |
| states | `/api/db/states/` |
| weather | `/api/db/weather/` |

> Note: 3 entities use hyphens in their API path (`maintenance-events`,
> `energy-prices`, `charge-cards`) but underscores in their folder name.
> This is intentional — folder name matches `entity_name` in config,
> API path matches the actual Django URL route.

---

## After the script finishes

### 1. Verify data in ADLS
Go to **Azure Portal → evdatalakedev → bronze → api** — confirm one folder per
entity with JSON files partitioned by date.

### 2. Check the audit CSV
Go to **bronze → audit → pipeline_audit.csv** — 17 new rows, one per entity,
all with `status = succeeded`.

### 3. Check the watermark files
Go to **bronze → audit** — you should see 17 `watermark_<entity>.csv` files.
Each contains one row with `watermark_value = 2026-07-10T00:00:00Z` (the run date).

These are what ADF reads on the next incremental run to know where to start.

### 4. Switch ADF to incremental
Run `pl_bronze_api_master_v4` with `p_load_type = incremental`.
Each entity child pipeline reads its own `watermark_<entity>.csv` and fetches
only records updated after that timestamp — incremental runs should take
minutes not hours.

---

## Why per-entity watermark files matter

The old approach used a single `pipeline_audit.csv` with `firstRowOnly: true`
in the ADF Lookup. That always returned **row 1** (the seed row for payments
with `watermark = 1900-01-01`). So every entity on every "incremental" run
was actually fetching all pages — making every run a 4-hour full load.

This script writes one `watermark_<entity>.csv` per entity. ADF reads the
correct file per entity so each gets its own watermark, and incremental runs
only fetch genuinely new data.

---

## Common errors

| Error | Cause | Fix |
|---|---|---|
| `VOLTGRID_USERNAME not set` | `.env` not found | Run from `day_5_metadata_driven_pipeline/` or project root |
| `ClientAuthenticationError` | Wrong SP credentials | Check `AZURE_PROJECT_CLIENT_ID` and `AZURE_PROJECT_CLIENT_SECRET` |
| `ResourceNotFoundError` on bronze | Container does not exist | Create `bronze` container in `evdatalakedev` |
| `403 Forbidden` on upload | SP missing storage role | Grant SP `Storage Blob Data Contributor` on `evdatalakedev` |
| `404` on an entity | API endpoint not live | That entity is skipped — others continue |
| `ConnectionError` / timeout | Network or API issue | Script retries 3 times automatically, then marks entity failed |

---

## Key settings you can change

Open `full_load_bronze.py` and adjust at the top:

| Setting | Default | Change if... |
|---|---|---|
| `MAX_PAGES` | `1000` | You want to cap pages fetched per entity |
| `MAX_WORKERS` | `4` | Your machine can handle more parallel entities |
| `PAGE_SIZE` | `500` | API max is 500 — do not increase |
| `RETRY_ATTEMPTS` | `3` | You want more retries on flaky network |
