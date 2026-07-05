# 02 — ADF Pipeline: API Payments → Bronze JSON
**Day 2 | Step 2 of 5**

Build a simple ADF pipeline that logs in to the VoltGrid API and copies one page of payment records as raw JSON into the Bronze layer.

> **Day 2 goal:** Verify end-to-end connectivity — Key Vault → API → ADLS. Pagination and incremental load are covered in Day 3.

---

## Pipeline Overview

```
pl_bronze_api_payments
│
├── act_get_username     Web Activity — GET username from Key Vault
├── act_get_password     Web Activity — GET password from Key Vault
├── act_api_login        Web Activity — POST /api/auth/login/ → token
├── act_set_token        Set Variable — store token in v_token
└── act_copy_payments    Copy Activity
                           Source: ds_voltgrid_payments_src
                                   GET /api/db/payments/?page=1&page_size=100
                                   Authorization: Token {v_token}
                           Sink:   ds_bronze_payments_sink
                                   bronze/api/payments/raw/payments.json
```

---

## API Behaviour

```
GET /api/db/payments/?page=1&page_size=100
Authorization: Token abc123...

Response:
{
  "data": [ { ...payment record... }, ... ],
  "pagination": {
    "page": 1,
    "page_size": 100,
    "total": 125430,
    "total_pages": 1255
  }
}
```

- Records are under `"data"` — NOT `"results"`
- `page_size` max is 100
- All 18 endpoints also accept `updated_after` for incremental load — covered in Day 3

**Payment record fields (confirmed):**
```json
{
  "id": 1343098,
  "payment_id": "PAY-AU-TRX-00839957",
  "session_id": "SESS-20250807-00839957",
  "customer_id": "CUST-AU-2025-001440",
  "gateway": "Square",
  "amount_aud": "486.69",
  "gst": "48.67",
  "payment_mode": "CreditCard",
  "status": "Failed",
  "processed_at": "2026-06-05T14:02:00Z",
  "created_at": "2026-07-04T14:02:37.670040Z",
  "updated_at": "2026-07-04T14:02:37.670049Z"
}
```

---

## Part A — Create Datasets

### Dataset 1: VoltGrid Payments REST Source (`ds_voltgrid_payments_src`)

**UI Steps:**

1. ADF Studio → **Author** → **Datasets** → **+ New dataset**
2. Search `REST` → **REST** → **Continue**
3. Fill in:
   - **Name:** `ds_voltgrid_payments_src`
   - **Linked service:** `ls_voltgrid_api`
   - **Relative URL:** `/api/db/payments/`
4. Click **OK**
5. **Parameters** tab → **+ New** — add 2 parameters:
   - `p_page` | Type: Int | Default: `1`
   - `p_page_size` | Type: Int | Default: `100`
6. **Connection** tab → **Relative URL** → click **Add dynamic content** → paste:
   ```
   @{concat('/api/db/payments/?page=', string(dataset().p_page), '&page_size=', string(dataset().p_page_size))}
   ```
7. Click **Publish all**

**Or paste via Code button** — use `adf_pipeline_json/ds_voltgrid_payments_src.json`

---

### Dataset 2: Bronze Payments JSON Sink (`ds_bronze_payments_sink`)

**Why JSON and not CSV?**
JSON stores the API response exactly as received — no flattening, no data loss. This is the "store as-is" Bronze principle. The Databricks notebook reads these files and writes Delta.

**Flow:**
```
ADF Copy Activity  →  bronze/api/payments/raw/payments.json  (raw JSON)
                                ↓
Databricks notebook  →  bronze/api/payments/delta/  (Delta table)
```

**UI Steps:**

1. **Datasets** → **+ New dataset**
2. Search `Azure Data Lake Storage Gen2` → **Continue**
3. Search `JSON` → **JSON** → **Continue**
4. Fill in:
   - **Name:** `ds_bronze_payments_sink`
   - **Linked service:** `ls_adls_bronze`
   - **File path (Container):** `bronze`
   - **File path (Directory):** `api/payments/raw`
   - **File path (File):** `payments.json`
5. **Connection** tab:
   - **File pattern:** `Set of objects`
   - **Encoding:** `UTF-8`
6. Click **OK**
7. Click **Publish all**

**Or paste via Code button** — use `adf_pipeline_json/ds_bronze_payments_sink.json`

---

## Part B — Create Pipeline `pl_bronze_api_payments`

**Or paste the full pipeline** — use `adf_pipeline_json/pl_bronze_api_payments.json` → skip to Part C.

**UI Steps:**

1. **Author** → **Pipelines** → **+ New pipeline**
2. **Name:** `pl_bronze_api_payments`
3. **Parameters** tab → **+ New**:
   - `p_page` | Type: Int | Default: `1`
   - `p_page_size` | Type: Int | Default: `100`
4. **Variables** tab → **+ New**:
   - `v_token` | Type: String

---

### Step 1 — Web Activity: Get username from Key Vault

1. Drag **Web** activity onto canvas
2. **Name:** `act_get_username`
3. **URL:** `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-username/?api-version=7.0`
4. **Method:** GET
5. **Authentication:** Managed Identity
6. **Resource:** `https://vault.azure.net`

---

### Step 2 — Web Activity: Get password from Key Vault

1. Add another **Web** activity → connect after `act_get_username`
2. **Name:** `act_get_password`
3. **URL:** `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-password/?api-version=7.0`
4. **Method:** GET
5. **Authentication:** Managed Identity
6. **Resource:** `https://vault.azure.net`

---

### Step 3 — Web Activity: Login and get token

1. Add another **Web** activity → connect after `act_get_password`
2. **Name:** `act_api_login`
3. **URL:** `https://ev-project-navy-mu.vercel.app/api/auth/login/`
4. **Method:** POST
5. **Headers:**
   - `Content-Type` : `application/json`
6. **Body** (dynamic content):
   ```
   @{concat('{"username":"', activity('act_get_username').output.value, '","password":"', activity('act_get_password').output.value, '"}')}
   ```
7. Output used: `activity('act_api_login').output.token`

---

### Step 4 — Set Variable: Store token

1. Add **Set Variable** activity → connect after `act_api_login`
2. **Name:** `act_set_token`
3. **Variable:** `v_token`
4. **Value** (dynamic content): `@{activity('act_api_login').output.token}`

---

### Step 5 — Copy Activity: Fetch page and write JSON

1. Add **Copy data** activity → connect after `act_set_token`
2. **Name:** `act_copy_payments`

**Source tab:**
- Dataset: `ds_voltgrid_payments_src`
- Dataset parameters:
  - `p_page`: `@{pipeline().parameters.p_page}`
  - `p_page_size`: `@{pipeline().parameters.p_page_size}`
- **Additional headers** (set in JSON — not visible in UI, but active):
  - `Authorization`: `@{concat('Token ', variables('v_token'))}`

**Sink tab:**
- Dataset: `ds_bronze_payments_sink`

**Mapping tab:**
- Leave as **Auto mapping**

3. Click **Publish all**

---

## Part C — Trigger the Pipeline

### Manual trigger — UI

1. Open `pl_bronze_api_payments`
2. **Add trigger** → **Trigger now**
3. Enter parameters:
   - `p_page`: `1`
   - `p_page_size`: `100`
4. Click **OK**
5. **Monitor** tab → all 5 activities should show green

### Manual trigger — CLI

> **CMD / PowerShell users:** Use single-line version.

**Single line (CMD / PowerShell):**
```cmd
az datafactory pipeline create-run --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --pipeline-name "pl_bronze_api_payments" --parameters "{\"p_page\": 1, \"p_page_size\": 100}"
```

**Multi-line (bash / Git Bash only):**
```bash
az datafactory pipeline create-run \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
  --pipeline-name "pl_bronze_api_payments" \
  --parameters '{"p_page": 1, "p_page_size": 100}'
```

---

## Verify in ADLS (Databricks)

After the pipeline runs:

```python
display(dbutils.fs.ls("abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/"))
```

Expected: `payments.json` file present.

```python
df_raw = spark.read.option("multiLine", "true").json(
    "abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/payments.json"
)
print(f"Row count (pages): {df_raw.count()}")
display(df_raw.limit(3))
```

Each row = one full API page response with `data` array and `pagination` object.

```python
from pyspark.sql.functions import explode, col
payments = df_raw.select(explode(col("data")).alias("payment")).select("payment.*")
print(f"Payment records: {payments.count():,}")
display(payments.limit(5))
```

Expected: 100 individual payment records with all fields flattened.

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `act_get_username` 403 | ADF MI missing `Key Vault Secrets User` role | Day 2 Part 3 — assign role, wait 2 min, retry |
| `act_api_login` 401 | Wrong credentials in Key Vault | Check `voltgrid-username` and `voltgrid-password` values |
| `act_copy_payments` 401 | Token not set in `v_token` | Check `act_api_login` output — confirm `.output.token` exists |
| `act_copy_payments` 403 on sink | ADF MI missing `Storage Blob Data Contributor` on `evdatalakedev` | Day 2 Part 2 — assign role, wait 2 min, retry |
| Output file is empty | API returned 0 records | Reduce `p_page_size` to 10 for a quick test |

---

## What Day 3 Adds

Day 2 proves the connection works — one page, one file, manually triggered.

Day 3 adds:
- Full load — paginate all pages automatically
- Incremental load — `updated_after` filter, watermark per run
- Output partitioned by `ingestion_date` — one folder per run
- Databricks notebook reads Bronze JSON → writes Delta table

→ See `day_3_bronze_layer_ingestion_using_adf_and_databricks/`

---

## Next Step

→ `03_ADF_PIPELINE_BLOB_SESSIONS.md` — blob sessions ingestion pipeline
