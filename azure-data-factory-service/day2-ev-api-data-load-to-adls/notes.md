# Day 2 — EV API Data Load to ADLS (Bronze Ingestion with ADF)

> **Goal:** Build an ADF pipeline that authenticates to the VoltGrid EV API, pulls payment records, and lands raw JSON in the Bronze layer of ADLS Gen2 — the first real data movement step of the project.

---

## Concept 1: What Are We Building and Why

### The Bronze Ingestion Pattern

In a lakehouse architecture data flows through three layers:

```
External Source (VoltGrid API)
        ↓  ADF pipeline
Bronze (ADLS Gen2)  ← raw, unmodified, append-only
        ↓  Databricks
Silver (ADLS Gen2)  ← cleaned, typed, deduplicated
        ↓  Databricks
Gold   (ADLS Gen2)  ← aggregated, business-ready
```

**Bronze = raw landing zone.** You store exactly what the API returned — no transformation, no filtering. If upstream data is wrong, you can always reprocess from Bronze. This is why Bronze is the most important layer to get right.

**Why ADF and not a Python script?**

| Concern | Python cron | ADF |
|---|---|---|
| Retry on failure | You build it | Built in (retry count + interval per activity) |
| Monitoring | You build it | Monitor panel — full activity-level logs |
| Credential management | `.env` files, risky | Key Vault integration — no secrets in code |
| Parallelism for multiple endpoints | You build it | ForEach, parallel execution built in |
| Non-engineer visibility | None | Visual pipeline anyone can read |

### VoltGrid API — What We Are Ingesting

**Base URL:** `https://ev-project-navy-mu.vercel.app`

| Endpoint | What it returns |
|---|---|
| `POST /api/auth/login/` | Auth token (valid per session) |
| `GET /api/db/payments/` | EV charging payment records (paginated, 100/page) |
| `GET /api/db/sessions/` | Charging session records |
| `GET /api/db/customers/` | Customer records |
| `GET /api/db/stations/` | Charging station records |
| `GET /api/db/vehicles/` | Vehicle records |

**Auth pattern:** The API does not accept username/password in a header. You must call `POST /api/auth/login/` first to get a token, then pass `Authorization: Token <value>` on every subsequent GET request.

**Day 2 scope:** One endpoint (`/api/db/payments/`), one page (100 records), manual trigger. Day 3 adds pagination (all pages) and incremental load (`updated_after` watermark).

---

## Concept 2: ADF Components Used Today

### 2.1 Linked Services

A Linked Service is ADF's saved connection definition — it stores HOW to connect. Pipelines never store credentials; they reference a linked service by name.

**Day 2 uses 3 linked services:**

| Linked Service | Type | What it connects to |
|---|---|---|
| `ls_keyvault` | Azure Key Vault | `key-vault-session-ded` — stores API credentials |
| `ls_voltgrid_api` | REST | VoltGrid API base URL |
| `ls_adls_bronze` | ADLS Gen2 | `evdatalakedev` storage account — the Bronze layer |

**Why Key Vault for credentials?**
Never store API passwords in ADF linked service fields — they appear in ARM templates and Git history. Key Vault stores the secret; ADF's Managed Identity reads it at runtime. The pipeline JSON never contains a password.

**Why Anonymous auth on the REST linked service?**
ADF's built-in auth types (Basic, OAuth2, Service Principal) don't support the VoltGrid pattern: get a short-lived token via POST, then use it as a header. So the linked service uses Anonymous, and we handle the token ourselves via Web Activity → Set Variable → Copy Activity header.

### 2.2 Datasets

A Dataset says WHERE the data is and WHAT it looks like — it points to a specific path or endpoint within a linked service.

| Dataset | Linked Service | What it points to |
|---|---|---|
| `ds_voltgrid_payments_src` | `ls_voltgrid_api` | `GET /api/db/payments/?page=P&page_size=N` (parameters) |
| `ds_bronze_payments_sink` | `ls_adls_bronze` | `bronze/api/payments/raw/payments.json` |

**Why parameters on the source dataset?**
`p_page` and `p_page_size` make the dataset reusable — you can test with page 2 or 10 records without changing the dataset definition. The pipeline passes values at runtime.

### 2.3 Pipeline: `pl_bronze_api_payments`

**5 activities in sequence:**

```
act_get_username  ─On Success→  act_get_password  ─On Success→  act_api_login
                                                                       │
                                                                  On Success
                                                                       ↓
                                                               act_set_token
                                                                       │
                                                                  On Success
                                                                       ↓
                                                            act_copy_payments
                                                         (reads API → writes Bronze)
```

| Activity | Type | What it does |
|---|---|---|
| `act_get_username` | Web Activity | Calls Key Vault REST API → returns `voltgrid-username` secret value |
| `act_get_password` | Web Activity | Calls Key Vault REST API → returns `voltgrid-password` secret value |
| `act_api_login` | Web Activity | POST to `/api/auth/login/` with credentials → returns `{"token": "..."}` |
| `act_set_token` | Set Variable | Stores `activity('act_api_login').output.token` into pipeline variable `v_token` |
| `act_copy_payments` | Copy Activity | GET `/api/db/payments/` with `Authorization: Token {v_token}` → writes JSON to Bronze |

**Pipeline parameters:**
- `p_page` (int, default `1`) — which page of the API to fetch
- `p_page_size` (int, default `100`) — how many records per page

**Pipeline variable:**
- `v_token` (String) — holds the token between `act_set_token` and `act_copy_payments`

### 2.4 How Key Vault Web Activity Works

The Key Vault REST API endpoint for reading a secret is:
```
GET https://<vault-name>.vault.azure.net/secrets/<secret-name>/?api-version=7.0
```

In ADF's Web Activity, you set:
- **Authentication:** System Assigned Managed Identity
- **Resource:** `https://vault.azure.net`

ADF uses its Managed Identity to get a token from Azure AD for the Key Vault scope, then calls the REST API. The secret value is returned in the `value` field of the response:
```json
{ "value": "voltgrid_demo", "id": "...", ... }
```

So `activity('act_get_username').output.value` is the actual username string.

### 2.5 ADF Managed Identity and RBAC

ADF gets a free System-assigned Managed Identity — no password, no rotation. You assign it RBAC roles:

| Resource | Role | Why |
|---|---|---|
| `key-vault-session-ded` | Key Vault Secrets User | Read secrets at pipeline runtime |
| `evdatalakedev` | Storage Blob Data Contributor | Write JSON files to Bronze container |

Without these roles: Key Vault Web Activities fail with 403, Copy Activity sink fails with 403.

---

## Concept 3: Step-by-Step Build

### Step 1 — Create ADF Instance

1. Portal → search **Data factories** → **+ Create**
2. Fill in:
   - Resource group: `rg-ev-intelligence-dev`
   - Name: `adf-datalake-dev-ded`
   - Region: `Central India`
   - Version: `V2`
3. **Review + Create** → **Create** (~1 minute)
4. Click **Launch studio** → bookmarks `https://adf.azure.com`

### Step 2 — Grant Managed Identity Access

**On Key Vault:**
1. Portal → **Key vaults** → `key-vault-session-ded` → **Access Control (IAM)**
2. **+ Add** → **Add role assignment** → `Key Vault Secrets User`
3. Members → Managed identity → Data factory (V2) → `adf-datalake-dev-ded`
4. **Review + assign**

**On ADLS Gen2:**
1. Portal → **Storage accounts** → `evdatalakedev` → **Access Control (IAM)**
2. **+ Add** → **Add role assignment** → `Storage Blob Data Contributor`
3. Members → Managed identity → Data factory (V2) → `adf-datalake-dev-ded`
4. **Review + assign**

Wait 2 minutes after assigning roles before testing any linked service.

### Step 3 — Create Linked Services (in order)

**ls_keyvault:**
1. ADF Studio → **Manage** → **Linked services** → **+ New**
2. Search `Key Vault` → **Azure Key Vault** → **Continue**
3. Name: `ls_keyvault` | Azure Key Vault: `key-vault-session-ded` | Auth: System Assigned Managed Identity
4. **Test connection** → green → **Create**

**ls_voltgrid_api:**
1. **+ New** → search `REST` → **REST** → **Continue**
2. Name: `ls_voltgrid_api` | Base URL: `https://ev-project-navy-mu.vercel.app` | Auth type: `Anonymous`
3. **Test connection** → green → **Create**

**ls_adls_bronze:**
1. **+ New** → search `Azure Data Lake Storage Gen2` → **Continue**
2. Name: `ls_adls_bronze` | Auth: `System Assigned Managed Identity` | Storage account: `evdatalakedev`
3. **Test connection** → green → **Create**

### Step 4 — Create Datasets

**Source dataset — `ds_voltgrid_payments_src`:**
1. **Author** → **Datasets** → **+ New dataset** → **REST** → **Continue**
2. Name: `ds_voltgrid_payments_src` | Linked service: `ls_voltgrid_api` | Relative URL: `/api/db/payments/`
3. **Parameters** tab → add:
   - `p_page` — int — default: `1`
   - `p_page_size` — int — default: `100`
4. **Connection** tab → Relative URL field → **Add dynamic content**:
   ```
   /api/db/payments/?page=@{dataset().p_page}&page_size=@{dataset().p_page_size}
   ```
5. **Publish all**

**Sink dataset — `ds_bronze_payments_sink`:**
1. **+ New dataset** → **Azure Data Lake Storage Gen2** → format: **JSON** → **Continue**
2. Name: `ds_bronze_payments_sink` | Linked service: `ls_adls_bronze`
3. File path: `bronze` / `api/payments/raw` / `payments.json`
4. **Publish all**

### Step 5 — Build the Pipeline

1. **Author** → **Pipelines** → **+** → **New pipeline**
2. Name: `pl_bronze_api_payments`
3. **Parameters** tab (bottom panel) → add `p_page` (int, default 1), `p_page_size` (int, default 100)
4. **Variables** tab → add `v_token` (String)

**Activity 1 — act_get_username:**
- Drag **Web Activity** onto canvas → rename to `act_get_username`
- Settings tab:
  - URL: `https://key-vault-session-ded.vault.azure.net/secrets/voltgrid-username/?api-version=7.0`
  - Method: `GET`
  - Authentication: `System Assigned Managed Identity`
  - Resource: `https://vault.azure.net`

**Activity 2 — act_get_password:**
- Drag another **Web Activity** → rename to `act_get_password`
- Draw success arrow from `act_get_username` → `act_get_password`
- Settings: same as above but URL: `.../secrets/voltgrid-password/...`

**Activity 3 — act_api_login:**
- Drag **Web Activity** → rename to `act_api_login`
- Connect `act_get_password` → `act_api_login`
- Settings:
  - URL: `https://ev-project-navy-mu.vercel.app/api/auth/login/`
  - Method: `POST`
  - Headers: `Content-Type` = `application/json`
  - Body → **Add dynamic content**:
    ```
    @concat('{"username":"', activity('act_get_username').output.value, '","password":"', activity('act_get_password').output.value, '"}')
    ```

**Activity 4 — act_set_token:**
- Drag **Set Variable** → rename to `act_set_token`
- Connect `act_api_login` → `act_set_token`
- Settings:
  - Variable: `v_token`
  - Value → **Add dynamic content**: `@activity('act_api_login').output.token`

**Activity 5 — act_copy_payments:**
- Drag **Copy data** → rename to `act_copy_payments`
- Connect `act_set_token` → `act_copy_payments`
- **Source** tab:
  - Dataset: `ds_voltgrid_payments_src`
  - Dataset parameters: `p_page` = `@pipeline().parameters.p_page`, `p_page_size` = `@pipeline().parameters.p_page_size`
  - Additional headers → Add: Name `Authorization`, Value → **Add dynamic content**: `@concat('Token ', variables('v_token'))`
  - Request method: `GET`
- **Sink** tab:
  - Dataset: `ds_bronze_payments_sink`
  - File pattern: `setOfObjects`

6. **Publish all**

### Step 6 — Trigger and Monitor

1. **Author** → `pl_bronze_api_payments` → **Add trigger** → **Trigger now**
2. Parameters: `p_page = 1`, `p_page_size = 100` → **OK**
3. **Monitor** panel → **Pipeline runs** → watch 5 activities turn green (~15–20 sec)
4. Click the run → **Activity runs** → click `act_copy_payments` → **Output** — confirms rows read/written

**Verify the file landed:**
```python
# In a Databricks notebook
display(dbutils.fs.ls("abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/"))

df = spark.read.option("multiLine", "true").json(
    "abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/payments.json"
)
display(df.limit(5))
print(f"Record count: {df.count()}")
```

---

## Concept 4: Pipeline JSON (Import Shortcut)

Instead of building each activity by hand in the UI, you can paste the full pipeline JSON directly:

**Author** → **Pipelines** → `pl_bronze_api_payments` → click **{ }** (Code button, top right) → select all → paste JSON → OK → **Publish all**

Do the same for datasets — **Author** → **Datasets** → dataset name → **{ }** Code button.

The JSON for all pipeline resources is in the `adf_pipeline_json/` folder of the Day 3 materials in the main project (`day_3_bronze_layer_ingestion_using_adf_and_databricks/adf_pipeline_json/`) for the full paginated version. The Day 2 single-page version is shown in the practice exercises.

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `act_get_username` 403 | ADF MI missing `Key Vault Secrets User` on Key Vault | Portal → Key Vault → IAM → assign role → wait 2 min |
| `act_api_login` 401 | Wrong credentials in Key Vault secrets | Check `voltgrid-username` / `voltgrid-password` values in Key Vault |
| `act_copy_payments` 401 | Token not in `v_token` — wrong output key | Monitor → `act_api_login` Output tab → verify response has `token` key |
| `act_copy_payments` 403 on sink | ADF MI missing `Storage Blob Data Contributor` on `evdatalakedev` | Portal → Storage → IAM → assign role → wait 2 min |
| Dataset not found when pasting pipeline | Datasets not published before pipeline | Create + publish both datasets first, then paste pipeline JSON |
| Empty output file | API returned 0 records | Reduce `p_page_size` to `10` for test, check linked service URL |

---

## What Day 3 Adds

| Feature | Day 2 | Day 3 |
|---|---|---|
| Pages fetched | 1 (manual parameter) | All pages (Until loop) |
| Load type | Full only | Full + incremental (`updated_after` watermark) |
| Sink path | Fixed `payments.json` | Partitioned `ingestion_date=YYYY-MM-DD/page_N.json` |
| Endpoints | Payments only | All 6 endpoints via ForEach |
| Watermark | None | Auto-read from `pipeline_audit` Delta table |
