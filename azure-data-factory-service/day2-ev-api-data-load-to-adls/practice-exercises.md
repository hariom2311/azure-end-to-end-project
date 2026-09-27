# Day 2 — Practice Exercises: EV API Data Load to ADLS

> Build the full Bronze ingestion pipeline from scratch. Each exercise builds on the previous one.

---

## Exercise 1 — Provision ADF and Assign Permissions (20 min)

**Goal:** Create the ADF factory and grant its Managed Identity the minimum permissions it needs.

### Tasks

**1.1 — Create the ADF instance**
- Go to Azure Portal → Data factories → **+ Create**
- Resource group: `rg-ev-intelligence-dev`
- Name: `adf-datalake-dev-ded`
- Region: `Central India`
- Version: `V2`
- Click **Review + Create** → **Create**
- Once deployed, click **Launch studio** and bookmark the URL

**1.2 — Find the Managed Identity Object ID**
- Portal → Data factories → `adf-datalake-dev-ded` → **Properties**
- Copy the **Managed Identity Object ID**

**1.3 — Grant Key Vault access**
- Portal → Key vaults → `key-vault-session-ded` → **Access Control (IAM)**
- Add role assignment: `Key Vault Secrets User` → Managed identity → Data factory → `adf-datalake-dev-ded`

**1.4 — Grant ADLS Gen2 access**
- Portal → Storage accounts → `evdatalakedev` → **Access Control (IAM)**
- Add role assignment: `Storage Blob Data Contributor` → Managed identity → `adf-datalake-dev-ded`

**Verify:**
- Key Vault → **Role assignments** tab → confirm `adf-datalake-dev-ded` appears under `Key Vault Secrets User`
- Storage → **Role assignments** tab → confirm `adf-datalake-dev-ded` appears under `Storage Blob Data Contributor`

**Key Vault secrets to confirm exist** (if missing, add them):

| Secret Name | Value |
|---|---|
| `voltgrid-api-base-url` | `https://ev-project-navy-mu.vercel.app` |
| `voltgrid-username` | `voltgrid_demo` |
| `voltgrid-password` | `EVcharge@AU2025` |

---

## Exercise 2 — Create 3 Linked Services (20 min)

**Goal:** Connect ADF to Key Vault, the VoltGrid API, and ADLS Gen2 — in that order.

### Task 2.1 — ls_keyvault

1. ADF Studio → **Manage** → **Linked services** → **+ New**
2. Search `Key Vault` → **Azure Key Vault** → **Continue**
3. Name: `ls_keyvault`
4. Azure Key Vault name: `key-vault-session-ded`
5. Authentication method: **System Assigned Managed Identity**
6. **Test connection** → must show green
7. **Create**

> If test fails: the 2-minute RBAC propagation delay may not have passed yet. Wait and retry.

### Task 2.2 — ls_voltgrid_api

1. **+ New** → search `REST` → **REST** → **Continue**
2. Name: `ls_voltgrid_api`
3. Base URL: `https://ev-project-navy-mu.vercel.app`
4. Authentication type: `Anonymous`
5. Enable server certificate validation: checked
6. **Test connection** → green → **Create**

> Anonymous auth is correct here — we handle the VoltGrid token inside the pipeline, not at the linked service level.

### Task 2.3 — ls_adls_bronze

1. **+ New** → search `Azure Data Lake Storage Gen2` → **Continue**
2. Name: `ls_adls_bronze`
3. Authentication method: **System Assigned Managed Identity**
4. Storage account name: `evdatalakedev`
5. **Test connection** → green → **Create**

**Checkpoint:** Manage → Linked services → you should see all 3 listed. Test each — all green.

---

## Exercise 3 — Create Source and Sink Datasets (15 min)

**Goal:** Define the source (VoltGrid API payments endpoint) and sink (ADLS Gen2 Bronze path).

### Task 3.1 — Source Dataset: ds_voltgrid_payments_src

1. **Author** → **Datasets** → **+ New dataset** → search `REST` → **REST** → **Continue**
2. Name: `ds_voltgrid_payments_src`
3. Linked service: `ls_voltgrid_api`
4. Relative URL: `/api/db/payments/`
5. Click **OK**
6. **Parameters** tab → click **+ New** and add both:
   - Name: `p_page` | Type: `int` | Default: `1`
   - Name: `p_page_size` | Type: `int` | Default: `100`
7. **Connection** tab → click the Relative URL field → **Add dynamic content**:
   ```
   /api/db/payments/?page=@{dataset().p_page}&page_size=@{dataset().p_page_size}
   ```
8. **Publish all**

**Test this dataset manually:**
- On the dataset, click **Preview data** — it will fail with 401 (expected — this API requires a token header, which only the pipeline can add)
- This is normal. The dataset alone cannot authenticate.

### Task 3.2 — Sink Dataset: ds_bronze_payments_sink

1. **+ New dataset** → search `Azure Data Lake Storage Gen2` → **Continue**
2. Select format: **JSON** → **Continue**
3. Name: `ds_bronze_payments_sink`
4. Linked service: `ls_adls_bronze`
5. File path:
   - Container: `bronze`
   - Directory: `api/payments/raw`
   - File: `payments.json`
6. **Publish all**

**Checkpoint:** Author → Datasets → both datasets listed and published (no unsaved changes indicator).

---

## Exercise 4 — Build the Pipeline (30 min)

**Goal:** Build `pl_bronze_api_payments` — the 5-activity chain that authenticates to VoltGrid and copies payments to Bronze.

### Task 4.1 — Create the pipeline shell

1. **Author** → **Pipelines** → **+** → **New pipeline**
2. Name: `pl_bronze_api_payments`
3. **Parameters** tab (bottom panel) → add:
   - `p_page` | int | default `1`
   - `p_page_size` | int | default `100`
4. **Variables** tab → add:
   - `v_token` | String

### Task 4.2 — Activity 1: act_get_username

1. In the Activities panel (left), expand **General** → drag **Web Activity** onto the canvas
2. Click the activity → rename to `act_get_username`
3. **Settings** tab:
   - URL: `https://key-vault-session-ded.vault.azure.net/secrets/voltgrid-username/?api-version=7.0`
   - Method: `GET`
   - Authentication: **System Assigned Managed Identity**
   - Resource: `https://vault.azure.net`

### Task 4.3 — Activity 2: act_get_password

1. Drag another **Web Activity** onto canvas → rename to `act_get_password`
2. Hover over `act_get_username` → drag the **green arrow** to `act_get_password` (this creates an On Success dependency)
3. **Settings** tab:
   - URL: `https://key-vault-session-ded.vault.azure.net/secrets/voltgrid-password/?api-version=7.0`
   - Method: `GET`
   - Authentication: **System Assigned Managed Identity**
   - Resource: `https://vault.azure.net`

### Task 4.4 — Activity 3: act_api_login

1. Drag **Web Activity** → rename to `act_api_login`
2. Connect `act_get_password` → `act_api_login` (green arrow)
3. **Settings** tab:
   - URL: `https://ev-project-navy-mu.vercel.app/api/auth/login/`
   - Method: `POST`
   - Headers → **+ New**: Name `Content-Type`, Value `application/json`
   - Body → **Add dynamic content**:
     ```
     @concat('{"username":"', activity('act_get_username').output.value, '","password":"', activity('act_get_password').output.value, '"}')
     ```

> **What this expression does:**
> - `activity('act_get_username').output.value` → the Key Vault secret value for username
> - `activity('act_get_password').output.value` → the Key Vault secret value for password
> - `concat(...)` builds the JSON body string dynamically

### Task 4.5 — Activity 4: act_set_token

1. Expand **General** → drag **Set Variable** → rename to `act_set_token`
2. Connect `act_api_login` → `act_set_token`
3. **Settings** tab:
   - Variable name: `v_token`
   - Value → **Add dynamic content**: `@activity('act_api_login').output.token`

> The login response is `{"token": "abc123..."}`. `.output.token` extracts the token string.

### Task 4.6 — Activity 5: act_copy_payments

1. Expand **Move & transform** → drag **Copy data** → rename to `act_copy_payments`
2. Connect `act_set_token` → `act_copy_payments`
3. **Source** tab:
   - Source dataset: `ds_voltgrid_payments_src`
   - Dataset properties:
     - `p_page` → **Add dynamic content**: `@pipeline().parameters.p_page`
     - `p_page_size` → **Add dynamic content**: `@pipeline().parameters.p_page_size`
   - Additional headers → **+ New**:
     - Name: `Authorization`
     - Value → **Add dynamic content**: `@concat('Token ', variables('v_token'))`
   - Request method: `GET`
4. **Sink** tab:
   - Sink dataset: `ds_bronze_payments_sink`
   - File pattern: `setOfObjects`
5. **Settings** tab: leave defaults

6. **Publish all**

### Task 4.7 — Debug Run

1. Click **Debug** (not Trigger now) → parameters: `p_page = 1`, `p_page_size = 10` (smaller for a quick test)
2. Watch the activity progress at the bottom of the canvas
3. All 5 activities should turn green
4. Click `act_copy_payments` → **Output** → confirm `dataRead`, `dataWritten`, `rowsCopied` show values

**If any activity fails:** click it → **Error** tab → read the full error message before changing anything.

---

## Exercise 5 — Trigger, Monitor, and Verify (15 min)

**Goal:** Run the full pipeline with 100 records and verify the file in ADLS Gen2.

### Task 5.1 — Manual trigger

1. **Add trigger** → **Trigger now**
2. Parameters: `p_page = 1`, `p_page_size = 100`
3. **OK**

### Task 5.2 — Monitor the run

1. **Monitor** → **Pipeline runs** → find `pl_bronze_api_payments`
2. Status should change: Queued → In progress → Succeeded (~15–20 seconds)
3. Click the run → **Activity runs** → confirm all 5 activities succeeded
4. Click `act_api_login` → **Output** tab → confirm the response contains a `token` key
5. Click `act_copy_payments` → **Output** tab → note:
   - `rowsRead`: should be 100
   - `rowsCopied`: should be 100
   - `dataWritten`: size in bytes

### Task 5.3 — Verify in ADLS Gen2 (Portal)

1. Portal → **Storage accounts** → `evdatalakedev` → **Containers** → `bronze`
2. Navigate to `api/payments/raw/`
3. Confirm `payments.json` exists
4. Click the file → **Download** → open in a text editor → confirm it's valid JSON with payment records

### Task 5.4 — Verify in Databricks (optional)

If you have a running Databricks cluster, run this in a notebook:

```python
df = spark.read.option("multiLine", "true").json(
    "abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/payments.json"
)
display(df.limit(5))
print(f"Total records: {df.count()}")
print(f"Columns: {df.columns}")
```

Expected: 100 rows, columns include `payment_id`, `session_id`, `customer_id`, `amount_aud`, `status`, `payment_date`.

---

## Exercise 6 — Extend to a Second Endpoint (Bonus, 20 min)

**Goal:** Add sessions ingestion alongside payments — introducing the idea of reusable linked services.

### Task 6.1 — Create a sessions source dataset

1. **Author** → **Datasets** → **+ New dataset** → **REST** → **Continue**
2. Name: `ds_voltgrid_sessions_src`
3. Linked service: `ls_voltgrid_api` (same linked service, different endpoint)
4. **Parameters** tab → add `p_page` (int, 1), `p_page_size` (int, 100)
5. **Connection** tab → Relative URL → **Add dynamic content**:
   ```
   /api/db/sessions/?page=@{dataset().p_page}&page_size=@{dataset().p_page_size}
   ```

### Task 6.2 — Create a sessions sink dataset

1. **+ New dataset** → **Azure Data Lake Storage Gen2** → **JSON** → **Continue**
2. Name: `ds_bronze_sessions_sink`
3. Linked service: `ls_adls_bronze`
4. File path: `bronze` / `api/sessions/raw` / `sessions.json`

### Task 6.3 — Add sessions copy to the pipeline

1. Open `pl_bronze_api_payments`
2. Drag a new **Copy data** activity → name it `act_copy_sessions`
3. Connect `act_set_token` → `act_copy_sessions` (same token — runs in parallel with payments copy!)
4. **Source**: `ds_voltgrid_sessions_src`, same parameters and Authorization header as `act_copy_payments`
5. **Sink**: `ds_bronze_sessions_sink`

> Note: both Copy activities (`act_copy_payments` and `act_copy_sessions`) both depend on `act_set_token` — ADF will run them **in parallel** because they have no dependency on each other.

6. **Publish all** → **Trigger now** → monitor both copies running simultaneously

**Reflection questions:**
- How many linked services did you need to add for the sessions endpoint? (0 — same `ls_voltgrid_api`)
- What does this tell you about the value of separating linked services from datasets?
- If the API base URL changes tomorrow, how many places do you update?
