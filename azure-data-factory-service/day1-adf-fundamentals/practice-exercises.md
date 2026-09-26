# Day 1 — Practice Exercises: ADF Introduction & Terminologies

> All exercises are done in the **Azure Portal** and **ADF Studio**.
> You need `rg-datalake-dev` and `stadlsdev001` (ADLS Gen2) from ADLS Day 1.
> API credentials: VoltGrid EV API — base URL and credentials are in the project `.env`.
> Estimated total time: 90–120 minutes.

**API credentials (from project .env):**
```
API_BASE_URL  = https://your-app.vercel.app
API_USERNAME  = voltgrid_demo
API_PASSWORD  = EVcharge@AU2025

Endpoints used today:
  POST  /api/auth/login/     → returns {"token": "..."}
  GET   /api/db/payments/    → returns paginated payment records
  GET   /api/db/sessions/    → returns paginated charging session records
```

---

## Exercise 1 — Create ADF and Explore the 4 Studio Panels

**Concept:** What is ADF, ADF Studio UI

**1a. Create the ADF instance**
- Go to `https://portal.azure.com` → search "Data factories" → **+ Create**

| Field | Value |
|---|---|
| Resource group | `rg-datalake-dev` |
| Region | Australia East |
| Name | `adf-datalake-dev` (add numbers if taken) |
| Version | V2 |

- Git configuration: **"Configure Git later"**
- Click **"Review + create"** → **"Create"** → wait ~60 seconds
- Click **"Go to resource"** → **"Launch Studio"**

**1b. Explore each panel — write one sentence for each**
Navigate to each icon and note what you see:

| Icon | Panel | What's inside (write your own answer) |
|---|---|---|
| ✏️ | Author | |
| 📺 | Monitor | |
| 🔧 | Manage | |
| 🎓 | Learn | |

**1c. Find Integration Runtime**
- Go to **🔧 Manage → Integration runtimes**
- You should see **AutoResolveIntegrationRuntime** already created
- Answer: What type is it (Azure or Self-Hosted)? What does it mean that it "auto-resolves"?

**1d. Browse the template gallery**
- Go to **🎓 Learn → Pipeline template gallery**
- Search for "HTTP" — find a template that copies HTTP data to ADLS Gen2
- Do NOT deploy it — just look at what linked services and activities it creates automatically
- Answer: How many activities does it create? What are they?

**1e. Tag your ADF resource**
- Go to the Azure portal → `adf-datalake-dev` → **Tags** → add:

| Name | Value |
|---|---|
| Environment | dev |
| Project | adf-day1 |
| Owner | your-name |

> **Checkpoint:** ADF Studio opens, all 4 panels are accessible, AutoResolveIntegrationRuntime exists.

---

## Exercise 2 — Create Linked Services (ADLS Gen2 + VoltGrid API)

**Concept:** Linked Services — what they are and how to create them

**2a. Create the ADLS Gen2 Linked Service**
- **🔧 Manage → Linked services → + New**
- Search: **"Azure Data Lake Storage Gen2"** → Continue

| Field | Value |
|---|---|
| Name | `ls_adls_gen2` |
| Description | Connects to stadlsdev001 ADLS Gen2 data lake |
| Authentication method | Account key |
| Storage account name | `stadlsdev001` |

- Click **"Test connection"** — must show **"Connection successful"**
- Click **"Create"**

**2b. Create the VoltGrid API Linked Service**
- **+ New** → search: **"HTTP"** → Continue

| Field | Value |
|---|---|
| Name | `ls_voltgrid_api` |
| Description | VoltGrid EV platform REST API |
| Base URL | `https://your-app.vercel.app` |
| Authentication type | Anonymous |

- Click **"Test connection"** → **"Create"**

**2c. Verify both appear in the list**
- Both `ls_adls_gen2` and `ls_voltgrid_api` should be listed with a green status

**2d. Answer these questions:**
1. Why is `ls_voltgrid_api` set to Anonymous even though the API requires a token?
2. If you had 10 pipelines all reading from the same VoltGrid API, how many Linked Services do you need for the API?
3. If you wanted to use Managed Identity instead of Account key for ADLS Gen2, what Azure RBAC role would the ADF instance need on the storage account?

**2e. Explore authentication options (don't save)**
- Edit `ls_adls_gen2` → click the **Authentication method** dropdown
- Note the options: Account key, Service Principal, System Managed Identity, User Managed Identity
- Which one requires no secrets at all?
- Close without saving

---

## Exercise 3 — Create Datasets (API source + ADLS Gen2 sink)

**Concept:** Datasets — describing where data lives and what it looks like

**3a. Create the source Dataset — VoltGrid Payments API**
- **✏️ Author → "+" next to Datasets → New dataset**
- Search: **"HTTP"** → select HTTP → Continue → select **JSON** → Continue

| Field | Value |
|---|---|
| Name | `ds_voltgrid_payments` |
| Linked service | `ls_voltgrid_api` |
| Relative URL | `/api/db/payments/` |
| Request method | GET |
| Import schema | None |

- Go to **Parameters** tab → **+ New** → Name: `auth_token`, Type: `String`
- Go back to **Connection** tab → **Additional headers** → **+ New**:
  - Header name: `Authorization`
  - Header value: `@{concat('Token ', dataset().auth_token)}`
- Click **OK**

**3b. Create the sink Dataset — ADLS Gen2 Bronze JSON**
- **"+" next to Datasets → New dataset**
- Search: **"Azure Data Lake Storage Gen2"** → Continue → **JSON** → Continue

| Field | Value |
|---|---|
| Name | `ds_adls_bronze_json` |
| Linked service | `ls_adls_gen2` |
| File path — Container | `bronze` |
| File path — Directory | `payments/@{dataset().run_date}` |
| File path — File | `payments_raw.json` |

- Go to **Parameters** tab → **+ New** → Name: `run_date`, Type: `String`
- Go to **Connection** tab → confirm directory field shows `payments/@{dataset().run_date}`
- Click **OK**

**3c. Click "Publish All"**
- Confirm the diff shows both datasets and both linked services
- Click **"Publish"**

**3d. Answer these questions:**
1. The sink directory is `payments/@{dataset().run_date}`. If `run_date = 2026-01-15`, what is the full path including container name?
2. Why is the sink format JSON (not Parquet) for the Bronze layer in this exercise?
3. Two datasets (`ds_voltgrid_payments` and a future `ds_voltgrid_sessions`) both use `ls_voltgrid_api`. If you needed to change the API base URL, how many places in ADF would you update?

---

## Exercise 4 — Build the Pipeline with Web Activity + Copy Activity

**Concept:** Pipelines, Activities, Parameters, Expressions, dependency arrows

**4a. Create the pipeline**
- **"+" next to Pipelines → New pipeline**
- Name: `pl_ingest_voltgrid_payments_to_bronze`
- Description: Fetches VoltGrid payment data from REST API and loads to ADLS Gen2 bronze layer

**4b. Add pipeline parameters**
- Click the canvas background → **Parameters** tab at the bottom → **+ New** for each:

| Name | Type | Default value |
|---|---|---|
| `run_date` | String | `2026-01-15` |
| `api_username` | String | `voltgrid_demo` |
| `api_password` | String | `EVcharge@AU2025` |

**4c. Add Web Activity — GetAuthToken**
- Activities pane → expand **"General"** → drag **"Web"** onto canvas
- Name it: `GetAuthToken`
- **Settings tab:**

| Field | Value |
|---|---|
| URL | `https://your-app.vercel.app/api/auth/login/` |
| Method | POST |
| Headers | Click **+ New**: Name = `Content-Type`, Value = `application/json` |
| Body | `@{concat('{"username":"', pipeline().parameters.api_username, '","password":"', pipeline().parameters.api_password, '"}')}` |

**4d. Add Copy Activity — CopyPaymentsToBronze**
- Drag **"Copy data"** from "Move & transform" onto canvas
- Name it: `CopyPaymentsToBronze`
- Draw the **green arrow** from `GetAuthToken` to `CopyPaymentsToBronze` (On Success dependency)

**Source tab:**

| Field | Value |
|---|---|
| Source dataset | `ds_voltgrid_payments` |
| Dataset property: `auth_token` | `@activity('GetAuthToken').output.token` |

**Sink tab:**

| Field | Value |
|---|---|
| Sink dataset | `ds_adls_bronze_json` |
| Dataset property: `run_date` | `@pipeline().parameters.run_date` |

**Settings tab:**

| Field | Value |
|---|---|
| Data integration units | Auto |
| Fault tolerance | Skip incompatible rows |
| Enable logging | Checked |
| Log Linked service | `ls_adls_gen2` |
| Log folder path | `logs/copy-errors/` |

**4e. Add Web Activity — AlertOnFailure**
- Drag another **"Web"** onto canvas, name it: `AlertOnFailure`
- Draw the **red arrow** from `CopyPaymentsToBronze` to `AlertOnFailure` (On Failure)
- **Settings tab:**

| Field | Value |
|---|---|
| URL | `https://httpbin.org/post` (test endpoint — swap for real Teams/Slack webhook in prod) |
| Method | POST |
| Body | `@{concat('{"text":"Pipeline FAILED: ', pipeline().Pipeline, ' at ', utcnow(), '"}')}` |

**4f. Validate the pipeline**
- Click **"Validate"** in the top toolbar
- Fix any errors shown in the output panel

**4g. Debug run**
- Click **"Debug"**
- In the Parameters dialog, leave defaults (`run_date = 2026-01-15`)
- Click **"OK"**
- Watch the activities: `GetAuthToken` → green → `CopyPaymentsToBronze` → green

**4h. Inspect the Copy Activity output**
- In the Output tab at the bottom, click the **👓 glasses icon** on `CopyPaymentsToBronze`
- Record:
  - Rows copied: ____
  - Data written (MB): ____
  - DIUs used: ____
  - Duration: ____

**4i. Verify file in portal**
- Open `stadlsdev001` → `bronze` container → navigate to `payments/2026-01-15/`
- Confirm `payments_raw.json` exists

**4j. Publish All**
- Click **"Publish All"** → **"Publish"**

**Answer these questions:**
1. The expression `@activity('GetAuthToken').output.token` — what does `.output` refer to? Where does this JSON come from?
2. You set `run_date = 2026-01-15` in the debug run. What path was the file written to in ADLS Gen2?
3. If `GetAuthToken` fails (wrong password), does `CopyPaymentsToBronze` still run? Why?
4. You clicked Debug but did NOT Publish. A colleague opens ADF Studio — will they see the pipeline?

---

## Exercise 5 — Create a Schedule Trigger and Monitor Runs

**Concept:** Triggers, Monitoring

**5a. Create a Schedule Trigger**
- With `pl_ingest_voltgrid_payments_to_bronze` open, click **"Add trigger"** → **"New/Edit"**
- Click **"+ New"**

| Field | Value |
|---|---|
| Name | `trg_daily_payments` |
| Type | Schedule |
| Start date | Today |
| Time | 02:00 AM UTC |
| Recurrence | Every 1 Day |
| End | No end |

- In the trigger parameter mapping:

| Parameter | Value |
|---|---|
| `run_date` | `@{formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')}` |
| `api_username` | `voltgrid_demo` |
| `api_password` | `EVcharge@AU2025` |

- Click **OK** → **"Publish All"**

**5b. Verify the trigger is active**
- Go to **🔧 Manage → Triggers**
- Confirm `trg_daily_payments` shows status: **Started** (green dot)

**5c. Manually trigger a run**
- Go to **✏️ Author** → click `pl_ingest_voltgrid_payments_to_bronze`
- Click **"Add trigger"** → **"Trigger Now"**
- Set `run_date = 2026-01-16` → **"OK"**
- This runs the pipeline immediately with your specified parameters (not via the schedule trigger)

**5d. Monitor the run**
- Go to **📺 Monitor → Pipeline runs**
- Find the run triggered by "Manual" (from step 5c)
- Click on it → see the Activity runs
- Click the 👓 glasses on `GetAuthToken` → check the Output — you should see the token (first 20 chars visible)
- Click the 👓 glasses on `CopyPaymentsToBronze` → record the metrics

**5e. Check the file in the portal**
- Go to `stadlsdev001` → `bronze/payments/2026-01-16/`
- Confirm `payments_raw.json` exists
- Click the file → **"Edit"** to preview the JSON content

**5f. Set up a pipeline failure alert**
- Go to the Azure portal (not ADF Studio) → `adf-datalake-dev` → **Monitoring → Alerts**
- Click **"+ New alert rule"**
- Condition: search for "Failed pipeline runs" metric → Threshold: > 0
- Action group → create new → add your email address
- Alert rule name: `alert-adf-pipeline-failures`
- Save

**Answer these questions:**
1. What is the difference between `trg_daily_payments` (Schedule Trigger) and a Tumbling Window Trigger? When would you use Tumbling Window?
2. The trigger fires at 2:00 AM UTC on January 16. What value will `@{formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')}` produce?
3. If the pipeline fails on Monday and you fix the issue on Tuesday, will the Schedule Trigger automatically rerun Monday's data?
4. You set up the email alert. Under what condition will Azure send an email to your inbox?

---

## Exercise 6 — Enable Git Integration

**Concept:** Git integration, source control for ADF resources

**Pre-requisite:** A GitHub account and the `azure-ev-end-to-end-project` repository.

**6a. Configure Git**
- Go to **🔧 Manage → Git configuration → Configure**
- Select: **GitHub**

| Field | Value |
|---|---|
| GitHub account | your GitHub username |
| Repository name | `azure-ev-end-to-end-project` |
| Collaboration branch | `aug-batch` |
| Publish branch | `adf_publish` |
| Root folder | `/azure-data-factory-service/adf-resources` |

- Click **"Apply"**

**6b. Observe what changes in the Studio**
- The top bar now shows a branch selector and a **"Save"** button (instead of just "Publish All")
- Each resource (pipeline, dataset, linked service) now has a Save that commits to Git
- **"Publish All"** now deploys from the collaboration branch to the live factory

**6c. Make a small change and commit to Git**
- Open `pl_ingest_voltgrid_payments_to_bronze`
- Click on the `GetAuthToken` Web Activity → change the Name to `GetAuthToken_v2`
- Click **"Save"** (top bar) — ADF commits this change to the `aug-batch` branch
- Go to your GitHub repo → browse to `/azure-data-factory-service/adf-resources/pipeline/`
- You should see `pl_ingest_voltgrid_payments_to_bronze.json` — open it and find the `name` field showing `GetAuthToken_v2`

**6d. Understand the branch model**
Answer:
1. When you click **"Save"** in ADF Studio with Git connected, where does the change go?
2. When you click **"Publish All"** with Git connected, what does ADF actually deploy?
3. A developer works on a feature branch `feature/add-sessions-pipeline`. They save their changes but do not merge to `aug-batch`. Will the live ADF factory execute their new pipeline? Why not?

---

## Bonus Challenge — Add Sessions Pipeline to the Metadata-Driven Design

You have a working `pl_ingest_voltgrid_payments_to_bronze` pipeline. Now extend it to also ingest `sessions` and `chargers` data from the same API without creating separate pipelines.

**Approach: use Execute Pipeline activity**

1. Create a new generic pipeline `pl_generic_api_ingest` with parameters:
   - `api_endpoint` (String) — e.g. `/api/db/payments/`
   - `bronze_folder` (String) — e.g. `payments`
   - `run_date` (String)
   - `api_username` (String)
   - `api_password` (String)

2. Move the `GetAuthToken` + `CopyToBronze` + `AlertOnFailure` activities into this generic pipeline

3. Create a new orchestrator pipeline `pl_orchestrate_all_ingestion` that:
   - Uses a **ForEach Activity** over a JSON array of table configs
   - Inside ForEach: uses **Execute Pipeline Activity** to call `pl_generic_api_ingest` with the right parameters for each table

4. The items array for ForEach:
```json
[
  {"endpoint": "/api/db/payments/", "folder": "payments"},
  {"endpoint": "/api/db/sessions/", "folder": "sessions"}
]
```

5. Inside ForEach, the Execute Pipeline parameters:
   - `api_endpoint` → `@item().endpoint`
   - `bronze_folder` → `@item().folder`
   - `run_date` → `@pipeline().parameters.run_date`

6. Debug the orchestrator — confirm both inner pipeline runs appear in Monitor under the parent run

---

## Summary Checklist

By the end of today you should be able to:

- [ ] Create an ADF instance and navigate all 4 Studio panels
- [ ] Explain what a Linked Service is and why one per source system
- [ ] Create an ADLS Gen2 Linked Service and test connection
- [ ] Create an HTTP Linked Service (VoltGrid API)
- [ ] Create a JSON Dataset with a parameter-driven path
- [ ] Build a pipeline with Web Activity (get token) → Copy Activity (fetch data) → On Failure alert
- [ ] Pass a pipeline parameter into a dataset parameter using an expression
- [ ] Reference a Web Activity output in a downstream activity: `@activity('GetAuthToken').output.token`
- [ ] Run a Debug run and inspect the activity output (rows copied, DIUs, throughput)
- [ ] Verify the output file in ADLS Gen2 via the Azure portal
- [ ] Create a Schedule Trigger with a dynamic `run_date` parameter
- [ ] Enable Git integration and commit a change to the collaboration branch
- [ ] Monitor a pipeline run and drill into activity-level detail
- [ ] Set up an Azure Monitor alert rule for pipeline failures
