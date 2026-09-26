# Day 1 — Interview Solutions: ADF Introduction & Terminologies

---

## Concept 1: What is ADF

**Q1 — What is Azure Data Factory? What problem does it solve?**

Azure Data Factory is Microsoft's managed, cloud-native data integration and orchestration service. It moves data between 90+ source/sink types and orchestrates the workflow of data pipelines — all without managing servers.

**What a cron job + Python script cannot do out of the box:**
- **Connectivity:** You write SDK code per source. ADF has built-in connectors for REST APIs, SQL Server, Oracle, Salesforce, SAP, S3, ADLS Gen2, and 85+ more.
- **Monitoring:** A failed cron job sends no alert unless you build one. ADF logs every run to the Monitor panel with full activity-level detail, and integrates with Azure Monitor for email/Teams alerts.
- **Retry logic:** You implement retries manually. ADF has configurable retry count and interval per activity.
- **Scaling:** Your script runs on one machine. ADF auto-scales Copy Activity compute (DIUs) based on data volume.
- **Non-engineer maintainability:** No one else can read a Python file without Python knowledge. ADF's visual Studio is readable by anyone.

---

**Q2 — ETL vs. ELT. Which is preferred in Azure data lake + Databricks?**

**ETL:** Extract → Transform (in a middle tier) → Load clean data to destination.
**ELT:** Extract → Load raw data to destination → Transform inside the destination.

| | ETL | ELT |
|---|---|---|
| Transform location | Middle-tier compute (ADF Data Flow, SSIS) | Inside destination (Databricks Spark, Synapse SQL) |
| Best for | Structured relational targets | Cloud data lakes with powerful compute |
| Destination receives | Clean, shaped data | Raw data |

**Preferred in Azure: ELT**

ADF's Copy Activity loads raw API/CSV data into Bronze (ADLS Gen2). Databricks Spark then transforms Bronze → Silver → Gold. The destination (Databricks) is powerful enough to do the transformation — no middle tier needed. ADF acts as the orchestration layer only.

---

**Q3 — 4 ADF Studio panels. Which for a 3am failure?**

| Panel | What you do |
|---|---|
| **Author** | Build and edit pipelines, datasets, linked services, data flows |
| **Monitor** | See every pipeline run, activity run, trigger run, and error details |
| **Manage** | Create linked services, configure integration runtimes, triggers, Git |
| **Learn** | Browse pipeline templates and tutorials |

**For a 3am failure: Monitor panel.**
- Go to Pipeline runs → find the failed run → click into Activity runs → click the 👓 glasses icon on the failed activity → read the full error message in the Output/Error tab.

---

**Q4 — Three scenarios where ADF beats a Python script**

1. **Multiple on-premises sources:** Your company has SQL Server, Oracle, and SAP on-premises behind a firewall. With ADF, you install one Self-Hosted IR agent on a machine inside the network and ADF handles all three. With Python, you need VPN tunneling, driver installation, and separate scripts for each source.

2. **Non-technical team ownership:** A BI team needs to run ad-hoc data loads from Salesforce. In ADF, they click "Trigger Now" in the Studio. With Python, they need a developer every time.

3. **Complex orchestration with retry and alerting:** 20 pipelines must run nightly in a specific order. If pipeline 7 fails, pipelines 8–20 should be skipped and the on-call engineer should receive a Teams alert. ADF handles this with On Failure dependency arrows + Web Activity + Azure Monitor alerts. In Python/cron, you build all of this from scratch.

---

**Q5 — Integration Runtime types**

An Integration Runtime (IR) is the compute engine that executes ADF activities.

| | Azure IR | Self-Hosted IR |
|---|---|---|
| Setup | None — fully managed by Microsoft | Install agent on a machine in your network |
| Reaches | Public internet and Azure PaaS services | On-premises servers, private VNet resources |
| Use case | REST APIs, ADLS Gen2, Azure SQL | On-prem SQL Server, Oracle, SAP, private VNet DB |
| Cost | Included in Copy Activity cost | Cost of the VM running the agent |

**Self-Hosted IR is required when:**
- The source/sink is on-premises (not accessible from the public internet)
- The source/sink is in a private Azure VNet with public access disabled
- You need to copy data from a network-isolated resource (e.g., SQL MI with no public endpoint)

---

**Q6 — What does "Publish All" do? Save vs. Publish with Git.**

**Without Git:**
- "Save" in ADF = save to ADF's internal storage (immediately live)
- "Publish All" = same as save — deploys to the live factory

**With Git connected:**
- "Save" = commit the pipeline/dataset JSON to the **collaboration branch** in Git (e.g., `main` or `aug-batch`) — NOT live yet
- "Publish All" = ADF reads from the collaboration branch, compiles an ARM template, and deploys it to the **live factory** (writes to `adf_publish` branch)

This means with Git, your changes go through a review cycle before they affect production. Without Git, every save is immediately live — dangerous on a shared factory.

---

**Q7 — 5 engineers overwriting each other — solution**

Enable **Git integration** in ADF (Manage → Git configuration).

How it solves the problem:
- Each engineer works in their own **feature branch** — their changes do not affect others
- Changes merge to the **collaboration branch** via Pull Request — peer review required
- Only the collaboration branch gets **Published** to the live factory — no one can accidentally overwrite production by clicking Save

Without Git, all 5 engineers share one draft state — the last person to save wins, overwriting everyone else's work.

---

**Q8 — DIUs. How to fix a slow 200 GB Copy Activity?**

A **Data Integration Unit (DIU)** is a bundle of CPU, memory, and network capacity. ADF uses multiple DIUs in parallel to copy data faster.

**Fix for slow 200 GB copy:**
- Go to the Copy Activity → **Settings tab** → **Data integration units**
- Change from **Auto** to **32** (maximum parallel threads)
- ADF now uses 32 parallel read/write threads → roughly 8–16x faster

**Trade-off:** Cost. Each DIU costs money per hour. Doubling DIUs roughly halves copy time but doubles the compute cost. For a nightly batch that has a 4-hour window, the extra cost is usually worth the faster completion.

---

**Q9 — Fault tolerance in Copy Activity**

Fault tolerance defines what ADF does when it encounters a row it cannot copy (type mismatch, null in a NOT NULL column, encoding error).

| Setting | What happens |
|---|---|
| Fail on first error (default) | Pipeline fails immediately — zero rows written |
| Skip incompatible rows | Bad row is skipped, copy continues for all other rows |
| Enable logging | Skipped rows written to an ADLS Gen2 log file for review |

Always enable logging when skipping — silent discard hides data quality problems. The log file shows the exact row, error code, and reason for each skip.

---

**Q10 — Debug without Publish — what does the trigger run?**

The trigger runs the **previously published version** — not your Debug changes.

**Why:** In ADF (with Git), Debug runs against the **current draft** in your browser session. Triggers always run against the **published ARM template** in the live factory. Publishing deploys the draft to the live factory. If you close without publishing, your changes exist only in the collaboration branch (or in ADF's internal draft if Git is not configured) — never in the live factory's execution engine.

This is an important safety mechanism: you can iterate on a pipeline with Debug all day without affecting production trigger runs.

---

## Concept 2: ADF Terminologies

**Q11 — What is a Linked Service? Why one per source system?**

A Linked Service is ADF's saved connection definition — it stores the connection URL, authentication method, and credential reference for a data source or destination.

**Why one per source system (not one per pipeline):**

If you create one Linked Service per pipeline for the same database:
- 50 pipelines → 50 Linked Services → all with the same credentials
- When the database password rotates, you update 50 Linked Services — high risk of missing one → production pipeline failure

With one Linked Service per source system:
- Password rotation: update 1 Linked Service (or 1 Key Vault secret) → all 50 pipelines automatically use the new credential
- Consistent connection settings: all pipelines use the same timeout, retry, and auth configuration

---

**Q12 — Linked Service vs. Dataset. Can two Datasets share one Linked Service?**

| | Linked Service | Dataset |
|---|---|---|
| Stores | HOW to connect (credentials, server URL) | WHERE the data is + WHAT it looks like |
| Example | "Connect to this ADLS Gen2 account with account key X" | "The `bronze/payments/` path, Parquet format, these columns" |
| Created | Once per source system | Once per data entity (table, file, endpoint) |

**Yes — two Datasets absolutely share one Linked Service.** Example:
- `ls_voltgrid_api` — one Linked Service for the VoltGrid API
- `ds_voltgrid_payments` → uses `ls_voltgrid_api`, path `/api/db/payments/`
- `ds_voltgrid_sessions` → uses `ls_voltgrid_api`, path `/api/db/sessions/`

One connection, multiple dataset definitions.

---

**Q13 — Four activity types — one from each category**

**Data Movement:**
- **Copy Activity** — reads from a source Dataset, writes to a sink Dataset. Handles format conversion, schema mapping, and parallelism. The workhorse of ADF.

**Data Transformation:**
- **Data Flow** — no-code Spark transformation designer. Filter, join, aggregate, pivot, split data. Runs on an auto-provisioned Spark cluster.
- **Databricks Notebook Activity** — runs a Databricks notebook (Python/Scala/SQL) as a pipeline step.

**Control Flow:**
- **ForEach Activity** — loops over an array, runs inner activities for each item (parallel or sequential).
- **Web Activity** — calls any HTTP endpoint — used to get tokens, send Slack/Teams alerts, or trigger external webhooks.
- **Lookup Activity** — runs a query and returns the result for use in downstream activities (e.g., read a config table to drive ForEach).

---

**Q14 — REST API with Bearer token — which ADF activities and order?**

```
Step 1: Web Activity (GetAuthToken)
  Method: POST
  URL:    https://api.example.com/auth/login/
  Body:   {"username": "...", "password": "..."}
  Output: {"token": "abc123..."}

Step 2: Copy Activity (CopyData)   ← On Success dependency from Step 1
  Source dataset: HTTP dataset pointing to /api/db/payments/
  Dataset parameter: auth_token = @activity('GetAuthToken').output.token
  Authorization header on dataset: Token @{dataset().auth_token}
  Sink: ADLS Gen2 dataset
```

The Web Activity output (`@activity('GetAuthToken').output.token`) is referenced in the Copy Activity's source dataset parameter — injecting the token into the Authorization header of every API request.

---

**Q15 — Pipeline Parameter vs. Dataset Parameter**

**Pipeline Parameter:** Defined on the pipeline — controls the pipeline's behaviour at runtime. Set by triggers or when you click "Trigger Now".

**Dataset Parameter:** Defined on the dataset — controls WHERE the dataset points (file path, table name). Set by the pipeline when it uses the dataset.

**When you need both:**
```
Pipeline has: run_date (String) = "2026-01-15"
Dataset has:  run_date (String) = used in path: bronze/payments/@{dataset().run_date}

In the Copy Activity Sink tab:
  Dataset property run_date = @pipeline().parameters.run_date
```

The trigger sets the pipeline parameter → the pipeline passes it to the dataset parameter → the dataset builds the correct file path. Without both levels, you cannot make the path dynamic AND have the trigger control the date.

---

**Q16 — ADF expressions**

**1. Dynamic path from `run_date` pipeline parameter:**
```
@{concat('bronze/payments/', pipeline().parameters.run_date, '/')}
```
Result: `bronze/payments/2026-01-15/`

**2. Today's date as `yyyy-MM-dd`:**
```
@{formatDateTime(utcnow(), 'yyyy-MM-dd')}
```
Result: `2026-01-15` (whatever today's UTC date is)

---

**Q17 — Activity A copies 0 rows → prevent Activity B from running**

**Option 1 — Get Metadata + If Condition:**
```
Copy Activity (A)
    → On Success →
Get Metadata Activity (check file size in ADLS Gen2)
    → If Condition: @{greater(activity('GetMeta').output.size, 0)}
        True  → Databricks Notebook (B)
        False → Fail Activity ("Zero rows — aborting transform")
```

**Option 2 — Check Copy Activity output directly:**
```
If Condition: @{greater(activity('CopyData').output.rowsCopied, 0)}
    True  → Databricks Notebook (B)
    False → Web Activity (send "empty source" alert)
```

`activity('CopyData').output.rowsCopied` is available in the pipeline after the Copy Activity completes. This is the simplest pattern.

---

**Q18 — Copy Activity vs. Data Flow**

| | Copy Activity | Data Flow |
|---|---|---|
| Purpose | Move data — minimal transformation | Transform, enrich, reshape data |
| Transformations | Column rename + type cast only | Filter, join, aggregate, pivot, split, lookup, deduplicate |
| Compute | ADF managed movement service | Apache Spark (auto-provisioned) |
| Speed | Very fast for bulk movement | Spark startup overhead (~2–3 min) — slower for small data |
| Cost | Cheap — per DIU-hour | More expensive — per vCore-hour |
| Code required | Zero | Zero (visual) or ADF expression language |

**Use Copy Activity for:** Bronze ingestion — move raw data from API/DB to ADLS Gen2 as-is.
**Use Data Flow for:** Silver transformation — clean nulls, deduplicate, join reference tables, aggregate.

---

**Q19 — File arrives at unknown time — trigger design**

**Best approach: Storage Event Trigger**
```
Trigger type: Storage Event
Storage account: stadlsdev001
Container: bronze
Blob path begins with: landing/
Event: Blob created
```
The pipeline fires the moment the external team drops the file — no polling, no waiting loop. Works at 6am or noon automatically.

**Alternative if Storage Event is not available:**
Use a **Schedule Trigger** (e.g., every 30 minutes) + **Until Activity** inside the pipeline:
```
Until Activity (timeout: 12 hours):
    Get Metadata → check if file exists
    If not exists → Wait Activity (15 minutes) → retry
Copy Activity ← runs once Until exits
```

**For "never arrives" case:** The Until Activity has a timeout. If the file never arrives in 12 hours, the pipeline fails → Azure Monitor alert fires.

---

**Q20 — ForEach Activity — Sequential vs. Batch**

The **ForEach Activity** loops over an array and runs one or more inner activities for each item.

| Mode | Behaviour | When to use |
|---|---|---|
| Sequential | Items processed one at a time | When order matters, or source cannot handle concurrent connections |
| Batch (default, max 50) | Items processed in parallel up to batch count | When items are independent and you want speed |

**Real example:**
A Lookup reads 20 table names from a config database. ForEach processes each with a Copy Activity.

```
Lookup → [{"table": "payments"}, {"table": "sessions"}, ...]
ForEach (Is Sequential: false, Batch count: 20):
    Copy Activity:
        source: SELECT * FROM @{item().table}
        sink:   bronze/@{item().table}/
```

With batch count 20, all 20 copies run simultaneously — total time = slowest single copy, not sum of all.

---

## Concept 3: Hands-on / Mixed Senior

**Q21 — REST API returns only first 100 records — how to get all pages?**

ADF's Copy Activity has built-in **pagination support** in the HTTP connector.

In the Source tab of the Copy Activity → **Pagination rules:**

```
Pagination rule type: NextPageUrl
NextPageUrl expression: @{body().next}   ← reads the "next" field from each response
Stop condition: @{equals(body().next, null)}   ← stop when "next" is null
```

ADF will keep calling the next page URL until `next` is null — automatically fetching all pages into a single output file. This is the correct built-in solution, no ForEach loop needed.

If the API uses page numbers instead of next URLs:
```
Pagination rule type: TotalItemCount
Page size: 100
Total item count: @{body().pagination.total_count}
```

---

**Q22 — ADF Git integration — collaboration vs. publish branch**

**Git integration** stores all ADF resources (pipelines, datasets, linked services) as JSON files in a Git repository (GitHub or Azure DevOps).

**Branch model:**
```
feature/new-pipeline  ← developer works here
        ↓ Pull Request + review
collaboration branch (e.g. main / aug-batch)  ← merged, reviewed changes
        ↓ "Publish All" in ADF Studio
adf_publish branch  ← ARM template deployed to the live factory
        ↓
Live ADF Factory  ← triggers and schedules run against this
```

- **Collaboration branch:** Where developers merge reviewed changes. "Save" in ADF Studio commits here.
- **Publish branch:** ADF writes the compiled ARM template here when you click "Publish All". The live factory is deployed from this branch — not from the collaboration branch directly.

**Key point:** You can have 100 commits on the collaboration branch that are not published — the live factory only changes on Publish.

---

**Q23 — Token expires in 1 hour, pipeline runs 90 minutes**

**Problem:** The Web Activity fetches a token at pipeline start. After 60 minutes, the token expires. ForEach iterations 41–50 call the API with an expired token → `401 Unauthorized` errors.

**Fix options:**

1. **Refresh token inside ForEach:** Move the GetAuthToken Web Activity inside the ForEach loop — each iteration gets a fresh token before calling the API. Cost: 50 extra Web Activity calls. Benefit: token is always valid.

2. **Token refresh activity at intervals:** Use a counter variable — every 10 iterations, re-call GetAuthToken and update a pipeline variable `current_token`. Downstream activities reference `@variables('current_token')`.

3. **Use Managed Identity instead of token auth:** If the API supports Azure AD authentication, configure the Linked Service with Managed Identity — no token expiry issue at all.

**Best practice for production:** Design APIs accessed by ADF to use short-lived tokens + refresh tokens, or use Azure AD OAuth 2.0 with Managed Identity — no manual token management in pipelines.

---

**Q24 — Tumbling Window for guaranteed daily completeness**

**Answer: Tumbling Window Trigger**

| | Schedule Trigger | Tumbling Window |
|---|---|---|
| Tracks completed windows | No | Yes |
| Reruns failed windows | No — just fires again next day | Yes — reruns Monday automatically when fixed |
| Backfill support | No | Yes — set start date in the past |

**Why not Schedule:** If Monday's pipeline fails, the Schedule Trigger fires on Tuesday and runs Tuesday's data. Monday is permanently lost unless someone manually reruns it.

**Why Tumbling Window:** Each day is a "window". The trigger tracks whether each window completed. When the bug is fixed, ADF automatically reruns the failed Monday window — guaranteed completeness across the time series.

```
Tumbling Window Trigger:
  Start: 2026-01-01 00:00 UTC (past date = backfill from Jan 1)
  Frequency: 1 Day
  Max concurrency: 1 (process one day at a time)
  Retry: 3 times, 30 min interval
```

---

**Q25 — Foreign key constraint failure — diagnosing in Monitor**

**In the Monitor panel:**

1. Go to **Pipeline runs** → find the failed run → click it
2. Go to **Activity runs** → find the failed Copy Activity → click 👓 glasses
3. In the **Error** tab: read the full `SqlException` — it will name the constraint and the table
4. In the **Output** tab: check `rowsCopied` vs. `rowsRead` — if rows were partially copied before the error, note the count

**Finding the exact error row:**
- If fault tolerance with logging was enabled: go to ADLS Gen2 → `logs/copy-errors/` → open the log file → each skipped row is listed with its data and error reason
- If fault tolerance was NOT enabled (fail fast mode): the error message contains the problematic value — use it to query the source table: `SELECT * FROM source WHERE fk_column = 'failing_value'`

**Root cause for FK violation:** The source data references a foreign key that does not exist in the destination table — e.g., copying `payments` before `customers`, where `payments.customer_id` references `customers.id`. Fix: load `customers` first (add dependency arrow).

---

**Q26 — Production authentication for ADLS Gen2**

**Recommended: System-assigned Managed Identity**

```
ADF Linked Service → Authentication: System Managed Identity
Azure RBAC on stadlsdev001 → Storage Blob Data Contributor → ADF's Managed Identity
```

**Why Managed Identity:**
- No secrets to store, rotate, or leak
- Azure automatically provisions and rotates the underlying credentials
- If the ADF instance is deleted, its Managed Identity is deleted — access is revoked automatically
- Full audit trail in Azure AD sign-in logs

**Why Account key is bad in production:**
1. Account key = full control over the entire storage account — all containers, all operations. Over-privileged.
2. Keys do not expire — a leaked key stays valid until manually rotated.
3. Rotation breaks every service using that key simultaneously — no zero-downtime rotation.
4. No audit trail — you cannot see who used the key or when.

---

**Q27 — ForEach with 50 tables taking 4 hours — reduce to under 1 hour**

**Setting to change:** ForEach Activity → **Batch count** (degree of parallelism)

**Current likely state:** `Is Sequential: true` or `Batch count: 1` → tables processed one at a time → 4 hours / 50 tables = ~5 minutes per table

**Change to:**
```
ForEach: Is Sequential = false
         Batch count   = 50 (maximum in ADF)
```

Now all 50 Copy Activities run simultaneously → total time = time of the slowest single table (likely 5–10 minutes).

**Also check:**
- Each Copy Activity: set DIUs to a fixed number (e.g., 4) rather than Auto — prevents one large table from consuming all available DIUs and starving others
- Source database: ensure it can handle 50 concurrent connections — add a read replica if not
- ADLS Gen2 sink: can handle high parallelism natively — no concern

---

**Q28 — Web Activity — two real production use cases**

1. **Get an auth token from a REST API before copying data:**
   ```
   Web Activity: POST /api/auth/login/ → {"token": "abc123"}
   Copy Activity: uses @activity('GetToken').output.token in Authorization header
   ```
   This is how you handle token-based API auth in ADF without storing credentials in a Dataset.

2. **Send a Teams/Slack alert on pipeline failure:**
   ```
   Web Activity: POST to Teams webhook URL
   Condition: On Failure dependency from the failed Copy Activity
   Body: {"text": "Pipeline FAILED: pl_ingest_payments at @{utcnow()}"}
   ```
   Production pipelines have this pattern on every critical activity — instant notification to the on-call engineer without needing to check the Monitor panel at 3am.

3. *(Bonus)* **Trigger an external system:** Call an Azure Logic App, an Azure Function, or a third-party webhook to notify downstream systems that data has landed — e.g., "bronze/payments/2026-01-15/ is ready, please start your Databricks job."

---

**Q29 — 80 Linked Services with hardcoded credentials — blast radius and fix**

**Blast radius of password rotation:**
- All 80 Linked Services immediately fail to connect → all 200 pipelines that use them fail
- Any scheduled trigger that fires between rotation and manual fix causes a failure
- Manual fix required: update credentials in all 80 Linked Services one by one

**Redesign:**

**Step 1 — One Linked Service per source system:**
```
ls_sql_paymentsdb  ← 1 Linked Service for the SQL DB
All 200 pipelines reference this one Linked Service
```

**Step 2 — Store credential in Azure Key Vault:**
```
Key Vault Secret: sql-db-password = "actual-password"
Linked Service: references Key Vault secret — not the password itself
```

On password rotation:
1. DBA rotates password
2. Update ONE Key Vault secret value
3. All 200 pipelines automatically use the new password on next run — zero Linked Service changes needed

---

**Q30 (System design) — VoltGrid EV nightly ingestion**

| Requirement | ADF Component & Configuration |
|---|---|
| Run at 1:00 AM UTC nightly | **Tumbling Window Trigger** — start Jan 1, frequency 1 day, fires at 01:00 UTC. Tumbling Window (not Schedule) so failed nights are automatically retried. |
| All 5 endpoints complete within 2 hours | **ForEach Activity** with `Is Sequential: false`, `Batch count: 5` — all 5 endpoints copy in parallel. Total time = slowest single endpoint. |
| Individual failure doesn't cascade | ForEach runs each endpoint independently. A failure in one item does not stop others — ForEach continues by default. |
| Retry once before alerting | Each Copy Activity inside ForEach: **Settings → Retry count: 1, Retry interval: 5 minutes** |
| Teams alert if > 2 endpoints fail | After ForEach completes, use a **Script Activity or Set Variable** to count failures from pipeline run metadata. Then **If Condition**: `@{greater(variables('failure_count'), 2)}` → **Web Activity** → POST to Teams webhook |
| Bronze/{table}/{run_date}/ as JSON | ADLS Gen2 sink Dataset with parameters: `bronze/@{dataset().table_name}/@{dataset().run_date}/` |
| Minimal changes to add a 6th endpoint | **Metadata-driven design:** store endpoint configs in a JSON file in ADLS Gen2. Lookup Activity reads the config. ForEach iterates over it. Adding a 6th endpoint = add one row to the config file, no pipeline changes. |

**Full pipeline structure:**
```
Tumbling Window Trigger (daily 01:00 UTC)
    ↓ passes run_date = trigger().scheduledTime
Web Activity: GetAuthToken
    ↓ On Success
ForEach (5 endpoint configs, parallel):
    Inner: Copy Activity
        source: ls_voltgrid_api → @item().endpoint
        sink:   bronze/@{item().table_name}/@{pipeline().parameters.run_date}/
        Retry: 1, interval: 5 min
        Fault tolerance: skip incompatible rows, log to bronze/logs/
    ↓ ForEach completes (all iterations done)
Script Activity: count failed iterations
    ↓
If Condition: failure_count > 2
    True → Web Activity: POST to Teams webhook
    False → (no action)
```

**Config file** (`bronze/config/endpoint_config.json`) — adding endpoints requires only editing this file:
```json
[
  {"table_name": "payments",   "endpoint": "/api/db/payments/"},
  {"table_name": "sessions",   "endpoint": "/api/db/sessions/"},
  {"table_name": "customers",  "endpoint": "/api/db/customers/"},
  {"table_name": "vehicles",   "endpoint": "/api/db/vehicles/"},
  {"table_name": "stations",   "endpoint": "/api/db/stations/"}
]
```
