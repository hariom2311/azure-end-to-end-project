# Day 3 — Pipeline Control, Triggers & Monitoring

> **Goal:** Extend the Day 2 `pl_bronze_api_payments` pipeline to understand variables, parameters, system variables, and dynamic content expressions — then learn every way to schedule it, monitor it, and get alerted when it fails.

---

## Concept 1: Variables, Parameters, and Dynamic Content

The Day 2 pipeline had two hardcoded weaknesses: the page number was a manual trigger input, and the ingestion date was never captured. Day 3 fixes both using ADF's expression language.

### 1.1 Pipeline Parameters vs. Variables — The Core Difference

| | Parameter | Variable |
|---|---|---|
| Set by | Caller — trigger definition or "Trigger Now" form | Pipeline itself — via Set Variable activity |
| Mutability | Read-only inside the pipeline — cannot be changed after the run starts | Read/write — any Set Variable activity can change it |
| Accessible in | All activities, all expressions | All activities after the Set Variable runs |
| Use for | Inputs the caller controls: `run_date`, `load_type`, `page_size` | Internal state the pipeline manages: `v_token`, `v_current_page`, `v_ingestion_date` |

**Rule of thumb:** If a value comes from outside (the caller decides it), use a Parameter. If a value is computed inside the pipeline at runtime, use a Variable.

### 1.2 Parameters in the Payments Pipeline

The Day 2 `pl_bronze_api_payments` pipeline already has two parameters:

| Parameter | Type | Default | Who sets it |
|---|---|---|---|
| `p_page` | int | `1` | Trigger / Trigger Now form |
| `p_page_size` | int | `100` | Trigger / Trigger Now form |

**Adding a third parameter — `p_load_type`:**
In the pipeline Parameters tab, add:
- `p_load_type` | String | default: `full`

This lets a Schedule Trigger pass `"full"` on Sundays and `"incremental"` on weekdays — same pipeline, different behaviour.

**Reference a parameter in an expression:**
```
@pipeline().parameters.p_page
@pipeline().parameters.p_load_type
```

### 1.3 Variables in the Payments Pipeline

Variables hold runtime state. Add these in the Variables tab:

| Variable | Type | Purpose |
|---|---|---|
| `v_token` | String | Auth token from VoltGrid login |
| `v_ingestion_date` | String | Today's date — used in the Bronze sink path |
| `v_current_page` | int | Loop counter for pagination (Day 3+) |
| `v_total_pages` | int | Total pages returned by API (Day 3+) |

**Set a variable in a Set Variable activity:**
- Variable name: `v_ingestion_date`
- Value (dynamic content): `@formatDateTime(utcnow(), 'yyyy-MM-dd')`

**Read a variable in another activity:**
```
@variables('v_ingestion_date')
@variables('v_token')
@variables('v_current_page')
```

### 1.4 System Variables

System variables are built-in — ADF sets them automatically. You cannot set them yourself.

| System Variable | Type | What it returns | Example value |
|---|---|---|---|
| `@pipeline().runId` | String | Unique GUID for this pipeline run | `"a1b2c3d4-..."` |
| `@pipeline().pipelineName` | String | Name of the current pipeline | `"pl_bronze_api_payments"` |
| `@pipeline().groupId` | String | The trigger group (Tumbling Window) | `"grp-001"` |
| `@pipeline().parameters.x` | Any | Value of parameter `x` | `1` |
| `@trigger().scheduledTime` | DateTime | When the trigger was scheduled to fire | `"2026-09-28T01:00:00Z"` |
| `@trigger().startTime` | DateTime | When the trigger actually started executing | `"2026-09-28T01:00:03Z"` |
| `@trigger().outputs.windowStartTime` | DateTime | Tumbling Window start (Tumbling Window only) | `"2026-09-27T00:00:00Z"` |
| `@trigger().outputs.windowEndTime` | DateTime | Tumbling Window end (Tumbling Window only) | `"2026-09-28T00:00:00Z"` |

**Most useful in practice:**
- `@trigger().scheduledTime` — use this as the `run_date` in your sink path so data is partitioned by the scheduled date, not the actual wall-clock time of execution.
- `@pipeline().runId` — stamp this in audit log rows so you can trace exactly which run produced a Bronze file.

### 1.5 Dynamic Content Functions

These are functions you use inside `@{...}` or `@function()` expressions in any ADF field.

#### String Functions

| Function | What it does | Example |
|---|---|---|
| `concat(a, b, ...)` | Join strings | `@concat('Token ', variables('v_token'))` → `Token abc123` |
| `string(x)` | Convert any type to string | `@string(variables('v_current_page'))` → `"3"` |
| `toUpper(s)` / `toLower(s)` | Case conversion | `@toUpper('full')` → `"FULL"` |
| `trim(s)` | Remove leading/trailing whitespace | `@trim(' hello ')` → `"hello"` |
| `replace(s, old, new)` | Find and replace | `@replace('page 1', '1', '2')` → `"page 2"` |
| `substring(s, start, len)` | Extract part of string | `@substring('2026-09-28', 0, 4)` → `"2026"` |
| `length(s)` | Length of string or array | `@length('hello')` → `5` |

#### Date Functions

| Function | What it does | Example |
|---|---|---|
| `utcnow()` | Current UTC datetime | `"2026-09-28T14:30:00.0000000Z"` |
| `formatDateTime(dt, fmt)` | Format a datetime as string | `@formatDateTime(utcnow(), 'yyyy-MM-dd')` → `"2026-09-28"` |
| `addDays(dt, n)` | Add N days | `@addDays(utcnow(), -1)` → yesterday |
| `addHours(dt, n)` | Add N hours | `@addHours(utcnow(), -24)` → 24 hrs ago |
| `convertTimeZone(dt, from, to)` | Convert timezone | `@convertTimeZone(utcnow(), 'UTC', 'AUS Eastern Standard Time')` |
| `startOfDay(dt)` | Midnight of the given day | `@startOfDay(utcnow())` → `"2026-09-28T00:00:00Z"` |

#### Math Functions

| Function | What it does | Example |
|---|---|---|
| `add(a, b)` | Addition | `@add(variables('v_current_page'), 1)` → `4` |
| `sub(a, b)` | Subtraction | `@sub(10, 3)` → `7` |
| `mul(a, b)` | Multiplication | `@mul(3, 4)` → `12` |
| `div(a, b)` | Integer division | `@div(10, 3)` → `3` |
| `mod(a, b)` | Remainder | `@mod(10, 3)` → `1` |
| `greater(a, b)` | a > b | `@greater(5, 3)` → `true` |
| `greaterOrEquals(a, b)` | a >= b | `@greaterOrEquals(variables('v_current_page'), variables('v_total_pages'))` |
| `less(a, b)` / `lessOrEquals(a, b)` | Comparisons | |
| `equals(a, b)` | Strict equality (case-sensitive) | `@equals(pipeline().parameters.p_load_type, 'full')` |

#### Logical / Conditional

| Function | What it does | Example |
|---|---|---|
| `if(cond, true, false)` | Ternary | `@if(equals(pipeline().parameters.p_load_type, 'full'), '1900-01-01T00:00:00Z', pipeline().parameters.p_watermark)` |
| `and(a, b)` | Logical AND | `@and(greater(x, 0), less(x, 100))` |
| `or(a, b)` | Logical OR | |
| `not(x)` | Logical NOT | |
| `empty(x)` | True if string/array/object is empty | `@empty(pipeline().parameters.p_watermark)` |
| `coalesce(a, b, ...)` | First non-null value | `@coalesce(pipeline().parameters.p_watermark, '1900-01-01T00:00:00Z')` |

#### Activity Output

| Expression | What it reads |
|---|---|
| `@activity('act_api_login').output.token` | `token` field from login response |
| `@activity('act_get_username').output.value` | Secret value from Key Vault response |
| `@activity('act_get_total_pages').output.pipelineReturnValue` | Return value from child pipeline |
| `@activity('act_copy_payments').output.rowsCopied` | Rows written by Copy Activity |

### 1.6 Expression Syntax — Two Forms

ADF has two ways to write expressions:

**Form 1 — Full expression (entire field is an expression):**
```
@pipeline().parameters.p_page
@variables('v_ingestion_date')
@activity('act_api_login').output.token
```
Use when the field value IS the expression — no surrounding text.

**Form 2 — Interpolation inside a string (mixed text + expression):**
```
bronze/api/payments/ingestion_date=@{formatDateTime(utcnow(), 'yyyy-MM-dd')}/
page_@{variables('v_current_page')}.json
Token @{variables('v_token')}
```
Use `@{...}` when embedding an expression inside a larger string. The `@{...}` part is evaluated and the rest is literal text.

### 1.7 Upgrading the Day 2 Pipeline with Variables

**What we add to `pl_bronze_api_payments`:**

1. **Variable `v_ingestion_date`** — set by a new Set Variable activity after login:
   - Activity: `act_set_ingestion_date`
   - Value: `@formatDateTime(utcnow(), 'yyyy-MM-dd')`

2. **Dynamic sink path** — update `ds_bronze_payments_sink` to use a dataset parameter:
   - Add parameter `p_run_date` to the dataset
   - Folder path: `api/payments/ingestion_date=@{dataset().p_run_date}`
   - File: `page_@{pipeline().parameters.p_page}.json`

3. **Copy Activity sink** — pass the variable to the dataset:
   - Dataset parameter `p_run_date` = `@variables('v_ingestion_date')`

**Result:** Each pipeline run writes to a separate dated folder instead of overwriting the same `payments.json`. Running the pipeline on 2026-09-28 writes to `bronze/api/payments/ingestion_date=2026-09-28/page_1.json`.

**Updated pipeline variable flow:**
```
act_get_username → act_get_password → act_api_login
                                            ↓
                                      act_set_token  (v_token = login output)
                                            ↓
                               act_set_ingestion_date  (v_ingestion_date = today's date)
                                            ↓
                                     act_copy_payments  (sink path uses v_ingestion_date)
```

---

## Concept 2: Triggers — All Ways to Schedule a Pipeline

A trigger defines WHEN a pipeline runs. ADF has four trigger types.

### 2.1 Manual Trigger (Trigger Now)

Not a saved trigger — you click **Add trigger → Trigger now** in the Studio. Used for:
- One-off backfills
- Testing before setting up a real trigger
- Ad-hoc reruns after fixing a bug

No scheduling logic. Fires immediately. Parameters entered in the pop-up dialog.

### 2.2 Schedule Trigger

Runs on a cron-style fixed schedule. Configured with:
- **Start time:** when to begin firing
- **Recurrence:** every N minutes / hours / days / weeks / months
- **End time:** optional — stop firing after this date/time
- **Time zone:** the schedule is evaluated in this timezone

**Key limitation:** Schedule Trigger has NO memory of past runs. If the pipeline fails on Monday night, the trigger simply fires again on Tuesday — Monday's data is never retried automatically.

**Creating a Schedule Trigger for daily payments load:**
1. ADF Studio → **Manage** → **Triggers** → **+ New**
2. Type: **Schedule**
3. Name: `trg_payments_daily`
4. Start time: today at `01:00 UTC`
5. Recurrence: every `1 Day`
6. Parameters:
   - `p_page`: `1`
   - `p_page_size`: `100`
   - `p_load_type`: `incremental`
7. **OK** → **Publish all** → **Activate**

**Accessing schedule time in the pipeline:**
```
@trigger().scheduledTime   → "2026-09-28T01:00:00Z"  (scheduled fire time)
@trigger().startTime       → "2026-09-28T01:00:04Z"  (actual execution start)
```

Use `scheduledTime` for the sink path — not `utcnow()` — so a delayed execution still writes to the correct date's folder.

### 2.3 Tumbling Window Trigger

The most important trigger type for data engineering. Unlike Schedule, it tracks every time window and guarantees completeness.

**Key properties:**
- **Window size:** fixed interval (e.g. 1 hour, 1 day)
- **Start time:** can be in the past — ADF backfills all missed windows automatically
- **Concurrency:** how many windows can run in parallel (max 50)
- **Retry policy:** per window — if a window fails, ADF retries it automatically

**How it differs from Schedule:**

| | Schedule Trigger | Tumbling Window |
|---|---|---|
| Tracks past windows | No | Yes — each window has a status |
| Auto-retry failed windows | No | Yes — retries based on retry policy |
| Backfill support | No | Yes — set start date in the past |
| Data partitioning | Use `trigger().scheduledTime` | Use `trigger().outputs.windowStartTime` |
| Use for | Simple recurring tasks | Time-series data where every window must complete |

**Tumbling Window system variables:**
```
@trigger().outputs.windowStartTime   → start of the current window
@trigger().outputs.windowEndTime     → end of the current window
```

**Example — daily payments with Tumbling Window:**
1. Manage → Triggers → **+ New** → Type: **Tumbling window**
2. Name: `trg_payments_tumbling`
3. Start time: `2026-09-01T01:00:00Z` (past date — ADF backfills Sep 1 through today)
4. Recurrence: every `1 Day`
5. Max concurrency: `1` (process one day at a time — avoids API overload)
6. Retry: `2` retries, `30 minutes` interval
7. Pipeline parameters:
   - Pass `p_run_date` = `@formatDateTime(trigger().outputs.windowStartTime, 'yyyy-MM-dd')`

**Backfill scenario:** Your pipeline broke on Sep 15. You fix it on Sep 20. The Tumbling Window trigger sees windows Sep 15–19 as Failed/Queued and automatically reruns them — no manual intervention needed.

### 2.4 Storage Event Trigger

Fires when a blob is created or deleted in ADLS Gen2 / Blob Storage.

**When to use:** A partner team drops a CSV file into `bronze/landing/` at an unpredictable time — could be 6am or 3pm. Instead of polling on a schedule, the pipeline fires the moment the file arrives.

**Setup:**
1. Manage → Triggers → **+ New** → Type: **Storage events**
2. Azure subscription: yours
3. Storage account: `evdatalakedev`
4. Container name: `bronze`
5. Blob path begins with: `landing/payments/`
6. Event: **Blob created**
7. **OK** → Publish all

**System variable for event triggers:**
```
@trigger().outputs.body.fileName      → name of the file that arrived
@trigger().outputs.body.folderPath    → folder it landed in
```

### 2.5 Manual / On-demand via REST API (for CI/CD)

You can trigger pipelines programmatically via the ADF REST API or Azure CLI — used in CI/CD pipelines:

```bash
az datafactory pipeline create-run \
  --factory-name adf-datalake-dev-ded \
  --resource-group rg-ev-intelligence-dev \
  --name pl_bronze_api_payments \
  --parameters '{"p_page": 1, "p_page_size": 100}'
```

This is how Azure DevOps release pipelines trigger ADF runs after a deployment.

### 2.6 Trigger Comparison — Which to Use When

| Scenario | Trigger |
|---|---|
| Nightly batch, missed runs are OK | Schedule |
| Nightly batch, every night must complete | Tumbling Window |
| File arrives at unknown time | Storage Event |
| Backfill 3 months of historical data | Tumbling Window (set start in past) |
| Run once manually for a test | Manual (Trigger Now) |
| CI/CD pipeline triggers ADF after deploy | REST API / CLI |

---

## Concept 3: Integration Runtime

The Integration Runtime (IR) is the compute infrastructure that executes ADF activities. Think of it as the engine room.

### 3.1 Azure IR (Default)

- Fully managed by Microsoft — no setup required
- Runs in Azure data centres
- Auto-selects a region (or you pin to a specific Azure region)
- Connects to: public internet endpoints, Azure PaaS services (ADLS Gen2, Azure SQL, Cosmos DB, etc.)
- **Used by Day 2/3 pipeline** — VoltGrid API is on the public internet, ADLS Gen2 is an Azure service

**Setting in Linked Service:** Leave as **AutoResolveIntegrationRuntime** — ADF picks the closest region automatically.

**When to pin a region:** If your ADLS Gen2 is in `Central India`, pin the Azure IR to `Central India` too — data doesn't cross regions, lower latency and no egress charges.

### 3.2 Self-Hosted IR

- You install an agent on a machine inside your private network
- Connects to: on-premises SQL Server, Oracle, SAP, file shares — anything not reachable from the public internet
- The agent communicates outbound to ADF over HTTPS — no inbound firewall rules needed
- Can be installed on a VM, a bare-metal server, or a Windows machine

**When you need it:**
- Source database is behind a corporate firewall
- Azure SQL MI with no public endpoint
- On-premises file server

**Not needed** for the VoltGrid project — all sources are public or Azure PaaS.

### 3.3 Azure-SSIS IR

- Lifts-and-shifts existing SQL Server Integration Services (SSIS) packages to Azure
- Only relevant for organisations migrating legacy ETL
- Not relevant to this course

### 3.4 IR in the Context of the Day 2 Pipeline

All 3 linked services (`ls_keyvault`, `ls_voltgrid_api`, `ls_adls_bronze`) use **AutoResolveIntegrationRuntime** — the default Azure IR. This is correct because:
- Key Vault: Azure PaaS — reachable via Azure IR
- VoltGrid API: public HTTPS endpoint — reachable via Azure IR
- ADLS Gen2: Azure PaaS — reachable via Azure IR

No Self-Hosted IR is needed unless you add an on-premises source later.

---

## Concept 4: Monitor Panel

The Monitor panel is your operational control centre. Every pipeline run, activity run, and trigger run is logged here.

### 4.1 Pipeline Runs View

**Monitor → Pipeline runs** shows:
- Pipeline name
- Run status: Queued / In progress / Succeeded / Failed / Cancelled
- Triggered by: trigger name or "Manual"
- Duration
- Run start time

**Filter by:**
- Time range (last 24h, 7d, 30d, or custom)
- Pipeline name
- Status

Click a run → **Activity runs** to see each activity's status, duration, and bytes read/written.

### 4.2 Activity Runs Detail

For each activity:
- **Input tab:** the exact JSON ADF sent to the activity (request body, URL, headers — with sensitive values masked by Key Vault)
- **Output tab:** the full JSON response — including `rowsCopied`, `dataRead`, `dataWritten`, `duration`
- **Error tab:** full error message and error code when the activity fails

**Diagnosing a 401 on `act_copy_payments`:**
1. Monitor → click the failed run → Activity runs
2. Click `act_api_login` → Output tab → confirm `token` key exists in the response
3. Click `act_set_token` → Output tab → confirm `v_token` is set
4. Click `act_copy_payments` → Error tab → read the full error

### 4.3 Trigger Runs View

**Monitor → Trigger runs** shows every time a trigger fired — including whether it successfully queued a pipeline run. Useful to confirm:
- Did the Schedule Trigger actually fire at 01:00 UTC?
- Did the Storage Event Trigger detect the file arrival?
- How many Tumbling Window windows are in the queue?

### 4.4 Rerun from Monitor

If a pipeline fails, you can rerun it directly from Monitor:
- Monitor → Pipeline runs → find the failed run → click **Rerun**
- Parameters from the original run are pre-filled — change any if needed

For Tumbling Window: failed windows appear in **Monitor → Trigger runs** and are automatically retried based on the retry policy. You can also manually rerun a specific window.

---

## Concept 5: Alerts on Failure — Email Notification

ADF pipelines do not send emails natively. There are two ways to get email alerts.

### 5.1 Method 1 — Azure Monitor Alert (Recommended, no pipeline changes)

Azure Monitor watches ADF metrics and fires an action (email, SMS, Teams, webhook) when a condition is met.

**Step-by-step:**

**Step 1 — Create an Action Group** (who gets notified):
1. Portal → **Monitor** → **Alerts** → **Action groups** → **+ Create**
2. Resource group: `rg-ev-intelligence-dev`
3. Name: `ag-adf-failures` | Display name: `ADF Failures`
4. **Notifications** tab → **+ Add**:
   - Type: `Email/SMS/Push/Voice`
   - Name: `email-on-failure`
   - Email: your email address
5. **Review + Create** → **Create**

**Step 2 — Create an Alert Rule:**
1. Portal → **Monitor** → **Alerts** → **+ Create** → **Alert rule**
2. **Scope:** click **Select scope** → search `adf-datalake-dev-ded` → select → **Done**
3. **Condition** tab → **+ Add condition**:
   - Signal: `Failed pipeline runs count`
   - Threshold: `Static`, Operator: `Greater than`, Value: `0`
   - Aggregation: `Count`, Period: `5 minutes`
4. **Actions** tab → **+ Select action groups** → select `ag-adf-failures`
5. **Details** tab:
   - Alert rule name: `alert-adf-pipeline-failure`
   - Severity: `2 - Warning`
6. **Review + Create** → **Create**

From now on, any ADF pipeline failure in `adf-datalake-dev-ded` sends an email to your address within 5 minutes.

**What the email contains:**
- Alert name
- Severity
- Resource: `adf-datalake-dev-ded`
- Fired time
- Link to the Monitor panel showing the failed run

### 5.2 Method 2 — Web Activity inside the Pipeline (On Failure)

Add a Web Activity with an `On Failure` dependency on the Copy Activity. When the Copy fails, the Web Activity fires — calling a Teams webhook, a Slack webhook, or any HTTP endpoint.

**Step-by-step (using a Teams webhook):**

1. Get a Teams incoming webhook URL from your Teams channel (Channel → Connectors → Incoming Webhook)
2. In `pl_bronze_api_payments`, drag a **Web Activity** onto the canvas → name it `act_alert_failure`
3. Right-click the arrow from `act_copy_payments` → change to **On Failure** (turns red)
   - Or: hover over `act_copy_payments` → drag arrow to `act_alert_failure` → in the dialog select **Failure**
4. **Settings** tab:
   - Method: `POST`
   - URL: your Teams webhook URL
   - Headers: `Content-Type` = `application/json`
   - Body:
     ```json
     {
       "text": "ADF Pipeline FAILED: @{pipeline().pipelineName} | Run ID: @{pipeline().runId} | Time: @{utcnow()}"
     }
     ```
5. **Publish all**

**Dependency arrow colours in ADF:**
| Arrow colour | Condition | Meaning |
|---|---|---|
| Green | On Success | Activity B runs only if A succeeded |
| Red | On Failure | Activity B runs only if A failed |
| Blue | On Completion | Activity B runs regardless of A's status |
| Grey | On Skipped | Activity B runs only if A was skipped |

### 5.3 Method 1 vs. Method 2

| | Azure Monitor Alert | Web Activity On Failure |
|---|---|---|
| Setup | Once — covers all pipelines | Per pipeline — must add to each |
| Scope | Any ADF failure, any pipeline | Only the specific pipeline it's in |
| Email support | Native | Requires a webhook or Logic App |
| Teams/Slack | Via Action Group | Direct webhook |
| Pipeline JSON changes | None | Yes — adds activities |
| Best for | Production monitoring of all pipelines | Custom per-pipeline logic (e.g. log to DB, trigger compensation) |

**Recommended approach:** Use Azure Monitor Alert for email (covers everything automatically) AND add a Web Activity On Failure for Teams notification (immediate, targeted).

---

## Summary — What Was Added to the Pipeline

**Original Day 2 pipeline:** 5 activities, fixed sink path `payments.json`, no date tracking.

**After Day 3 enhancements:**

| Enhancement | How |
|---|---|
| Dynamic sink path with today's date | `act_set_ingestion_date` (Set Variable) + dataset parameter |
| Sink partitioned by `ingestion_date=YYYY-MM-DD` | Dataset `p_run_date` parameter in folder path |
| Schedule trigger at 01:00 UTC daily | `trg_payments_daily` Schedule Trigger |
| Guaranteed completeness if pipeline fails | Replace Schedule with Tumbling Window Trigger |
| Email on any ADF failure | Azure Monitor Alert → Action Group |
| Teams alert in pipeline | Web Activity `act_alert_failure` with On Failure dependency |

**Updated pipeline structure:**
```
act_get_username → act_get_password → act_api_login → act_set_token
                                                            ↓
                                               act_set_ingestion_date
                                                            ↓
                                                    act_copy_payments ──(On Failure)──► act_alert_failure
```
