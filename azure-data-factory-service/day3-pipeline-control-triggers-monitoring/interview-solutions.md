# Day 3 — Interview Solutions: Pipeline Control, Triggers & Monitoring

---

## Concept 1: Variables, Parameters, and Dynamic Content

**Q1 — Pipeline Parameter vs. Pipeline Variable**

**Parameter:** Set by the caller (trigger or Trigger Now) before the pipeline starts. Read-only inside the pipeline — no activity can change it during a run.

Example from `pl_bronze_api_payments`:
- `p_page` — set to `1` by the trigger. Every activity that needs the page number reads `@pipeline().parameters.p_page`. Nothing inside the pipeline can change this to `2` mid-run.

**Variable:** Declared on the pipeline, set by Set Variable activities at runtime. Read/write — the pipeline itself computes and stores the value.

Example from `pl_bronze_api_payments`:
- `v_token` — starts empty. `act_set_token` sets it to the value returned by `act_api_login`. `act_copy_payments` then reads it for the Authorization header. The caller cannot set `v_token` — only the pipeline's own activities can.

---

**Q2 — Expression for dynamic Bronze path**

```
Folder path expression (in dataset Connection tab):
api/payments/ingestion_date=@{variables('v_ingestion_date')}

File name expression:
page_@{pipeline().parameters.p_page}.json
```

Or as a single concatenated string if the field takes one value:
```
@concat('api/payments/ingestion_date=', variables('v_ingestion_date'), '/page_', string(pipeline().parameters.p_page), '.json')
```

Result for `v_ingestion_date = '2026-09-28'` and `p_page = 1`:
```
api/payments/ingestion_date=2026-09-28/page_1.json
```

---

**Q3 — Three system variables and use cases**

1. **`@pipeline().runId`**
   Unique GUID for the current pipeline run. Use it to stamp audit log rows — every Bronze file or Delta row that came from this run carries the same runId, so you can trace data lineage back to the exact pipeline execution.

2. **`@trigger().scheduledTime`**
   The time the trigger was scheduled to fire — not the time ADF actually started executing. Use this for the sink path `ingestion_date` partition — ensures even a delayed execution writes to the correct date's folder. Contrast with `utcnow()` which returns the actual wall-clock time and could be wrong if the pipeline was delayed.

3. **`@trigger().outputs.windowStartTime`**
   Available only with Tumbling Window triggers. Returns the start of the current time window (e.g. `2026-09-27T00:00:00Z` for the Sep 27 window). Use this to build `?updated_after=<windowStart>` in the API URL for incremental loads — guarantees you fetch exactly the data that changed during this window.

---

**Q4 — `@trigger().scheduledTime` vs `@utcnow()`**

| | `@trigger().scheduledTime` | `@utcnow()` |
|---|---|---|
| What it returns | When the trigger was scheduled to fire | The actual current UTC time at moment of evaluation |
| Stable across retries | Yes — same scheduled time on every retry | No — changes every time the expression is evaluated |
| Correct for data partitioning | Yes | No — could produce wrong date on a delayed/retried run |

**Use `@trigger().scheduledTime` in the Bronze sink path.**

Concrete example: The trigger is scheduled at 01:00 UTC. The pipeline is delayed and actually starts at 01:47 UTC. A worker node then retries an activity at 02:03 UTC.
- `@utcnow()` at 02:03 = `2026-09-29T02:03:00Z` (could produce tomorrow's date if the trigger fires at 23:55 and runs past midnight)
- `@trigger().scheduledTime` = `2026-09-28T01:00:00Z` always — the sink path is always `ingestion_date=2026-09-28` regardless of delays

---

**Q5 — Risk of `utcnow()` in sink path with delayed execution**

**Yes, there is a risk — specifically around midnight.**

Scenario: Schedule Trigger fires at 23:55 UTC (55 minutes before midnight). Due to heavy load, the pipeline starts executing at 00:03 UTC the next day. `@utcnow()` returns `2026-09-29T00:03:00Z` — the data lands in `ingestion_date=2026-09-29` even though it represents Sep 28's data.

**Correct expression:**
```
@formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')
```

This always evaluates to the date the trigger was scheduled for — immune to execution delays.

**Note:** During Debug runs, `trigger().scheduledTime` is not available (no trigger fires during Debug). For Debug only, `utcnow()` is acceptable. In production, always use `trigger().scheduledTime`.

---

**Q6 — Two syntax forms for ADF expressions**

**Form 1 — Full expression (the entire field IS the expression, starts with `@`):**
```
@variables('v_token')
@pipeline().parameters.p_page
@activity('act_api_login').output.token
```
The field value is entirely the result of the expression. No surrounding text.

**Form 2 — Interpolation (expression embedded inside a string, uses `@{...}`):**
```
Token @{variables('v_token')}
api/payments/ingestion_date=@{formatDateTime(utcnow(), 'yyyy-MM-dd')}/page_@{pipeline().parameters.p_page}.json
Pipeline @{pipeline().pipelineName} failed at @{utcnow()}
```
The `@{...}` blocks are evaluated; everything outside is literal text. The result is a concatenated string.

**When to use which:**
- Form 1: When the field takes exactly one typed value (int, bool, string that IS the expression)
- Form 2: When you need to embed a computed value inside a larger string (paths, messages, JSON bodies)

---

**Q7 — Self-reference prohibition and the temp variable workaround**

ADF evaluates Set Variable activities atomically — it reads all referenced variables at the start of the evaluation, then writes the result. If `v_current_page` appears on both sides:
```
v_current_page = @add(variables('v_current_page'), 1)
```
ADF reads `v_current_page` (say, `3`), computes `add(3, 1) = 4`, and tries to write `4` back to `v_current_page`. ADF blocks this because the write target is the same variable that was read in the same expression — it prevents race conditions in parallel branches.

**Correct workaround — two Set Variable activities:**
```
act_set_temp_page:
    v_temp_page = @add(variables('v_current_page'), 1)   ← reads current_page, writes temp

act_increment_page:
    v_current_page = @variables('v_temp_page')            ← reads temp, writes current_page
```

Two separate activities — two separate evaluations. No self-reference in either.

---

**Q8 — Login Web Activity body expression**

```
@concat('{"username":"', activity('act_get_username').output.value, '","password":"', activity('act_get_password').output.value, '"}')
```

Breaking it down:
- `activity('act_get_username').output.value` → the Key Vault secret value (e.g. `voltgrid_demo`)
- `activity('act_get_password').output.value` → the Key Vault secret value (e.g. `EVcharge@AU2025`)
- `concat(...)` stitches them into a valid JSON string: `{"username":"voltgrid_demo","password":"EVcharge@AU2025"}`

The result is sent as the POST body to `/api/auth/login/`.

---

**Q9 — `@if()` for full vs. incremental watermark**

```
@if(equals(pipeline().parameters.p_load_type, 'full'), '1900-01-01T00:00:00Z', pipeline().parameters.p_watermark)
```

Breaking it down:
- `equals(pipeline().parameters.p_load_type, 'full')` → `true` or `false` (case-sensitive)
- If `true` → watermark is `'1900-01-01T00:00:00Z'` — effectively "fetch everything" since no records predate 1900
- If `false` → watermark is the `p_watermark` parameter value passed by the caller

This single expression replaces an If Condition activity for simple two-branch logic.

---

**Q10 — Which value does the pipeline receive for `p_page_size`?**

The pipeline receives **`50`** — the value set in the Schedule Trigger definition.

Parameter precedence in ADF:
1. **Trigger definition** — the trigger's configured parameter values override the pipeline's defaults
2. **Trigger Now form** — if you don't fill in a parameter in the Trigger Now dialog, it falls back to the trigger's configured value, then to the pipeline default

When clicking "Trigger Now" without specifying `p_page_size`, ADF uses the **trigger's** configured value of `50` — not the pipeline's default of `100`. The pipeline default only applies when there is no trigger and no explicit value passed.

---

## Concept 2: Triggers

**Q11 — Schedule vs. Tumbling Window**

| | Schedule | Tumbling Window |
|---|---|---|
| Memory of past runs | None | Full — every window has a tracked status |
| Failed window retry | Never — just fires next scheduled time | Automatic — retries based on retry policy |
| Backfill old data | Not possible | Set start date in the past |

**Always use Tumbling Window when:** The pipeline processes time-series data where every interval must produce output — financial transactions, IoT readings, daily ingestion loads. If Monday fails, Monday's data must eventually be ingested — a Schedule Trigger cannot guarantee this.

Use Schedule only for stateless tasks where missing one run is acceptable — e.g. sending a daily report where a missed day doesn't matter.

---

**Q12 — Monday's data is lost with Schedule Trigger**

With a Schedule Trigger, Monday's failed run is simply logged as Failed. Tuesday's trigger fires at 01:00 UTC and runs Tuesday's data. Monday's pipeline run never automatically reruns — the trigger has no concept of outstanding windows.

**Monday's data is permanently lost unless someone manually reruns the pipeline.**

**Fix:** Replace the Schedule Trigger with a Tumbling Window Trigger. Each calendar day is a window. The trigger tracks Monday's window as Failed. When the bug is fixed (say, Wednesday), ADF automatically reruns Monday's window — guaranteed completeness.

---

**Q13 — Tumbling Window with past start date activated today**

ADF sees 27 unfired windows (Sep 1 through Sep 27) that should have run but didn't. It immediately queues all 27.

With `max concurrency = 1`: they run one at a time, oldest first (Sep 1, then Sep 2, ...). With 20 minutes per run, all 27 windows complete in ~9 hours.

With `max concurrency = 3`: three run in parallel at a time — total ~3 hours.

**This is the backfill behaviour.** It is intentional — Tumbling Window guarantees every window from the start date onward is eventually processed.

---

**Q14 — File arrives at unpredictable time — trigger type**

**Storage Event Trigger.**

Key configuration:
- **Storage account:** `evdatalakedev`
- **Container:** `bronze`
- **Blob path begins with:** `landing/payments/`
- **Event:** `Blob created`

The pipeline fires within seconds of the file landing — no polling, no wasted Schedule runs when the file hasn't arrived yet, no 30-minute wait if a Schedule Trigger missed the window.

**System variable to access the file:**
```
@trigger().outputs.body.fileName    → the exact filename
@trigger().outputs.body.folderPath  → the folder it landed in
```

---

**Q15 — Tumbling Window system variables for correct date partitioning**

```
@trigger().outputs.windowStartTime   → start of current window, e.g. "2026-09-27T00:00:00Z"
@trigger().outputs.windowEndTime     → end of current window,   e.g. "2026-09-28T00:00:00Z"
```

**Correct sink path expression using window start:**
```
api/payments/ingestion_date=@{formatDateTime(trigger().outputs.windowStartTime, 'yyyy-MM-dd')}
```

For the Sep 27 window: `ingestion_date=2026-09-27` — regardless of when the pipeline actually executes (could be Sep 28 if backfilling).

**For incremental API call:**
```
?updated_after=@{trigger().outputs.windowStartTime}&updated_before=@{trigger().outputs.windowEndTime}
```
Fetches exactly the records that changed during the Sep 27 window — no overlap, no gaps.

---

**Q16 — Concurrency 1 vs. 7 for 7 backfill windows**

**Max concurrency = 1:**
- One window runs at a time, oldest first
- 7 windows × 20 minutes each = **140 minutes** total

**Max concurrency = 7:**
- All 7 windows run simultaneously
- Total time = slowest single window = **~20 minutes**

**Trade-offs of concurrency = 7:**
- VoltGrid API receives 7× the concurrent connections — may hit rate limits or return 429 Too Many Requests
- ADLS Gen2 receives 7× concurrent writes — generally fine (Azure handles this)
- ADF runs 7 pipeline instances simultaneously — higher DIU-hour cost

**Recommended:** Keep concurrency = 1 for API sources that may rate-limit. Use concurrency = 7 only if you've confirmed the API handles parallel load.

---

**Q17 — Different page sizes on weekdays vs. weekends**

A single trigger cannot dynamically change a parameter value based on day of week — triggers have fixed parameter values.

**Recommended design: Two triggers on the same pipeline**

```
trg_payments_weekday:
    Schedule: Mon–Fri at 01:00 UTC
    p_page_size: 100

trg_payments_weekend:
    Schedule: Sat–Sun at 01:00 UTC
    p_page_size: 500
```

Same pipeline `pl_bronze_api_payments`, two triggers with different parameter values. The pipeline receives whichever trigger's value is applicable for that day.

ADF Schedule Triggers support day-of-week filtering: Recurrence → Advanced → **On these days** → select specific days.

---

**Q18 — Storage Event Trigger use case**

A Storage Event Trigger fires when a blob is created or deleted in ADLS Gen2 / Blob Storage.

**Best use case:** External partner delivers a daily settlements file to `bronze/landing/settlements/` at an unpredictable time. Requirements:
- Process the file within minutes of arrival
- Don't run the pipeline when no file has arrived (waste of resources)
- Don't miss a file because a Schedule Trigger fired before the file arrived

With a Storage Event Trigger: the moment `settlements_2026-09-28.csv` lands, the pipeline fires. No polling, no fixed schedule, zero delay.

**Why Schedule is wrong here:** If the file arrives at 09:23 and the schedule is 01:00, you wait 15+ hours. If the file arrives at 00:58 and the schedule fires at 01:00, there's a race condition.

---

**Q19 — Two triggers on the same pipeline**

Yes, ADF allows multiple triggers on the same pipeline. Both can be active simultaneously.

**What happens if both fire at the same time:**
Two independent pipeline runs are created — one from each trigger. They run concurrently. Both write to the same sink path (unless they use different parameter values that produce different paths). This is usually undesirable — concurrent writes to the same ADLS Gen2 path can cause data corruption.

**Correct approach:** If two triggers must fire at overlapping times, ensure they pass different parameters that produce non-overlapping sink paths, or use pipeline-level concurrency controls (not available in ADF directly — handle via Logic Apps or a monitoring check).

---

**Q20 — Tumbling Window failure retry sequence**

Configuration: retry count = 2, retry interval = 30 minutes.

1. **T+00:00** — pipeline runs, fails
2. **T+30:00** — ADF automatically retries (Attempt 2 of 3)
3. **T+30:00** — if Attempt 2 fails:
4. **T+60:00** — ADF automatically retries (Attempt 3 of 3, final)
5. **T+60:00** — if Attempt 3 fails:
6. Window status → **Failed**. Azure Monitor alert fires (if configured). The window stays in Failed state and can be manually rerun from Monitor → Trigger runs.

Total time from first failure to final failure: 60 minutes (30 min × 2 intervals).

The window remains in Failed status until either:
- Manually rerun and it succeeds
- The trigger is deactivated

---

## Concept 3: Integration Runtime, Monitor & Alerts

**Q21 — Integration Runtime and why no Self-Hosted IR for VoltGrid**

An Integration Runtime (IR) is the compute engine that executes ADF activities. It handles the network connection between ADF and the data source/sink.

**Why no Self-Hosted IR for VoltGrid:**
All three sources in the pipeline are publicly reachable:
- Key Vault: Azure PaaS with a public `vault.azure.net` endpoint
- VoltGrid API: public HTTPS on `ev-project-navy-mu.vercel.app`
- ADLS Gen2: Azure PaaS with a public `dfs.core.windows.net` endpoint

Azure IR (the default, fully managed by Microsoft) can reach all of these. Self-Hosted IR is only needed for resources behind a firewall or in a private VNet — none of which apply here.

---

**Q22 — Pinning Azure IR to a region**

**When to pin:** When your data source and/or sink is in a specific Azure region and cross-region data movement would incur egress costs or latency.

**Example:** ADLS Gen2 `evdatalakedev` is in `Central India`. With AutoResolve, ADF may route the Copy Activity through a different region (e.g. East Asia) to reach the VoltGrid API. Data flows: VoltGrid → East Asia IR → Central India ADLS. This crosses regions, paying Azure egress charges and adding 50–100ms latency.

**Risk of AutoResolve:** For this project it's minimal (small JSON payloads). For 100GB+ data movement between Azure services in the same region, AutoResolve could accidentally route cross-region and add significant cost.

**Fix:** In the linked service → Integration runtime → select **Fixed Azure IR** → region: `Central India`.

---

**Q23 — Self-Hosted IR for on-premises SQL Server**

**IR type needed:** Self-Hosted IR

**High-level steps:**
1. In ADF Studio → **Manage** → **Integration runtimes** → **+ New**
2. Type: **Self-Hosted** → **Next** → Give it a name (e.g. `ir-onprem-sql`)
3. ADF generates a registration key
4. On a Windows machine inside the corporate network:
   - Download the Self-Hosted IR installer from Microsoft
   - Install it → paste the registration key → the agent registers with ADF
5. In ADF Studio → the IR shows as **Running** once the agent is connected
6. Create a SQL Server Linked Service → set Integration runtime to `ir-onprem-sql`

The agent communicates outbound to ADF on HTTPS port 443 — no inbound firewall rules needed on the corporate network.

---

**Q24 — Input vs. Output tab in Monitor for a failed Copy Activity**

**Input tab:** Shows what ADF sent to the activity — the request configuration:
- Source dataset reference and parameters
- Additional headers (Authorization header value is masked if from Key Vault)
- Source URL being called
- Sink dataset reference and path

**Output tab:** Shows what came back — the result:
- `rowsRead`, `rowsCopied`, `dataRead`, `dataWritten`
- `duration`
- Error details if the activity failed partially
- `filesWritten`, `filesSkipped`

**For a 401 error:** Check the **Input** tab first — confirm the Authorization header value starts with `Token ` (correct format). If the header is missing or malformed, the issue is in `act_set_token` or the Copy Activity header expression. Then check `act_api_login` Output tab to confirm the token was actually received.

---

**Q25 — `rowsRead: 100` but `rowsCopied: 0` — two causes**

**Cause 1: Fault tolerance set to "Skip incompatible rows"**
If all 100 rows failed the sink schema validation (e.g. a NOT NULL column received null), ADF skipped all 100. `rowsRead = 100`, `rowsCopied = 0`, `rowsSkipped = 100`.

Diagnose: Copy Activity → Settings tab → Fault tolerance → check if "Skip incompatible rows" is enabled and check the log file path configured for skipped rows.

**Cause 2: Sink write silently failed**
The Copy Activity read the data but the write to ADLS Gen2 failed (permissions, path issue, network). Some ADF versions report this as `rowsCopied: 0` without a hard failure.

Diagnose: Copy Activity → Output tab → look for `filesWritten: 0` and any error in the `errors` array. Cross-check: go to Portal → ADLS Gen2 → confirm the file was not written.

---

**Q26 — Pipeline runs vs. Trigger runs; trigger fires but no pipeline run**

**Monitor → Trigger runs:** Shows every time a trigger fired — the scheduling event. Each row represents ADF attempting to queue a pipeline run.

**Monitor → Pipeline runs:** Shows every pipeline execution — after the trigger successfully queued the run.

**Trigger fires but no pipeline run appears — likely causes:**

1. **Pipeline was not published:** The trigger references a pipeline version that hasn't been published. The trigger fires but fails to create a run. Check Trigger runs → Error column for "pipeline not found."
2. **Trigger was not published after activation:** You activated the trigger but forgot to click Publish all. The trigger doesn't actually exist in the live factory.
3. **Concurrency limit reached:** Tumbling Window with `max concurrency = 1` — prior window still running, new window is queued in Trigger runs but not yet a Pipeline run.
4. **Pipeline was deleted or renamed** after the trigger was created.

---

**Q27 — Two email alert methods compared**

**Method 1: Azure Monitor Alert**

| Attribute | Detail |
|---|---|
| Setup | Once in Azure Portal — covers all pipelines in the factory |
| Scope | Any ADF pipeline failure, any trigger type |
| Email content | Alert name, severity, resource name, fired time, link to Monitor |
| Delay | Up to 5 minutes (metric aggregation period) |
| Pipeline changes | None required |

**Method 2: Web Activity On Failure**

| Attribute | Detail |
|---|---|
| Setup | Per pipeline — must add activity to each pipeline you want to monitor |
| Scope | Only the specific pipeline it's in |
| Email content | Custom — you build the message body with expressions |
| Delay | Near-instant — fires as soon as the dependent activity fails |
| Pipeline changes | Yes — adds an activity to the pipeline |

**Best practice:** Use both. Azure Monitor for broad coverage (any failure, any pipeline) and Web Activity for pipeline-specific context (e.g. include `rowsCopied`, `pipeline().runId`, which endpoint failed).

---

**Q28 — Email timing after 01:00 UTC failure**

Azure Monitor evaluates alert conditions every 5 minutes (the "check every" setting). The failed run must be captured within the evaluation window.

**Timeline:**
- 01:00:00 — pipeline starts
- 01:00:45 — Copy Activity fails
- 01:00:46 — ADF marks pipeline run as Failed
- 01:05:00 — Azure Monitor next evaluation window closes, sees 1 failed pipeline run in last 5 min → threshold exceeded
- ~01:05:30 — Alert fires, Action Group sends email
- ~01:06:00 — Email arrives in inbox (depending on mail server speed)

**Factors that delay it:**
- Azure Monitor evaluation is eventually consistent — the 5-minute window is approximate
- Large factories with many metrics may have slightly longer propagation
- Email delivery from Azure depends on mail server and spam filters (some corporate mail takes 2–5 extra minutes)

Worst case: ~10–12 minutes from failure to email. Typical: ~6–8 minutes.

---

**Q29 — Pipeline status when On Failure Web Activity succeeds**

**Pipeline status: Failed.** An On Failure activity that succeeds does NOT change the overall pipeline status. The pipeline failed because `act_copy_payments` failed — the pipeline run status reflects the worst outcome of any activity.

**`act_alert_failure` status: Succeeded** — it ran its POST request and got a 200 response.

**What you see in Monitor:**
```
Pipeline run: FAILED
  act_get_username:    Succeeded
  act_get_password:    Succeeded
  act_api_login:       Succeeded
  act_set_token:       Succeeded
  act_copy_payments:   Failed      ← caused pipeline failure
  act_alert_failure:   Succeeded   ← ran because of On Failure dependency
```

The On Failure pattern is specifically designed this way — the alert fires AND the pipeline correctly shows as Failed so monitoring systems know something went wrong.

---

**Q30 — Production operational setup for `pl_bronze_api_payments`**

| Requirement | Component | Configuration |
|---|---|---|
| Nightly at 01:00 UTC, guaranteed | **Tumbling Window Trigger** `trg_payments_tumbling` | Start: today at 01:00 UTC, Recurrence: 1 Day, Max concurrency: 1 |
| Retry 2 times before alerting | **Tumbling Window Trigger** → Retry policy | Retry count: 2, Interval: 30 min |
| Dynamic sink partitioned by scheduled date | **Dataset parameter** `p_run_date` + **Set Variable** `act_set_ingestion_date` | Value: `@formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')` — NOT `utcnow()` |
| Sink path expression | Dataset `ds_bronze_payments_sink` | Folder: `api/payments/ingestion_date=@{dataset().p_run_date}` |
| Email within 5 min of any failure | **Azure Monitor Alert rule** | Scope: `adf-datalake-dev-ded`, Condition: `Failed pipeline runs count > 0`, Period: 5 min, Action: `ag-adf-failures` email action group |
| Teams alert with pipeline name + runId | **Web Activity** `act_alert_failure` (On Failure dependency on `act_copy_payments`) | Body: `@concat('{"text":"Pipeline ', pipeline().pipelineName, ' FAILED. RunId: ', pipeline().runId, '"}')` |
| IR — public API + Azure ADLS | **AutoResolveIntegrationRuntime** pinned to `Central India` | All 3 linked services: IR = Fixed Azure IR, region = Central India |

**Trigger parameter to pass the scheduled date into the pipeline:**
```
Trigger parameter: p_run_date
Value expression:  @formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')
```

**Pipeline receives `p_run_date`** → `act_set_ingestion_date` copies it to `v_ingestion_date` → `act_copy_payments` sink dataset receives `p_run_date = @variables('v_ingestion_date')` → folder is `ingestion_date=2026-09-28`.

**After 2 failed retries (90 min after first failure):** Tumbling Window marks window as Failed → Azure Monitor sees `Failed pipeline runs count > 0` → email fires within 5 min → total alert time from first failure: up to ~95 minutes. For a faster alert, also keep the Web Activity On Failure inside the pipeline — it fires after the first failure, before retries exhaust.
