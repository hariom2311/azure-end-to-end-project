# Day 2 — Interview Solutions: EV API Data Load to ADLS

---

## Concept 1: Bronze Ingestion Pattern

**Q1 — What is Bronze? Why store raw data?**

Bronze is the raw landing zone in a lakehouse. Data arrives exactly as the source produced it — no type casting, no filtering, no business logic applied.

**Why raw:**
1. **Reprocessability:** If a bug in your Silver transform corrupts data, you rerun from Bronze. If Bronze is transformed, you have no source of truth to reprocess from.
2. **Audit and debugging:** Raw responses let you verify exactly what the API returned on a given day — useful for disputes, compliance, and debugging schema changes.
3. **Schema evolution:** APIs add/remove fields over time. Raw storage captures the full response. You only decide which fields matter when building Silver — and you can revise that decision later without re-ingesting.

---

**Q2 — Full load vs. incremental. Which is Day 2?**

**Full load:** Fetch all records every run. Simple — no state to track. Cost: slow and expensive on large datasets.

**Incremental load:** Fetch only records updated since the last run. Requires a watermark (e.g. `max(updated_at)` from the previous run). Cost: complex — needs watermark storage and `updated_after` parameter support in the source.

**Day 2: Full load.** We fetch page 1 (100 records) with no date filter. This is intentional — Day 2 proves the connectivity works (Key Vault → API → ADLS). Day 3 adds the `updated_after` watermark and pagination loop for a proper incremental load.

---

**Q3 — Why can't ADF handle the token auth in the Linked Service?**

ADF's built-in REST linked service auth types are: Anonymous, Basic, Service Principal, Managed Identity, OAuth2 client credentials. None of these support a custom token obtained by calling a POST endpoint first.

The VoltGrid pattern is:
1. POST with credentials → receive `{"token": "xyz"}`
2. Subsequent GETs → `Authorization: Token xyz`

ADF's built-in OAuth2 handles standard flows (client credentials, authorization code) — not custom token issuance. So:
- Linked service: `Anonymous` (no built-in auth)
- Inside the pipeline: Web Activity (login) → Set Variable (store token) → Copy Activity (inject token as a header)

This pattern — fetch token at runtime, pass via variable — is the standard ADF approach for any API with a non-standard auth flow.

---

**Q4 — Risks of hardcoding credentials in Web Activity body**

**Risk 1 — Credentials in ADF JSON (ARM template):**
ADF pipelines are stored as ARM templates. If Git integration is enabled, the pipeline JSON is committed to the repository. Anyone with repo read access sees the plaintext credentials.

**Risk 2 — Credentials visible in Monitor:**
When a Web Activity runs, the full request body is logged in the Monitor panel → Activity runs → Input tab. Any ADF user with Monitor access sees the credentials in plaintext.

**Correct solution:**
- Store username and password in Azure Key Vault as secrets (`voltgrid-username`, `voltgrid-password`)
- Use two Web Activities with Managed Identity authentication to read the secrets at runtime
- The pipeline JSON only contains the Key Vault URL — never the credentials themselves
- Monitor logs show the Key Vault response structure (with `value` masked by Key Vault) — not the raw credential

---

**Q5 — Is Anonymous auth on ls_voltgrid_api a security misconfiguration?**

No. The confusion is between "Anonymous" at the linked service level and "unauthenticated access to the API."

`ls_voltgrid_api` using Anonymous means: ADF sends requests without any pre-configured credential at the connection level. This is correct because:
- The authentication credential (the Bearer token) is **dynamic** — it changes every pipeline run
- It is injected by the Copy Activity at runtime as a header: `Authorization: Token {v_token}`
- If you used Basic auth on the linked service, ADF would inject a static Base64 credential on every request — wrong for a session-based token system

The VoltGrid API is **not** unauthenticated. Every data request carries `Authorization: Token <value>`. The "Anonymous" label only describes how the linked service itself is configured — not whether the downstream API requires auth.

---

**Q6 — ADF Managed Identity vs. Service Principal**

| | Managed Identity | Service Principal (client secret) |
|---|---|---|
| Provisioned by | Azure automatically (no action) | You create it in Azure AD |
| Credential | No credential — Azure handles token exchange | Client secret (password) that expires |
| Rotation | Automatic — no action ever needed | Manual — expires, must be updated everywhere |
| Audit trail | Full in Azure AD sign-in logs | Full, but tied to the SP, not the individual service |
| Risk if leaked | None — no secret exists | Client secret can be used by anyone until rotated |

**Better for production: Managed Identity.** No secret to rotate, no risk of a leaked credential, no password to expire. ADF gets one for free — you just assign it RBAC roles.

---

**Q7 — RBAC assigned but 403 still occurs — why?**

Azure RBAC assignments propagate through Azure AD — this is not instantaneous. The typical propagation time is **1–2 minutes**, sometimes up to 5 minutes in busy regions.

If you run the pipeline immediately after creating the role assignment, ADF's Managed Identity token may not yet include the new role claim. Result: 403 Forbidden even though the portal shows the assignment exists.

**Fix:** Wait 2 minutes after the assignment appears in the IAM Role assignments tab, then retry.

---

**Q8 — Why Set Variable instead of referencing activity output directly in Copy Activity?**

Both approaches work technically. The reason for Set Variable is readability and debuggability:

1. **Debuggability:** With Set Variable, you can see the token value in the Monitor panel under Variables — confirming the token was received before the Copy Activity runs.
2. **Reusability:** Once in `v_token`, any subsequent activity in the pipeline can reference `variables('v_token')`. Without Set Variable, each activity would need `activity('act_api_login').output.token` — verbose and harder to read.
3. **Clarity:** The intent is explicit — "store the token here for downstream use" vs. a long expression buried in a header field.

In Day 3 (ForEach loop), the variable becomes essential — the token must be accessible inside the loop where activity outputs from outside the loop are not directly accessible.

---

**Q9 — Hardcoded page parameters in dataset URL vs. parameters**

If you hardcode `?page=1&page_size=100` directly in the dataset URL:
- You cannot test with a different page without editing the dataset
- The dataset cannot be reused in a pagination loop (Day 3's Until Activity passes different page numbers per iteration)
- Every time you want to change the page size, you edit and republish the dataset — error-prone

With parameters (`p_page`, `p_page_size`), the dataset is a reusable template. The pipeline (or loop iteration) passes the page number at runtime. The same dataset works for page 1, page 50, or any future page.

---

**Q10 — What happens when the pipeline runs twice with the same parameters?**

The sink is a fixed path: `bronze/api/payments/raw/payments.json`. The second run **overwrites** the file with new content.

**Is this a problem in Day 2?** No — Day 2's goal is to prove connectivity. One file is fine.

**Is this a problem in production?** Yes. In production you need either:
- A **partitioned path** (e.g. `bronze/api/payments/ingestion_date=2026-09-27/payments.json`) — every run gets its own file, no overwriting, full history preserved
- Or an **append pattern** (Delta Lake `APPEND` mode) — new records are added to the existing table

Day 3 adds the date-partitioned path to solve this.

---

## Concept 2: ADF Components

**Q11 — Linked Service vs. Dataset with Day 2 examples**

**Linked Service:** HOW to connect — connection string, auth method, server address.
- Example: `ls_voltgrid_api` — type REST, base URL `https://ev-project-navy-mu.vercel.app`, auth Anonymous. This says "here is a REST server at this address."

**Dataset:** WHERE the data is and WHAT it looks like — a specific path or endpoint within a linked service.
- Example: `ds_voltgrid_payments_src` — uses `ls_voltgrid_api`, points to `/api/db/payments/?page=1&page_size=100`. This says "within that REST server, I want this specific endpoint."

One linked service can serve multiple datasets — `ds_voltgrid_sessions_src` would use the same `ls_voltgrid_api`.

---

**Q12 — Why two separate Web Activities for username and password?**

The Key Vault REST API returns one secret per call. The `/secrets/voltgrid-username/` endpoint returns only the username value; `/secrets/voltgrid-password/` returns only the password value. There is no batch-read endpoint in Key Vault.

The two activities run sequentially in Day 2 (for simplicity). In an optimised pipeline you would run them in parallel — see Q13.

---

**Q13 — Why are they chained sequentially? How to parallelise?**

`act_get_username` → `act_get_password` are chained sequentially in Day 2 for simplicity. They do not actually depend on each other — the password fetch does not need the username value.

**To run in parallel:**
- Remove the dependency arrow between `act_get_username` and `act_get_password`
- Both now have no `dependsOn` — ADF runs them simultaneously
- `act_api_login` depends on both (two incoming arrows) — it waits until both complete before running

```
act_get_username ─┐
                  ├─ On Success → act_api_login → ...
act_get_password ─┘
```

Parallel execution saves a few hundred milliseconds — negligible for this pipeline but good practice to demonstrate.

---

**Q14 — What does `@concat('Token ', variables('v_token'))` produce?**

It produces: `Token abc123xyz...` (where `abc123xyz...` is the stored token value).

**Why the space after "Token"?**
The VoltGrid API (and most Django REST Framework APIs) expect the Authorization header in the exact format:
```
Authorization: Token <value>
```
The space between `Token` and the token string is part of the format — without it, the header becomes `Tokenabc123...` which the API cannot parse → 401 Unauthorized.

This is different from Bearer tokens (`Bearer <value>`) — VoltGrid uses `Token` prefix (Django REST Framework's `TokenAuthentication` scheme).

---

**Q15 — `act_set_token` fails with "variable v_token not found"**

You forgot to declare the variable on the pipeline. Variables must be declared in the **Variables** tab of the pipeline before any activity can reference them.

**Fix:**
1. Click anywhere on the empty canvas (deselect all activities)
2. Bottom panel → **Variables** tab → **+ New**
3. Name: `v_token`, Type: `String`
4. Publish all → retry Debug

---

**Q16 — Why is Authorization header on the Copy Activity, not the Linked Service?**

The token value (`v_token`) is a **runtime variable** — it changes every pipeline run, obtained fresh from the login call. Linked Services are **static configuration** — they store fixed connection properties like base URL and auth type.

You cannot put a dynamic expression (`@concat('Token ', variables('v_token'))`) in a linked service field because linked services don't have access to pipeline runtime state — they are evaluated once at connection time, not per activity run.

The Copy Activity's `additionalHeaders` field accepts dynamic expressions, making it the correct place to inject a runtime-computed header value.

---

**Q17 — What is `filePattern: setOfObjects` on the JSON sink?**

When ADF writes multiple JSON objects to a single file, it needs to know the format:

- **setOfObjects:** Each object is written as a separate root object in the file. The result is one JSON object per line (JSONL/newline-delimited JSON) — or in some settings, a JSON array. This is the standard format for writing API response arrays.
- Without this setting, ADF may write the objects concatenated without separators, producing invalid JSON.

For 100 payment records, `setOfObjects` produces a file where each record is a valid JSON object. Spark's `spark.read.json()` can read this format directly (it handles both arrays and newline-delimited JSON).

---

**Q18 — `rowsRead: 100` but `rowsCopied: 0` — what happened?**

The Copy Activity read 100 rows from the API but wrote 0 to the sink. Possible causes:

1. **Fault tolerance with "skip incompatible rows":** If all 100 rows failed schema validation against the sink dataset schema, they were all skipped. Check the fault tolerance log file path (if logging was enabled).
2. **Sink write failed silently:** The write to ADLS Gen2 failed but ADF reported `rowsCopied: 0` rather than failing. Check the Output tab for error details and `filesWritten` count.
3. **Schema mismatch:** The source returns nested JSON but the sink dataset expects flat schema — ADF cannot map the fields.

**First check:** Copy Activity → Output tab → look for `errors` array and `filesWritten`. Then Monitor → Activity runs → Error tab if available.

---

**Q19 — Minimum RBAC roles for ADF Managed Identity**

| Resource | Role | Why this and not less |
|---|---|---|
| Key Vault | `Key Vault Secrets User` | Read-only secret access. `Key Vault Reader` only reads metadata, not values — not enough. |
| ADLS Gen2 | `Storage Blob Data Contributor` | Read + write + delete blobs. `Storage Blob Data Reader` is read-only — Copy Activity sink write would fail. |

**Why not Owner for both?**
Owner includes all permissions including deleting the storage account, Key Vault, and all their contents. Principle of least privilege: give only what is needed. If ADF's identity is compromised, Owner means an attacker could delete your entire data lake. Contributor scoped to blob data cannot even delete the storage account itself.

---

**Q20 — Debug vs. trigger — which version runs?**

**Debug:** Uses the current **draft** state — including unpublished changes in the current browser session. Linked services and datasets are also taken from draft if modified.

**Trigger (scheduled or manual Trigger Now):** Uses the **last published** version of the pipeline. Unpublished changes in the author canvas are invisible to triggers.

This is why the workflow is: make changes → Debug to test → Publish all → triggers pick up the new version. If you close the browser without publishing, your changes are saved in the Git collaboration branch (if Git is connected) but the live factory still runs the old version.

---

## Concept 3: Hands-on / Senior

**Q21 — Token expiry in a multi-hour pagination loop**

In Day 3's Until loop running hundreds of iterations over 3 hours, the token obtained at the start expires after 1 hour.

**Fix: Refresh the token inside the loop**

Move `act_get_username`, `act_get_password`, `act_api_login`, and `act_set_token` inside the Until Activity body, before the Copy Activity in each iteration.

```
Until loop (condition: no more pages):
    act_get_username → act_get_password → act_api_login → act_set_token → act_copy_page_N
```

Each iteration fetches a fresh token → token is always valid → no 401s regardless of how long the loop runs.

**Trade-off:** 4 extra activity runs per iteration (Key Vault reads + login). For 100 pages = 400 extra activity runs. Still within ADF's free tier (1,000 runs/month) for a dev pipeline. For production with thousands of pages, cache the token for 55 minutes (use an If Condition to re-fetch only when near expiry, tracking elapsed time via `utcnow()` comparison in a variable).

---

**Q22 — Production Bronze path structure**

Fixed path `bronze/api/payments/raw/payments.json` has two problems:
1. Every run overwrites the previous — no history, no reprocessability
2. You can't tell when a file was ingested

**Production path pattern:**
```
bronze/api/payments/ingestion_date=2026-09-27/page_001.json
bronze/api/payments/ingestion_date=2026-09-27/page_002.json
```

This uses **Hive-style partitioning** by date. Benefits:
- Full history preserved — yesterday's files are never touched
- Spark can use partition pruning: `WHERE ingestion_date = '2026-09-27'` reads only that day's files
- Each run is idempotent — if you rerun 2026-09-27's pipeline, it overwrites only that date's files

In ADF, the sink dataset uses:
```
folderPath: api/payments/ingestion_date=@{formatDateTime(utcnow(), 'yyyy-MM-dd')}
fileName:   page_@{pipeline().parameters.p_page}.json
```

---

**Q23 — Paginating through all API pages**

The API returns `{"results": [...], "next": "...?page=2", "count": 1542}`. To get all pages:

```
Pipeline variables: v_token, v_current_page (int, 1), v_total_pages (int, 0)

act_get_page_count (Copy Activity or Web Activity):
  Call page=1, extract 'count' from response
  Set v_total_pages = ceil(count / page_size)

Until Activity (condition: @{greaterOrEquals(variables('v_current_page'), variables('v_total_pages'))}):
    act_copy_page:
        Source: ds_voltgrid_payments_src, p_page = @{variables('v_current_page')}
        Sink: ds_bronze_payments_sink, path includes page number
    act_increment_page (Set Variable):
        v_current_page = @{add(variables('v_current_page'), 1)}
```

ADF does not have a native `for i in range(n)` — you simulate it with Until + a counter variable. This is the standard ADF pagination pattern.

---

**Q24 — What does the `Resource` field in Managed Identity Web Activity do?**

The `Resource` field tells ADF which Azure service the token should be scoped to. Azure AD issues tokens scoped to a specific audience — a token for Key Vault cannot be used to call Storage, and vice versa.

- `Resource: https://vault.azure.net` → ADF gets a token valid for Key Vault APIs
- If you set `Resource: https://storage.azure.com` → ADF gets a token valid for Storage APIs

If you set the wrong Resource on a Key Vault Web Activity (e.g. `https://storage.azure.com`), ADF gets a Storage token, presents it to Key Vault → Key Vault rejects it → 401 Unauthorized. The error message typically says "Bearer token has wrong audience."

---

**Q25 — Metadata-driven design for N endpoints**

Instead of one Copy Activity per endpoint hardcoded in the pipeline:

**Step 1 — Config file in ADLS Gen2** (`bronze/config/endpoint_config.json`):
```json
[
  {"name": "payments",  "path": "/api/db/payments/"},
  {"name": "sessions",  "path": "/api/db/sessions/"},
  {"name": "customers", "path": "/api/db/customers/"},
  {"name": "stations",  "path": "/api/db/stations/"},
  {"name": "vehicles",  "path": "/api/db/vehicles/"}
]
```

**Step 2 — Lookup Activity:** Reads the config file → returns the array.

**Step 3 — ForEach Activity** (Batch count 5, parallel):
```
ForEach item in Lookup output:
    Copy Activity:
        Source: ds_voltgrid_generic_src (dataset with path parameter)
            path param = @{item().path}
        Sink: ds_bronze_generic_sink (dataset with name + date params)
            folder = bronze/api/@{item().name}/ingestion_date=@{formatDateTime(utcnow(), 'yyyy-MM-dd')}
```

**Adding a 6th endpoint:** Edit the JSON config file — no pipeline changes. No new dataset needed if the generic dataset accepts a path parameter.

---

**Q26 — Pipeline Parameters vs. Key Vault for credentials**

**Approach A — Pipeline Parameters:**
- Credentials set at trigger time (in the trigger definition or "Trigger Now" form)
- Anyone who can trigger the pipeline sees the credentials in the trigger run input
- Credentials visible in Monitor → Pipeline run → Input tab in plaintext
- Risk: developer copies a trigger JSON into a PR — credentials in Git history

**Approach B — Key Vault + Web Activity (Day 2 approach):**
- Credentials never in the pipeline JSON, never in Monitor inputs, never in Git
- Only ADF's Managed Identity can read them — and only if the RBAC role is assigned
- Rotation: update one Key Vault secret → all pipelines automatically use the new value
- Audit: every secret read is logged in Key Vault's diagnostic logs with timestamp and caller identity

**Winner: Approach B.** The only time you'd use Approach A is for non-sensitive runtime parameters like `run_date` or `page_size`.

---

**Q27 — Schedule Trigger + failure alert**

**Add the trigger:**
1. Author → `pl_bronze_api_payments` → **Add trigger** → **New/Edit**
2. Trigger type: **Schedule**
3. Start: today, time: `02:00 UTC`
4. Recurrence: Every 1 Day
5. **OK** → **Publish all**

**Add failure alert:**
1. Add a **Web Activity** named `act_alert_on_failure` at the end of the pipeline
2. Set dependency: `act_copy_payments` → `act_alert_on_failure` with condition **On Failure** (change the arrow colour by right-clicking)
3. Configure Web Activity to POST to a Teams webhook URL:
   ```json
   {
     "text": "@{concat('Pipeline FAILED: pl_bronze_api_payments at ', utcnow())}"
   }
   ```

**Alternative (no Web Activity needed):**
Azure Monitor → Alerts → New alert rule → scope `adf-datalake-dev-ded` → condition: "Failed pipeline runs count > 0" → action group: send email to the team. This fires for any pipeline failure without touching the pipeline JSON.

---

**Q28 — Key Vault secret response structure and versioned secrets**

The Key Vault REST API always returns the secret in the `value` field:
```json
{
  "value": "EVcharge@AU2025",
  "contentType": null,
  "id": "https://key-vault-session-ded.vault.azure.net/secrets/voltgrid-password/abc123version",
  "attributes": { "enabled": true, "created": ..., "updated": ... }
}
```

`activity('act_get_password').output.value` → `"EVcharge@AU2025"` — the actual secret string.

**For a versioned secret** (`my-secret` at version `abc123`), the URL becomes:
```
https://key-vault-session-ded.vault.azure.net/secrets/my-secret/abc123/?api-version=7.0
```
Omitting the version (as in Day 2) always returns the **latest enabled version** — which is what you want. Pin to a specific version only if you need to guarantee a specific secret value regardless of future rotations.

---

**Q29 — API password rotated — minimum changes to fix**

**1 change total.** Update the secret value in Key Vault:

1. Portal → Key vaults → `key-vault-session-ded` → **Secrets** → `voltgrid-password`
2. Click **+ New Version** → enter the new password → **Create**

That's it. The pipeline JSON is unchanged. The linked service is unchanged. The next run of `act_get_password` reads the new secret value from Key Vault automatically — because the Web Activity always reads the latest enabled version.

This is the entire value proposition of Key Vault integration: credential rotation in one place, zero downstream changes.

---

**Q30 — System design: 5 endpoints, shared token, independent failure, date-partitioned**

**Components:**

| Requirement | ADF Component | Configuration |
|---|---|---|
| Run nightly at 01:00 UTC | **Tumbling Window Trigger** | Start today, frequency 1 day, time 01:00 UTC. Tumbling Window (not Schedule) so a missed run is automatically retried |
| One login call per run | **3 Web Activities + Set Variable** before ForEach | `act_get_username` → `act_get_password` → `act_api_login` → `act_set_token` — runs once at pipeline start |
| All 5 endpoints in parallel | **ForEach Activity** | `Is Sequential: false`, `Batch count: 5`. Items = Lookup output from config file |
| Single endpoint failure doesn't cascade | **ForEach default behaviour** | ForEach continues all items even if one fails — item failures don't stop the loop |
| Retry once before failing | Copy Activity **Settings tab** | `Retry: 1`, `Retry interval: 2 min` — ADF retries the Copy Activity once before marking the iteration failed |
| `bronze/api/{name}/{date}/page_{n}.json` | **Dataset parameters** | Sink dataset: `folderPath = bronze/api/@{dataset().endpoint_name}/ingestion_date=@{dataset().run_date}`, `fileName = page_@{dataset().p_page}.json` |
| 6th endpoint needs no pipeline change | **Lookup Activity + config file** | Config JSON in ADLS Gen2 lists endpoints. Adding row to config file is the only change needed |

**Full pipeline structure:**
```
Tumbling Window Trigger (01:00 UTC daily)
    ↓ run_date = formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')
act_get_username → act_get_password → act_api_login → act_set_token
    ↓
Lookup (reads bronze/config/endpoint_config.json)
    ↓
ForEach (parallel, batch=5, items=Lookup output):
    Until (all pages fetched for this endpoint):
        Copy Activity (source: generic REST dataset, sink: partitioned ADLS path)
            Retry: 1, interval: 2 min
        Set Variable (increment page counter)
    ↓
act_alert_on_failure (Web Activity → Teams webhook)
    dependency: ForEach → On Failure
```

**Config file** (`bronze/config/endpoint_config.json`):
```json
[
  {"name": "payments",  "path": "/api/db/payments/"},
  {"name": "sessions",  "path": "/api/db/sessions/"},
  {"name": "customers", "path": "/api/db/customers/"},
  {"name": "stations",  "path": "/api/db/stations/"},
  {"name": "vehicles",  "path": "/api/db/vehicles/"}
]
```

Adding `/api/db/energy-prices/` = add one JSON object to this file. Zero ADF changes.
