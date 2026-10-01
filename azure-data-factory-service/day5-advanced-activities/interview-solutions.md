# Day 5 — Interview Solutions: ADF Advanced Activities

---

## Mapping Data Flow

**A1**
Copy Activity moves data as-is from A to B — no transformations, no type changes. It reads REST JSON and writes it directly to Bronze ADLS as JSON. Mapping Data Flow adds a visual transformation graph on top — it reads Bronze JSON, applies Filter, Derived Column, Select, and writes clean typed Parquet to Silver ADLS.

In the VoltGrid pipeline:
- **Bronze ingestion** → Copy Activity (`act_copy_payments`) — raw, fast, no compute overhead
- **Silver transform** → Mapping Data Flow (`df_silver_payments`) — clean, typed, partitioned Parquet

---

**A2**
Debug mode starts a Spark cluster in the background that powers the **Data preview** feature — it lets you click any transformation node and see a sample of the data flowing through it. The 3–5 minute wait is the Spark cluster cold-start time (VM provisioning + Spark runtime init).

Production pipelines (triggered by a schedule or event) do **not** need debug mode. The Data Flow Activity in the pipeline starts its own Spark session using the Azure IR settings (core count, compute type). Debug is only for development in ADF Studio.

---

**A3**
Use the **Derived Column** transformation. Add a new column:
- Column name: `amount_aud`
- Expression: `toDouble(amount)`

`toDouble()` casts the string `"45.80"` to the numeric value `45.80` of type Double. The original `amount` column (string) can then be removed in a downstream **Select** transformation.

---

**A4**
`equals(status, 'active')` is case-sensitive in ADF Data Flow expressions. `'Active'` does not equal `'active'`, so those rows are filtered out.

Fix: normalize the column before comparison using `lower()`:
```
equals(lower(status), 'active')
```
Or use `equalsIgnoreCase(status, 'active')` which is available in ADF expression language and handles case automatically.

---

**A5**
Four Mapping Data Flow transformations:

| Transformation | What it does |
|---|---|
| **Filter** | Removes rows that don't match a condition — like a SQL WHERE clause |
| **Derived Column** | Adds or updates columns using expressions — like SQL computed columns |
| **Aggregate** | Groups rows and computes SUM, COUNT, AVG — like SQL GROUP BY |
| **Join** | Combines two data streams on a key — like SQL LEFT/INNER JOIN |

VoltGrid examples:
- **Filter:** `notEquals(status, 'failed')` — remove failed payment rows before writing to Silver
- **Derived Column:** `toDouble(amount)` to cast the API's string amount to a numeric Silver column `amount_aud`

---

**A6**
Mapping Data Flow runs on a Spark cluster. Even for 500 rows, Spark has fixed overhead:
- Cluster startup: ~2–3 minutes (provisioning VMs, initialising Spark)
- Job planning and serialisation: ~1 minute
- Actual data processing: a few seconds

This is the trade-off: Spark is powerful for millions of rows but has fixed overhead regardless of data size. For small datasets, a Copy Activity (which runs without Spark) is faster. Data Flow is worth it when the transformation logic is complex or the dataset is large (millions of rows).

---

**A7**
Two likely causes of an empty join result:

1. **Case mismatch on the join key.** If Bronze has `payment_method = "credit_card"` and the reference table has `payment_method = "Credit Card"`, the join key doesn't match. Fix: use `lower()` on both sides — `lower(left@payment_method) == lower(right@payment_method)`.

2. **Wrong join type.** An INNER JOIN returns only matching rows — if the reference table is missing some keys that appear in payments, those payment rows disappear. Fix: use a LEFT JOIN to keep all payment rows and get `null` for unmatched reference rows.

---

**A8**
| | Lookup (inside Mapping Data Flow) | Lookup Activity (in pipeline) |
|---|---|---|
| Where it runs | Inside a Data Flow's transformation graph | In the ADF pipeline as a standalone activity |
| Returns | Enriches every row in the data stream with matching reference data | Returns an array of rows as a JSON object |
| Downstream use | Used by downstream transformations in the same Data Flow | Used by ForEach, Filter, If Condition in the pipeline |
| Use when | You need to join reference data row-by-row during transformation | You need to load a config list to drive pipeline logic |

Example: Use the Data Flow Lookup to enrich each payment row with a station name from a reference table. Use the pipeline Lookup Activity to load the list of endpoints from `endpoint_config.json` before a ForEach.

---

## Databricks Notebook Activity

**A9**
The Databricks Notebook Activity runs a notebook in an Azure Databricks workspace from inside an ADF pipeline. ADF submits a job to Databricks, waits for it to complete, and reports success or failure back to Monitor.

Minimum setup:
1. An Azure Databricks workspace
2. A Databricks access token stored in Key Vault
3. A Linked Service (`ls_databricks`) connecting ADF to the workspace
4. A notebook in the workspace at a known path (e.g. `/VoltGrid/silver/process_payments`)

---

**A10**
Choose Databricks Notebook over Mapping Data Flow when:

1. **Complex logic required** — ML feature engineering, multi-step Spark operations, Delta Lake time-travel queries, or any logic that goes beyond the built-in Data Flow transformations (Filter, Aggregate, Join, etc.)

2. **Team is already on Databricks** — if data engineers already write and version notebooks in Databricks, running them from ADF is more natural than rebuilding the logic in a visual canvas. Also enables testing the notebook directly in Databricks before wiring it into the pipeline.

---

**A11**
In Azure Databricks Python notebooks, ADF base parameters are available as Databricks widgets:

```python
run_date = dbutils.widgets.get("run_date")
```

If the widget is not set (e.g., when running the notebook manually in Databricks without ADF), it throws an error. Add a default for local testing:

```python
dbutils.widgets.text("run_date", "2026-10-01")
run_date = dbutils.widgets.get("run_date")
```

---

**A12**
The cluster was deleted or auto-terminated (Databricks auto-terminates idle clusters by default). When ADF tries to start a job on a hardcoded cluster ID that no longer exists, the call fails.

Recommended fix: use **New job cluster** in the Databricks linked service instead of an existing interactive cluster. A new job cluster is provisioned on demand for each run and terminated automatically when the job finishes — no dependency on a specific cluster ID. It costs more per run but is reliable and requires no cluster maintenance.

---

**A13**
Two reasons the notebook could succeed but not write data:

1. **Exception was caught in the notebook.** If the notebook has a broad `try/except` that swallows the error without re-raising it, Databricks reports the run as succeeded even if the write failed. Fix: ensure exceptions in the write path are re-raised so Databricks reports failure.

2. **Wrong ADLS path.** The notebook wrote to a different path than expected — e.g., `run_date` parameter was empty so the partition path was malformed. Fix: check Monitor → Activity Input tab to confirm the `run_date` value that was passed, then search ADLS for files at that exact path.

Diagnosis: click the `runPageUrl` in the activity output → open the Databricks run page → review the notebook's output cells and any exception messages.

---

## Stored Procedure Activity

**A14**
The Stored Procedure Activity calls a stored procedure on a relational database from inside an ADF pipeline. It supports:
- Azure SQL Database
- Azure SQL Managed Instance
- SQL Server (via Self-hosted IR for on-premises)
- Azure Synapse Analytics (dedicated SQL pool)

It does not move data — it executes a procedure that runs server-side SQL logic.

---

**A15**
Copy Activity can only write to a dataset using its built-in sink logic — it cannot execute arbitrary SQL, call a procedure, or do conditional upserts. If you need to INSERT a row with computed values (pipeline name, current timestamp, rows copied), that logic must live in SQL.

Stored Procedure Activity is better because:
- The procedure can do UPSERT (`MERGE`), conditional logic, multi-table joins
- The audit logic is in SQL and testable independently of ADF
- Security: the procedure can be granted execute permissions without granting write access to the table directly

---

**A16**
The stored procedure parameter `@rows_copied` is declared as `INT` in SQL. If ADF passes it as `String`, SQL Server will attempt an implicit conversion. For a valid integer string like `"100"`, the conversion usually succeeds. However, if `rowsCopied` is `null` (e.g., the copy activity output had no `rowsCopied` field), passing a null string to an INT parameter will cause a SQL type conversion error and the activity will fail.

Best practice: always match the ADF parameter type to the SQL parameter type exactly.

---

**A17**
The second student is correct — with an important nuance. `returnCode: 0` means the stored procedure was executed without a SQL error being raised back to ADF. But the procedure could execute successfully (no error) while the INSERT fails silently if:
- The INSERT was inside a `TRY...CATCH` that swallowed the exception without re-raising
- A constraint violation was handled internally
- The wrong database was targeted

To verify: query `SELECT * FROM pipeline_audit ORDER BY run_timestamp DESC` directly in Azure SQL after the run. If no row appears, the INSERT did not happen despite the success status.

---

**A18**
```sql
CREATE PROCEDURE usp_update_watermark
    @pipeline_name VARCHAR(200),
    @last_run_time DATETIME
AS
BEGIN
    MERGE watermark_table AS target
    USING (SELECT @pipeline_name AS pipeline_name, @last_run_time AS last_run_time) AS source
        ON target.pipeline_name = source.pipeline_name
    WHEN MATCHED THEN
        UPDATE SET last_run_time = source.last_run_time
    WHEN NOT MATCHED THEN
        INSERT (pipeline_name, last_run_time)
        VALUES (source.pipeline_name, source.last_run_time);
END;
```

`MERGE` handles both INSERT (first run — no row exists yet) and UPDATE (subsequent runs — row already exists). This is safer than a separate INSERT or UPDATE which would fail on duplicates or no-ops respectively.

---

## Validation Activity

**A19**
Both verify file existence, but they work differently:

| | Validation Activity | Get Metadata + If Condition |
|---|---|---|
| Behaviour | Polls repeatedly until file appears or timeout | Checks once at that moment |
| File not there yet | Waits and retries | Immediately fails or branches |
| Use when | File might not have arrived yet but will | File should already be there |

Validation is the right choice when the file is expected to arrive after the pipeline starts (e.g., an upstream team uploads it within a few minutes). Get Metadata + If Condition is the right choice when you want to check right now and branch based on whether it's there.

---

**A20**
| Setting | What it controls | VoltGrid example value |
|---|---|---|
| **Timeout** | Max time to wait before giving up. Format: `D.HH:MM:SS` | `0.00:10:00` (10 minutes — Bronze copy should finish in < 2 min, give 10 to be safe) |
| **Sleep** | Seconds between each poll attempt | `30` (check every 30 seconds — frequent enough, not wasteful) |
| **Minimum size** | File must be at least this many bytes — rejects empty/partial files | `1024` (1 KB — a valid payments JSON should be at least 1 KB) |

---

**A21**
The pipeline succeeds. The Validation Activity polls every 60 seconds (Sleep = 60). At 4 minutes 30 seconds, the file arrives. On the next poll (at 5 minutes), the file is found — before the 5-minute timeout. Status = Succeeded and the pipeline continues.

Key point: the timeout is the maximum wait time, not a cutoff at exactly 5 minutes. As long as the file is found before the timeout expires, the activity succeeds regardless of which poll iteration finds it.

---

**A22**
The Validation Activity checks file existence and size — it does not validate file content. A file can exist and be larger than the minimum size but contain invalid or incomplete JSON (e.g., the write was interrupted and the file is truncated, or the API returned an error response that was written to the file).

Root cause: the Bronze copy may have written a partial response or an error body to the JSON file. The file passes the size check but the Data Flow reads it and finds no valid records.

Diagnosis: Download the Bronze file from ADLS and inspect its content. Add a Get Metadata + If Condition check on `size` with a more realistic minimum (e.g., 50KB for a full payments page) to catch suspiciously small files.

---

**A23**
```
[act_wait_payments]          [act_wait_sessions]
  Validation Activity          Validation Activity
  Dataset: payments.json       Dataset: sessions.json
  Timeout: 10 min              Timeout: 10 min
       │                               │
       └───────────┬───────────────────┘
                   │  (both must succeed — On Success from both)
          [act_silver_transform]
            Data Flow Activity
            (reads both files)
```

Both Validation Activities have no dependencies from each other — they start simultaneously (in parallel). The Data Flow Activity depends on **both** (On Success from `act_wait_payments` AND On Success from `act_wait_sessions`). In ADF, a multi-dependency (two incoming arrows with On Success) means both must succeed before the downstream activity runs.

---

## Azure Function Activity

**A24**
| | Web Activity | Azure Function Activity |
|---|---|---|
| Target | Any HTTP endpoint | Azure Function specifically |
| Linked service | Not required | Required (`ls_azure_function`) |
| Auth | MSI, Basic, Client Cert, None | Function key (from Key Vault) |
| Use when | Key Vault secrets, REST API webhooks | Custom server-side code (email, validation, Cosmos DB write) |

Web Activity scenario: Call Key Vault to read `voltgrid-username` secret (`GET https://...vault.azure.net/secrets/...`).

Azure Function scenario: Call `SendPipelineAlert` function on pipeline failure — the function reads Teams webhook URL from its own config and sends a formatted card message.

---

**A25**
ADF authenticates to an Azure Function using the **Function host key** — a secret key that Azure Functions generates for the Function App. Any HTTP request with this key in the `x-functions-key` header (or `code` query parameter) is allowed to invoke the function.

The key should be stored in **Azure Key Vault** — not hardcoded in the linked service or ADF JSON. The linked service references the Key Vault secret:
- AKV linked service: `ls_keyvault`
- Secret name: `function-host-key`

Reason: if the key is hardcoded, anyone who can read the ADF JSON (e.g., from a git repository) can call the function. Key Vault ensures the key is only read at runtime by the ADF Managed Identity.

---

**A26**
Use dependency condition **Failed** (also called "On Failure"). In ADF Studio:
1. Draw the arrow from `act_copy_payments` to `act_alert_on_failure`
2. Click the arrow → in the dropdown, select **Failed**

The arrow turns **red** in the ADF canvas — red arrows indicate "on failure" dependencies. Green = On Success, Blue = On Completion (any result), Orange = On Skip.

---

**A27**
Two reasons the Function returned 200 but no alert was sent:

1. **Exception was caught silently inside the Function.** If the Function's alert logic (e.g., Teams webhook POST) throws an exception that is caught without being re-raised, the Function still returns 200 to ADF. Check the Function App's **Application Insights** or **Log Stream** in Azure Portal to see if the exception appears in logs.

2. **Wrong Teams webhook URL.** The webhook URL may have expired, been revoked, or have a typo. The Teams API returns 200 even for some invalid webhook calls. Check the Function's logs for the HTTP response from the Teams POST.

---

**A28**
Wire the Azure Function Activity with dependency condition **Succeeded** from the last successful activity (e.g., `act_log_audit`):

```
act_log_audit → act_notify_success (On Success)
```

Body JSON to send:
```
@concat('{"pipeline":"', pipeline().pipelineName,
         '","run_date":"', variables('v_ingestion_date'),
         '","rows_copied":"', string(activity('act_copy_payments').output.rowsCopied),
         '","status":"success"}')
```

The Azure Function receives this, formats a Teams card message, and POSTs to the Teams incoming webhook URL. The message would say something like: "Pipeline `pl_bronze_api_payments` completed for `2026-10-01` — 100 rows copied."

---

## Mixed / Senior

**A29**
Complete end-to-end pipeline — `pl_ev_daily_ingestion`:

| # | Activity Name | Type | Key Configuration |
|---|---|---|---|
| 1 | `act_get_username` | Web Activity | GET Key Vault secret `voltgrid-username`, MSI auth |
| 2 | `act_get_password` | Web Activity | GET Key Vault secret `voltgrid-password`, MSI auth (parallel with act_get_username) |
| 3 | `act_api_login` | Web Activity | POST `/api/auth/login/` with username+password in body, depends on both above |
| 4 | `act_set_token` | Set Variable | `v_token = @activity('act_api_login').output.token` |
| 5 | `act_set_ingestion_date` | Set Variable | `v_ingestion_date = @formatDateTime(utcnow(), 'yyyy-MM-dd')` |
| 6 | `act_copy_payments` | Copy Data | Source: REST `ds_voltgrid_payments_src` with Auth header; Sink: Bronze ADLS |
| 7 | `act_wait_for_bronze` | Validation | Dataset: `ds_bronze_payments_expected`; Timeout: 10 min; Sleep: 30s; Min size: 1024 |
| 8 | `act_silver_payments` | Data Flow | Data Flow: `df_silver_payments`; IR: AutoResolve; Core count: 8 |
| 9 | `act_log_audit` | Stored Procedure | Linked service: `ls_azure_sql`; Procedure: `usp_log_pipeline_run`; params: pipeline name, date, rows |
| 10 | `act_alert_on_failure` | Azure Function | Linked service: `ls_azure_function`; Function: `SendPipelineAlert`; Dependency: On Failure from `act_copy_payments` |

Dependencies:
- 1 and 2 start in parallel (no dependency)
- 3 depends on both 1 and 2 (On Success)
- 4 → 5 → 6 → 7 → 8 → 9 (On Success chain)
- 10 depends on 6 with On Failure dependency (fires only when copy fails)

---

**A30**
The student is partially right — a Get Metadata + If Condition + Until loop can poll for a file and loop until it appears. But the Validation Activity is better for this specific purpose:

**Advantages of Validation Activity:**
- Built-in polling with configurable Sleep interval — no Until loop to build and maintain
- Built-in timeout with clean failure message — Until needs manual timeout logic
- Minimum size check is built-in — Until needs an extra If Condition for this
- Fewer activities = simpler pipeline canvas and Monitor output

**Advantages of Get Metadata + If Condition + Until:**
- More flexible — can check multiple conditions per iteration (e.g., file exists AND row count in metadata > 0)
- Can perform custom actions between polls (log a message, call a Web Activity)
- No dependency on a specific Validation dataset — the dataset type is irrelevant

**Verdict:** Use Validation Activity when you simply need to wait for a file to appear — it is purpose-built for this and requires zero custom logic. Use the manual Until loop only when you need custom actions between polls or conditions that the Validation Activity cannot express.
