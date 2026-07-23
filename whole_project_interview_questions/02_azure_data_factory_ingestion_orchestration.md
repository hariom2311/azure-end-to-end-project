# Topic 02 — Azure Data Factory: Ingestion & Orchestration
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 3 Questions | 2–8 years experience | Scenario-based

---

### Q5. Explain how the high-watermark incremental load pattern works in ADF for this project. What happens if a pipeline fails halfway through?

**Answer:**

**How watermark tracking works:**

A watermark is the value of the "last processed" record's timestamp column (e.g., `updated_at`, `plug_in_date`). At the start of every pipeline run:

1. ADF reads the current watermark from `azure-sql-watermarks/pipeline_watermark` table: `SELECT watermark_value FROM pipeline_watermarks WHERE entity_name = 'charging_sessions'`.
2. ADF executes a Copy Activity: `GET /api/charging_sessions?updated_after=<watermark_value>`.
3. On success, ADF writes the new watermark (max `updated_at` of the copied batch) back to the table.
4. Next run picks up from the new watermark.

**What happens on mid-pipeline failure:**

If the Copy Activity succeeds but the watermark write fails:
- Next run re-processes the same records (duplicates land in Bronze).
- This is **by design** — Bronze is append-only. Duplicates at Bronze are acceptable; Silver has the deduplication MERGE that makes it idempotent.

If the Copy Activity itself fails:
- Watermark is never updated — next run re-reads the same window.
- ADF retry policy (3 retries, exponential backoff) handles transient failures.

**Idempotency guarantee:** The Silver MERGE operation uses `MERGE INTO silver_table USING source ON (primary_key) WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`. Even if the same Bronze record arrives twice (from a failed watermark update), the Silver MERGE only keeps one version — exactly-once semantics at Silver regardless of at-least-once at Bronze.

**In this project:** `bronze/api/charging_sessions/` might have two identical `session_id` rows after a failed run. When Silver runs the MERGE keyed on `session_id`, it upserts — no duplicate session in Silver.

---

### Q6. You designed a metadata-driven pipeline that handles 17 entities with a single pipeline pair. Walk me through that design and explain why this is better than 17 individual pipelines.

**Answer:**

**The metadata config approach:**

A `pipeline_metadata_config.json` file stored in `bronze/config/` defines each entity:

```json
[
  {
    "entity_name": "payments",
    "api_endpoint": "/api/payments",
    "watermark_column": "updated_at",
    "bronze_path": "bronze/api/payments/",
    "load_type": "incremental"
  },
  {
    "entity_name": "customers",
    "api_endpoint": "/api/customers",
    "watermark_column": "customer_created_at",
    "bronze_path": "bronze/api/customers/",
    "load_type": "full"
  }
  // ...17 entities total
]
```

**Pipeline architecture:**

- **Master pipeline (`pl_bronze_api_master_v4`):** Reads the config JSON using a Lookup Activity. Passes each config row to a ForEach Activity with parallel execution enabled (`batchCount: 17` — all 17 run simultaneously).
- **Child pipeline (`pl_bronze_api_ingest_v4`):** Receives parameters (`entity_name`, `api_endpoint`, `watermark_column`, `bronze_path`). Performs: auth → read watermark → Copy Activity → write watermark → audit log.

**Why better than 17 individual pipelines:**

| Metric | 17 individual pipelines | 1 metadata-driven pair |
|---|---|---|
| Adding entity #18 | New pipeline + 2 datasets + testing | Add 1 JSON row |
| Fixing an auth bug | Fix in 17 places | Fix in 1 child pipeline |
| Parallel execution | Manual coordination | ForEach `batchCount` |
| Monitoring | 17 separate run histories | 1 master run + child details |
| Watermark tracking | 17 separate state stores | 1 parameterised CSV/table |

**Failure isolation:** Because the master uses a ForEach with child pipelines, one entity failing (e.g., fleet API is down) does not block the other 16. The master marks the failed entity as `FAILED` in the audit table, and the others complete.

---

### Q7. An ADF pipeline that reads from the Payment Gateway REST API has been failing intermittently with HTTP 429 (Too Many Requests). How do you handle this without data loss?

**Answer:**

HTTP 429 means the API is rate-limiting our calls. The solution has three layers:

**Layer 1 — ADF built-in retry:**
- In the Copy Activity settings, configure `Retry: 3`, `Retry interval: 60 seconds` (with exponential backoff: 60s, 120s, 240s).
- For 429 specifically, the Retry-After header tells us how long to wait — ADF respects this if the linked service is configured for it.

**Layer 2 — Pagination and batch sizing:**
- Instead of fetching all payments in one request, paginate: `GET /api/payments?page=1&page_size=500`.
- Smaller requests are less likely to hit rate limits.
- ADF Pagination rules in the REST linked service handle this automatically.

**Layer 3 — Pipeline-level watermark:**
- Because of high-watermark tracking, a partial failure only means the current page wasn't saved.
- On retry, ADF resumes from the last committed watermark — it doesn't re-read all historical data.

**Monitoring:**
- Set up an Azure Monitor alert on ADF pipeline run status = Failed for `pl_bronze_api_ingest_v4`.
- Log Analytics query surfaces the error type (429 vs 500 vs network) for faster diagnosis.

**In practice for VoltGrid:** The Payment Gateway is the most critical source — billing and revenue dashboards depend on it. We set a higher retry count (5) and an alert SLA: if payments data is not in Bronze by 3:00 AM, an ops team member is paged.
