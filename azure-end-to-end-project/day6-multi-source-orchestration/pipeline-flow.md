# pl_bronze_master — Pipeline Flow

**Pipeline:** `pl_bronze_master`
**Purpose:** Orchestrates all three Bronze sources — payments, fleet, weather — sequentially by calling `pl_bronze_incremental_load_v2` three times with different `p_source_name` values. Each child pipeline reads its full configuration from the control table.

---

## Design Principle — Control Table Driven

All source-specific configuration (endpoint, sink path, file prefix, page size, load type, watermark) lives in **`ev_catalog.ctl.pipeline_control`**, not in any pipeline. The master pipeline only knows source names. To add a 4th source:

1. `INSERT` one row into the control table
2. Add one `ExecutePipeline` activity to `pl_bronze_master`

No other changes needed.

---

## Quick Summary

```
pl_bronze_master
  │
  ▼  STEP 1 (sequential)
  act_run_payments  → calls pl_bronze_incremental_load_v2 (p_source_name='payments')
  │                   waits for completion
  ▼  STEP 2 (only runs if payments succeeded)
  act_run_fleet     → calls pl_bronze_incremental_load_v2 (p_source_name='fleet')
  │                   waits for completion
  ▼  STEP 3 (only runs if fleet succeeded)
  act_run_weather   → calls pl_bronze_incremental_load_v2 (p_source_name='weather')
                      waits for completion
```

---

## Master Pipeline Flow Diagram

```
╔══════════════════════════════════════════════════════════════════════╗
║                        pl_bronze_master                              ║
║   Parameter: p_load_type_override (string, default "")               ║
╚══════════════════════════════════════════════════════════════════════╝
                             │
                             ▼
          ┌──────────────────────────────────────────┐
          │            act_run_payments               │
          │   type: ExecutePipeline                   │
          │   calls: pl_bronze_incremental_load_v2    │
          │   p_source_name  = "payments"             │
          │   p_load_type_override = (passed through) │
          │   waitOnCompletion: true                  │
          └──────────────────┬───────────────────────┘
                             │ Succeeded
                             ▼
          ┌──────────────────────────────────────────┐
          │            act_run_fleet                  │
          │   type: ExecutePipeline                   │
          │   calls: pl_bronze_incremental_load_v2    │
          │   p_source_name  = "fleet"                │
          │   p_load_type_override = (passed through) │
          │   waitOnCompletion: true                  │
          └──────────────────┬───────────────────────┘
                             │ Succeeded
                             ▼
          ┌──────────────────────────────────────────┐
          │            act_run_weather                │
          │   type: ExecutePipeline                   │
          │   calls: pl_bronze_incremental_load_v2    │
          │   p_source_name  = "weather"              │
          │   p_load_type_override = (passed through) │
          │   waitOnCompletion: true                  │
          └──────────────────┬───────────────────────┘
                             │
                             ▼
                       PIPELINE END
```

---

## What Each Child Pipeline Does

Each `ExecutePipeline` call triggers `pl_bronze_incremental_load_v2` which:

1. Fetches credentials from Key Vault
2. Reads config for `p_source_name` from `ev_catalog.ctl.pipeline_control`
3. Logs in → gets token
4. Fetches page 1 metadata → gets `total_pages`
5. Loops pages 1..N → copies each page → Bronze ADLS
6. Updates watermark in control table

The child pipeline is **completely self-contained** — master just passes the source name.

---

## Control Table: All Three Sources

```sql
SELECT source_name, endpoint, sink_base_path, load_type, last_watermark
FROM ev_catalog.ctl.pipeline_control
ORDER BY source_name;
```

| source_name | endpoint | sink_base_path | load_type | last_watermark |
|---|---|---|---|---|
| fleet | `/api/fleet/` | `api/fleet` | full | `1900-01-01T00:00:00Z` |
| payments | `/api/db/payments/` | `api/payments` | incremental | `<last run ts>` |
| weather | `/api/weather/` | `api/weather` | full | `1900-01-01T00:00:00Z` |

---

## Bronze Output After a Full Run

```
bronze/
├── api/
│   ├── payments/
│   │   └── ingestion_date=2026-10-18/
│   │       ├── payments_page_1.json
│   │       ├── payments_page_2.json
│   │       └── ... (N pages)
│   ├── fleet/
│   │   └── ingestion_date=2026-10-18/
│   │       ├── fleet_page_1.json
│   │       └── ... (N pages)
│   └── weather/
│       └── ingestion_date=2026-10-18/
│           ├── weather_page_1.json
│           └── ... (N pages)
```

---

## Failure Behaviour

| Failure point | Result |
|---|---|
| `act_run_payments` fails | `act_run_fleet` and `act_run_weather` are **skipped** — dependency condition is `Succeeded` |
| `act_run_fleet` fails | `act_run_weather` is skipped, payments data is in Bronze |
| `act_run_weather` fails | Fleet and payments data are in Bronze; only weather watermark not updated |

> If a child run fails mid-way, the child's watermark is NOT updated (watermark update is the last step). Re-run the master — the failed source retries from the last successful watermark.

---

## Execution Mode

| Parameter | How to Set | Behaviour |
|---|---|---|
| `p_load_type_override = ""` (default) | Trigger without params | Each source uses its own `load_type` from control table |
| `p_load_type_override = "full"` | Pass "full" at trigger | All three sources do a full reload (ignores control table watermark) |

---

## How to Add a New Source

1. **Insert a row** into `ev_catalog.ctl.pipeline_control`:
   ```sql
   INSERT INTO ev_catalog.ctl.pipeline_control
       (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
   VALUES
       ('my_new_source', '/api/my-source/', 'api/my-source', 'my_source', 1000, 'full', '1900-01-01T00:00:00Z', true);
   ```

2. **Add one ExecutePipeline activity** to `pl_bronze_master`:
   ```json
   {
       "name": "act_run_my_new_source",
       "type": "ExecutePipeline",
       "dependsOn": [{ "activity": "act_run_weather", "dependencyConditions": ["Succeeded"] }],
       "typeProperties": {
           "pipeline": { "referenceName": "pl_bronze_incremental_load_v2", "type": "PipelineReference" },
           "parameters": {
               "p_source_name": { "value": "my_new_source", "type": "Expression" },
               "p_load_type_override": { "value": "@pipeline().parameters.p_load_type_override", "type": "Expression" }
           },
           "waitOnCompletion": true
       }
   }
   ```

3. Publish. The child pipeline `pl_bronze_incremental_load_v2` needs no changes.

---

## Activity Reference Table

| Activity | Type | Depends On | Child Pipeline | p_source_name |
|---|---|---|---|---|
| `act_run_payments` | ExecutePipeline | — (runs first) | `pl_bronze_incremental_load_v2` | `"payments"` |
| `act_run_fleet` | ExecutePipeline | `act_run_payments` Succeeded | `pl_bronze_incremental_load_v2` | `"fleet"` |
| `act_run_weather` | ExecutePipeline | `act_run_fleet` Succeeded | `pl_bronze_incremental_load_v2` | `"weather"` |

---

## Why Sequential, Not Parallel?

The three sources could run in parallel (ForEach or 3 independent Execute Pipeline activities with no dependsOn). Sequential is chosen here for:

- **Simplicity** — easy to see which source is running in the ADF monitoring view
- **Controlled resource usage** — Databricks cluster shared across all three sources; parallel runs would spin up multiple job clusters or queue
- **Predictable watermark updates** — if one source fails, it's clear exactly which one and from where to re-run
- **Beginner-friendly** — one thing at a time is easier to debug

For production with large data volumes, consider running sources in parallel using three independent trigger-scheduled pipelines or a ForEach over a source list.
