# Topic 11 — CI/CD, Monitoring & Observability
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 2 Questions | 2–8 years experience | Scenario-based

---

### Q27. How do you deploy a change to a Silver transformation notebook from development to production without breaking the production pipeline?

**Answer:**

**The CI/CD pipeline in Azure DevOps:**

**Repo structure:**
```
/databricks/
  silver/
    03_silver_all_entities_optimised_v3.ipynb
    tests/
      test_silver_payments.py
      test_silver_customers.py
/adf/
  arm_templates/
    pl_silver_api_transformation.json
/infra/
  bicep/
    storage.bicep
    databricks.bicep
```

**Deployment flow:**

**Step 1 — Feature branch:**
Developer creates branch `feature/silver-gst-fix`. Changes the GST split logic in `v3` notebook.

**Step 2 — PR gate (automated):**
Azure DevOps pipeline triggers on PR:
- `pylint` / `flake8` lint check on the notebook code.
- Unit tests run on a dev Databricks cluster: `pytest databricks/silver/tests/test_silver_payments.py`.
- DQ checks: run the notebook on a sample of Bronze data, assert `quarantine_pct < 1%`.
- Schema validation: assert output Silver schema matches the expected schema contract.

**Step 3 — Dev environment deployment:**
After PR approval and merge to `dev` branch:
- ADF ARM template deployed to `adf-ev-intelligence-dev` (Dev environment).
- Notebook deployed to `dev` Databricks workspace.
- End-to-end integration test: trigger the ADF Silver pipeline on dev data.

**Step 4 — QA / UAT:**
Same ARM template deployed with environment-specific parameters (different storage accounts, Key Vault names).

**Step 5 — Prod release (with approval gate):**
- Production release requires a manual approval from the team lead in Azure DevOps.
- Deployment happens in a maintenance window (2:00–4:00 AM AEST) when batch pipelines are idle.
- Blue-green: the new notebook version is deployed to `v_new` path, ADF is updated to point to it, then `v_old` is kept for 24 hours as rollback option.

**Rollback:** If the production Silver job fails after deployment, ADF pipeline is updated to point back to the previous notebook version in <5 minutes (without redeploying code).

**Environment promotion pipeline:**

```
Dev → QA → UAT → Prod
 ↑         ↑      ↑
Auto    Manual  Manual + Approval Gate
merge   promote promote
```

---

### Q28. How do you know if your data pipeline is healthy? What monitoring do you have in place?

**Answer:**

**Four layers of monitoring in VoltGrid:**

**Layer 1 — Infrastructure (Azure Monitor + Log Analytics):**

| Metric | Alert threshold | Action |
|---|---|---|
| ADF pipeline run status = Failed | Any failure | Email + Teams to data engineering |
| Databricks job exit code ≠ 0 | Any non-zero | Alert with job name + error message |
| Streaming consumer lag > 5 min | 5 minutes | Streaming cluster auto-restart check |
| ADLS spend > monthly budget | 110% of budget | Cost alert to team lead |
| Query > 30 sec duration | 30 seconds | Synapse/Power BI performance alert |

**Layer 2 — Data freshness checks:**

A custom DQ audit table in `gold/data_quality_audit/` tracks the last successful write per entity:
```sql
SELECT entity_name, MAX(run_timestamp) as last_success
FROM gold.data_quality_audit
WHERE status = 'SUCCESS'
GROUP BY entity_name
HAVING DATEDIFF(minute, MAX(run_timestamp), CURRENT_TIMESTAMP) > 120  -- >2 hours stale
```
Alert fires if any entity hasn't been updated in 2 hours during the batch window.

**Layer 3 — Data volume anomaly detection:**

```kql
-- Azure Monitor: row count < 80% of 7-day average
let avg_rows = customEvents
  | where name == "silver_row_count"
  | summarize avg(toreal(customDimensions.row_count)) by entity_name;
customEvents
  | where name == "silver_row_count"
  | join avg_rows on entity_name
  | where toreal(customDimensions.row_count) < 0.8 * avg_row_count
```

If payments suddenly drops from 1,200 rows/day to 50 rows, the API may be returning empty pages (a common paging bug).

**Layer 4 — Business rule checks (domain-specific):**

- Total Gold revenue never decreases day-over-day unless there are refunds.
- Active sessions count at 2:00 PM AEST should be > 100 (if zero, streaming is likely down).
- `reconciliation_status = MISMATCH` rate < 1% across all sessions.

**Dashboards:** A Power BI "Data Quality Dashboard" reads the `gold/data_quality_audit/` table and shows: entity freshness, quarantine rates, DQ check pass/fail history — giving the business visibility into pipeline health without needing to read logs.

**Monitoring coverage map:**

| Layer | Tool | What it catches |
|---|---|---|
| ADF | Azure Monitor | Pipeline failures, Copy Activity errors |
| Databricks | Azure Monitor + Databricks job UI | Job failures, OOM, schema errors |
| Streaming | Databricks metrics | Consumer lag, checkpoint staleness |
| Data quality | Custom DQ audit table | Quarantine spikes, freshness gaps |
| Volume anomaly | Log Analytics KQL | Row count drops (silent API failures) |
| Business rules | Power BI DQ dashboard | Revenue anomalies, utilisation drops |
| Cost | Azure Cost Management | Storage + compute spend |
