# Day 16 — Interview Solutions: Databricks CI/CD with GitHub Actions

---

## Databricks Asset Bundles

**A1**
A Databricks Asset Bundle (DAB) is Databricks' native Infrastructure-as-Code tool. You define Databricks resources — jobs, clusters, pipelines, permissions — as YAML files in your Git repo, then deploy them to the workspace using the Databricks CLI.

| | DAB | UI / REST API |
|---|---|---|
| Style | Declarative — define what you want | Imperative — write scripts that call endpoints |
| Version control | YAML lives in Git — every change is a PR | No built-in history; UI changes are invisible to Git |
| Environment promotion | Built-in targets (`staging`, `production`) | Manual — re-run scripts with different params |
| Validation | `bundle validate` before any deploy | No dry-run; errors only surface after deploy |
| Rollback | `git revert` + `bundle deploy` | Re-run old REST API payload manually |

DAB is the recommended approach for production Databricks deployments.

---

**A2**
`mode: development` automatically prefixes all resource names with `[dev <your-username>]`.

Example: if Alice deploys `--target staging`, her job is named `[dev alice] EV Streaming Pipeline`. If Bob deploys staging, his is `[dev bob] EV Streaming Pipeline`. They coexist in the same workspace without overwriting each other.

Without this, two engineers deploying staging would both try to create a job called `EV Streaming Pipeline` — the second deploy overwrites the first.

Production uses `mode: production` — no prefix. There is exactly one job named `EV Streaming Pipeline` in production, and it is the canonical live version. The mode distinction is what makes environment isolation safe in a single workspace without needing to provision two separate workspaces.

---

**A3**
Three changes are needed:

1. **Branch protection on `main`** — enable branch protection rules: require PRs, require CI to pass, disallow direct pushes. The developer's local `bundle deploy --target production` bypasses GitHub Actions but cannot be blocked at the CLI level — the process gate must prevent bad code from reaching `main` in the first place.

2. **Environment-scoped tokens** — move `DATABRICKS_TOKEN` to a GitHub Environment secret scoped to the `production` environment. Give the staging token only `Can Run` permission and the production token `Can Manage`. The developer's local machine uses their personal PAT — give it only `Can Run` on production, preventing `bundle deploy` from modifying production jobs.

3. **Production workspace isolation** — the most robust fix: point the production target at a completely separate Databricks workspace that the developer's personal token has no access to at all. Only the GitHub Actions service principal token (stored as a GitHub secret, never shared with developers) can deploy there.

---

**A4**
`databricks bundle validate`:
- Resolves all YAML variables and bundle references
- Checks YAML structure and field names against the DAB schema
- Verifies notebook paths exist in the workspace (when authenticated)
- Prints a JSON summary of what would be deployed
- Makes NO changes to the workspace
- Used in CI on every PR — fast, cheap, safe

`databricks bundle deploy`:
- Creates or updates actual Databricks resources (jobs, clusters, permissions)
- Idempotent — running it twice with the same config has no effect
- Requires authentication with sufficient permissions (`Can Manage`)
- Used in CD after CI passes and (for production) after manual approval

---

## GitHub Actions and Deployment Flow

**A5**
Step by step:

1. The CD job runs: `databricks bundle run job_ev_streaming_pipeline --target staging`
2. The Databricks CLI submits the job run to the staging workspace and then **blocks** — it does not return until the Databricks job finishes (there is no `--no-wait` flag)
3. When the Databricks job completes, the CLI exits with code `0` if the job status is `SUCCEEDED`, or a non-zero code if `FAILED` or `CANCELED`
4. The shell captures the exit code: `JOB_EXIT=${PIPESTATUS[0]}`
5. The step checks: `if [ $JOB_EXIT -ne 0 ]; then exit 1; fi`
6. A non-zero exit fails the GitHub Actions step → the `run-integration-tests` job fails → the `deploy-production` job (which `needs: run-integration-tests`) never starts
7. The production approval gate is never reached — a broken version cannot get to production

---

**A6**
The `concurrency` setting:
```yaml
concurrency:
  group: cd-${{ github.ref }}
  cancel-in-progress: false
```

When the second CD pipeline starts, GitHub sees that a run in group `cd-refs/heads/main` is already active. Because `cancel-in-progress: false`, the second run is **queued** — it waits for the first to complete rather than cancelling it.

The first run continues waiting for the production approval. Once approved and complete, the second run starts from the beginning.

**If `cancel-in-progress: true`:** The second run would **cancel the first run immediately**. The first run is mid-way through the production approval wait — the approval is cancelled, the deploy never happens. The engineer who approved sees their approval silently disappear. The second run starts fresh and goes through staging again. This is dangerous: changes that the first run intended to deploy may be lost, and approvers lose trust in the system.

---

**A7**
Partial answer — `bundle validate` does have two modes depending on authentication:

**Without authentication (no `DATABRICKS_HOST`/`DATABRICKS_TOKEN`):**
- The CLI validates YAML structure, schema, and variable resolution locally
- It does NOT verify that notebook paths exist in the workspace
- It cannot confirm cluster policies or permissions are valid
- This is a syntax check only — it is fast but incomplete

**With authentication:**
- The CLI also verifies notebook paths exist at the specified workspace paths
- Validates that referenced cluster policies exist
- Checks that the service principal has sufficient permissions

For CI to be meaningful, `bundle validate` should run with credentials — otherwise a job referencing a notebook that does not exist in the workspace passes validation but fails at deploy time. The secrets are required to make validation actually useful.

---

## Unit Testing

**A8**
Unit tests run locally with PySpark because:

| Reason | Detail |
|---|---|
| Speed | Local PySpark starts in ~10 seconds; a Databricks cluster cold-starts in 3–5 minutes. CI runs many times per day — a 5-minute delay per run compounds quickly |
| Cost | Databricks compute is billed per DBU. Running 36 unit test runs per day on a cluster costs real money; local PySpark costs nothing |
| Isolation | Unit tests validate pure transform logic (DQ rules, aggregations, dedup). They should not need ADLS connectivity, Unity Catalog permissions, or Event Hub access |
| Reliability | Local tests have no network dependency — they pass even when Azure is having an incident |

Trade-off: local PySpark cannot test `dbutils`, actual Delta MERGE against a real table, Unity Catalog permissions, or ADLS OAuth. Those are covered by integration tests — the `bundle run job_ev_streaming_pipeline --target staging` step in CD runs on a real cluster and catches what unit tests cannot.

---

**A9**
Three reasons the unit tests could pass while integration tests fail:

1. **`dbutils` calls fail** — the notebook uses `dbutils.secrets.get(...)` to fetch Key Vault credentials. Unit tests mock away or skip `dbutils` (it does not exist in local PySpark). On the cluster, if the Key Vault scope or secret name is wrong, the notebook fails at startup before any DQ logic runs.

2. **ADLS path does not exist** — unit tests use in-memory DataFrames; the notebook reads from `abfss://bronze@...` and writes to `abfss://silver@...`. If the ADLS container, path, or service principal OAuth config is wrong, the cluster job fails on the first read even though all the DQ functions are correct.

3. **Schema mismatch** — the unit test creates rows with a fixed schema using `Row(...)`. The actual Bronze Delta table may have additional columns (e.g. `_rescue_data`, `_metadata`) added by the streaming reader that the notebook's DQ logic does not handle, causing a column-not-found error on the cluster.

---

## Streaming Patterns

**A10**
`foreachBatch` is used because Spark Structured Streaming's native output modes (`append`, `complete`, `update`) do not support Delta `MERGE` (upsert).

The Silver notebook needs MERGE to be idempotent — if the same `event_id` arrives twice (because the Event Hub consumer replays a partition), the second occurrence updates the existing row rather than inserting a duplicate. This is not possible with `writeStream.format("delta").mode("append")`.

`foreachBatch` receives each micro-batch as a **static DataFrame**, which allows:
- Row-level DQ filtering with complex logic and reason strings
- Splitting one batch into two outputs (clean → Silver, bad → quarantine)
- Running `DeltaTable.merge()` on the clean rows
- Per-batch logging and metrics

The trade-off is complexity: you give up some streaming guarantees and must make the function idempotent yourself.

---

**A11**
Not quite correct — `foreachBatch` with MERGE gives **at-least-once** processing, not exactly-once.

Here is why:

- Structured Streaming guarantees that every input record will be processed **at least once** when using `foreachBatch` with a checkpoint
- If the cluster crashes after a batch is processed but before the checkpoint is written, the same batch will be re-processed on restart
- The MERGE on `event_id` makes re-processing **safe** — replaying the same batch updates existing rows rather than inserting duplicates

So the combination of `foreachBatch` + checkpoint + MERGE on `event_id` gives:
- **At-least-once delivery** (guaranteed by Structured Streaming + checkpoint)
- **Idempotent processing** (guaranteed by MERGE on the natural key)
- The effective observable outcome is **exactly-once** — no duplicates in the Silver table — but this is a result of idempotency, not a Spark guarantee

For true exactly-once, you would need a transactional sink (Delta Lake with `txnAppId` + `txnVersion` options, or a database with ACID transactions).

---

## Rollback and Operations

**A12**
Rollback has two parts: the job definition and the data.

**Part 1 — Roll back the Databricks job definition:**

1. Every successful production deploy is git-tagged: `release-YYYYMMDD-HHmmss-<sha8>`
2. Find the last good tag:
```bash
git log --tags --simplify-by-decoration --oneline
```
3. Check out the tag:
```bash
git checkout release-20261102-120000-abc12345
```
4. Redeploy the bundle:
```bash
cd azure-end-to-end-project/day16-cicd-databricks/databricks
export DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
export DATABRICKS_TOKEN=<prod-token>
databricks bundle deploy --target production
```
5. The production job is now running the previous (correct) notebook version

**Part 2 — Roll back the corrupted Gold Delta table data:**

Delta Lake maintains a transaction log with a snapshot for every commit. Use `RESTORE TABLE` to revert to a point before the bad job ran:

```sql
-- Find the snapshot before the bad deploy
DESCRIBE HISTORY delta.`abfss://gold@evdatalakedev2.dfs.core.windows.net/mart_charging_progress_live/`

-- Restore to the last good version (e.g. version 47)
RESTORE TABLE delta.`abfss://gold@evdatalakedev2.dfs.core.windows.net/mart_charging_progress_live/`
TO VERSION AS OF 47

-- Or restore to a timestamp
RESTORE TABLE delta.`abfss://gold@evdatalakedev2.dfs.core.windows.net/mart_charging_progress_live/`
TO TIMESTAMP AS OF '2026-11-02 23:00:00'
```

After restoring the table, trigger the corrected batch job to reprocess the affected windows and fill in any legitimate data that arrived between the last good snapshot and now.

**Important:** `RESTORE TABLE` is a metadata operation — it does not delete the data files, just points the table to an older snapshot. The bad data files remain in storage until the next `VACUUM` run.
