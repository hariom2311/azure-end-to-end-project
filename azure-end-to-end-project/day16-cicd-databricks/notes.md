# Day 16 — CI/CD for Databricks using GitHub Actions & Databricks Asset Bundles

## What We Built

```
day16-cicd-databricks/
├── .github/workflows/
│   ├── ci.yml          ← lint + unit tests + bundle validate (staging + production)
│   └── cd.yml          ← deploy staging → integration test → deploy production
├── databricks/
│   ├── databricks.yml  ← Databricks Asset Bundle root config
│   └── resources/
│       └── jobs.yml    ← streaming pipeline + batch pipeline job definitions
└── tests/
    ├── conftest.py                     ← local PySpark session fixture
    ├── test_silver_vehicle_battery.py  ← 20 DQ + transform unit tests
    └── test_gold_charging_progress.py  ← 16 Gold aggregation unit tests
```

---

## Core Concepts

### What is a Databricks Asset Bundle (DAB)?

A DAB is Databricks' native Infrastructure-as-Code tool. You define Databricks
resources (jobs, clusters, pipelines, permissions) as YAML files in your repo,
then deploy them via the Databricks CLI.

```
Git repo (YAML definitions)
        │
        │  databricks bundle deploy --target staging
        ▼
Databricks workspace (actual jobs created/updated)
```

Key advantages over managing jobs via the UI or REST API:
- Resources version-controlled in Git — every change is reviewed via PR
- Environment promotion built in — same YAML, different targets
- `bundle validate` catches errors before any changes reach the workspace
- Rollback = `git revert` + redeploy

### DAB Targets: staging vs production

| Setting | staging | production |
|---|---|---|
| `mode` | `development` | `production` |
| Resource name prefix | `[dev username]` auto-added | No prefix — deployed exactly as named |
| Purpose | Safe sandbox; won't clash with prod names | Live environment |
| Deploy trigger | Push to `main` or `aug-batch` | Push to `main` only + manual approval |

`mode: development` is critical — it prefixes all job and cluster names so two
engineers deploying to staging at the same time never overwrite each other's resources.

### GitHub Actions Environments

A GitHub Environment (`staging`, `production`) gives you:
- Manual approval gates before a job runs (essential for production)
- Environment-scoped secrets (different tokens per environment)
- Deployment history in the GitHub UI

```
GitHub repo → Settings → Environments → production → Required reviewers → add yourself
```

Without the approval gate, CD deploys straight to production on every merge to `main`.

---

## Step-by-Step Setup

### Step 1 — Generate a Databricks Token

**Option A: Personal Access Token (PAT) — for getting started quickly**
1. Databricks workspace → top right avatar → Settings → Developer → Access Tokens
2. Generate New Token — label: `github-cicd`, Lifetime: 90 days
3. Copy it immediately — it is shown only once

**Option B: Service Principal — recommended for production**
1. Azure Portal → Azure Active Directory → App Registrations → New Registration
2. Name: `sp-ev-github-cicd`, copy the Application (client) ID
3. Databricks workspace → Settings → Identity and Access → Service Principals → Add
4. Grant the SP `Can Manage` on the workspace
5. Generate a client secret (Certificates and secrets) — use it as `DATABRICKS_TOKEN`

---

### Step 2 — Add GitHub Secrets

`GitHub repo → Settings → Secrets and variables → Actions → New repository secret`

| Secret | Value |
|---|---|
| `DATABRICKS_HOST` | `https://adb-XXXXXXXX.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | PAT or service principal secret from Step 1 |

---

### Step 3 — Create GitHub Environments

`GitHub repo → Settings → Environments`

**Create `staging`:**
- No required reviewers — deploys automatically after CI passes

**Create `production`:**
- Required reviewers → add yourself (or team lead)
- This is the manual approval gate — production deploy pauses until someone clicks Approve

---

### Step 4 — Install Databricks CLI Locally

```bash
# macOS / Linux
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Windows (PowerShell)
winget install Databricks.DatabricksCLI

# Verify
databricks --version   # should print 0.221.x or later
```

Configure:
```bash
databricks configure
# Host  : https://adb-XXXXXXXX.azuredatabricks.net
# Token : <your PAT>
```

---

### Step 5 — Validate the Bundle Locally

```bash
cd azure-end-to-end-project/day16-cicd-databricks/databricks

# Validate both targets before pushing
databricks bundle validate --target staging
databricks bundle validate --target production
```

A passing validate prints a JSON summary of all resources that will be deployed.
Fix any YAML errors here before pushing — it is much faster than waiting for CI.

---

### Step 6 — Deploy to Staging

```bash
databricks bundle deploy --target staging
```

This creates in your workspace:
- `[dev hariom] EV Streaming Pipeline` — job with 3 tasks (Bronze → Silver → Gold)
- `[dev hariom] EV Batch Pipeline — E2E` — daily batch job with full DAG

Verify in Databricks UI → Workflows → Jobs.

---

### Step 7 — Run the Integration Test Job

```bash
databricks bundle run job_ev_streaming_pipeline --target staging
```

The CLI blocks until the Databricks job finishes and exits 0 on success, non-zero on
failure. This is exactly what the `run-integration-tests` job in `cd.yml` does.

---

### Step 8 — Deploy to Production via GitHub Actions

1. Merge a PR to `main`
2. CI pipeline triggers: lint → unit tests → bundle validate
3. If CI passes, CD pipeline starts automatically:
   - **Job 1:** Deploy bundle to staging
   - **Job 2:** Trigger `job_ev_streaming_pipeline` on staging, wait for result
   - **Job 3:** Pause — GitHub sends you an approval notification
4. Go to GitHub → Actions → the running CD workflow → Review deployments → Approve
5. Production deploys and auto-tags: `release-20261103-143022-a1b2c3d4`

---

### Step 9 — Run Unit Tests Locally

```bash
cd azure-end-to-end-project/day16-cicd-databricks

pip install pyspark==3.5.0 delta-spark==3.0.0 pytest pytest-cov pytest-html pytest-xdist

pytest tests/ -v \
  --cov=. \
  --cov-report=term-missing \
  --cov-fail-under=80
```

All tests run locally using PySpark with Delta support — no Databricks cluster needed.

---

## What Each File Does

### `databricks/databricks.yml`

Bundle root. Defines:
- Bundle name
- Variables (`adls_account`, `ev_catalog`) — overrideable per target
- `staging` target: `mode: development`, auto-prefixes resource names
- `production` target: `mode: production`, no prefix
- `include: resources/*.yml` — loads all job definitions

### `databricks/resources/jobs.yml`

Defines two Databricks jobs:

**`job_ev_streaming_pipeline`** — used by CD integration test
```
bronze_stream_vehicle_battery  → reads EventHub → Bronze Delta
        ↓
silver_stream_vehicle_battery  → 9 DQ rules + MERGE into Silver
        ↓
gold_stream_charging_progress  → 5-min tumbling window Gold mart
```

**`job_ev_batch_pipeline`** — daily batch, control-table driven
```
list_bronze_sources
        ↓
list_silver_notebooks
        ↓
silver_payments  silver_sessions  silver_customers  ...  (parallel)
        ↓
list_gold_notebooks
        ↓
gold_dim_station  gold_dim_customer  ...  (parallel dims)
        ↓
gold_fact_charging_session  gold_fact_payments  (parallel facts)
        ↓
gold_mart_revenue_by_geo_month
```

Scheduled at 2 AM UTC daily — paused by default, unpause after first deploy validation.

### `tests/conftest.py`

Creates one PySpark session shared across all tests (`scope="session"`). Much faster
than creating a new session per test. Includes Delta Lake catalog extension.

### `tests/test_silver_vehicle_battery.py` — 20 tests

Tests the DQ logic from `nb_silver_stream_vehicle_battery`:
- Rules R01–R09 each tested individually with a dedicated test
- Boundary values: `battery_pct=0.0` passes, `-0.01` fails
- Parametrized tests cover all out-of-range values
- Clean/quarantine split counts verified
- Dedup: duplicate `event_id` keeps the row with the latest `_ingestion_ts`
- Schema: all 13 required input columns present, all 11 DQ columns added

### `tests/test_gold_charging_progress.py` — 16 tests

Tests the Gold mart aggregation from `nb_gold_stream_charging_progress`:
- `battery_pct_start`, `battery_pct_end`, `battery_pct_avg` correct
- `overtemp_flag` is `False` when all temps ≤ 45.0°C, `True` when any > 45.0°C
- Exactly 45.0°C does NOT trigger the flag (condition is strictly `> 45.0`)
- `eta_minutes_to_full` is the minimum across all events in the window
- Window grouping: events within 5 minutes are in one row; spanning two windows = two rows
- Different `vehicle_id` or `station_id` always produce separate rows
- `overtemp_flag` column dtype is `boolean`
- `window_start` is always before `window_end`

---

## How the Full CI/CD Flow Works

```
Developer pushes code to main
            │
            ▼
┌───────────────────────────────────────────┐
│              CI Pipeline                  │
│                                           │
│  Job 1: Lint (flake8 + ruff)             │
│          │                                │
│          ├── Job 2: Unit Tests            │  ← parallel after lint
│          │         pytest ≥80% coverage   │
│          │                                │
│          └── Job 3: Bundle Validate       │  ← parallel after lint
│                    staging + production   │
└───────────────────────┬───────────────────┘
                        │ all pass
                        ▼
┌───────────────────────────────────────────┐
│              CD Pipeline                  │
│                                           │
│  Job 1: Deploy Staging                    │
│         bundle deploy --target staging    │
│                │                          │
│  Job 2: Integration Tests                 │
│         bundle run job_ev_streaming_pipeline│
│         (blocks until job completes)      │
│                │ SUCCEEDED               │
│  Job 3: Deploy Production                 │
│         ⏸ Manual approval gate           │
│         ✅ Approved                       │
│         bundle deploy --target production │
│         git tag release-YYYYMMDD-...      │
└───────────────────────────────────────────┘
```

---

## Practice Exercises

### Exercise 1 — Add a New Silver Entity to the Bundle

**Goal:** Wire `nb_silver_maintenance_events` into `job_ev_batch_pipeline`.

Steps:
1. Open [databricks/resources/jobs.yml](databricks/resources/jobs.yml)
2. Under `job_ev_batch_pipeline`, add after `list_silver_notebooks`:
```yaml
- task_key: silver_maintenance_events
  depends_on:
    - task_key: list_silver_notebooks
  job_cluster_key: batch_cluster
  notebook_task:
    notebook_path: ../../day10-silver-control-table-and-dynamic-pipeline/notebooks/nb_silver_maintenance_events
    base_parameters:
      env: ${bundle.target}
  timeout_seconds: 1800
  max_retries: 2
```
3. Add `silver_maintenance_events` to the `depends_on` list of `list_gold_notebooks`
4. Run `databricks bundle validate --target staging` — fix any YAML errors
5. Write at least 2 DQ rule tests in `tests/test_silver_maintenance_events.py`

---

### Exercise 2 — Use Two Separate Databricks Workspaces

**Goal:** Deploy staging and production to different Databricks workspaces.

Steps:
1. In `databricks.yml`, add explicit `host` to each target:
```yaml
staging:
  workspace:
    host: https://adb-STAGING.azuredatabricks.net
production:
  workspace:
    host: https://adb-PRODUCTION.azuredatabricks.net
```
2. Add two pairs of GitHub secrets:
   - `DATABRICKS_HOST_STAGING` + `DATABRICKS_TOKEN_STAGING`
   - `DATABRICKS_HOST_PRODUCTION` + `DATABRICKS_TOKEN_PRODUCTION`
3. Update each job in `cd.yml` to set the correct env vars:
```yaml
deploy-staging:
  env:
    DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST_STAGING }}
    DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_STAGING }}

deploy-production:
  env:
    DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST_PRODUCTION }}
    DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_PRODUCTION }}
```

---

### Exercise 3 — Add Slack Notification on Production Failure

**Goal:** Send a Slack alert when production deploy fails.

Replace the `Notify on failure` step in `cd.yml`:
```yaml
- name: Notify production failure
  if: failure()
  uses: slackapi/slack-github-action@v1
  with:
    payload: |
      {
        "text": "PRODUCTION DEPLOY FAILED\nCommit: ${{ github.sha }}\nActor: ${{ github.actor }}\nLogs: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}"
      }
  env:
    SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
```

Add `SLACK_WEBHOOK_URL` as a GitHub secret (get it from Slack API → Incoming Webhooks).

---

### Exercise 4 — Test the Deduplication Logic End to End

**Goal:** Write an additional test that verifies dedup + DQ together in one batch.

In `tests/test_silver_vehicle_battery.py`, add:
```python
def test_dedup_preserves_only_clean_latest(spark):
    ts_old = datetime(2026, 11, 15, 10, 0, 0, tzinfo=timezone.utc)
    ts_new = datetime(2026, 11, 15, 10, 0, 5, tzinfo=timezone.utc)
    rows = [
        _good_row(event_id="e1", battery_pct=40.0, _ingestion_ts=ts_old),  # clean, older
        _good_row(event_id="e1", battery_pct=55.0, _ingestion_ts=ts_new),  # clean, newer → keep
        _good_row(event_id="e2", battery_pct=-5.0, _ingestion_ts=ts_new),  # bad — quarantine
    ]
    df = apply_dq_rules(spark.createDataFrame(rows))
    clean = df.filter(F.col("_dq_passed"))
    result = dedup_by_event_id(clean)
    assert result.count() == 1
    assert result.collect()[0]["battery_pct"] == 55.0
```

---

### Exercise 5 — Add a Daily Schedule to the Streaming Job

**Goal:** Schedule `job_ev_streaming_pipeline` to run every hour.

In `jobs.yml`, add a `schedule` block under `job_ev_streaming_pipeline`:
```yaml
schedule:
  quartz_cron_expression: "0 0 * * * ?"   # every hour at :00
  timezone_id: UTC
  pause_status: PAUSED   # unpause manually after first validation
```

Validate and redeploy to staging. Check Databricks UI → the job should show
a "Scheduled" badge with the trigger.

---

## Interview Questions — Databricks CI/CD

**Q1. What is a Databricks Asset Bundle and how does it differ from the REST API approach?**

A DAB is declarative — you define what you want in YAML and the CLI creates/updates
it. The REST API is imperative — you write scripts that call API endpoints.

DAB advantages:
- Resources version-controlled as code, reviewed in PRs
- `mode: development` auto-prefixes names — no staging/prod conflicts
- `bundle validate` catches errors before any changes reach the workspace
- Built-in environment promotion via targets
- Rollback = git revert + redeploy

---

**Q2. What does `mode: development` do in a DAB target and why does it matter?**

It prefixes all resource names with `[dev <your-username>]`. If Alice deploys
staging and Bob deploys staging, their jobs are `[dev alice] EV Streaming Pipeline`
and `[dev bob] EV Streaming Pipeline` — they never overwrite each other.

Production uses `mode: production` (no prefix) — there is exactly one production
job with the canonical name. This mode distinction is what makes environment
isolation safe without needing separate workspaces.

---

**Q3. How do you prevent anyone from accidentally deploying to production?**

Three layers:
1. **Branch protection on `main`** — requires a reviewed and approved PR; no direct push
2. **GitHub Environment approval** — the `production` environment requires a named
   reviewer to click Approve in the GitHub UI before the deploy job runs
3. **CI must pass first** — the CD pipeline's `deploy-production` job has
   `needs: run-integration-tests`, which has `needs: deploy-staging`; all three
   must pass in order

Running `databricks bundle deploy --target production` locally still works if you have
the token — the CI gate is enforced by process, not by the CLI itself.

---

**Q4. What is the difference between `bundle validate` and `bundle deploy`?**

`validate` — dry run. Resolves variables, checks YAML structure, verifies notebook
paths exist in the workspace (when authenticated). No changes to the workspace.
Fast — used in CI on every PR.

`deploy` — creates or updates real resources in Databricks. Idempotent: running it
twice with the same config has no effect. Slow — used in CD after CI passes.

---

**Q5. How does the CD pipeline know the Databricks job succeeded?**

`databricks bundle run job_ev_streaming_pipeline --target staging` (no `--no-wait`)
blocks the GitHub Actions runner until the Databricks job finishes. The CLI exits
with code 0 on `SUCCEEDED` and non-zero on `FAILED` or `CANCELED`.

The pipeline step captures the exit code via `${PIPESTATUS[0]}` and fails the step
if it is non-zero. This stops the CD pipeline before the production approval gate,
preventing a known-broken version from ever reaching production.

---

**Q6. Why run unit tests with local PySpark instead of on a Databricks cluster?**

- **Speed** — local tests run in seconds; a cluster cold-starts in 3–5 minutes
- **Cost** — cluster compute is billed per DBU; CI runs many times per day
- **Isolation** — unit tests validate pure transform logic (DQ rules, aggregations),
  not ADLS connectivity or Unity Catalog permissions
- **Reliability** — no network dependency; tests run offline in any environment
  with Java 11 + Python 3.10

Integration tests (the actual Databricks job run in staging) cover what unit tests
cannot: real Delta MERGE, ADLS auth, Event Hub connectivity.

---

**Q7. What is `foreachBatch` and why is it used instead of plain `writeStream`?**

Spark Structured Streaming's native output modes (`append`, `complete`, `update`)
do not support Delta MERGE (upsert). `foreachBatch` receives each micro-batch as a
static DataFrame, which allows:
- Complex DQ with row-level filtering and reason strings
- Splitting clean vs quarantine rows per batch
- Running `DeltaTable.merge()` for idempotent upserts on `event_id`
- Per-batch logging and metrics

The trade-off: `foreachBatch` weakens exactly-once guarantees — you must make the
function idempotent yourself. The MERGE on `event_id` achieves this: replaying
the same batch updates existing rows rather than inserting duplicates.

---

**Q8. What are the concurrency settings in `cd.yml` and why do they matter?**

```yaml
concurrency:
  group: cd-${{ github.ref }}
  cancel-in-progress: false
```

Only one CD run per branch can be active at a time. `cancel-in-progress: false`
means a second merge that triggers CD while a production approval is pending will
queue, not cancel the in-flight run. This prevents two deploys racing to production.

If `cancel-in-progress: true`, a fast second commit could cancel a CD run that is
waiting for production approval — the first deploy would never complete.

---

**Q9. How would you roll back a bad production Databricks deploy?**

1. Every successful production deploy is git-tagged: `release-YYYYMMDD-HHmmss-<sha>`
2. Find the last good tag: `git log --tags --simplify-by-decoration --oneline`
3. Check out the tag and redeploy:
```bash
git checkout release-20261102-120000-abc12345
cd azure-end-to-end-project/day16-cicd-databricks/databricks
databricks bundle deploy --target production
```
4. This redeploys the previous job definitions — Databricks updates the jobs in place
5. If the bad deploy corrupted data, also restore Delta tables using `RESTORE TABLE`
   to a pre-deploy snapshot

---

**Q10. How does the DAB handle notebook paths across different environments?**

Notebook paths in `jobs.yml` use relative paths from the bundle root
(e.g. `../../day13.../notebooks/nb_bronze_stream_vehicle_battery`). The Databricks
CLI resolves these relative to the `workspace.root_path` defined per target:

- Staging: `/Shared/ev-intelligence/staging/...`
- Production: `/Shared/ev-intelligence/production/...`

If you use `mode: development`, the CLI also injects the user prefix into the path,
keeping staging notebooks isolated from production notebooks in the same workspace.

---

**Q11. What secrets are required and how should they be scoped?**

| Secret | Recommended scope | Used by |
|---|---|---|
| `DATABRICKS_HOST` | Repository | CI (validate) + CD (deploy) |
| `DATABRICKS_TOKEN` | Environment-level | Staging token for staging env; prod token for prod env |
| `GITHUB_TOKEN` | Auto-provided | CD git tag push |
| `SLACK_WEBHOOK_URL` | Repository | Failure notifications |

Best practice: move `DATABRICKS_TOKEN` to environment-level secrets. Give the
staging token only `Can Run` permission; give the production token `Can Manage`.
This way a leaked staging token cannot deploy to production.

---

**Q12. How does Databricks CI/CD differ from ADF CI/CD (Day 14)?**

| Aspect | Databricks (Day 16) | ADF (Day 14) |
|---|---|---|
| Config format | YAML (hand-written, version-controlled) | ARM JSON (auto-generated by ADF Studio) |
| Deploy tool | Databricks CLI (`bundle deploy`) | `az deployment group create` or Azure DevOps ARM task |
| Source of truth | Git repo — developers edit YAML directly | ADF Studio — developers click Publish |
| Environment isolation | `mode: development/production` in DAB | Separate ARM parameter files per environment |
| Rollback | `git revert` + `bundle deploy` | Redeploy previous ARM template version |
| Pre-deploy step | None needed | Must stop ADF triggers before ARM deploy |
| Integration testing | Run Databricks job via `bundle run` | Trigger ADF pipeline via PowerShell or REST API |
| Approval gate | GitHub Environment required reviewers | Azure DevOps Environment approval + branch control |

---

## Key Commands Reference

```bash
# Install Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Authenticate locally
databricks configure

# Bundle lifecycle
databricks bundle validate  --target staging
databricks bundle validate  --target production
databricks bundle deploy    --target staging
databricks bundle deploy    --target production
databricks bundle run       job_ev_streaming_pipeline --target staging
databricks bundle run       job_ev_batch_pipeline     --target staging
databricks bundle destroy   --target staging           # remove staging resources

# List jobs in workspace
databricks jobs list

# Run unit tests locally
cd azure-end-to-end-project/day16-cicd-databricks
pip install pyspark==3.5.0 delta-spark==3.0.0 pytest pytest-cov pytest-html pytest-xdist
pytest tests/ -v --cov=. --cov-report=term-missing --cov-fail-under=80
```
