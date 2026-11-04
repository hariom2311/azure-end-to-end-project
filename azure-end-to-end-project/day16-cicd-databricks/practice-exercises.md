# Day 16 — Practice Exercises: Databricks CI/CD with GitHub Actions

> Complete these exercises in order. Each builds on the bundle and test suite built in the notes.
> All exercises use the `day16-cicd-databricks/` directory.

---

## Exercise 1 — Add a New Silver Entity to the Bundle

**Goal:** Wire `nb_silver_maintenance_events` into `job_ev_batch_pipeline` so it runs as part of the daily batch.

Steps:
1. Open [databricks/resources/jobs.yml](databricks/resources/jobs.yml)
2. Under `job_ev_batch_pipeline`, add a new task after `list_silver_notebooks`:

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
4. Run locally: `databricks bundle validate --target staging` — fix any YAML errors before pushing
5. Write at least 2 DQ rule tests in `tests/test_silver_maintenance_events.py` (use the same pattern as `test_silver_vehicle_battery.py`)

**Verify:** `databricks bundle validate --target staging` exits 0. Both new tests pass with `pytest tests/test_silver_maintenance_events.py -v`.

---

## Exercise 2 — Deploy to Two Separate Databricks Workspaces

**Goal:** Point staging and production at different Databricks workspace URLs instead of using the same workspace with different paths.

Steps:
1. In `databricks/databricks.yml`, add explicit `host` to each target:

```yaml
staging:
  workspace:
    host: https://adb-STAGING-WORKSPACE.azuredatabricks.net
    root_path: /Shared/ev-intelligence/staging

production:
  workspace:
    host: https://adb-PRODUCTION-WORKSPACE.azuredatabricks.net
    root_path: /Shared/ev-intelligence/production
```

2. In GitHub → Settings → Secrets, add two pairs:
   - `DATABRICKS_HOST_STAGING` + `DATABRICKS_TOKEN_STAGING`
   - `DATABRICKS_HOST_PRODUCTION` + `DATABRICKS_TOKEN_PRODUCTION`

3. In `.github/workflows/cd.yml`, update each job to use the correct pair:

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

4. Push and watch the CD pipeline deploy to two separate workspaces

**Verify:** Databricks UI on both workspaces shows the jobs created under `/Shared/ev-intelligence/staging` and `/Shared/ev-intelligence/production` respectively.

---

## Exercise 3 — Add Slack Notification on Production Deploy Failure

**Goal:** When the production deploy job fails, send a Slack message with the commit SHA, actor name, and a link to the failed run.

Steps:
1. Create an Incoming Webhook in Slack: Slack → Your workspace → Manage apps → Incoming Webhooks → Add
2. Copy the webhook URL → GitHub → Settings → Secrets → add `SLACK_WEBHOOK_URL`
3. In `.github/workflows/cd.yml`, replace the `Notify on failure` step in `deploy-production`:

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

4. Deliberately break the bundle (e.g. delete a required field in `jobs.yml`), push, and trigger a failure to test the notification

**Verify:** Slack receives the failure message with the correct commit SHA and run URL.

---

## Exercise 4 — Write an End-to-End Dedup + DQ Test

**Goal:** Add a test that exercises both DQ filtering and deduplication together in a single batch — closer to how a real batch of events arrives.

In `tests/test_silver_vehicle_battery.py`, add this test at the end of the file:

```python
def test_dedup_preserves_only_clean_latest_row(spark):
    ts_old = datetime(2026, 11, 15, 10, 0, 0, tzinfo=timezone.utc)
    ts_new = datetime(2026, 11, 15, 10, 0, 5, tzinfo=timezone.utc)
    rows = [
        _good_row(event_id="e1", battery_pct=40.0, _ingestion_ts=ts_old),  # clean, older
        _good_row(event_id="e1", battery_pct=55.0, _ingestion_ts=ts_new),  # clean, newer — keep
        _good_row(event_id="e2", battery_pct=-5.0, _ingestion_ts=ts_new),  # bad — quarantined
    ]
    df = apply_dq_rules(spark.createDataFrame(rows))
    clean = df.filter(F.col("_dq_passed"))
    result = dedup_by_event_id(clean)
    assert result.count() == 1
    assert result.collect()[0]["battery_pct"] == 55.0
```

Run: `pytest tests/test_silver_vehicle_battery.py -v -k "test_dedup_preserves"`

**Verify:** Test passes. Coverage report shows the `dedup_by_event_id` path is exercised by the new test.

---

## Exercise 5 — Schedule the Streaming Job and Unpause It

**Goal:** Add an hourly schedule to `job_ev_streaming_pipeline` in the bundle, deploy it, and unpause it from the Databricks UI.

Steps:
1. In `databricks/resources/jobs.yml`, add a `schedule` block under `job_ev_streaming_pipeline`:

```yaml
schedule:
  quartz_cron_expression: "0 0 * * * ?"   # top of every hour
  timezone_id: UTC
  pause_status: PAUSED   # always start paused — unpause manually after validation
```

2. Run `databricks bundle validate --target staging`
3. Run `databricks bundle deploy --target staging`
4. Go to Databricks UI → Workflows → `[dev hariom] EV Streaming Pipeline`
5. Click the schedule toggle to unpause it
6. Wait for the next top-of-hour trigger → verify the job starts automatically

**Verify:** The job shows "Scheduled — Every hour" in the UI. The first automated run completes with all 3 tasks green.
