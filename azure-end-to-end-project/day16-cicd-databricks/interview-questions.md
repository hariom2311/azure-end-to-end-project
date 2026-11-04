# Day 16 — Interview Questions: Databricks CI/CD with GitHub Actions

> 12 questions covering Databricks Asset Bundles, GitHub Actions environments, unit testing with PySpark, streaming patterns, and rollback strategies.
> Attempt each question before checking `interview-solutions.md`.

---

## Databricks Asset Bundles

**Q1 (Warm-up)**
What is a Databricks Asset Bundle? How does it differ from managing Databricks jobs through the UI or REST API?

---

**Q2 (Conceptual)**
What does `mode: development` do in a DAB target? Why does it matter when multiple engineers are deploying to the same staging workspace?

---

**Q3 (Scenario)**
A developer runs `databricks bundle deploy --target production` from their laptop and accidentally overwrites the production job with a broken version. What process and tooling changes would prevent this from happening?

---

**Q4 (Conceptual)**
What is the difference between `databricks bundle validate` and `databricks bundle deploy`? When is each used in the CI/CD pipeline?

---

## GitHub Actions and Deployment Flow

**Q5 (Conceptual)**
How does the CD pipeline know whether the Databricks integration test job succeeded or failed? Walk through the mechanism step by step.

---

**Q6 (Scenario)**
Two engineers merge PRs to `main` within 30 seconds of each other. Both trigger the CD pipeline. The first is waiting for a production approval. What does the `concurrency` setting in `cd.yml` do in this situation? What would happen if `cancel-in-progress: true` was set instead?

---

**Q7 (Tricky)**
The `bundle-validate` job in `ci.yml` requires `DATABRICKS_HOST` and `DATABRICKS_TOKEN` secrets. A new engineer asks: "Can't we validate the YAML without connecting to Databricks at all?" What is the answer and why?

---

## Unit Testing

**Q8 (Conceptual)**
Why are unit tests run locally with PySpark instead of on a Databricks cluster in CI? What are the trade-offs?

---

**Q9 (Scenario)**
The Silver DQ test suite has 100% coverage of the `apply_dq_rules` function but the staging integration test (running the full notebook on a cluster) still fails. What are three possible reasons the unit tests passed but the integration test failed?

---

## Streaming Patterns

**Q10 (Conceptual)**
What is the `foreachBatch` pattern and why is it used in the Silver streaming notebook instead of plain `writeStream.format("delta")`?

---

**Q11 (Tricky)**
The Silver notebook uses a MERGE on `event_id` inside `foreachBatch`. A colleague says this guarantees exactly-once delivery. Is that correct? Explain.

---

## Rollback and Operations

**Q12 (Advanced)**
A bad version is deployed to production — the Gold mart job is computing wrong window aggregations. Walk through the full rollback procedure for both the Databricks job definition and the data in the Gold Delta table.
