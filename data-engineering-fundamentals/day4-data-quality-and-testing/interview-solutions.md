# Day 4 — Interview Solutions: Data Quality & Testing

> Complete answers for all 40 questions in `interview-questions.md`.

---

## Concept 1: Dimensions of Data Quality

**Q1 — Six dimensions with example checks**

| Dimension | Definition | Example check on transactions |
|---|---|---|
| Completeness | Required fields are populated | `SELECT COUNT(*) WHERE customer_id IS NULL` |
| Uniqueness | No duplicate rows on a key | `GROUP BY transaction_id HAVING COUNT(*) > 1` |
| Validity | Values conform to business rules | `WHERE status NOT IN ('pending','completed','refunded','cancelled')` |
| Consistency | Values agree across tables/columns | `JOIN settlements WHERE ABS(t.amount - s.settled_amount) > 0.01` |
| Timeliness | Data arrives within expected SLA | `WHERE MAX(transaction_date) < CURRENT_DATE - INTERVAL '1 day'` |
| Accuracy | Values reflect real-world truth | Compare product price to authoritative reference catalogue |

---

**Q2 — Validity vs. accuracy**

**Validity:** The value conforms to defined rules (type, range, enumeration, format). Can be checked against the data itself without external reference.

**Accuracy:** The value correctly represents the real-world fact it describes. Requires an external authoritative source to verify.

**Example:** A transaction has `amount = 45.00` for product P101 (Wireless Headphones, reference price = 120.50). This passes all validity checks — positive, numeric, valid currency. But it is inaccurate: the amount does not match the authoritative product price. Detecting this requires joining to the products reference table. Validity alone cannot catch it.

---

**Q3 — Measuring completeness and setting thresholds**

```sql
SELECT
    'customer_id'                                          AS column_name,
    COUNT(*)                                               AS total,
    COUNT(customer_id)                                     AS non_null,
    ROUND(COUNT(customer_id) * 100.0 / COUNT(*), 2)       AS completeness_pct
FROM transactions;
```

**Threshold by column type:**

| Column type | Threshold | Rationale |
|---|---|---|
| Primary key (`transaction_id`) | 100% | Zero nulls acceptable — every event must be identifiable |
| Foreign key (`customer_id`) | 99%+ | Guest purchases create legitimate nulls; agreed threshold with business |
| Measure (`amount`) | 100% | Cannot compute revenue without amount |
| Optional attribute (`discount_pct`) | Flexible | Often 0 or null; threshold depends on how many promotions run |

**Rule:** Completeness thresholds must be agreed with the business and encoded in the data contract, not chosen arbitrarily by the DE team.

---

**Q4 — 5% null customer_id from guest purchases**

This is **not a data quality error** — it is a known, expected business condition (guest checkout). However, it requires deliberate handling:

1. **Document it in the data contract:** `customer_id: nullable: true, null_meaning: "guest purchase"`
2. **Do not quarantine these rows:** Null `customer_id` is valid business data; quarantining it loses revenue records
3. **Add a completeness metric:** Track the null percentage over time. If guest purchase rate suddenly jumps from 5% to 30%, that is an anomaly worth investigating — it might indicate a source system bug, not a real business shift
4. **Handle in Gold models:** Revenue by customer reports must include a `'Guest'` bucket or use `COALESCE(customer_id, 'GUEST')` to avoid silently dropping guest revenue

---

**Q5 — Identifying the anomalous day**

- Tuesday: 950,000 (−5% from 1M mean) — within normal variation; probably not a problem
- Wednesday: 1,050,000 (+5%) — within normal variation; probably not a problem
- Thursday: 200,000 (−80%) — clearly anomalous; this requires investigation

**How to tell the difference automatically:**

```sql
-- Z-score threshold: flag if |z| > 2
WITH stats AS (
    SELECT AVG(daily_count) AS mean, STDDEV(daily_count) AS std
    FROM daily_load_counts
    WHERE load_date < :check_date
),
today AS (
    SELECT COUNT(*) AS cnt FROM transactions WHERE load_date = :check_date
)
SELECT
    (t.cnt - s.mean) / NULLIF(s.std, 0) AS z_score,
    CASE WHEN ABS((t.cnt - s.mean) / NULLIF(s.std, 0)) > 2 THEN 'ANOMALY' ELSE 'OK' END
FROM today t, stats s;
```

Thursday at 200K has z ≈ −53 — extreme anomaly, auto-alert. To distinguish pipeline failure from business event, cross-reference the alert with a business calendar (public holiday? promotional event?). Build a `business_calendar` dimension table and suppress alerts on known low-traffic dates.

---

**Q6 — Consistency vs. validity**

**Validity:** A single value conforms to its own rules (type, range, format). Checked in isolation.

**Consistency:** Multiple values across columns or tables agree with each other. Requires comparison.

**Example of consistency violation that passes all validity checks:**
```sql
-- Both values are individually valid:
order_items (order_id=1001, quantity=5, unit_price=20.00, line_total=150.00)
-- quantity=5, unit_price=20.00 → expected line_total=100.00
-- actual line_total=150.00 → passes not_null, positive, numeric — but is wrong
```

The consistency check is:
```sql
WHERE ABS(line_total - quantity * unit_price) > 0.01
```
No column-level check catches this — only a cross-column formula check does.

---

**Q7 — Transactions vs. settlements revenue discrepancy**

**Three possible root causes:**

1. **Timing issue (most likely):** Settlements are processed 1–2 business days after transactions. The $135 difference may be transactions from the end of the period that are not yet settled. Check: compare by `settlement_date` vs. `transaction_date` to see if the gap closes.

2. **Refunds and chargebacks:** Refunded transactions reduce settlement totals. If the transactions table counts refunds as revenue but settlements net them out, a gap is expected. Check: filter out `status = 'refunded'` from transactions before comparing.

3. **Data quality issue:** Missing rows, double-counting, or a pipeline bug. Check: compare row counts, not just totals. `COUNT(*)` discrepancy between tables points to a load issue.

**Investigation process:** Never assume the explanation — pull row-level reconciliation, not just aggregate totals.

---

## Concept 2: SQL Validation Patterns

**Q8 — The assertion pattern**

An assertion is a SQL query that **returns zero rows when data is clean** and **returns the offending rows when data is bad**. This makes it directly executable as a CI/CD step: `SELECT COUNT(*) FROM assertion_query` — if the result is 0, the check passes; if > 0, it fails.

```sql
-- Assertion: no null customer_ids
SELECT transaction_id FROM transactions WHERE customer_id IS NULL;
-- If this returns 0 rows → PASS
-- If this returns any rows → FAIL (and those rows tell you exactly what broke)
```

**Why zero rows = clean:** The assertion expresses a violation condition, not a success condition. Any returned row is a counterexample to the rule. This pattern is used by dbt tests, Great Expectations SQL dialect, and custom validation scripts.

---

**Q9 — Write-Audit-Publish (WAP) pattern**

**Problem it solves:** If you write directly to the production Silver table and then run quality checks, consumers may have already read bad data between the write and the check. WAP makes the publish step conditional on quality check results.

```
WRITE   → hidden staging table (consumers cannot see this)
AUDIT   → run assertions against staging; halt and alert on failure
PUBLISH → atomic rename staging → production (consumers see only clean data)
```

**Key property:** From a consumer's perspective, the table either has the new data (all quality checks passed) or has the previous good data (checks failed, swap was aborted). There is never a window where partially-written or dirty data is visible.

---

**Q10 — Quarantine 2% bad rows or fail the pipeline?**

**Decision framework:**

| Factor | Fail pipeline | Quarantine and continue |
|---|---|---|
| Is the bad data a primary key or foreign key? | → Fail (corrupts joins) | — |
| Is it a non-critical optional field? | — | → Quarantine |
| Do downstream systems require 100% completeness? | → Fail | — |
| Is the issue isolated and expected? | — | → Quarantine |
| Is there a business SLA on data timeliness? | — | → Quarantine (delay = SLA breach) |

**For `store_id` at 2% null:** If `store_id` is used for store-level reporting, null rows will appear in an "Unknown" store bucket or be dropped — degrading report quality but not corrupting it. Quarantine the 2%, alert the team, and continue. Document the null rate in a data quality metric. If the null rate grows, escalate.

**The answer an interviewer wants:** "It depends on the severity and the downstream impact. I quarantine unless the bad data would corrupt primary keys, financial totals, or violate a contractual SLA."

---

**Q11 — Detect duplicates on composite key**

```sql
SELECT transaction_id, transaction_date, COUNT(*) AS occurrences
FROM transactions
GROUP BY transaction_id, transaction_date
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;
```

To see the actual duplicate rows:
```sql
WITH dupes AS (
    SELECT transaction_id, transaction_date
    FROM transactions
    GROUP BY transaction_id, transaction_date
    HAVING COUNT(*) > 1
)
SELECT t.*
FROM transactions t
JOIN dupes d ON t.transaction_id = d.transaction_id
           AND t.transaction_date = d.transaction_date
ORDER BY t.transaction_id;
```

---

**Q12 — Golden dataset test vs. unit test**

A **unit test** verifies one function or rule in isolation with synthetic inputs. It is fast and focused but cannot catch bugs that emerge from the interaction of multiple pipeline stages.

A **golden dataset test** runs the entire pipeline on a fixed, hand-verified input and asserts the output matches a known-correct expected result. It catches:
- Bugs at stage boundaries (transformation output feeds into the next transformation incorrectly)
- Ordering bugs (dedup keeps wrong row when there are ties)
- Silent schema changes (column removed upstream, NULL propagates silently)
- Regression bugs (a new feature breaks existing behaviour)

**Why more valuable for pipelines:** Data pipelines are integration-heavy by nature. The failure mode is usually not "function X returns wrong value" but "the composition of stages A → B → C produces wrong output." The golden dataset test verifies the composition.

---

**Q13 — Late-arriving correction with dedup on `loaded_at`**

If the deduplication uses `ORDER BY loaded_at DESC` (keep the latest loaded row), the correction that arrives 2 hours later has a newer `loaded_at` timestamp than the original row. On the next incremental run:
- The correction row is loaded into staging
- The MERGE/dedup logic detects the same `transaction_id` with a newer `loaded_at` → overwrites the original row in Silver with the corrected `amount`

**The consumer 2 hours after the correction:** Depends on when the next Silver load runs. If Silver runs hourly and the correction arrived in the current hour's batch, the consumer sees the corrected amount at the next Silver refresh. If the consumer queries Silver between the correction arriving in staging and the next Silver load, they still see the old amount.

**The mechanism:** `MERGE INTO silver ON transaction_id MATCHED → UPDATE SET amount = source.amount` using `loaded_at` ordering to select the canonical row. Idempotent MERGE with `unique_key = transaction_id` handles this correctly.

---

**Q14 — Row count reconciliation SQL**

```sql
WITH source_count AS (
    SELECT COUNT(*) AS cnt
    FROM staging.transactions
    WHERE load_date = :load_date
),
target_count AS (
    SELECT COUNT(*) AS cnt
    FROM silver.transactions
    WHERE load_date = :load_date
),
recon AS (
    SELECT
        s.cnt                                                    AS source_rows,
        t.cnt                                                    AS silver_rows,
        s.cnt - t.cnt                                           AS missing_rows,
        ROUND((s.cnt - t.cnt)::NUMERIC / NULLIF(s.cnt, 0) * 100, 2) AS missing_pct
    FROM source_count s, target_count t
)
SELECT *,
    CASE WHEN missing_pct > 1.0 THEN 'FAIL' ELSE 'PASS' END AS status
FROM recon;
```

**Important:** The tolerance of 1% accounts for intentional quarantine of bad rows. If the pipeline quarantines bad rows by design, the reconciliation check should be: `source = silver + quarantine` (unaccounted = 0), not `source ≈ silver`.

---

## Concept 3: Pipeline Testing Strategies

**Q15 — Three levels of the testing pyramid**

| Level | Speed | Confidence | Examples |
|---|---|---|---|
| **Unit** | Milliseconds | Low (isolated) | Test one transformation function, one SQL assertion |
| **Component/SQL** | Seconds | Medium (one stage) | Test one dbt model with synthetic inputs using DuckDB |
| **Integration/E2E** | Minutes | High (full pipeline) | Run staging → Silver → Gold with real-ish data, verify Golden dataset output |

**Fastest:** Unit tests — run in < 1ms each.  
**Most confidence:** End-to-end — verifies the full pipeline composition, catches integration bugs.

**Practical ratio for a mature pipeline:** ~70% unit, ~20% component, ~10% E2E. The E2E tests are expensive so you keep them few but high-value.

---

**Q16 — Unit test cases for currency conversion**

A function `convert_to_aud(amount, currency)` should have tests for:

1. **Happy path — AUD input:** `convert_to_aud(100, 'AUD')` → `100.0` (no conversion)
2. **Happy path — USD input:** `convert_to_aud(100, 'USD')` → `152.0` (at 1.52 rate)
3. **Unknown currency:** `convert_to_aud(100, 'XYZ')` → raises `ValueError`
4. **Zero amount:** `convert_to_aud(0, 'USD')` → `0.0` (zero × rate = zero)
5. **Negative amount:** `convert_to_aud(-50, 'AUD')` → `-50.0` (function does not validate sign; that is a separate concern — test that it passes through without modification)

---

**Q17 — Should you unit test SQL transformations?**

**Partially agree.** You should not test database engine internals (`SUM`, `GROUP BY`, `JOIN` mechanics). But you absolutely should test:

- **Transformation logic:** Does your `CASE WHEN` correctly classify amounts into size buckets?
- **Deduplication logic:** Does `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` keep the correct row when there are ties?
- **NULL handling:** Does `COALESCE(discount_pct, 0)` produce the right result when `discount_pct` is NULL?
- **Business rule encoding:** Does the `WHERE status IN (...)` filter correctly exclude the right statuses?

These are things you wrote — the database engine executes them faithfully, but if your logic is wrong, the engine executes the wrong logic faithfully. Test your logic, not the engine.

---

**Q18 — Mocking vs. real test database**

**Mocking the database connection:**
- Pro: Fast (no I/O), runs in any environment, no setup cost
- Con: The mock may behave differently from the real database (type coercions, NULL handling, index usage differ). A bug that only appears with real data is not caught.
- Use when: Testing business logic that does not depend on database-specific behaviour (pure Python functions, schema validation, data type checks)

**Real (test) database:**
- Pro: Catches database-specific bugs (implicit type casts, NULL semantics, constraint violations)
- Con: Slower, requires setup/teardown, may need Docker or a dedicated test DB instance
- Use when: Testing SQL transformations, testing MERGE/upsert logic, integration tests

**Rule:** Unit tests → mock or in-memory DB (DuckDB is excellent for this). Integration tests → real database with a test schema, seeded with known data.

---

**Q19 — Testing only affected models after a change**

```bash
# dbt: run only stg_orders and all models downstream of it
dbt run --select stg_orders+

# dbt: run tests on all models downstream of stg_orders
dbt test --select stg_orders+
```

The `+` operator selects `stg_orders` and all descendants (models that `{{ ref('stg_orders') }}`). If `stg_orders` feeds into `int_orders_enriched` → `fct_sales` → `gold_revenue`, all three are included.

**Why this is safe:** dbt's dependency graph (built from `{{ ref() }}` calls) is authoritative. Models not downstream of `stg_orders` are unaffected and don't need to run. This makes targeted CI/CD possible for large projects.

---

**Q20 — Test for absence of data**

A "test for absence of data" verifies that certain rows do NOT exist — the quality signal is that a category, date, or entity is missing from the output when it should be present.

**Why harder:** Standard tests check for bad values in existing rows. A missing row leaves no trace in the dataset — there is nothing to flag.

**Example:** A daily sales table should have a row for every product on every day it is in stock. If product P101 sold zero units on Tuesday, a transaction fact table has no Tuesday row for P101. Column-level tests (`not_null`, `unique`) cannot detect the missing row.

**How to test:**
```sql
-- Factless eligibility table approach: LEFT JOIN expected to actual
SELECT e.product_id, e.date
FROM expected_active_products e
LEFT JOIN daily_sales s ON e.product_id = s.product_id AND e.date = s.date
WHERE s.product_id IS NULL;
-- Rows returned = products that should have data but don't
```

---

**Q21 — Post-load validation function (pseudocode)**

```python
def validate_incremental_load(table: str, load_timestamp: datetime) -> dict:
    """Run after every hourly incremental load."""
    results = {"passed": True, "failures": []}

    # Check 1: No nulls in key columns introduced in this load batch
    null_check = query(f"""
        SELECT COUNT(*) FROM {table}
        WHERE loaded_at >= :load_timestamp
          AND (transaction_id IS NULL OR customer_id IS NULL OR amount IS NULL)
    """, load_timestamp=load_timestamp)
    if null_check > 0:
        results["failures"].append(f"NULL_IN_KEY_COLUMNS: {null_check} rows")
        results["passed"] = False

    # Check 2: No duplicate primary keys introduced
    dup_check = query(f"""
        SELECT COUNT(*) FROM (
            SELECT transaction_id FROM {table}
            GROUP BY transaction_id HAVING COUNT(*) > 1
        )
    """)
    if dup_check > 0:
        results["failures"].append(f"DUPLICATE_PK: {dup_check} duplicate transaction_ids")
        results["passed"] = False

    # Check 3: Row count within 20% of previous hour
    current_count = query(
        f"SELECT COUNT(*) FROM {table} WHERE loaded_at >= :ts", ts=load_timestamp
    )
    prev_count = query(
        f"SELECT COUNT(*) FROM {table} WHERE loaded_at BETWEEN :t1 AND :t2",
        t1=load_timestamp - timedelta(hours=2),
        t2=load_timestamp - timedelta(hours=1)
    )
    if prev_count > 0:
        diff_pct = abs(current_count - prev_count) / prev_count * 100
        if diff_pct > 20:
            results["failures"].append(
                f"VOLUME_ANOMALY: {diff_pct:.1f}% deviation from previous hour ({prev_count} rows)"
            )
            results["passed"] = False

    return results
```

---

## Concept 4: Anomaly Detection & Statistical Quality Checks

**Q22 — Z-score for outlier detection**

A **Z-score** measures how many standard deviations a value is from the mean of the distribution:

```
Z = (value - mean) / standard_deviation
```

**Threshold:** `|Z| > 3` is the standard threshold, meaning the value is more than 3 standard deviations from the mean. In a normally distributed dataset, only ~0.3% of values exceed this — making false positives rare while catching genuine outliers.

```sql
WITH stats AS (
    SELECT AVG(amount) AS mu, STDDEV(amount) AS sigma FROM transactions
)
SELECT t.*, (t.amount - s.mu) / NULLIF(s.sigma, 0) AS z_score
FROM transactions t, stats s
WHERE ABS((t.amount - s.mu) / NULLIF(s.sigma, 0)) > 3;
```

**Caveat:** Z-score assumes normal distribution. For highly skewed distributions (amounts with long right tails), use IQR-based outlier detection instead.

---

**Q23 — Rule-based vs. statistical anomaly detection**

**Rule-based:** Fixed thresholds defined upfront. `WHERE amount < 0` or `WHERE status NOT IN (...)`. Catches known violations but misses anything not explicitly anticipated.

**Statistical:** Detects values that are unusual relative to historical patterns. No fixed threshold — the threshold adapts to the data distribution.

**Example rule-based checks would miss:**
- Amount of `$9,999,999` — positive, valid currency, valid status — passes all rules
- Row count drops from 500K to 50K — all 50K rows pass validity checks; the volume drop is the anomaly
- Product category "Electronics" disappears from today's data — every remaining row is valid; the absence is the problem

Statistical checks catch the first two. The third requires a factless fact (eligibility) approach.

---

**Q24 — Z-score calculation for 485,000 rows**

```
Mean = 500,000
Std  = 15,000
z = (485,000 - 500,000) / 15,000 = -15,000 / 15,000 = -1.0
```

`|z| = 1.0` — well within the normal range (threshold is typically 2 or 3). **This is not an anomaly.** A 5% drop on any given day is well within one standard deviation of historical variation.

**The correct answer:** Tuesday's 485K load is statistically normal. Do not alert. Set the alert threshold at z > 2 (which corresponds to roughly 470K or below for this dataset).

---

**Q25 — Legitimate bulk order flagged as Z-score outlier**

**The risk of auto-rejecting:** You would quarantine or drop a valid $500,000 corporate order, causing revenue to be missing from reports. The financial team would escalate, trust in the pipeline would be damaged.

**Correct handling:**

1. **Alert, don't auto-reject:** High-Z-score rows go to a review queue, not quarantine. A human confirms or dismisses.
2. **Whitelist known outlier categories:** Corporate orders, bulk purchase orders, annual licensing fees — tag these at source and exclude from the statistical baseline.
3. **Use `mostly` parameter:** dbt/GX's `mostly=0.999` allows 0.1% of rows to violate the range check — legitimate outliers won't fail the pipeline.
4. **Context-aware thresholds:** Track Z-scores per `customer_segment`. A $500K order from a `Wholesale` customer has a different baseline than from a `Retail` customer.

---

**Q26 — Distribution drift vs. row count check**

**Row count check:** Detects that fewer (or more) rows arrived than expected. Blind to internal composition.

**Distribution drift:** Detects when the composition of a categorical column shifts significantly from its historical baseline.

**Example where row counts look normal but distribution has shifted:**
- Daily orders: 500K rows today (normal count)
- But today: `status = 'refunded'` is 30% of rows vs. historical 3%
- Row count check: PASS (500K is normal)
- Distribution drift check: FAIL (refund rate 10× normal — mass refund event? fraud? source system bug coding all rows as refunded?)

This is exactly the kind of silent data corruption that destroys trust in Gold dashboards. The numbers look "right" until an analyst notices revenue is 27% lower than expected.

---

**Q27 — Anomaly detection system design**

```
Component 1 — Volume anomaly (hourly):
  Metric: row count per load batch
  Baseline: 30-day rolling mean ± 2 standard deviations
  Alert: |z| > 2 → PagerDuty to DE on-call
  Suppress: known low-traffic hours (2am–5am), public holidays

Component 2 — Value outlier (per load):
  Metric: Z-score on amount column
  Threshold: |z| > 3 → route to human review queue, do not auto-reject
  Per-segment baseline: Retail vs. Wholesale vs. Corporate separate baselines
  Tune: review false-positive rate monthly; adjust per-segment thresholds

Component 3 — Categorical distribution drift (daily):
  Metric: status distribution vs. 7-day baseline
  Alert: any status category shifts > 10 percentage points
  Alert: any status category disappears entirely (0% today vs. >0% baseline)
  Route: Slack alert to analytics + DE

Tuning strategy:
  - Start thresholds loose (|z| > 4, drift > 20%) for first month
  - Review all alerts weekly: label true positive / false positive
  - Tighten thresholds where false-positive rate < 5%
  - Build business calendar to suppress alerts on known events
```

---

## Concept 5: Data Contracts & Schema Enforcement

**Q28 — What is a data contract**

A **data contract** is a formal, versioned agreement between a data producer and consumer that defines: schema (columns, types, nullability), semantics (business meaning of each column), quality SLAs (freshness, completeness, row count bounds), and a versioning/change notification process.

**Problem it solves:** In a large organisation, many teams write to shared datasets that many other teams read. Without a contract, a producer can rename a column, add a breaking type change, or drop a column — all of which silently break downstream consumers. With a contract:
- Producers cannot make breaking changes without a versioned release and consumer notification
- Consumers know exactly what to expect and can validate incoming data against the contract
- Breaking changes are caught at deployment time, not at query time when an analyst notices the dashboard is wrong

---

**Q29 — Breaking vs. non-breaking schema changes**

**Non-breaking (safe to deploy without consumer coordination):**
1. Add a nullable column — consumers ignore unknown columns; existing queries unaffected
2. Widen a numeric type (INT → BIGINT) — existing values still valid; queries return wider type, which is compatible

**Breaking (requires consumer coordination and migration window):**
1. Rename a column — consumers referencing old name get NULL or an error immediately
2. Remove a column — consumers referencing it get an error immediately
3. Change a column's semantic meaning (without renaming) — most dangerous: passes schema checks but produces wrong query results silently

---

**Q30 — Column rename breaks consumers silently**

**How a data contract prevents this:**

1. **Contract enforcement at publish time:** A schema validation step before publish compares the new schema against the committed contract version. A renamed column (`customer_city → city`) would fail the validation: `customer_city` is expected in v1.0 but absent in the new schema.

2. **Automated consumer notification:** When a producer proposes a new contract version with a breaking change, an automated system notifies all registered consumers (listed in the contract's `consumers` section) before deployment.

3. **Backward compatibility test:** Run all consumer queries against the new schema in a staging environment before promoting to production.

**Enforcement mechanism:** dbt `source freshness` checks, Great Expectations schema validation in the CI pipeline, or a dedicated schema registry (Apache Schema Registry, Confluent Schema Registry for streaming data).

---

**Q31 — Evolving the contract to allow negative amounts**

**Process:**

1. **Create contract v1.1.0:** Change `amount.min` from `0.01` to `-999999.99`. Add `amount.note: "Negative amounts represent refunds"`.

2. **Notify all consumers:** All teams listed in the contract's `consumers` section receive a change notification. They must verify their queries handle negative amounts correctly (e.g., `SUM(amount)` already handles negatives; `WHERE amount > 0` in a revenue report would now exclude refunds — this may need updating).

3. **Non-breaking additive change:** Adding negative amounts to an existing column is technically non-breaking from a schema perspective (the column type does not change). However, it is **semantically breaking** — any consumer that assumes `amount > 0` always will silently produce wrong results.

4. **Deprecation period:** Run both old and new validation rules simultaneously for one sprint. Old rule alerts (doesn't fail); new rule becomes the contract.

---

**Q32 — Consumer pinned to v1.0.0, producer ships v2.1.0**

This depends on the versioning strategy:

**SemVer (Major.Minor.Patch):**
- v1.x.x → v2.x.x is a major version bump = breaking change
- A v1.0.0 consumer cannot be guaranteed to read v2.1.0 data safely
- Solution: maintain a v1 endpoint (table/view) alongside v2, give consumers a migration window

**Append-only / backward-compatible strategy:**
- Only non-breaking changes allowed; breaking changes create a new table name
- `transactions_silver` (v1) and `transactions_silver_v2` (v2) coexist
- Consumers migrate at their own pace; old table is retired after all consumers upgrade

**Date-based versioning:** No semver compatibility guarantee; consumers must pin to a specific snapshot. Least robust — avoid for shared production datasets.

**Best practice:** Use SemVer with a contract registry. Consumers register their version dependency. A breaking change blocks deployment until all consumers have confirmed compatibility or migrated.

---

**Q33 — Great Expectations `mostly` parameter**

`mostly` is a float between 0 and 1 that specifies the minimum fraction of rows that must satisfy the expectation for it to pass.

`mostly=1.0` (default): every single row must satisfy the rule — zero tolerance.  
`mostly=0.99`: 99% of rows must satisfy the rule; up to 1% can violate without failing.

**When to use `mostly=0.99` instead of `mostly=1.0`:**
- When the source system legitimately produces a small fraction of edge cases (e.g., 0.1% of orders have a null `store_id` due to a known legacy system limitation)
- When quarantining those edge cases separately and want the pipeline to continue for the 99.9%
- For columns that are contractually "mostly" not null but allow rare nulls by agreement

**The risk `mostly` introduces:**
- It hides data quality trends. If null rate grows from 0.5% → 0.9% → 2.5% over weeks, `mostly=0.99` keeps passing until the problem becomes catastrophic. A separate quality metric tracking the null percentage over time is essential alongside a `mostly` expectation.

---

## Mixed / Senior-Level Questions

**Q34 — Testing with production data in staging**

**Risk 1 — Privacy / compliance:** Production data contains PII (customer names, emails, transaction amounts). Using it in staging breaches GDPR/CCPA requirements — staging environments typically have weaker access controls, wider team access, and may be logged or monitored by third-party tools.

**Risk 2 — Volume mismatch with real bugs hidden:** Production data is large and varied. Tests against it tend to pass because real-world data is "mostly clean." Synthetic test data is designed to hit edge cases that rarely appear in production — a unit test with controlled inputs is more likely to find a specific bug than running against 10M real rows.

**Standard alternative:** Synthetic data generation — create realistic but fake datasets that cover edge cases (nulls, duplicates, boundary values, malformed dates) without containing real PII. Tools: Faker (Python), Mimesis, or custom seed scripts. For schema-realistic but anonymised data: data masking of a production snapshot.

---

**Q35 — Revenue 12% lower, all column checks pass**

**Category of issue:** Business-logic quality failure / semantic error — the data is structurally valid but logically wrong.

**Three root causes column-level checks would miss:**

1. **Wrong join logic:** A MERGE in the pipeline matched on a non-unique key, causing some rows to be overwritten rather than inserted. All remaining rows have valid column values; the missing rows are simply absent.

2. **Silent filter bug:** A `WHERE` clause in a dbt model inadvertently excludes certain `status` values (e.g., `WHERE status != 'refunded'` was changed to `WHERE status = 'completed'`). All `completed` rows are valid — the `pending` rows are missing, but no column check catches absence.

3. **Partitioning bug:** An incremental load uses `WHERE order_date >= MAX(order_date) - INTERVAL '1 day'` but the table was rebuilt with a wrong `MAX(order_date)`, skipping 7 days of data. All loaded rows are valid; the missing days produce no error.

**The fix:** Volumetric tests + reconciliation checks + row count comparison against source. These catch absence; column tests cannot.

---

**Q36 — Route quality alerts to the right team**

```
Quality failure type               → Alert destination
───────────────────────────────────────────────────────
Source system missing rows         → Source system owner (not DE)
Null in a column that source sends → Source system owner
Schema change (unexpected column)  → DE team
Transformation bug (wrong logic)   → DE team
Referential integrity failure       → DE team
Volume anomaly (20% drop)          → DE on-call + analytics team lead
SLA freshness breach               → DE on-call
Gold metric anomaly (revenue -12%) → Analytics team + business stakeholders
PII in unexpected column           → Data governance / DPO

Routing logic:
IF failure.layer == 'staging' AND failure.type == 'schema':
    → alert('source-system-owner', 'DE-team')
IF failure.layer == 'silver' AND failure.type == 'transformation':
    → alert('DE-team')
IF failure.layer == 'gold' AND failure.type == 'volume':
    → alert('DE-team', 'analytics-lead')
IF failure.sla_breach:
    → page('DE-oncall')
```

Implement routing via PagerDuty (critical), Slack (warning), and email (info). Tag every alert with `severity`, `layer`, and `table` so routing rules are clear.

---

**Q37 — Adding quality checks to a 2-year-old pipeline**

**Phase 1 — Baseline (no alerts, observation only):**
- Add quality metrics as SELECT queries that log results to a `dq_metrics` table but do not fail the pipeline
- Run for 2–4 weeks to establish baseline distributions (null rates, row counts, status distributions)
- Never fail a pipeline you cannot explain failing

**Phase 2 — Implement checks with wide tolerances:**
- Set thresholds at 3× the observed baseline variance
- Route to Slack alerts only (no pipeline failures yet)
- Tune thresholds week by week based on alert signal vs. noise

**Phase 3 — Tighten to production thresholds:**
- Once false-positive rate is < 10%, switch critical checks to fail the pipeline
- Add quarantine logic for recoverable failures
- Document all thresholds in the data contract

**The key principle:** Never introduce a quality check that immediately fails a working pipeline. Observe first, set baselines, then enforce.

---

**Q38 — Quality at rest vs. quality in motion**

**Quality at rest:** Validating data that is already stored in a table. Run assertions as SQL queries. Used for Silver/Gold layer validation, end-of-load checks, and scheduled quality reports.

Tools: dbt tests, Great Expectations with a SQL datasource, custom SQL assertion scripts.

**Quality in motion:** Validating data as it flows through a streaming pipeline, before it reaches persistent storage. Checks must be low-latency and stateless (or use a small rolling window).

Tools: Apache Flink (DataStream API with side outputs for quarantine), Kafka Streams (filter operators), Spark Structured Streaming (filter + writeStream to quarantine topic).

**Pattern for streaming:**
```
Kafka input topic
    → Flink/Spark validation step (check nulls, types, ranges per event)
    → Good events → Silver Kafka topic → Delta Lake Silver table
    → Bad events → DLQ Kafka topic → Delta Lake quarantine table
```

The fundamental difference: at-rest checks can reprocess historical data; in-motion checks must decide in milliseconds per event and cannot look at the full dataset for statistics (use approximate streaming statistics instead).

---

**Q39 — `discount_pct IS NOT NULL` check on a 95%-zero column**

**Is the check useful? No — it is a false confidence check.**

The column is 0 for 95% of rows and non-zero for 5%. Every row has a non-null value (0 is not NULL). The `not_null` check passes 100% of the time regardless of whether the column is correct.

**What better check to write:**

```sql
-- Check 1: discount_pct must be between 0 and 100 (valid range)
SELECT * FROM transactions WHERE discount_pct < 0 OR discount_pct > 100;

-- Check 2: discount_pct should only be non-zero for rows where a promotion applies
-- (requires joining to a promotions reference table)
SELECT t.transaction_id, t.discount_pct
FROM transactions t
LEFT JOIN active_promotions p ON t.transaction_id = p.transaction_id
WHERE t.discount_pct > 0 AND p.transaction_id IS NULL;
-- Returns discounted rows with no associated promotion — data quality error

-- Check 3: distribution drift — if discount rate suddenly jumps from 5% to 40%
SELECT
    COUNT(*) FILTER (WHERE discount_pct > 0) * 100.0 / COUNT(*) AS discount_rate_pct
FROM transactions
WHERE transaction_date = CURRENT_DATE;
-- Alert if this jumps significantly from the baseline
```

---

**Q40 — Data quality dashboard for a senior stakeholder**

**What to show:**

| Metric | Granularity | Format |
|---|---|---|
| Overall pipeline health | Daily, per table | RAG status (Red/Amber/Green) |
| SLA freshness | Per table | Hours since last successful load |
| Row count trend | Daily, 30-day chart | Line chart with ±1 std dev band |
| Quality score | Per table | % rows passing all checks |
| Top quality issues | Weekly | Table: issue, row count, first seen, last seen |
| Mean time to detect / resolve | Monthly | Trend line |

**Communicating a breach that happened 3 days ago and is now fixed:**

Use a status timeline: show when the breach started, when it was detected, when it was resolved, and what the root cause was. Do NOT show only the current green status — stakeholders trust a dashboard more when it shows honest historical states, not just "everything is fine now."

Example communication: "Silver transactions table experienced a data freshness breach from Monday 14:00 to Tuesday 09:00 (19 hours). Root cause: source system maintenance window was not communicated to the DE team. Gold revenue data for Monday was delayed but is now complete and reconciled. We are implementing a source system downtime notification process to prevent recurrence."
