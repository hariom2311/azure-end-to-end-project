# Day 4 — Data Quality & Testing

## Overview

Bad data is worse than no data — it produces confident wrong answers. Data quality is not a one-time cleanup task; it is an ongoing discipline that must be built into every layer of the pipeline. Day 4 covers five core concepts: the dimensions of data quality, SQL-based validation patterns, pipeline testing strategies, anomaly detection, and data contracts. These are the skills that separate engineers who ship reliable pipelines from those who spend every Monday explaining why the dashboard was wrong over the weekend.

**The 5 concepts:**
1. Dimensions of Data Quality
2. SQL-Based Data Validation Patterns
3. Pipeline Testing Strategies — Unit, Integration & End-to-End
4. Anomaly Detection & Statistical Quality Checks
5. Data Contracts & Schema Enforcement

---

## Concept 1: Dimensions of Data Quality

Data quality is multi-dimensional. A dataset can pass one dimension and fail another. Knowing the six dimensions lets you write targeted checks rather than vague "data is bad" tickets.

### The six dimensions

| Dimension | Definition | Example violation | How to check |
|---|---|---|---|
| **Completeness** | Required fields are populated | `customer_id IS NULL` | `COUNT(*) WHERE col IS NULL` |
| **Uniqueness** | No duplicate rows on a key | Duplicate `transaction_id` | `GROUP BY key HAVING COUNT(*) > 1` |
| **Validity** | Values conform to business rules | `amount < 0`, `status = 'unknown'` | `WHERE amount < 0`, `WHERE status NOT IN (...)` |
| **Consistency** | Values agree across tables/columns | `amount` in transactions ≠ `amount` in settlements | Cross-table JOIN comparison |
| **Timeliness** | Data arrives within expected SLA | Yesterday's data not yet loaded at 8am | `MAX(load_date) < CURRENT_DATE` |
| **Accuracy** | Values reflect real-world truth | Product price in warehouse ≠ price in source system | Compare to authoritative reference |

### Why dimensions matter in interviews

When an interviewer says "how do you ensure data quality?", listing all six dimensions with one check per dimension is a complete, structured answer. Most candidates say only "check for nulls" — demonstrating understanding of all six dimensions signals seniority.

### Completeness

The most commonly checked dimension. Measures what fraction of expected values are present.

```sql
-- Completeness check: fraction of non-null customer_ids
SELECT
    COUNT(*)                                    AS total_rows,
    COUNT(customer_id)                          AS non_null_customer,
    COUNT(*) - COUNT(customer_id)               AS null_count,
    ROUND(COUNT(customer_id)::NUMERIC / COUNT(*) * 100, 2) AS completeness_pct
FROM transactions;
```

**Completeness threshold:** Define acceptable thresholds per column. `transaction_id` must be 100% complete (not negotiable). `discount_pct` might be 80% complete (nullable for some product types).

### Uniqueness

```sql
-- Find duplicate transaction_ids
SELECT transaction_id, COUNT(*) AS occurrences
FROM transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- Deduplication: keep the latest load per transaction_id
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY loaded_at DESC) AS rn
    FROM transactions
)
SELECT * FROM ranked WHERE rn = 1;
```

### Validity

Validity checks verify that values conform to known business rules — ranges, enumerations, formats, referential integrity.

```sql
-- Enumeration check: status must be in allowed set
SELECT * FROM transactions
WHERE status NOT IN ('pending', 'completed', 'refunded', 'cancelled');

-- Range check: amount must be positive (gift vouchers excepted)
SELECT * FROM transactions
WHERE amount <= 0 AND product_id != 'P110';

-- Format check: transaction_id must match pattern TXN + digits
SELECT * FROM transactions
WHERE transaction_id NOT SIMILAR TO 'TXN[0-9]+';

-- Referential integrity: every product_id must exist in products table
SELECT t.transaction_id, t.product_id
FROM transactions t
LEFT JOIN products p ON t.product_id = p.product_id
WHERE p.product_id IS NULL;
```

### Consistency

Consistency checks compare values across tables or columns that should agree.

```sql
-- Cross-table consistency: settled amount should match transaction amount
SELECT t.transaction_id,
       t.amount          AS txn_amount,
       s.settled_amount  AS settled_amount,
       t.amount - s.settled_amount AS discrepancy
FROM transactions t
JOIN settlements s ON t.transaction_id = s.transaction_id
WHERE ABS(t.amount - s.settled_amount) > 0.01;  -- allow penny rounding

-- Intra-row consistency: total must equal sum of line items
SELECT order_id,
       total_amount,
       SUM(line_amount) AS calculated_total,
       total_amount - SUM(line_amount) AS discrepancy
FROM order_lines
GROUP BY order_id, total_amount
HAVING ABS(total_amount - SUM(line_amount)) > 0.01;
```

### Timeliness

```sql
-- Check data freshness: alert if Silver table hasn't received data in last 24 hours
SELECT
    MAX(transaction_date)                          AS latest_record,
    CURRENT_TIMESTAMP - MAX(transaction_date)      AS data_age,
    CASE
        WHEN MAX(transaction_date) < CURRENT_DATE - INTERVAL '1 day'
        THEN 'STALE'
        ELSE 'FRESH'
    END AS freshness_status
FROM transactions;
```

---

## Concept 2: SQL-Based Data Validation Patterns

### The assertion pattern

Every data quality check can be written as a SQL query that returns **zero rows when the data is good** and **non-zero rows when it is bad**. This is called the assertion pattern and is the foundation of dbt tests, Great Expectations SQL dialect, and custom validation frameworks.

```sql
-- ASSERTION: transaction_id must be unique
-- Returns zero rows if clean, problem rows if dirty
SELECT transaction_id, COUNT(*) AS cnt
FROM transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- ASSERTION: amount must be positive
SELECT transaction_id, amount
FROM transactions
WHERE amount <= 0;

-- ASSERTION: every transaction references a valid product
SELECT t.transaction_id, t.product_id
FROM transactions t
LEFT JOIN products p ON t.product_id = p.product_id
WHERE p.product_id IS NULL;
```

**The rule:** A quality check is a `SELECT` that should return `0 rows`. If it returns any rows, the pipeline fails (or alerts, depending on severity).

### Quarantine pattern

When a quality check fails, bad rows should not be silently dropped or allowed to corrupt the Silver table. They go to a quarantine (DLQ) table for investigation.

```sql
-- Load good rows to Silver
INSERT INTO silver.transactions
SELECT * FROM staging.transactions
WHERE customer_id IS NOT NULL
  AND amount > 0
  AND status IN ('pending', 'completed', 'refunded', 'cancelled')
  AND transaction_id SIMILAR TO 'TXN[0-9]+';

-- Route bad rows to quarantine
INSERT INTO dq.transactions_quarantine (
    source_table, reason, raw_data, quarantined_at
)
SELECT
    'staging.transactions',
    CASE
        WHEN customer_id IS NULL              THEN 'null_customer_id'
        WHEN amount <= 0                      THEN 'non_positive_amount'
        WHEN status NOT IN ('pending','completed','refunded','cancelled')
                                              THEN 'invalid_status'
        WHEN transaction_id NOT SIMILAR TO 'TXN[0-9]+'
                                              THEN 'invalid_transaction_id_format'
        ELSE 'other'
    END,
    ROW(t.*)::TEXT,
    NOW()
FROM staging.transactions t
WHERE customer_id IS NULL
   OR amount <= 0
   OR status NOT IN ('pending','completed','refunded','cancelled')
   OR transaction_id NOT SIMILAR TO 'TXN[0-9]+';
```

### Write-Audit-Publish (WAP) pattern

The WAP pattern delays making data visible to consumers until it passes all quality checks.

```
Step 1 — WRITE:   Write transformed data to a hidden staging table (not visible to consumers)
Step 2 — AUDIT:   Run all quality assertions against the staging table
Step 3 — PUBLISH: If all assertions pass, atomically swap staging → production table
                  If any assertion fails, halt and alert — consumers see last good data
```

```sql
-- Step 1: Write to staging
CREATE TABLE silver.transactions_staging AS
SELECT * FROM staging.transactions WHERE ...;

-- Step 2: Audit
DO $$
DECLARE
    null_count INT;
    dup_count  INT;
BEGIN
    SELECT COUNT(*) INTO null_count
    FROM silver.transactions_staging WHERE customer_id IS NULL;

    SELECT COUNT(*) INTO dup_count
    FROM silver.transactions_staging
    GROUP BY transaction_id HAVING COUNT(*) > 1
    LIMIT 1;

    IF null_count > 0 OR dup_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: nulls=%, dups=%', null_count, dup_count;
    END IF;
END $$;

-- Step 3: Publish (atomic swap)
BEGIN;
  DROP TABLE IF EXISTS silver.transactions_old;
  ALTER TABLE silver.transactions RENAME TO transactions_old;
  ALTER TABLE silver.transactions_staging RENAME TO transactions;
COMMIT;
```

### Row count reconciliation

Confirm that source and target row counts match within tolerance after a load.

```sql
WITH source_count AS (
    SELECT COUNT(*) AS cnt FROM staging.transactions
    WHERE transaction_date = CURRENT_DATE - 1
),
target_count AS (
    SELECT COUNT(*) AS cnt FROM silver.transactions
    WHERE transaction_date = CURRENT_DATE - 1
),
reconciliation AS (
    SELECT
        s.cnt AS source_rows,
        t.cnt AS target_rows,
        s.cnt - t.cnt AS row_diff,
        ROUND(ABS(s.cnt - t.cnt)::NUMERIC / NULLIF(s.cnt, 0) * 100, 2) AS diff_pct
    FROM source_count s, target_count t
)
SELECT *,
    CASE WHEN diff_pct > 1 THEN 'FAIL' ELSE 'PASS' END AS status
FROM reconciliation;
-- Fails if more than 1% of source rows are missing from Silver
```

### Golden dataset testing

Keep a small, fixed, hand-verified dataset ("golden dataset") and run it through your pipeline on every deployment. The output must exactly match the known-correct output.

```sql
-- Compare pipeline output to golden expected output
SELECT 'missing_from_output' AS issue, g.*
FROM golden_expected g
LEFT JOIN pipeline_output p ON g.transaction_id = p.transaction_id
WHERE p.transaction_id IS NULL

UNION ALL

SELECT 'extra_in_output', p.*
FROM pipeline_output p
LEFT JOIN golden_expected g ON p.transaction_id = g.transaction_id
WHERE g.transaction_id IS NULL

UNION ALL

SELECT 'value_mismatch', p.*
FROM pipeline_output p
JOIN golden_expected g ON p.transaction_id = g.transaction_id
WHERE p.amount != g.amount OR p.status != g.status;
-- Zero rows = pipeline output matches golden dataset
```

---

## Concept 3: Pipeline Testing Strategies

### The testing pyramid for data pipelines

```
          ┌─────────────────────────────┐
          │    End-to-End / Integration  │  Slow, expensive, full pipeline run
          │    (few, high-value)         │
          ├─────────────────────────────┤
          │    Component / SQL Tests     │  Medium speed, test one transformation
          │    (moderate number)         │
          ├─────────────────────────────┤
          │    Unit Tests                │  Fast, test one function/rule in isolation
          │    (many, cheap)             │
          └─────────────────────────────┘
```

### Unit testing transformation logic

Unit tests verify a single transformation function in isolation using small, controlled input data. They run in milliseconds.

```python
# Python unit test for a currency normalisation function
import pytest

def normalise_amount(amount: float, currency: str) -> float:
    """Convert all amounts to AUD."""
    rates = {'AUD': 1.0, 'USD': 1.52, 'EUR': 1.65, 'GBP': 1.93}
    if currency not in rates:
        raise ValueError(f"Unknown currency: {currency}")
    return round(amount * rates[currency], 2)

class TestNormaliseAmount:
    def test_aud_unchanged(self):
        assert normalise_amount(100.0, 'AUD') == 100.0

    def test_usd_converted(self):
        assert normalise_amount(100.0, 'USD') == 152.0

    def test_negative_amount_passes_through(self):
        # normalise_amount does not validate sign — that's a separate concern
        assert normalise_amount(-50.0, 'AUD') == -50.0

    def test_unknown_currency_raises(self):
        with pytest.raises(ValueError, match="Unknown currency"):
            normalise_amount(100.0, 'XYZ')
```

**Rule:** One test per business rule. Test the happy path, edge cases, and known failure modes separately.

### SQL transformation tests

Test a SQL transformation by comparing its output against a known-correct result set.

```python
import duckdb
import pytest

# The transformation under test
DEDUP_SQL = """
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY loaded_at DESC) AS rn
    FROM transactions
)
SELECT transaction_id, customer_id, amount
FROM ranked WHERE rn = 1
"""

def test_dedup_removes_duplicates():
    conn = duckdb.connect()
    conn.execute("""
        CREATE TABLE transactions AS
        SELECT * FROM (VALUES
            ('TXN001', 'C001', 100.0, TIMESTAMP '2024-01-15 10:00:00'),
            ('TXN001', 'C001', 100.0, TIMESTAMP '2024-01-15 11:00:00'),  -- duplicate, later
            ('TXN002', 'C002', 200.0, TIMESTAMP '2024-01-15 10:00:00')
        ) t(transaction_id, customer_id, amount, loaded_at)
    """)
    result = conn.execute(DEDUP_SQL).fetchdf()
    assert len(result) == 2, f"Expected 2 rows after dedup, got {len(result)}"
    assert result[result['transaction_id'] == 'TXN001']['amount'].iloc[0] == 100.0

def test_dedup_keeps_latest_on_conflict():
    # Verify the later timestamp wins when values differ
    conn = duckdb.connect()
    conn.execute("""
        CREATE TABLE transactions AS
        SELECT * FROM (VALUES
            ('TXN001', 'C001', 100.0, TIMESTAMP '2024-01-15 10:00:00'),
            ('TXN001', 'C001', 150.0, TIMESTAMP '2024-01-15 11:00:00')  -- later, different amount
        ) t(transaction_id, customer_id, amount, loaded_at)
    """)
    result = conn.execute(DEDUP_SQL).fetchdf()
    assert result['amount'].iloc[0] == 150.0, "Should keep the later-loaded row"
```

### Integration testing

Integration tests verify that two or more pipeline components work together correctly. They run against real (or realistic) test data, not mocks.

```python
def test_staging_to_silver_pipeline():
    """Full integration test: load staging → apply quality rules → write to Silver."""
    # 1. Set up realistic staging data (includes known-bad rows)
    staging_data = [
        ('TXN001', 'C001', 'P101', 120.50, 'AUD', 'completed', '2024-01-15'),
        ('TXN002',    None, 'P102',  85.00, 'AUD', 'completed', '2024-01-15'),  # null customer
        ('TXN003', 'C003', 'P103', -50.00, 'AUD', 'completed', '2024-01-15'),  # negative amount
    ]
    load_to_staging(staging_data)

    # 2. Run the pipeline
    run_staging_to_silver_pipeline()

    # 3. Assert Silver has only good rows
    silver_rows = query("SELECT COUNT(*) FROM silver.transactions")
    assert silver_rows == 1, "Only TXN001 should reach Silver"

    # 4. Assert bad rows went to quarantine
    quarantine_rows = query("SELECT COUNT(*) FROM dq.transactions_quarantine")
    assert quarantine_rows == 2, "TXN002 (null customer) and TXN003 (negative) quarantined"

    # 5. Assert Silver values are correct
    silver_txn = query("SELECT * FROM silver.transactions WHERE transaction_id = 'TXN001'")
    assert silver_txn['amount'] == 120.50
```

### What to test vs. what not to test

| Test | Yes/No | Reason |
|---|---|---|
| Transformation logic (dedup, normalisation, casting) | Yes | Core pipeline logic; bugs here cause silent data errors |
| Quality rule thresholds (null%, row count tolerance) | Yes | Thresholds are business decisions; wrong thresholds ship bad data |
| Database engine internals (`SUM`, `JOIN`) | No | Trust the engine; don't test what you didn't write |
| Infrastructure connectivity (can we reach S3?) | No — use smoke tests | Integration tests, not unit tests; test at deployment time |
| Golden dataset end-to-end | Yes | Regression safety net for the whole pipeline |

---

## Concept 4: Anomaly Detection & Statistical Quality Checks

### Why rule-based checks are not enough

Hard-coded rules (`WHERE amount < 0`) catch known problems. Anomaly detection catches unknown problems — things that are technically valid but statistically unusual.

**Example:** Amount of `$9,999,999` passes every validity rule (positive, numeric, valid currency) but is clearly an outlier. A rule-based check would not catch it. A statistical check would.

### Z-score anomaly detection

The Z-score measures how many standard deviations a value is from the mean.

```sql
WITH stats AS (
    SELECT
        AVG(amount)    AS mean_amount,
        STDDEV(amount) AS std_amount
    FROM transactions
    WHERE status = 'completed'
),
scored AS (
    SELECT
        t.transaction_id,
        t.amount,
        s.mean_amount,
        s.std_amount,
        (t.amount - s.mean_amount) / NULLIF(s.std_amount, 0) AS z_score
    FROM transactions t, stats s
    WHERE t.status = 'completed'
)
SELECT *
FROM scored
WHERE ABS(z_score) > 3  -- flag rows more than 3 standard deviations from mean
ORDER BY ABS(z_score) DESC;
```

**Rule of thumb:** `|z| > 3` flags ~0.3% of a normally distributed column. Adjust the threshold based on acceptable false-positive rate for your domain.

### Volume anomaly detection (row count checks)

Detect days where the row count is abnormally high or low compared to the historical average.

```sql
WITH daily_counts AS (
    SELECT
        transaction_date,
        COUNT(*) AS row_count
    FROM transactions
    GROUP BY transaction_date
),
stats AS (
    SELECT
        AVG(row_count)    AS mean_count,
        STDDEV(row_count) AS std_count
    FROM daily_counts
    WHERE transaction_date < CURRENT_DATE  -- exclude today from stats
),
today AS (
    SELECT COUNT(*) AS today_count FROM transactions WHERE transaction_date = CURRENT_DATE
)
SELECT
    td.today_count,
    s.mean_count,
    s.std_count,
    (td.today_count - s.mean_count) / NULLIF(s.std_count, 0) AS z_score,
    CASE
        WHEN ABS((td.today_count - s.mean_count) / NULLIF(s.std_count, 0)) > 2
        THEN 'ANOMALY'
        ELSE 'NORMAL'
    END AS status
FROM today td, stats s;
```

### Distribution drift detection

Detect when the distribution of a categorical column shifts significantly — a signal that source data has changed unexpectedly.

```sql
-- Compare status distribution today vs. 7-day rolling average
WITH today_dist AS (
    SELECT status,
           COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct_today
    FROM transactions
    WHERE transaction_date = CURRENT_DATE
    GROUP BY status
),
baseline_dist AS (
    SELECT status,
           COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct_baseline
    FROM transactions
    WHERE transaction_date BETWEEN CURRENT_DATE - 7 AND CURRENT_DATE - 1
    GROUP BY status
)
SELECT
    COALESCE(t.status, b.status) AS status,
    ROUND(t.pct_today, 1)       AS pct_today,
    ROUND(b.pct_baseline, 1)    AS pct_baseline,
    ROUND(ABS(t.pct_today - b.pct_baseline), 1) AS drift_pct
FROM today_dist t
FULL OUTER JOIN baseline_dist b ON t.status = b.status
ORDER BY drift_pct DESC;
-- Flag if any status drifts more than 10 percentage points from baseline
```

### Freshness and SLA monitoring

```sql
-- Table freshness check: how stale is the data?
SELECT
    table_name,
    MAX(transaction_date)                          AS latest_data,
    CURRENT_TIMESTAMP - MAX(transaction_date)      AS data_age,
    CASE
        WHEN MAX(transaction_date) < CURRENT_DATE - INTERVAL '1 day'
            THEN 'SLA_BREACH'
        WHEN MAX(transaction_date) < CURRENT_DATE - INTERVAL '4 hours'
            THEN 'WARNING'
        ELSE 'OK'
    END AS sla_status
FROM (
    SELECT 'silver.transactions' AS table_name, transaction_date FROM silver.transactions
    UNION ALL
    SELECT 'gold.revenue_summary', report_date FROM gold.revenue_summary
) combined
GROUP BY table_name;
```

---

## Concept 5: Data Contracts & Schema Enforcement

### What is a data contract?

A **data contract** is a formal, versioned agreement between a data producer (the team that writes data) and a data consumer (the team that reads it) that defines:
- **Schema:** column names, types, nullability
- **Semantics:** what each column means (business definition)
- **Quality SLAs:** freshness guarantees, completeness thresholds
- **Versioning:** how changes are communicated and backward compatibility rules

Without a data contract, a producer can rename a column and break 10 downstream consumers silently. With one, the producer must version the change and notify consumers.

### Schema enforcement in practice

```python
# Schema enforcement using Python dataclasses + validation
from dataclasses import dataclass
from typing import Optional
import pandas as pd

@dataclass
class TransactionSchema:
    """Data contract for the transactions Silver table."""
    transaction_id: str       # Required, format: TXN + digits
    customer_id: str          # Required, no nulls in Silver
    product_id: str           # Required, must exist in products reference
    amount: float             # Required, must be > 0 (except gift vouchers P110)
    currency: str             # Required, must be in ['AUD', 'USD', 'EUR', 'GBP']
    status: str               # Required, must be in ['pending','completed','refunded','cancelled']
    transaction_date: str     # Required, format: YYYY-MM-DD
    store_id: str             # Required

def validate_transactions(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (clean_df, quarantine_df).
    Clean rows satisfy all contract rules; quarantine rows failed at least one.
    """
    import re
    valid_statuses  = {'pending', 'completed', 'refunded', 'cancelled'}
    valid_currencies = {'AUD', 'USD', 'EUR', 'GBP'}

    mask_ok = (
        df['transaction_id'].str.match(r'^TXN\d+$', na=False) &
        df['customer_id'].notna() &
        df['amount'].gt(0) &
        df['currency'].isin(valid_currencies) &
        df['status'].isin(valid_statuses)
    )
    return df[mask_ok], df[~mask_ok]
```

### Schema evolution and breaking vs. non-breaking changes

| Change type | Breaking? | How to handle |
|---|---|---|
| Add a nullable column | Non-breaking | Consumers ignore unknown columns; safe to deploy |
| Add a NOT NULL column | Breaking | Must provide a default or migrate consumers first |
| Rename a column | Breaking | Deprecate old name → add new name → remove old after migration window |
| Change column type (INT → BIGINT) | Usually non-breaking | Widening types are safe; narrowing types break consumers |
| Remove a column | Breaking | Must negotiate removal with all consumers; use deprecation period |
| Change a column's semantic meaning | Breaking (silent) | Most dangerous — passes schema checks but produces wrong results |

### Contract enforcement with Great Expectations (concept)

Great Expectations (GX) is an open-source library that formalises data contracts as executable "expectations":

```python
import great_expectations as gx

context = gx.get_context()

# Define expectations (the data contract)
suite = context.add_expectation_suite("transactions_silver_contract")

# Completeness
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
    column="transaction_id"
))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
    column="customer_id"
))

# Uniqueness
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(
    column="transaction_id"
))

# Validity — enum
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column="status",
    value_set=["pending", "completed", "refunded", "cancelled"]
))

# Validity — range
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column="amount",
    min_value=0,
    mostly=0.99  -- allow 1% exceptions (e.g., gift vouchers with 0 amount)
))

# Volume
suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(
    min_value=1000,
    max_value=1000000
))

# Run validation
result = context.run_validation_operator(
    "action_list_operator",
    assets_to_validate=[batch],
    expectation_suite_name="transactions_silver_contract"
)
print("Contract passed:", result["success"])
```

### Data contract as code (YAML definition)

```yaml
# contracts/transactions_silver.yml
version: "1.2.0"
name: transactions_silver
owner: data-engineering@company.com
consumers:
  - team: finance
    table: gold.revenue_summary
  - team: analytics
    table: gold.customer_metrics

schema:
  - name: transaction_id
    type: VARCHAR
    nullable: false
    unique: true
    pattern: "^TXN[0-9]+$"

  - name: customer_id
    type: VARCHAR
    nullable: false

  - name: amount
    type: NUMERIC(12,2)
    nullable: false
    min: 0.01

  - name: status
    type: VARCHAR
    nullable: false
    allowed_values: [pending, completed, refunded, cancelled]

  - name: transaction_date
    type: DATE
    nullable: false

sla:
  freshness_hours: 24
  completeness_pct: 99.5
  row_count_min: 500

changelog:
  - version: "1.2.0"
    date: "2024-02-01"
    change: "Added store_id column (nullable, non-breaking)"
  - version: "1.1.0"
    date: "2024-01-01"
    change: "Added refunded to allowed status values"
  - version: "1.0.0"
    date: "2023-06-01"
    change: "Initial contract"
```

---

## Summary

| Concept | Core idea | Key interview term |
|---|---|---|
| Dimensions of Data Quality | Six measurable dimensions: completeness, uniqueness, validity, consistency, timeliness, accuracy | Data quality dimensions, threshold, DQ metric |
| SQL Validation Patterns | Assertion = SQL that returns 0 rows when clean; WAP pattern delays publish until checks pass | Assertion pattern, quarantine, Write-Audit-Publish |
| Pipeline Testing Strategies | Unit tests for functions, SQL tests for transformations, integration tests for end-to-end flow | Testing pyramid, golden dataset, quarantine |
| Anomaly Detection | Z-score for value outliers, volume checks for row count drift, distribution drift for category shifts | Z-score, volume anomaly, distribution drift |
| Data Contracts | Formal schema + SLA agreement between producer and consumer; breaking vs. non-breaking changes | Data contract, schema evolution, breaking change |
