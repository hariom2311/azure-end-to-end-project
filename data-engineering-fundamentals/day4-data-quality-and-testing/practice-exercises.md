# Day 4 — Practice Exercises: Data Quality & Testing

> Use `data/transactions.csv` and `data/products_reference.csv`.  
> The transactions file intentionally contains data quality problems — your job is to find and fix them.  
> Run SQL in PostgreSQL, DuckDB, or SQLite. Python exercises use pandas / pytest.

---

## Setup — Load the data

```sql
-- PostgreSQL / DuckDB
CREATE TABLE transactions AS SELECT * FROM read_csv_auto('data/transactions.csv');
CREATE TABLE products     AS SELECT * FROM read_csv_auto('data/products_reference.csv');

-- Inspect what you have
SELECT * FROM transactions LIMIT 5;
SELECT COUNT(*) FROM transactions;
```

---

## Exercise 1 — Audit All Six Quality Dimensions

**Concept:** Dimensions of Data Quality

**Scenario:**  
A colleague loaded `transactions.csv` into the staging layer and declared "it looks fine." Run a systematic quality audit across all six dimensions and produce a quality report.

**Tasks:**

**1a. Completeness.**  
Write a single SQL query that reports, for each column, the total row count, null count, and completeness percentage. Which columns have nulls? What is the business impact of each null?

**1b. Uniqueness.**  
Find all duplicate `transaction_id` values. How many duplicates exist? Write the deduplication SQL that keeps only the first occurrence (lowest `transaction_id` alphabetically — since there's no `loaded_at` timestamp in this file, use `ROWID` or `ctid` in PostgreSQL).

**1c. Validity — enumeration.**  
`status` must be one of: `pending`, `completed`, `refunded`, `cancelled`. Find all rows that violate this rule.

**1d. Validity — range.**  
`amount` must be greater than 0 for all products except `P110` (Gift Voucher, which has a reference price of 0). Find all amount violations.

**1e. Validity — date format.**  
`transaction_date` should be a valid date in `YYYY-MM-DD` format. Find any rows where the date cannot be parsed correctly. (Hint: look for `32024-01-26`.)

**1f. Validity — referential integrity.**  
Every `product_id` in transactions must exist in `products_reference.csv`. Find any orphan product references.

**1g. Validity — currency.**  
`currency` must be one of `AUD`, `USD`, `EUR`, `GBP`. Find violations. Then explain: is a `USD` transaction a data error or a legitimate edge case? What process would you use to decide?

**1h. Consistency — amount vs. unit price.**  
Using `products_reference.csv` as the authoritative unit price, verify that `amount = quantity * unit_price * (1 - discount_pct/100)` for each transaction (within $0.01 rounding). How many transactions fail this check?

**1i. Timeliness.**  
Write a query that would detect if today's data hasn't arrived. Simulate this by checking whether any `transaction_date` equals `CURRENT_DATE` (it won't — the data is historical). Show what the freshness alert query would look like.

**1j. Produce a quality scorecard.**  
Summarise your findings as a table:
```
Dimension       | Check                  | Rows Failing | Pass/Fail
Completeness    | customer_id not null   | ?            | ?
Uniqueness      | transaction_id unique  | ?            | ?
Validity        | status in allowed set  | ?            | ?
...
```

---

## Exercise 2 — Build the Quarantine Pipeline

**Concept:** SQL Validation Patterns

**Scenario:**  
Write a pipeline that separates good rows from bad rows and routes them to the right destination. Good rows go to `silver_transactions`; bad rows go to `quarantine_transactions` with a reason code.

**Tasks:**

**2a. Create the target tables:**

```sql
CREATE TABLE silver_transactions (
    transaction_id   TEXT,
    customer_id      TEXT,
    product_id       TEXT,
    amount           NUMERIC(12,2),
    currency         TEXT,
    status           TEXT,
    transaction_date DATE,
    store_id         TEXT,
    quantity         INT,
    discount_pct     NUMERIC(5,2)
);

CREATE TABLE quarantine_transactions (
    original_transaction_id TEXT,
    reason                  TEXT,
    raw_row                 TEXT,
    quarantined_at          TIMESTAMP DEFAULT NOW()
);
```

**2b. Write the INSERT into `silver_transactions`** that includes only rows passing ALL of:
- `transaction_id` matches pattern `TXN` + digits
- `customer_id` is not null
- `amount > 0`
- `status` in allowed set
- `transaction_date` is a valid date (exclude the malformed `32024-01-26` row)
- No duplicate `transaction_id` (keep one, discard the rest)

**2c. Write the INSERT into `quarantine_transactions`** for all rejected rows. Each row must have a clear `reason` string explaining why it failed. A row that fails multiple checks should have all reasons concatenated (e.g., `'null_customer_id|negative_amount'`).

**2d. Verify reconciliation:**
```sql
-- Source rows = Silver rows + Quarantine rows (no row is lost)
SELECT
    (SELECT COUNT(*) FROM transactions)            AS source_count,
    (SELECT COUNT(*) FROM silver_transactions)     AS silver_count,
    (SELECT COUNT(*) FROM quarantine_transactions) AS quarantine_count,
    (SELECT COUNT(*) FROM transactions) -
    (SELECT COUNT(*) FROM silver_transactions) -
    (SELECT COUNT(*) FROM quarantine_transactions) AS unaccounted;
-- unaccounted must be 0
```

**2e. Write the Write-Audit-Publish (WAP) wrapper** in SQL using a transaction and a DO block (PostgreSQL) that:
1. Loads to a `silver_transactions_staging` table
2. Runs two assertions (null check + uniqueness check) and raises an exception if either fails
3. Atomically renames staging → silver on success

---

## Exercise 3 — Write Unit Tests for Transformation Logic

**Concept:** Pipeline Testing Strategies

**Scenario:**  
Your pipeline has three transformation functions. Write unit tests that verify each one correctly — including edge cases and failure modes.

**3a. Test the `normalise_currency` function:**

```python
def normalise_currency(amount: float, currency: str, target: str = 'AUD') -> float:
    """Convert amount from currency to target currency using fixed rates."""
    rates_to_aud = {'AUD': 1.0, 'USD': 1.52, 'EUR': 1.65, 'GBP': 1.93}
    if currency not in rates_to_aud:
        raise ValueError(f"Unsupported currency: {currency}")
    if target != 'AUD':
        raise NotImplementedError("Only AUD target supported")
    return round(amount * rates_to_aud[currency], 2)
```

Write at least 5 tests covering:
- AUD input (no conversion)
- USD input (conversion applied)
- Unknown currency (error raised)
- Zero amount
- Negative amount (function does not validate sign — test that it passes through)

**3b. Test the `classify_transaction_size` function:**

```python
def classify_transaction_size(amount: float) -> str:
    """Classify transaction into Small / Medium / Large / Outlier buckets."""
    if amount < 0:
        raise ValueError("Amount must be non-negative")
    if amount < 50:
        return 'Small'
    elif amount < 200:
        return 'Medium'
    elif amount < 1000:
        return 'Large'
    else:
        return 'Outlier'
```

Write tests for: boundary values (exactly 50, exactly 200, exactly 1000), negative input, and zero.

**3c. Test a SQL deduplication transformation using DuckDB:**

```python
import duckdb

DEDUP_SQL = """
SELECT transaction_id, customer_id, amount
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) AS rn
    FROM transactions
) t
WHERE rn = 1
"""
```

Write two tests:
1. Input with one duplicate → output has no duplicates
2. Input with no duplicates → output is unchanged

**3d. Write a golden dataset test:**  
Define a 3-row "golden" input (you choose the values) and a 3-row expected output after the full validation pipeline runs. Write a test that runs the pipeline on the golden input and asserts the output matches expected exactly — comparing `transaction_id`, `amount`, and `status`.

---

## Exercise 4 — Anomaly Detection Queries

**Concept:** Anomaly Detection & Statistical Quality Checks

**Tasks:**

**4a. Z-score outlier detection on `amount`.**  
Write the SQL that computes Z-scores for all completed transactions and flags those with `|z| > 2`. Which transaction is the obvious outlier?

**4b. Volume anomaly check.**  
The transactions file covers Jan 15–27, 2024. Write a query that:
1. Counts transactions per day
2. Computes the mean and standard deviation of daily counts (excluding today)
3. Flags any day where the count is more than 2 standard deviations from the mean

Is any day anomalous?

**4c. Status distribution drift.**  
Write a query that compares the status distribution for January 15–20 (baseline period) against January 21–27 (current period). Are there any statuses whose percentage shifts by more than 10 percentage points?

**4d. Referential integrity drift.**  
An `amount` of `$9,999,999` passed all validity rules (positive, valid currency, valid status). Write a query that would detect this as an outlier using the IQR (Interquartile Range) method:
```
Lower fence = Q1 - 1.5 * IQR
Upper fence = Q3 + 1.5 * IQR
Flag rows outside [Lower fence, Upper fence]
```

Which transactions are flagged as IQR outliers?

---

## Exercise 5 — Write a Data Contract

**Concept:** Data Contracts & Schema Enforcement

**Tasks:**

**5a. Write a YAML data contract** for `silver_transactions` based on what you have learned from the data in Exercises 1–4. Your contract must include:
- Schema section: all 10 columns with type, nullable, and any constraints
- At least 3 quality SLA entries (freshness, completeness, row count)
- A `changelog` section with at least 2 hypothetical versions

**5b. Implement the contract as a Python validation function:**  
Write `validate_against_contract(df: pd.DataFrame) -> dict` that:
- Checks all nullability rules
- Checks all allowed-value sets
- Checks the amount range
- Returns `{"passed": bool, "violations": [{"column": ..., "rule": ..., "failing_rows": N}]}`

**5c. Breaking vs. non-breaking changes.**  
For each of the following proposed changes to the contract, state whether it is breaking or non-breaking and explain why:
1. Add a new nullable column `channel` (TEXT)
2. Rename `store_id` to `location_id`
3. Change `amount` type from `NUMERIC(12,2)` to `NUMERIC(15,4)`
4. Remove `discount_pct` column
5. Add `'gift'` to the allowed values for `status`
6. Change `customer_id` from nullable to NOT NULL

**5d. Schema enforcement query.**  
Write a SQL query that validates an incoming batch of transactions against the contract — producing one row per violation, with columns: `transaction_id`, `column_name`, `rule_violated`, `actual_value`.

---

## Bonus Challenge — Full Quality Pipeline

Using `transactions.csv`:

1. Run the full six-dimension audit (Exercise 1) and document every issue found
2. Build the quarantine pipeline (Exercise 2) — route bad rows, verify reconciliation = 0 unaccounted
3. Write unit tests for at least 2 transformation functions (Exercise 3)
4. Detect the Z-score outlier and the IQR outlier (Exercise 4) — are they the same row?
5. Write the data contract YAML (Exercise 5a)
6. Run the contract validation function against the original raw data — how many violations?
7. Run the contract validation function against your cleaned Silver data — it should pass
8. Write a paragraph explaining: if this pipeline ran in production daily, which quality dimension failure would be the hardest to detect automatically, and why?
