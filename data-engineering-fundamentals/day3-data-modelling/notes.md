# Day 3 — Data Modelling

## Overview

Data modelling is the discipline of structuring data so it is easy to query, understand, and trust. A well-modelled dataset answers business questions in one or two joins; a poorly modelled one requires six joins, produces wrong aggregates, and is rebuilt every six months. Day 3 covers five core modelling concepts that every data engineer must know — from dimensional modelling and the Star Schema to slowly changing dimensions, data vault, and the modern dbt-driven approach.

**The 5 concepts:**
1. Dimensional Modelling & Star Schema
2. Slowly Changing Dimensions (SCD)
3. Data Vault Modelling
4. One Big Table (OBT) & Denormalisation Patterns
5. dbt — Data Build Tool & Modelling in Practice

---

## Concept 1: Dimensional Modelling & Star Schema

### What is dimensional modelling?

**Dimensional modelling** (Ralph Kimball, 1996) is a design technique for analytical databases that organises data into two types of tables:

- **Fact tables** — store measurable, numeric business events (sales, clicks, payments). Each row is one event.
- **Dimension tables** — store the context of those events (who, what, where, when). Each row describes an entity.

The goal is to make analytical queries simple, fast, and business-readable.

### Star Schema

In a **Star Schema**, one central fact table is surrounded by dimension tables — the shape resembles a star.

```
                  dim_customer
                  (customer_id PK,
                   customer_name,
                   country,
                   segment)
                        |
                        |
dim_product ————— fact_sales ————— dim_date
(product_id PK,  (order_id PK,    (date_id PK,
 product_name,    date_id FK,      date,
 category,        customer_id FK,  month,
 brand)           product_id FK,   quarter,
                  quantity,        year,
                  revenue,         day_of_week)
                  discount)
                        |
                        |
                  dim_store
                  (store_id PK,
                   store_name,
                   city,
                   region)
```

**Fact table characteristics:**
- Narrow primary key (surrogate or composite of FKs)
- Mostly numeric measures: `revenue`, `quantity`, `duration_seconds`
- High row count — billions of rows is normal
- Append-only (new events are inserted, not updated)

**Dimension table characteristics:**
- Wide — many descriptive text columns
- Low-to-medium row count (thousands to millions)
- Queried for filtering and grouping: `WHERE country = 'AU'`, `GROUP BY category`

### Why not just use the normalised OLTP schema?

An OLTP schema for orders might have 8 tables: `orders`, `order_items`, `customers`, `addresses`, `products`, `categories`, `promotions`, `stores`. A simple revenue-by-category query requires 6+ joins and is slow on 100M rows.

In a star schema, the same query is:
```sql
SELECT p.category, SUM(f.revenue)
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category;
```

One join. The denormalised dimension table already has `category` on every product row.

### Grain: the most important decision in fact table design

The **grain** defines what one row in the fact table represents. You must declare it before designing any columns.

| Grain | One row = | Revenue column means |
|---|---|---|
| Order line item | One product in one order | Revenue for that line |
| Daily sales summary | One product per store per day | Total daily revenue |
| Order header | One entire order | Total order value |

**Rule:** Choose the finest grain that business questions require. You can always aggregate up; you cannot disaggregate down.

### Snowflake Schema (normalised dimensions)

A **Snowflake Schema** normalises dimension tables further — `dim_product` has a FK to `dim_category`, which has a FK to `dim_department`. This saves storage but requires extra joins.

**Recommendation:** Use Star Schema for analytics. The storage savings of Snowflake Schema are irrelevant at modern column-store costs; the extra joins add query complexity with no benefit.

### Fact table types

| Type | Description | Example |
|---|---|---|
| Transaction fact | One row per atomic event | Each sale, each click, each login |
| Periodic snapshot | One row per entity per time period | Account balance every month-end |
| Accumulating snapshot | One row per lifecycle instance, updated as it progresses | Order row updated through placed → shipped → delivered |

---

## Concept 2: Slowly Changing Dimensions (SCD)

### The problem: dimensions change over time

A customer moves from Sydney to Melbourne. A product changes its category. A sales rep is reassigned to a new region. If you overwrite the old value in the dimension table, historical fact rows now point to wrong context — revenue that was sold in Sydney is now attributed to Melbourne.

**Slowly Changing Dimensions (SCD)** is a set of strategies for handling changes to dimension attributes while preserving correct historical reporting.

### SCD Type 1 — Overwrite (no history)

Just update the column. The old value is permanently lost.

```sql
UPDATE dim_customer
SET city = 'Melbourne'
WHERE customer_id = 'C001';
```

**Use when:** The old value was wrong (data fix), not changed. Never use when history matters.

**Side effect:** All historical fact rows for C001 now report Melbourne revenue — even sales made while the customer was in Sydney.

### SCD Type 2 — Add a new row (full history)

Insert a new dimension row for every change. The old row is closed by setting an end date and `is_current = FALSE`.

```sql
-- Current state of dim_customer for C001:
-- customer_key | customer_id | city    | valid_from | valid_to   | is_current
--     1001     |    C001     | Sydney  | 2023-01-01 | 2024-06-14 | FALSE
--     1045     |    C001     | Melbourne | 2024-06-15 | 9999-12-31 | TRUE

-- When C001 moves to Melbourne:
-- 1. Close the old row
UPDATE dim_customer
SET valid_to = '2024-06-14', is_current = FALSE
WHERE customer_id = 'C001' AND is_current = TRUE;

-- 2. Insert the new row
INSERT INTO dim_customer (customer_id, city, valid_from, valid_to, is_current)
VALUES ('C001', 'Melbourne', '2024-06-15', '9999-12-31', TRUE);
```

**Fact table join with SCD Type 2:**
```sql
-- Revenue correctly attributed to Sydney for historical sales
SELECT c.city, SUM(f.revenue)
FROM fact_sales f
JOIN dim_customer c
  ON f.customer_id = c.customer_id
  AND f.order_date BETWEEN c.valid_from AND c.valid_to
GROUP BY c.city;
```

**Characteristics:**
- Surrogate key (`customer_key`) is generated for each version — fact table stores the surrogate key of the version that was current at the time of the event
- `valid_from` / `valid_to` or `is_current` flag for identifying the active row
- Enables accurate "as-of" reporting

### SCD Type 3 — Add a column (limited history)

Add a new column for the previous value. Only one version of history is stored.

```sql
ALTER TABLE dim_customer ADD COLUMN previous_city TEXT;

UPDATE dim_customer
SET previous_city = city, city = 'Melbourne'
WHERE customer_id = 'C001';
```

**Limitation:** You can compare current vs. previous, but not current vs. 2 years ago. Rarely used in practice.

### SCD Type 4 — History table

Keep the main dimension with current values only. Maintain a separate history table for all changes.

```sql
-- dim_customer: current values only (fast lookups)
-- dim_customer_history: all historical versions with timestamps
INSERT INTO dim_customer_history
SELECT *, NOW() as changed_at FROM dim_customer WHERE customer_id = 'C001';

UPDATE dim_customer SET city = 'Melbourne' WHERE customer_id = 'C001';
```

**Use when:** Dimension is queried mostly for current values, but occasional history lookups are needed.

### Choosing SCD type

| Scenario | SCD type |
|---|---|
| Data correction (value was just wrong) | Type 1 |
| Slowly changing attribute, history required for reporting | Type 2 |
| Two-period comparison only (current vs previous) | Type 3 |
| Mostly current lookups, occasional history | Type 4 |
| High-frequency changes (price changes hourly) | Periodic snapshot fact instead of SCD |

---

## Concept 3: Data Vault Modelling

### What is Data Vault?

**Data Vault** (Dan Linstedt, 2000) is a modelling methodology designed for enterprise data warehouses that need to:
- Integrate data from many heterogeneous source systems
- Handle schema changes without breaking existing models
- Maintain a full audit trail of every data load
- Load data in parallel without cross-table dependencies

Where dimensional modelling is optimised for query readability, Data Vault is optimised for **load flexibility and auditability**.

### The three building blocks

**1. Hub** — stores unique business keys from the source system. One row per distinct entity.

```sql
CREATE TABLE hub_customer (
    hub_customer_hk   CHAR(32) PRIMARY KEY,   -- hash of customer_id
    customer_id       TEXT     NOT NULL,        -- business key from source
    load_date         TIMESTAMPTZ NOT NULL,
    record_source     TEXT     NOT NULL         -- which system this came from
);
```

**2. Link** — stores relationships between hubs (many-to-many). One row per unique relationship instance.

```sql
CREATE TABLE link_order_customer (
    link_order_customer_hk  CHAR(32) PRIMARY KEY,  -- hash of (order_id, customer_id)
    hub_order_hk            CHAR(32) NOT NULL,
    hub_customer_hk         CHAR(32) NOT NULL,
    load_date               TIMESTAMPTZ NOT NULL,
    record_source           TEXT        NOT NULL
);
```

**3. Satellite** — stores descriptive attributes and their history. Multiple satellites per hub or link, one per subject area or source system.

```sql
CREATE TABLE sat_customer_details (
    hub_customer_hk  CHAR(32)    NOT NULL,
    load_date        TIMESTAMPTZ NOT NULL,
    load_end_date    TIMESTAMPTZ,              -- NULL = current record
    record_source    TEXT        NOT NULL,
    hash_diff        CHAR(32)    NOT NULL,     -- hash of all attributes; skip insert if unchanged
    customer_name    TEXT,
    email            TEXT,
    city             TEXT,
    PRIMARY KEY (hub_customer_hk, load_date)
);
```

### How Data Vault handles change

When a customer's email changes:
1. The **Hub** is not touched — the business key (`customer_id`) did not change
2. A new row is inserted into **sat_customer_details** with a new `load_date`
3. The previous row's `load_end_date` is set to close it

This is similar to SCD Type 2 but split by satellite (you can track email history and address history independently in separate satellites).

### Data Vault architecture

```
Raw Vault (exact copy of source, no transformations)
    ↓
Business Vault (business rules applied on top of Raw Vault)
    ↓
Information Mart (dimensional model or flat tables for BI tools)
```

### Data Vault vs. Dimensional Modelling

| Dimension | Data Vault | Dimensional / Star Schema |
|---|---|---|
| Primary goal | Auditability, parallel loading, source integration | Query performance, business readability |
| History handling | Native (satellites are append-only) | SCD Type 2 on dimension tables |
| Schema changes | Add a new satellite — existing loads unaffected | Alter dimension table — may break pipelines |
| Query complexity | High (many joins: hub + link + satellite) | Low (fact + dimension, 1–2 joins) |
| Best for | Large enterprises, many source systems, regulatory audit | Departmental analytics, self-service BI |
| Query layer | Always needs a dimensional presentation layer on top | Is the presentation layer |

**Practical advice:** Data Vault is rarely used as-is for BI. It is the raw/integration layer; a dimensional model or flat tables are still built on top for analysts.

---

## Concept 4: One Big Table (OBT) & Denormalisation Patterns

### What is One Big Table?

**One Big Table (OBT)** is the most aggressive form of denormalisation: all relevant dimension attributes are pre-joined and written into a single wide flat table alongside the fact measures. There are no joins at query time.

```sql
-- Traditional star schema query (2 joins)
SELECT p.category, d.month, SUM(f.revenue)
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_date d    ON f.date_id = d.date_id
GROUP BY p.category, d.month;

-- OBT query (zero joins)
SELECT product_category, order_month, SUM(revenue)
FROM obt_sales
GROUP BY product_category, order_month;
```

### When OBT makes sense

| Scenario | OBT appropriate? |
|---|---|
| BI tool used by non-SQL analysts (Tableau, Metabase) | Yes — they can't write joins |
| Sub-second query SLA on a large dataset | Yes — no join overhead |
| Dimensions change frequently (SCD) | No — every change requires rebuilding the entire OBT |
| Multiple fact tables sharing dimensions | No — redundant; each OBT duplicates all dimension columns |
| Exploration / ad-hoc analytics | Depends — OBT answers known questions fast, but poorly supports novel questions |

### How to build an OBT

```sql
CREATE TABLE obt_sales AS
SELECT
    -- Fact measures
    f.order_id,
    f.revenue,
    f.quantity,
    f.discount,
    -- Date attributes
    d.order_date,
    d.month          AS order_month,
    d.quarter        AS order_quarter,
    d.year           AS order_year,
    d.day_of_week,
    -- Customer attributes
    c.customer_name,
    c.city           AS customer_city,
    c.country        AS customer_country,
    c.segment        AS customer_segment,
    -- Product attributes
    p.product_name,
    p.category       AS product_category,
    p.brand          AS product_brand,
    -- Store attributes
    s.store_name,
    s.region         AS store_region
FROM fact_sales f
JOIN dim_date     d ON f.date_id     = d.date_id
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product  p ON f.product_id  = p.product_id
JOIN dim_store    s ON f.store_id    = s.store_id;
```

### Storage cost of denormalisation

In a columnar format (Parquet), duplicated string columns compress well because repeated values (e.g., `product_category = 'Electronics'` across 10M rows) are dictionary-encoded. The storage penalty for denormalisation in a Parquet-based lake is much smaller than in a row-oriented database.

**Rule of thumb for modern lakehouses:** Prefer pre-joining dimension attributes into wide fact tables (partial OBT) rather than forcing BI tools to join. Reserve full normalisation for the Silver layer; denormalise in Gold.

### Denormalisation patterns in practice

| Pattern | Description | Use case |
|---|---|---|
| Pre-joined flat table (OBT) | All dimensions collapsed into fact | BI tools, self-service analytics |
| Nested/repeated fields | Dimension stored as a JSON/struct column | Avoid fan-out for one-to-many relationships in BigQuery / Spark |
| Aggregated rollup table | Pre-computed GROUP BY results | Dashboard queries that run every second |
| Materialised views | Auto-refreshed derived tables | Snowflake / BigQuery: transparent acceleration |

---

## Concept 5: dbt — Data Build Tool & Modelling in Practice

### What is dbt?

**dbt (data build tool)** is an open-source SQL-first transformation framework that brings software engineering practices (version control, testing, documentation, modularity) to data modelling inside a warehouse or lakehouse.

dbt does **T** in ELT — it does not move data; it transforms data that is already in the warehouse using SQL `SELECT` statements. dbt compiles those SELECTs into `CREATE TABLE AS` or `CREATE VIEW AS` DDL and runs them in dependency order.

### The dbt project structure

```
my_dbt_project/
├── dbt_project.yml          -- project config: name, version, model materialisation defaults
├── profiles.yml             -- connection credentials (usually in ~/.dbt/)
├── models/
│   ├── staging/             -- thin wrappers over raw source tables (1:1 with sources)
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   └── _stg_sources.yml -- source definitions + freshness tests
│   ├── intermediate/        -- joins, business logic (not exposed to BI)
│   │   └── int_orders_enriched.sql
│   └── marts/               -- final dimensional or OBT models for BI consumption
│       ├── fct_sales.sql
│       ├── dim_customer.sql
│       └── _marts_schema.yml -- column-level tests and documentation
├── tests/
│   └── assert_revenue_positive.sql  -- custom SQL tests
├── macros/
│   └── generate_surrogate_key.sql   -- reusable Jinja macros
└── seeds/
    └── country_codes.csv            -- small static reference tables
```

### A dbt model: from staging to mart

**Staging model** (`stg_orders.sql`) — clean and rename columns; no business logic:
```sql
-- models/staging/stg_orders.sql
SELECT
    order_id,
    customer_id,
    CAST(order_date AS DATE)         AS order_date,
    CAST(amount AS NUMERIC(10,2))    AS amount,
    LOWER(TRIM(status))              AS status,
    COALESCE(currency, 'USD')        AS currency,
    _loaded_at                        AS source_loaded_at
FROM {{ source('raw', 'orders') }}
WHERE order_id IS NOT NULL
```

**Fact model** (`fct_sales.sql`) — joins, business logic, measures:
```sql
-- models/marts/fct_sales.sql
SELECT
    {{ dbt_utils.generate_surrogate_key(['o.order_id', 'oi.line_item_id']) }}
        AS sales_key,
    o.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price       AS revenue,
    oi.discount_pct,
    oi.quantity * oi.unit_price * (1 - oi.discount_pct / 100) AS net_revenue
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_order_items') }} oi ON o.order_id = oi.order_id
```

The `{{ ref('stg_orders') }}` call is critical — it tells dbt that `fct_sales` depends on `stg_orders`. dbt builds a DAG from these references and runs models in the correct order.

### Materialisation types

| Type | DDL dbt runs | Best for |
|---|---|---|
| `view` | `CREATE VIEW AS SELECT ...` | Staging models — no storage cost, always fresh |
| `table` | `CREATE TABLE AS SELECT ...` | Marts — fast to query, rebuilt on each `dbt run` |
| `incremental` | `INSERT INTO ... WHERE updated_at > last_run` | Large fact tables — only process new rows |
| `ephemeral` | Injected as CTE in downstream model | Intermediate logic not worth persisting |

**Incremental model example:**
```sql
-- models/marts/fct_sales.sql
{{ config(materialized='incremental', unique_key='sales_key') }}

SELECT ...
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_order_items') }} oi ON o.order_id = oi.order_id

{% if is_incremental() %}
  WHERE o.order_date >= (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

On the first run: full table build. On subsequent runs: only rows newer than the current max are processed and merged via `unique_key`.

### dbt tests

dbt has built-in tests that run after every build:

```yaml
# models/marts/_marts_schema.yml
models:
  - name: fct_sales
    columns:
      - name: sales_key
        tests:
          - unique
          - not_null
      - name: revenue
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: customer_id
        tests:
          - relationships:
              to: ref('dim_customer')
              field: customer_id
```

These compile to SQL `SELECT` queries that assert zero rows. If any row violates the constraint, `dbt test` fails — just like a unit test in application code.

### dbt lineage graph

Because every model uses `{{ ref() }}`, dbt can generate a complete **lineage DAG**:

```
raw.orders ──► stg_orders ──► int_orders_enriched ──► fct_sales
raw.order_items ──► stg_order_items ──────────────────────────►┘
raw.customers ──► stg_customers ──► dim_customer
raw.products ──► stg_products ──► dim_product
```

This DAG is explorable in dbt Cloud's UI or locally with `dbt docs generate && dbt docs serve`.

### dbt run commands

```bash
dbt run                              # build all models
dbt run --select fct_sales           # build one model
dbt run --select +fct_sales          # build fct_sales and all its ancestors
dbt run --select fct_sales+          # build fct_sales and all its descendants
dbt test                             # run all tests
dbt build                            # run + test in one command
dbt docs generate && dbt docs serve  # generate and serve lineage + docs
dbt source freshness                 # check if source tables are stale
```

### Why dbt matters for data engineers

| Without dbt | With dbt |
|---|---|
| Transformation logic in stored procedures or ad-hoc scripts | SQL models in version-controlled `.sql` files |
| No dependency management — run order is manual | DAG-based dependency resolution — correct order guaranteed |
| No tests — data quality discovered by analysts | Built-in column-level tests run after every build |
| Documentation is a spreadsheet someone forgot to update | Auto-generated docs from model code and YAML |
| "Just re-run the whole pipeline" | `--select` and `--exclude` for surgical rebuilds |

---

## Summary

| Concept | Core idea | Key interview term |
|---|---|---|
| Dimensional Modelling & Star Schema | Facts + dimensions, one grain per fact table | Grain, surrogate key, conformed dimension |
| Slowly Changing Dimensions (SCD) | How to handle attribute changes without losing history | SCD Type 2, valid_from / valid_to, surrogate key |
| Data Vault | Hub-Link-Satellite for auditable enterprise integration | Hash key, satellite, record source |
| One Big Table & Denormalisation | Pre-join dimensions into wide flat tables for query speed | OBT, fan-out, columnar compression |
| dbt | SQL models, DAG-based dependencies, built-in tests | ref(), materialisation, incremental model |
