# Topic 12 — SCD: Slowly Changing Dimensions & History
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 1 Question | 2–8 years experience | Scenario-based

---

### Q29. Explain the three most common SCD types and which ones are used in this project. Give a concrete scenario for each.

**Answer:**

**SCD Type 1 — Overwrite (no history):**

Current value replaces old value. No history kept.

Used for: `DimStation`, `DimCity`, `DimCharger` (for operational attributes).

Scenario: Station 101 changes its `site_type` from "Highway" to "Urban Commercial". The DimStation row is updated in place. Old sessions that happened when it was a "Highway" station will now show "Urban Commercial" when joined — this is acceptable because `site_type` is used for reporting categories, not financial calculations.

```python
# SCD1 — simple overwrite in MERGE
deltaTable.alias("target").merge(
    source_df.alias("source"),
    "target.station_id = source.station_id"
).whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute()
```

---

**SCD Type 2 — New row for each change (full history):**

A new row is inserted for each change. Old row is closed with `effective_to` and `is_current = FALSE`.

Used for: `DimTariff`, `DimCustomer` (loyalty tier), `DimCharger` (firmware version).

Scenario: Customer `C-10042` upgrades from `Silver` to `Gold` loyalty tier on July 15th.

```
customer_key | customer_id | loyalty_tier | effective_from | effective_to  | is_current
SK-001       | C-10042     | SILVER       | 2024-01-01     | 2025-07-14    | FALSE
SK-002       | C-10042     | GOLD         | 2025-07-15     | 9999-12-31    | TRUE
```

FactChargingSession stores `customer_key = SK-001` for sessions before July 15th and `customer_key = SK-002` for sessions after. Revenue analysis by loyalty tier is historically accurate.

```python
# SCD2 — close old row + insert new row
deltaTable.alias("target").merge(
    new_df.alias("source"),
    "target.customer_id = source.customer_id AND target.is_current = true"
).whenMatchedUpdate(
    condition="target.loyalty_tier != source.loyalty_tier",
    set={
        "effective_to": "source.effective_from - interval 1 day",
        "is_current": "false"
    }
).execute()

# Insert new current row separately
new_row_df.write.format("delta").mode("append").save(dim_customer_path)
```

**Gold join using effective date range:**
```python
fact_df.join(
    dim_customer_df,
    (fact_df.customer_id == dim_customer_df.customer_id) &
    (fact_df.session_date >= dim_customer_df.effective_from) &
    (fact_df.session_date <= dim_customer_df.effective_to)
)
```

---

**SCD Type 3 — Previous value column:**

Adds a `previous_value` column — only remembers the last change (not full history).

Not used in this project, but relevant as an example for: `DimCharger.previous_firmware_version`. If a charger was upgraded from firmware `v2.1` to `v2.3`, keeping `previous_firmware` helps correlate whether a fault rate change started with the firmware upgrade.

```
charger_id | firmware_version | previous_firmware | upgraded_on
CHG-0042   | v2.3             | v2.1              | 2025-06-01
```

Limitation: only keeps the last change. If the charger went v1 → v2.1 → v2.3, you only see v2.1 as previous — v1 is lost.

---

**When to choose which SCD type:**

| Scenario | SCD type | Reason |
|---|---|---|
| Attribute used in financial calculations (tariff rate) | SCD2 | Historical accuracy required |
| Attribute used in loyalty/discount calculations | SCD2 | Customer tier at time of session matters |
| Purely descriptive attribute (station phone number) | SCD1 | Historical accuracy not needed |
| Only need to know what changed last time | SCD3 | Lightweight, no extra rows |
| Full change history needed for compliance/audit | SCD2 | Regulator may ask for state at specific date |

**Why `9999-12-31` for open-ended `effective_to`:**
- Simplifies the join condition — you don't need `OR effective_to IS NULL`.
- Standard SCD2 pattern across the industry.
- Power BI can filter `is_current = TRUE` for current-state-only reports without a date range filter.
