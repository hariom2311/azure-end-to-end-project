# Topic 06 — Gold Layer: Dimensional Modelling & Star Schema
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 2 Questions | 2–8 years experience | Scenario-based

---

### Q16. Explain the star schema design for FactChargingSession. Why did you choose a star schema over a flat denormalised table for Power BI?

**Answer:**

**FactChargingSession structure:**

```
FactChargingSession (grain: one row per charging session)
├── session_key               (surrogate PK)
├── customer_key              (FK → DimCustomer)
├── station_key               (FK → DimStation)
├── charger_key               (FK → DimCharger)
├── vehicle_key               (FK → DimVehicle)
├── tariff_key                (FK → DimTariff — SCD2 snapshot at session time)
├── time_key                  (FK → DimTime)
├── energy_kwh
├── duration_min
├── session_status
├── expected_amount           (energy_kwh × tariff_rate)
├── billed_amount             (from FactPayments)
├── reconciliation_status     (MATCH / MISMATCH)
└── difference_amount
```

**Why star schema over flat denormalised:**

**1. Query performance in Power BI:**
Power BI uses a columnar in-memory engine (VertiPaq). Star schemas with narrow fact tables compress extremely well — the `session_key`, `customer_key` integer foreign keys are tiny vs storing full customer name/address in every row. A flat table with 50K sessions × 30 customer columns = 1.5M values. Star schema: 50K FK integers in the fact + 5K rows in DimCustomer.

**2. Single point of truth for dimensions:**
If a customer's loyalty tier changes, you update DimCustomer (one row). In a flat table, you'd need to update 500 session rows for that customer — expensive and error-prone.

**3. Flexible drill-down without extra joins:**
Power BI's auto-relationship detection builds the drill-down hierarchy automatically from star schema FKs:
`Australia → NSW → Sydney → Station-101 → Charger-42 → Session-level`
This is the exact geo hierarchy needed for all 14 dashboards.

**4. SCD2 correctness:**
The `tariff_key` in FactChargingSession points to the specific DimTariff version effective at session time. If you denormalise, you'd store the rate as a column in the fact — correct, but you lose the ability to query "how many sessions used the old tariff vs the new tariff" with a simple DimTariff filter.

**Trade-off of star schema:** Queries require joins. Mitigation: pre-aggregated marts (`mart_revenue_by_geo_month`) pre-join and aggregate for the most common Power BI queries — these run in Import mode, no join cost at report time.

---

### Q17. How did you calculate the billing reconciliation metric, and what does a MISMATCH tell the business?

**Answer:**

**Reconciliation formula:**

```python
# Applied at FactChargingSession grain
fact_df = fact_df.withColumn(
    "expected_amount",
    col("energy_kwh") * col("tariff_rate_per_kwh")  # from DimTariff join
).withColumn(
    "reconciliation_status",
    when(
        abs(col("billed_amount") - col("expected_amount")) <= 0.01,  # 1-cent tolerance
        lit("MATCH")
    ).otherwise(lit("MISMATCH"))
).withColumn(
    "difference_amount",
    col("billed_amount") - col("expected_amount")
)
```

**What a MISMATCH means:**

Three root causes possible:

| Cause | Example | `difference_amount` |
|---|---|---|
| Wrong tariff rate applied | Session used peak rate but was off-peak | Positive (customer overcharged) |
| Energy meter miscalibrated | Meter reported 15.2 kWh, actual was 14.8 kWh | Small negative |
| Payment gateway rounding | Gateway rounds to 2 decimal places, VoltGrid uses 4 | Small (<$0.01) |

**Business impact:**

The `mart_billing_reconciliation` mart aggregates MISMATCHes by station and franchise partner. If Station-101 in Sydney has 12% MISMATCH rate (industry threshold is <1%), it indicates either:
- A meter calibration issue → maintenance alert
- A tariff configuration error → operations team fix
- A payment gateway integration bug → tech team fix

The Finance dashboard shows: `Total revenue leakage = SUM(difference_amount WHERE reconciliation_status = 'MISMATCH')`. For VoltGrid with 50K sessions, even $0.50 average leakage = $25,000 in unrecovered revenue — this was one of the core business problems the platform solved.
