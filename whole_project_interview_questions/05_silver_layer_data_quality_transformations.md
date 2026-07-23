# Topic 05 — Silver Layer: Data Quality & Transformations
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 3 Questions | 2–8 years experience | Scenario-based

---

### Q13. You implemented a quarantine pattern for bad records. How does it work end-to-end, and how do you alert the data team when quarantine volume spikes?

**Answer:**

**Quarantine routing logic:**

```python
from pyspark.sql.functions import col, lit, when, current_timestamp

def validate_and_split(df, entity_name):
    # Check 1: null mandatory fields
    null_mask = col("charger_id").isNull() | col("session_id").isNull()
    # Check 2: range violations
    range_mask = (col("energy_kwh") < 0) | (col("energy_kwh") > 500)
    # Check 3: referential integrity (charger_id exists in charger_master)

    bad_mask = null_mask | range_mask

    clean_df = df.filter(~bad_mask)
    quarantine_df = df.filter(bad_mask).withColumn(
        "rejection_reason",
        when(null_mask, "NULL_MANDATORY_FIELD")
        .when(range_mask, "OUT_OF_RANGE")
        .otherwise("REFERENTIAL_INTEGRITY")
    ).withColumn("entity_name", lit(entity_name)) \
     .withColumn("quarantine_ts", current_timestamp())

    return clean_df, quarantine_df

clean_df, quarantine_df = validate_and_split(raw_df, "charging_sessions")
quarantine_df.write.format("delta").mode("append").save("silver/quarantine/")
clean_df  # proceed to Silver MERGE
```

**DQ Metrics table:**

Every job run writes a summary to `gold/data_quality_audit/`:
```
entity_name | run_date    | total_rows | clean_rows | quarantine_rows | quarantine_pct
sessions    | 2025-06-15  | 10000      | 9980       | 20              | 0.20%
```

**Alerting on quarantine spike:**

Azure Monitor query (Log Analytics KQL):
```kql
customEvents
| where name == "silver_dq_audit"
| where toreal(customDimensions.quarantine_pct) > 5.0
| project timestamp, entity_name, quarantine_pct
```

Alert fires if `quarantine_pct > 5%` for any entity → email + Teams notification to data engineering team.

**In practice:** An alert fired when the fleet API changed its `distance_km` field from a float to a string (`"125.4"` instead of `125.4`). The range check flagged all fleet records as quarantined. The team caught it within 30 minutes and updated the Silver cast logic.

---

### Q14. Explain how you implemented PCI DSS-compliant masking for charge card data. What exactly is masked, why, and where does masking happen?

**Answer:**

**What gets masked and why:**

| Field | Raw Bronze value | Silver/Gold value | Reason |
|---|---|---|---|
| `card_number` | `4242424242424242` | `************4242` | PCI DSS: full PAN must never be stored in non-PCI-scoped systems |
| `cvv` | `123` | discarded (`cvv_masked = "***"`) | PCI DSS: CVV must never be stored at all, even encrypted |
| `card_expiry` | `12/27` | kept as-is | Expiry alone is not sensitive; needed for card validation |
| `card_token` | UUID from gateway | kept as-is | Stable identifier for joins — token replaces PAN |

**Where masking happens — Bronze → Silver transition:**

```python
from pyspark.sql.functions import regexp_replace, lit, col

df = df.withColumn(
    "card_number_masked",
    regexp_replace(col("card_number"), r"^\d{12}", "************")
).withColumn(
    "cvv_masked",
    lit("***")
).drop("card_number", "cvv")  # remove raw values from Silver
```

**Why Bronze keeps the raw card number:**

Bronze is a restricted zone — `bronze/crm/charge_cards_raw/` has a dedicated RBAC role that only 2 people have. This is acceptable because:
1. Bronze is the source replication layer — changing it would break the immutability guarantee.
2. The raw data is needed for forensic audit if a payment dispute arises.
3. Access is logged at the Azure Monitor level — all reads of the restricted Bronze zone are auditable.

**Key principle:** The masking happens at the Bronze → Silver boundary, not at query time. This means Silver and Gold tables never contain raw PANs or CVVs — even if a data engineer runs a direct Spark query on Silver, they cannot reconstruct the card number.

**`card_token` as join key:** All downstream joins use `card_token`, not `card_number`. This is the standard tokenisation pattern — the payment gateway holds the token↔PAN mapping, and VoltGrid's systems never need the PAN.

---

### Q15. Explain SCD Type 2 implementation for tariff rates. A tariff rate changed from $0.35/kWh to $0.42/kWh on July 1st. How do you ensure charging sessions from June use the old rate and sessions from July use the new rate?

**Answer:**

**SCD2 table structure in `sl_tariffs_scd2`:**

```
tariff_id | rate_per_kwh | peak_offpeak | effective_from | effective_to  | is_current
T001      | 0.35         | PEAK         | 2025-01-01     | 2025-06-30    | FALSE
T001      | 0.42         | PEAK         | 2025-07-01     | 9999-12-31    | TRUE
```

**When the tariff change arrives in Bronze:**

Silver transformation detects the rate change and:
1. Finds the current active row for `T001` (`is_current = TRUE`).
2. Closes it: `effective_to = 2025-06-30`, `is_current = FALSE`.
3. Inserts the new row: `rate_per_kwh = 0.42`, `effective_from = 2025-07-01`, `effective_to = 9999-12-31`, `is_current = TRUE`.

**PySpark MERGE for SCD2:**

```python
deltaTable.alias("target").merge(
    new_tariff_df.alias("source"),
    "target.tariff_id = source.tariff_id AND target.is_current = true"
).whenMatchedUpdate(
    condition="target.rate_per_kwh != source.rate_per_kwh",
    set={"effective_to": "source.effective_from - interval 1 day", "is_current": "false"}
).whenNotMatchedInsertAll(
).execute()

# Insert the new version separately
new_row_df.write.format("delta").mode("append").save(silver_tariff_path)
```

**Gold layer join — using effective dates:**

```python
# FactChargingSession joins DimTariff using the session date
fact_df.join(
    dim_tariff_df,
    (fact_df.tariff_id == dim_tariff_df.tariff_id) &
    (fact_df.session_date >= dim_tariff_df.effective_from) &
    (fact_df.session_date <= dim_tariff_df.effective_to)
)
```

A June 25th session gets `rate_per_kwh = 0.35`. A July 5th session gets `rate_per_kwh = 0.42`. Revenue calculations are historically accurate.

**Why `9999-12-31` for open-ended rows:**
- Simplifies the join condition — you don't need `OR effective_to IS NULL`.
- Standard SCD2 pattern across the industry.
