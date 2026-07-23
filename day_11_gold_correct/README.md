# Day 11 — Gold Layer (Correct)

Replaces the incorrect day_10, day_11, day_12 Gold notebooks that referenced non-existent
columns (`first_name`, `last_name`, `power_kw`, `start_time`, `end_time`, etc.).
All column names here are verified against the actual Silver Delta table schemas.

## Notebooks

| File | Purpose |
|---|---|
| `01_gold_dims_v1.ipynb` | Builds 11 dimension tables from Silver |
| `02_gold_facts_v1.ipynb` | Builds 5 fact tables from Silver |
| `03_gold_job_params_v2.ipynb` | Combined dims + facts with ADF widget params |
| `04_gold_full_incremental_v3.ipynb` | Full/incremental load support — **use this for ADF** |

## Gold tables built

### Dimensions (11)
| Table | Type | Natural Key |
|---|---|---|
| DimState | SCD1 | state_code |
| DimCity | SCD1 | city_id |
| DimStation | SCD1 | station_id |
| DimCharger | SCD2 | charger_id |
| DimCustomer | SCD2 | customer_id |
| DimVehicle | SCD1 | vehicle_id |
| DimEmployee | SCD1 | employee_id |
| DimPartner | SCD1 | partner_id |
| DimChargeCard | SCD1 | card_id |
| DimTariff | SCD2 | tariff_id |
| DimTime | Generated | time_key (hour grain, 2020–2030) |

### Facts (5)
| Table | Grain Key | Source Silver tables |
|---|---|---|
| FactChargingSession | session_id | sessions + payments |
| FactPayments | payment_id | payments |
| FactComplaints | complaint_id | complaints |
| FactMaintenance | event_id | maintenance_events |
| FactRealtimeSession | session_id | realtime/charging_sessions (blob) |

## Gold paths (Unity Catalog volumes)

```
/Volumes/dbw_ev_intelligence_dev/default/gold-volume/dims/<table>
/Volumes/dbw_ev_intelligence_dev/default/gold-volume/facts/<table>
```

## Write strategies

| Strategy | When used | How |
|---|---|---|
| SCD1 | Dims with no history needed | Delta MERGE on natural key |
| SCD2 | DimCharger, DimCustomer, DimTariff | Expire old row (`is_current=False`) + insert new version |
| Fact MERGE | All fact tables | MERGE on grain key |
| DimTime | Generated once | Skip on incremental if already exists |

## Widget params (`04_gold_full_incremental_v3`)

| Param | Values | Default |
|---|---|---|
| `load_type` | `full` / `incremental` | `incremental` |
| `pipeline_id` | any string | `manual` |
| `silver_base` | volume path | `/Volumes/.../silver-volume` |
| `gold_base` | volume path | `/Volumes/.../gold-volume` |

## Databricks notebook path

Upload `04_gold_full_incremental_v3.ipynb` to:
```
/Users/hariomsuryawanshi68258@gmail.com/end-to-end-18-days-project-notebooks/
  gold-ingestion-notebooks/api-response-gold-layer-notebooks/04_gold_full_incremental_v3
```

## Silver columns used (verified)

All column names match the actual Bronze JSON keys confirmed by `scan_bronze_schemas.py`.
No invented columns — every join key, metric, and dimension attribute exists in Silver.
