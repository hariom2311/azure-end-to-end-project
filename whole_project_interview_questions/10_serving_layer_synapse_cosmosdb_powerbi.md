# Topic 10 — Serving Layer: Synapse, Cosmos DB & Power BI
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 2 Questions | 2–8 years experience | Scenario-based

---

### Q25. The mobile app needs to show live charging session progress with <2 second latency. Why did you use Cosmos DB instead of Synapse Analytics for this?

**Answer:**

**Synapse Analytics limitations for low-latency APIs:**

| Metric | Synapse Serverless | Synapse Dedicated Pool | Cosmos DB |
|---|---|---|---|
| Query latency | 1–30 seconds (cold) | 50ms–2 seconds (if cached) | <10ms (indexed document) |
| Concurrent connections | Limited (burst to ~100) | Hundreds with reservation units | Millions (globally distributed) |
| Pricing model | Per TB scanned | Fixed capacity (DTUs) | Per RU/s (request units) |
| Read pattern | Ad-hoc SQL | SQL | Document lookup by key |

The mobile app pattern is: `GET /api/session/S-10042` → return one session document. This is a key lookup, not a complex SQL aggregation. Cosmos DB is purpose-built for this: partition the `live_station_status` collection by `station_id`, and a read is a single partition lookup — single-digit millisecond latency.

**Architecture in VoltGrid:**

```
Gold Delta (Databricks)
    → ADF Copy (every 5 minutes)
    → Cosmos DB collections:
        - live_station_status    (active sessions + charger availability)
        - session_live           (per-session progress: kWh, ETA, status)
        - charger_availability   (available/in-use/offline per charger)
        - customer_history       (last 10 sessions per customer)
```

**Why not Redis / Azure Cache:**
Cosmos DB provides persistent storage + global replication (if needed) + built-in partitioning. Redis is a cache with TTL — if the pod restarts, data is lost until Gold refreshes it again. For billing-related data (session kWh delivered), persistence is required.

**Power BI still uses Synapse:**
Synapse serves complex analytical queries (revenue by state, YoY comparisons) where a 2-second latency is acceptable. Power BI Import mode caches aggregated mart data in-memory — most dashboard tiles load in <1 second because the data is already loaded into VertiPaq.

**Serving layer decision matrix:**

| Use case | Tool | Reason |
|---|---|---|
| Mobile app: live session status | Cosmos DB | Key lookup, <2 sec SLA |
| Power BI: revenue by state/month | Synapse + Import mode | Complex joins, pre-aggregated mart |
| Power BI: live charging map | Synapse DirectQuery | Near-real-time, tolerates 2 sec |
| Ad-hoc SQL queries by analysts | Synapse Serverless | No dedicated pool cost |

---

### Q26. A franchise owner in Queensland is complaining that Power BI shows them data for stations in NSW. What went wrong, and how do you fix it?

**Answer:**

**Root cause diagnosis:**

Row-Level Security (RLS) in Power BI is applied via DAX filter expressions and user role assignments. The most common failure modes:

**Failure Mode 1 — RLS role not assigned:**
The franchise owner's email is in the Power BI workspace but not assigned to the `FranchiseOwner` role:
```
Power BI Workspace → Semantic model → Security → FranchiseOwner role → Members
```
Check: is `qld-franchise-owner@voltgrid.com.au` listed? If not — that's the bug.

**Failure Mode 2 — Wrong DAX filter in the role:**
The role definition uses the wrong column:
```dax
-- Wrong: comparing state_name instead of franchise email
[state_name] = USERPRINCIPALNAME()

-- Correct for franchise owner:
DimFranchisePartner[owner_email] = USERPRINCIPALNAME()
```

**Failure Mode 3 — RLS not propagated to Synapse:**
If Power BI uses DirectQuery to Synapse, Synapse-level RLS must also be configured. If it's only set in Power BI (not Synapse), and someone queries Synapse directly via SQL, they bypass the filter.

**Fix for this scenario:**

1. Check the user's role membership in Power BI Security settings.
2. Verify the `DimFranchisePartner` table has `owner_email` populated correctly for Queensland stations.
3. Correct the DAX filter for `FranchiseOwner` role:
```dax
[partner_email] = USERPRINCIPALNAME()
```
4. Test: Power BI → "View As Role" → enter the franchise owner's email → confirm only Queensland stations appear.
5. For Synapse: add their email to the `franchise_rls_group` table and verify the RLS predicate is filtering correctly.

**Power BI RLS role hierarchy in VoltGrid:**

| Role | DAX Filter | Sees |
|---|---|---|
| Executive | (none — all data) | All Australia |
| StateManager | `DimState[state_name] = LOOKUPVALUE(...)` | Own state only |
| FranchiseOwner | `DimFranchisePartner[owner_email] = USERPRINCIPALNAME()` | Own stations only |

**Prevention:** Add an automated test that logs in as a test franchise owner account and asserts the row count matches only their assigned stations. Run this test on every Power BI model deployment.
