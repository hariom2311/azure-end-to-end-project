# Topic 09 — Unity Catalog & Data Governance
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 2 Questions | 2–8 years experience | Scenario-based

---

### Q23. Explain the 4-level namespace in Databricks Unity Catalog and how it's used in this project.

**Answer:**

Unity Catalog uses:
```
metastore  .  catalog  .  schema  .  table
```

**In VoltGrid:**
```
dbw_ev_intelligence_dev . dbw_ev_intelligence_dev . bronze . charger_telemetry
        (metastore)              (catalog)          (schema)    (table)
```

**Why 4 levels:**

Before Unity Catalog, Databricks used Hive metastore per workspace: `schema.table`. Tables in the `dev` workspace were invisible to the `prod` workspace.

Unity Catalog sits above all workspaces. The 4-level name is globally unique within a Databricks account — two workspaces in the same region share one metastore. This enables:
- Data engineers in `dev` workspace and analysts in `prod` workspace to access the same Gold tables.
- Central governance — permissions set on `catalog.schema` apply to all workspaces using that metastore.

**SQL usage:**

```sql
-- Full reference (always works)
SELECT * FROM dbw_ev_intelligence_dev.gold.FactChargingSession;

-- Set defaults to use shorthand
USE CATALOG dbw_ev_intelligence_dev;
USE SCHEMA gold;
SELECT * FROM FactChargingSession;
```

**Permission example:**
```sql
GRANT SELECT ON SCHEMA dbw_ev_intelligence_dev.gold TO `grp-voltgrid-analysts`;
GRANT SELECT, MODIFY ON SCHEMA dbw_ev_intelligence_dev.silver TO `grp-data-engineers`;
DENY SELECT ON SCHEMA dbw_ev_intelligence_dev.bronze TO `grp-voltgrid-analysts`;
```

**Key Unity Catalog concepts in this project:**

| Object | Name | Purpose |
|---|---|---|
| Catalog | `dbw_ev_intelligence_dev` | Top-level namespace for all VoltGrid objects |
| Schema | `bronze`, `silver`, `gold` | Groups tables by medallion layer |
| External Volume | `bronze_volume` | Maps `abfss://bronze@evdatalakedev...` as `/Volumes/...` path |
| External Location | `evdatalakedev-bronze` | Registers the ADLS path with a storage credential |
| Storage Credential | `cred-ev-intelligence-dev` | Wraps the Access Connector Managed Identity |

**Data lineage:** Unity Catalog automatically captures: which notebook wrote which table, which table fed which downstream table. The Lineage tab in the Catalog UI shows the full upstream/downstream graph for any table — no additional configuration.

---

### Q24. What is a Storage Credential in Unity Catalog and why is it needed? What would break if you deleted it?

**Answer:**

**What it is:**

A Storage Credential is a Unity Catalog metadata object that wraps an Azure identity (the Managed Identity of an Access Connector) and stores it securely at the metastore level.

In this project: `cred-ev-intelligence-dev` wraps the Managed Identity of `ac-ev-intelligence-dev`.

**Why it's needed — the trust chain:**

```
Notebook accesses /Volumes/...
  → Unity Catalog checks: user has READ VOLUME privilege? YES
  → UC looks up which External Location covers this path
  → External Location references Storage Credential: cred-ev-intelligence-dev
  → Storage Credential uses Access Connector Managed Identity
  → Azure IAM confirms MI has Storage Blob Data Contributor on evdatalakedev
  → ADLS returns the file
```

Without the Storage Credential, Unity Catalog has no identity to present to ADLS. The External Location cannot be validated.

**What breaks if deleted:**

Every External Location referencing `cred-ev-intelligence-dev` becomes invalid:
- `evdatalakedev-bronze`, `evdatalakedev-silver`, `evdatalakedev-gold` External Locations all fail validation.
- All UC Volumes (`bronze_volume`, `silver_volume`, `gold_volume`) become inaccessible.
- Any notebook using `/Volumes/...` paths throws `IOException: Unable to access path`.
- Unity Catalog tables (Delta tables registered via External Locations) cannot be read.
- All ADF pipelines using UC-registered tables fail.

**Auth flow when a notebook reads a Volume:**

```
You click bronze_volume in Catalog UI
       ↓
Unity Catalog metadata lookup
  External Location: bronze → abfss://bronze@evdatalakedev.dfs.core.windows.net/
  Storage Credential: cred-ev-intelligence-dev
       ↓
Access Connector (ac-ev-intelligence-dev)
  Uses its System-Assigned Managed Identity
  Azure manages this identity — no secret exists
       ↓
Azure Entra ID
  Verifies the Managed Identity token automatically
  returns: OAuth bearer token
       ↓
ADLS Gen2 (evdatalakedev)
  checks: does ac-ev-intelligence-dev Managed Identity
          have Storage Blob Data Contributor? YES
  returns: directory listing
```

**Why Access Connector over Service Principal for Storage Credentials:**

Access Connector's Managed Identity has no secret to manage — Azure rotates it automatically. A Storage Credential backed by a Service Principal would require secret rotation, which would break all External Locations if the secret expired. This is why Databricks recommends Access Connector for Unity Catalog.
