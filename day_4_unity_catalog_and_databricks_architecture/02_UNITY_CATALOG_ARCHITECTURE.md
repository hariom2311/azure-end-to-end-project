# 02 — Unity Catalog Architecture
**Day 4 | The governance layer on top of Databricks**

---

## What Unity Catalog Is (Plain English)

Unity Catalog (UC) is Databricks' **centralised governance system**. It answers three questions for every piece of data in your lakehouse:

1. **Where is it?** — the 4-level namespace (`metastore.catalog.schema.table`)
2. **Who can access it?** — fine-grained permissions (column-level, row-level)
3. **Where did it come from?** — data lineage (which notebook wrote this table, which table fed that dashboard)

Before Unity Catalog, each Databricks workspace had its own isolated Hive metastore — tables in workspace A were invisible to workspace B, and there was no central permission management. Unity Catalog solves this by sitting above all workspaces.

---

## Full Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           UNITY CATALOG METASTORE                                   │
│                    (one per Azure region — shared across workspaces)                │
│                    Name: dbw_ev_intelligence_dev                                    │
│                                                                                     │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │                            CATALOG                                           │   │
│  │                   (top-level namespace — like a database server)             │   │
│  │                   Name: dbw_ev_intelligence_dev                              │   │
│  │                                                                              │   │
│  │  ┌───────────────────────────────────────────────────────────────────────┐  │   │
│  │  │                          SCHEMA (DATABASE)                            │  │   │
│  │  │            (groups tables and volumes — like a database)              │  │   │
│  │  │            Name: default  (also: bronze, silver, gold planned)        │  │   │
│  │  │                                                                       │  │   │
│  │  │   ┌─────────────────┐  ┌─────────────────┐  ┌──────────────────┐    │  │   │
│  │  │   │  MANAGED TABLE  │  │ EXTERNAL TABLE   │  │     VOLUME       │    │  │   │
│  │  │   │                 │  │                  │  │                  │    │  │   │
│  │  │   │ pipeline_audit  │  │ charging_sessions│  │  bronze-volume   │    │  │   │
│  │  │   │                 │  │ (points to ADLS) │  │  (maps to ADLS   │    │  │   │
│  │  │   │ Data stored in  │  │                  │  │   container)     │    │  │   │
│  │  │   │ UC-managed      │  │ Data stays on    │  │                  │    │  │   │
│  │  │   │ storage         │  │ your ADLS path   │  │                  │    │  │   │
│  │  │   └─────────────────┘  └─────────────────┘  └──────────────────┘    │  │   │
│  │  │                                                                       │  │   │
│  │  │   ┌──────────────────────────────────────────────────────────────┐   │  │   │
│  │  │   │                    VIEW / FUNCTION                           │   │  │   │
│  │  │   │   Virtual objects — computed at query time, no storage       │   │  │   │
│  │  │   └──────────────────────────────────────────────────────────────┘   │  │   │
│  │  └───────────────────────────────────────────────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │                    STORAGE CREDENTIAL                                        │   │
│  │      cred-ev-intelligence-dev                                                │   │
│  │      Wraps the Managed Identity of Access Connector (ac-ev-intelligence-dev) │   │
│  │      Unity Catalog uses this identity to access ADLS on your behalf          │   │
│  └──────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │                    EXTERNAL LOCATIONS                                        │   │
│  │                                                                              │   │
│  │  evdatalakedev-bronze  →  abfss://bronze@evdatalakedev.dfs.core.windows.net/ │   │
│  │  evdatalakedev-silver  →  abfss://silver@evdatalakedev.dfs.core.windows.net/ │   │
│  │  evdatalakedev-gold    →  abfss://gold@evdatalakedev.dfs.core.windows.net/   │   │
│  └──────────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────┘
                    │  attached to
                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          DATABRICKS WORKSPACES                                      │
│                   (all workspaces in the same region share one metastore)           │
│                                                                                     │
│   ┌──────────────────────────┐    ┌──────────────────────────┐                     │
│   │  dbw-ev-intelligence-dev  │    │  dbw-ev-prod (future)    │                     │
│   │  (dev workspace)          │    │  (prod workspace)         │                     │
│   │  sees same catalog        │    │  sees same catalog        │                     │
│   └──────────────────────────┘    └──────────────────────────┘                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                    │  read/write via External Locations
                    ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          ADLS Gen2 — evdatalakedev                                  │
│                                                                                     │
│   bronze/  ──────── api/payments/raw/    charging_sessions/    audit/               │
│   silver/  ──────── (Delta tables — coming Day 5)                                   │
│   gold/    ──────── (aggregations — coming Day 6)                                   │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Every Term Explained

### Metastore
**What it is:** The top-level Unity Catalog object. One metastore exists per Azure region per Databricks account. All workspaces in the same region are attached to the same metastore — so they share catalogs, tables, and permissions.

**Why it exists:** Before UC, each workspace had its own isolated Hive metastore — no sharing possible. The UC metastore is account-level, not workspace-level — solving cross-workspace governance.

**In our project:** The metastore is named `dbw_ev_intelligence_dev`. Both a dev workspace and any future prod workspace in Central India would share this metastore.

**Where to see it in the UI:**
- Left sidebar → **Catalog** → top of the tree shows the metastore name
- Admin settings → **Data** tab → **Metastore** — shows metastore ID, region, storage root

**What "storage root" means:** The metastore has a default storage location on ADLS where managed table data is stored. For external tables and volumes in our project, we override this with our own ADLS containers.

---

### Catalog
**What it is:** The first level of the 4-level namespace (`catalog.schema.table`). A catalog is a logical container for schemas. Think of it like a database server — it holds multiple databases inside it.

**Default catalog:** Every metastore has a `main` catalog created automatically. We use `dbw_ev_intelligence_dev` as our catalog name.

**Why use a named catalog instead of `main`:** Named catalogs make it easy to separate environments — `dbw_ev_intelligence_dev.default.pipeline_audit` vs `dbw_ev_intelligence_prod.default.pipeline_audit` — same schema structure, different data, clear namespace.

**In our project:** All tables and volumes live under `dbw_ev_intelligence_dev.default.*`

**Where to see it in the UI:**
- Left sidebar → **Catalog** → expand the tree → first level = catalogs
- SQL: `SHOW CATALOGS;`

**How to create one:**
- UI: Catalog → + Create Catalog → name it → assign storage location
- SQL: `CREATE CATALOG dbw_ev_intelligence_dev;`

---

### Schema (Database)
**What it is:** The second level of the namespace (`catalog.schema.table`). A schema groups related tables and volumes together — equivalent to a "database" in traditional SQL systems.

**In our project:** We use `default` schema for now. Future planned schemas:
```
dbw_ev_intelligence_dev.bronze   — raw ingested tables
dbw_ev_intelligence_dev.silver   — cleaned, deduplicated Delta tables
dbw_ev_intelligence_dev.gold     — aggregated reporting tables
```

**Where to see it in the UI:**
- Catalog → expand catalog → second level = schemas
- SQL: `SHOW SCHEMAS IN dbw_ev_intelligence_dev;`

**How to create one:**
- UI: Catalog → right-click catalog → Create schema
- SQL: `CREATE SCHEMA dbw_ev_intelligence_dev.silver;`

---

### Managed Table
**What it is:** A Delta table where Unity Catalog owns both the metadata (schema, statistics) AND the data files (stored in the metastore's storage root or catalog's default location).

**What happens when you DROP a managed table:** Both the metadata AND the underlying Parquet/Delta files are deleted. Data is gone.

**When to use:** Internal tables used only within Databricks — no external tool needs to access the raw files.

**In our project:** `pipeline_audit` is a managed table (when written via Delta Lake approach). UC manages where the files live.

**SQL:**
```sql
CREATE TABLE dbw_ev_intelligence_dev.default.pipeline_audit (
  pipeline_name   STRING,
  load_type       STRING,
  watermark_value STRING,
  status          STRING,
  run_timestamp   TIMESTAMP
);
```

---

### External Table
**What it is:** A Delta (or Parquet/CSV) table where Unity Catalog owns the metadata (schema) but the data files live at a path YOU control on ADLS — registered via an External Location.

**What happens when you DROP an external table:** Only the metadata is deleted. The underlying files on ADLS remain. Data is safe.

**When to use:** When data needs to be accessed by tools outside Databricks (Azure Synapse, Power BI direct query, ADF) — the files stay at a fixed ADLS path.

**In our project:** The charging sessions data written by the Bronze migration job exists as raw files on ADLS. When we register it as an external table in Silver, it becomes queryable via SQL without moving data.

**SQL:**
```sql
CREATE TABLE dbw_ev_intelligence_dev.silver.charging_sessions
USING DELTA
LOCATION 'abfss://silver@evdatalakedev.dfs.core.windows.net/charging_sessions/';
```

---

### Volume
**What it is:** A Unity Catalog object that maps a path on your ADLS storage (via an External Location) to a browsable path in the Catalog UI and a `/Volumes/...` filesystem path accessible in notebooks.

**Two types:**

| Type | Data ownership | DROP behaviour |
|---|---|---|
| **Managed Volume** | UC owns the files | Files deleted on DROP |
| **External Volume** | You own the files on ADLS | Files preserved on DROP |

**In our project:** `bronze-volume` is an External Volume pointing to `abfss://bronze@evdatalakedev.dfs.core.windows.net/`. This is what gives you the path `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/` in notebooks.

**Why Volumes instead of direct `abfss://` paths:**
- No need to configure SAS tokens or service principals in each notebook
- UC enforces access control — only users with `READ VOLUME` privilege can access it
- Browsable in the Catalog UI tree
- Works with `dbutils.fs.ls("/Volumes/...")` — consistent across all clusters

**Where to see it in the UI:**
- Catalog → expand catalog → expand schema → **Volumes** section → click volume name → browse files
- In notebooks: `dbutils.fs.ls("/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/")`

---

### External Location
**What it is:** A Unity Catalog object that registers an ADLS path as accessible to Databricks via a Storage Credential. Without an External Location registered, no cluster can access that ADLS path — even if it has network access.

**In our project:**
```
evdatalakedev-bronze  →  abfss://bronze@evdatalakedev.dfs.core.windows.net/
evdatalakedev-silver  →  abfss://silver@evdatalakedev.dfs.core.windows.net/
evdatalakedev-gold    →  abfss://gold@evdatalakedev.dfs.core.windows.net/
```

**Why it exists:** It is the security gate. Before UC, you'd mount storage with a SAS token inside a notebook — anyone with notebook access could see the token. External Locations move the credential to the metastore level — notebooks never see the actual storage key.

**Where to see it in the UI:**
- Left sidebar → **Catalog** → click the **+** or go to **External Data** tab
- OR: Admin console → **Data** → **External Locations**
- Each External Location shows its path, which Storage Credential it uses, and which workspaces can access it

**How to test it in the UI:**
- Click an External Location → **Test connection** button → confirms Databricks can reach the ADLS path with the linked Storage Credential

---

### Storage Credential
**What it is:** A Unity Catalog object that wraps an Azure identity (Managed Identity of an Access Connector) and stores it securely in the metastore. External Locations reference a Storage Credential to know which identity to use when accessing ADLS.

**In our project:** `cred-ev-intelligence-dev` wraps the Managed Identity of `ac-ev-intelligence-dev` (the Access Connector resource in Azure).

**Why Access Connector and not a Service Principal:** Access Connector is a dedicated Azure resource for connecting Databricks to ADLS — its Managed Identity is stable (no credential rotation), managed by Azure, and assigned the `Storage Blob Data Contributor` role on the storage account.

**Chain of trust:**
```
Notebook code
    → accesses /Volumes/... path
    → Unity Catalog checks: does this user have READ VOLUME on bronze-volume?
    → YES → UC uses Storage Credential (cred-ev-intelligence-dev)
    → Storage Credential uses Access Connector Managed Identity
    → Azure IAM confirms: identity has Storage Blob Data Contributor on evdatalakedev
    → ADLS returns the file
```

**Where to see it in the UI:**
- Catalog → External Data → **Credentials** tab
- Each credential shows its type (Azure Managed Identity), Access Connector ID, and which External Locations use it

---

### 4-Level Namespace

Unity Catalog uses a 4-level name for every object:

```
metastore  .  catalog  .  schema  .  table/volume
               ↑              ↑           ↑
         dbw_ev_intel..    default    pipeline_audit
```

In SQL you reference tables with 3 levels (catalog is enough to be unique within a metastore):

```sql
SELECT * FROM dbw_ev_intelligence_dev.default.pipeline_audit;

-- Or set defaults so you can use shorter names:
USE CATALOG dbw_ev_intelligence_dev;
USE SCHEMA default;
SELECT * FROM pipeline_audit;
```

**Why this matters:** Before UC, table names were just `schema.table` — ambiguous across workspaces. The 3-level name is globally unique within an account — no ambiguity possible.

---

### Data Lineage
**What it is:** Unity Catalog automatically tracks which notebooks, jobs, and queries read from and write to each table. No configuration needed — UC captures this transparently.

**Where to see it in the UI:**
- Catalog → click any table → **Lineage** tab
- Shows upstream (what fed this table) and downstream (what reads from this table) as a graph

**In our project:** After the Bronze migration runs and writes files to the Volume, UC will show: `job_bronze_charging_sessions_hourly → wrote to → bronze-volume/realtime/charging_sessions/...`

---

### Column-Level Security
**What it is:** Unity Catalog allows you to grant access to specific columns of a table, not just the whole table. A user can `SELECT session_id, station_id` but not `SELECT user_id` if you restrict it.

**How to set:**
```sql
-- Deny access to user_id column for a specific group
DENY SELECT ON TABLE dbw_ev_intelligence_dev.silver.charging_sessions (user_id) TO `analysts`;
```

**In our project:** Not yet configured — relevant when the Gold layer exposes data to business users who should not see raw user identifiers.

---

### Row-Level Security
**What it is:** A policy that filters rows returned by a query based on who is querying. Implemented via a row filter function attached to a table.

**Example use case in our project:** A regional analyst can only see charging sessions from their region — the row filter checks the user's group membership and filters accordingly.
