# 03 — Unity Catalog vs No Unity Catalog: UI Comparison
**Day 4 | What changes in the Databricks workspace when UC is enabled**

---

## The Short Answer

Without Unity Catalog, your Databricks workspace looks like a traditional data warehouse tool — you see clusters, notebooks, and a basic Hive metastore. With Unity Catalog, you get a full governance layer: a 3-level namespace, browsable storage, access controls, and data lineage — all visible in the same web UI but on screens that either don't exist or behave completely differently.

---

## Side-by-Side: Left Sidebar Navigation

| Sidebar Item | Without Unity Catalog | With Unity Catalog |
|---|---|---|
| **Data / Catalog** | "Data" tab — shows legacy Hive metastore databases only | "Catalog" tab — full tree: metastore → catalogs → schemas → tables → volumes |
| **Compute** | Clusters only | Clusters + SQL Warehouses (for serverless SQL) |
| **Workflows** | Jobs + Pipelines | Same — no change |
| **SQL Editor** | Not available (need SQL Analytics add-on) | Available — runs on SQL Warehouse or cluster |
| **Marketplace** | Not available | Available in newer workspaces |

---

## Screen-by-Screen Comparison

### 1 — Data / Catalog Browser

**Without Unity Catalog:**
```
Data tab
└── Databases (Hive metastore)
      ├── default
      │     └── Tables listed as flat list — no volume/external location concept
      └── my_db
            └── some_table
                  Schema tab — column names and types only
                  No Lineage tab
                  No Permissions tab (access managed outside Databricks or not at all)
```

**With Unity Catalog (our project):**
```
Catalog tab
└── dbw_ev_intelligence_dev   (catalog)
      └── default              (schema)
            ├── Tables
            │     └── pipeline_audit
            │           ├── Schema tab        — columns, types, nullable
            │           ├── Sample Data tab   — click to preview rows (no notebook needed)
            │           ├── Details tab       — table type (managed/external), location, owner
            │           ├── Permissions tab   — grant/revoke SELECT, MODIFY per user or group
            │           └── Lineage tab       — visual graph: which jobs wrote this, which tables read it
            └── Volumes
                  └── bronze-volume
                        ├── Details tab       — volume type (External), ADLS path it maps to
                        └── [Browse files]    — click through folder hierarchy as if it's a file browser
```

**What this means for you:** In the UC version, you can verify that a copy job actually landed files by clicking: Catalog → default → Volumes → bronze-volume → realtime → charging_sessions → 2026 → 07 → 08 — no notebook needed.

---

### 2 — Cluster Configuration (Compute tab)

**Without Unity Catalog:**
```
Create Cluster form:
  - Cluster mode: Standard / High Concurrency
  - DBR version: any
  - Worker type / driver type: any VM size
  - Spark config: open text box — add any spark.hadoop.* config including storage credentials
  - Init scripts: allowed from DBFS paths
  - No security mode dropdown
```

**With Unity Catalog (our project):**
```
Create Cluster form — additional required fields:
  - Access mode:
      ┌─────────────────────────────────────────────────────────────────┐
      │ Single User       — only ONE user can run on this cluster        │
      │                    Full UC access. Good for dev/test.            │
      │                    Our dev-cluster uses this mode.               │
      │                                                                  │
      │ Shared            — multiple users share the cluster             │
      │                    UC enforces per-user permissions per table     │
      │                    Better isolation in team environments          │
      │                                                                  │
      │ No Isolation Shared — NO Unity Catalog enforcement               │
      │                    Anyone on cluster can read any ADLS path      │
      │                    Not recommended for UC environments           │
      └─────────────────────────────────────────────────────────────────┘
  - DBR version: must be 11.3 LTS or later for UC support
  - Spark config: you CAN'T add spark.hadoop.fs.azure.account.key.* directly anymore
                  (storage access goes through External Locations, not cluster config)
  - Init scripts: must come from a Volume path, not DBFS
```

**Key difference:** On a UC-enabled workspace, if you try to access an ADLS path directly via `spark.conf.set("fs.azure.account.key...")`, it works on clusters in "No Isolation Shared" or "Single User" mode — but Unity Catalog's access controls are bypassed. The secure path is always External Locations + Volumes.

**In our project:** `dev-cluster` is Single User mode (your user). That's why the hourly job running on that cluster inherits YOUR permissions to read the Bronze Volume.

---

### 3 — Notebook: Accessing Storage

**Without Unity Catalog (legacy approach):**
```python
# Mount ADLS to DBFS — done once per cluster restart
dbutils.fs.mount(
    source="abfss://bronze@evdatalakedev.dfs.core.windows.net/",
    mount_point="/mnt/bronze",
    extra_configs={
        "fs.azure.account.key.evdatalakedev.dfs.core.windows.net":
            dbutils.secrets.get(scope="kv-ev-scope", key="storage-account-key")
    }
)
# Then access via mount:
dbutils.fs.ls("/mnt/bronze/realtime/")
```

**Problems:**
- Mount persists across sessions — someone running on the same cluster sees the same `/mnt/bronze` path with the same permissions as whoever created the mount
- The storage account KEY is retrieved from Key Vault and passed to Spark config — it's visible in cluster logs
- No per-user permission enforcement — all users on the cluster share the mount
- Mounts must be recreated after cluster restarts or on new clusters

**With Unity Catalog (our project):**
```python
# No mounting needed — Volume path works everywhere
dbutils.fs.ls("/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/")

# Read a file directly:
df = spark.read.csv("/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/2026/07/08/")
```

**Why this is better:**
- Path `/Volumes/...` resolves via Unity Catalog — UC checks: does this user have READ VOLUME on bronze-volume?
- The actual ADLS credential (Access Connector Managed Identity) is never in your code or visible in logs
- Works identically on any cluster attached to the same workspace and metastore — no per-cluster mount setup
- Per-user enforcement: a user without READ VOLUME permission gets a permission denied, not a storage auth error

---

### 4 — External Data Tab (UC-only screen — doesn't exist without UC)

**Without Unity Catalog:** This tab does not exist. Storage access is configured at the cluster level (Spark config or mounts).

**With Unity Catalog:**
```
Catalog tab → External Data (sub-navigation)
├── External Locations
│     ├── evdatalakedev-bronze — abfss://bronze@evdatalakedev...  [Test connection]
│     ├── evdatalakedev-silver — abfss://silver@evdatalakedev...  [Test connection]
│     └── evdatalakedev-gold   — abfss://gold@evdatalakedev...    [Test connection]
├── Credentials
│     └── cred-ev-intelligence-dev
│           Type: Azure Managed Identity
│           Access Connector: ac-ev-intelligence-dev
│           Used by: evdatalakedev-bronze, evdatalakedev-silver, evdatalakedev-gold
└── Connections (for external databases — Lakehouse Federation)
```

**What "Test connection" does:** Sends a test `ls` operation from Databricks to the ADLS path using the linked credential. Green checkmark = Databricks can successfully read the container using the Access Connector's managed identity. This is how you verify setup without running a notebook.

---

### 5 — Table Permissions

**Without Unity Catalog:**
```
Table (in Data tab) → no Permissions tab exists
```
Permissions are managed via table ACLs at the Hive metastore level — a separate SQL command run in a privileged notebook. Not visible in UI. No column-level or row-level control.

```sql
-- Hive metastore ACL (legacy):
GRANT SELECT ON TABLE default.pipeline_audit TO `some_user@company.com`;
```

**With Unity Catalog:**
```
Catalog → select table → Permissions tab
  ┌────────────────────────────────────────────────────────────┐
  │ Principal          │ Privilege        │ Inherited from     │
  ├────────────────────┼──────────────────┼───────────────────┤
  │ hariom@simform...  │ ALL PRIVILEGES   │ (direct)          │
  │ data-engineers     │ SELECT, MODIFY   │ (group)           │
  │ analysts           │ SELECT           │ (group)           │
  └────────────────────┴──────────────────┴───────────────────┘
  [Grant] button — add user/group and pick privilege from dropdown
  [Revoke] button — remove a privilege
```

**Privilege types in UC:**
| Privilege | Allows |
|---|---|
| SELECT | Read table rows, query via SQL |
| MODIFY | INSERT, UPDATE, DELETE rows |
| CREATE TABLE | Create tables in this schema |
| CREATE VOLUME | Create volumes in this schema |
| READ VOLUME | Read files from a volume |
| WRITE VOLUME | Write files to a volume |
| ALL PRIVILEGES | Everything above |

---

### 6 — Lineage Tab (UC-only — doesn't exist without UC)

**Without Unity Catalog:** No lineage tracking. You cannot know which job wrote which table or which table feeds which dashboard — you have to trace it manually in notebook code.

**With Unity Catalog:**
```
Catalog → pipeline_audit → Lineage tab

  Upstream (what wrote this table):
    ┌─────────────────────────────────────────────────────┐
    │  ADF Pipeline: pl_bronze_api_payments_v3            │
    │  via Copy Activity: act_write_audit                 │
    │  Last write: 2026-07-04 01:05:00 UTC                │
    └─────────────────────────────────────────────────────┘

  Downstream (what reads from this table):
    ┌─────────────────────────────────────────────────────┐
    │  ADF Pipeline: pl_bronze_api_payments_v3            │
    │  via Lookup Activity: act_get_watermark             │
    └─────────────────────────────────────────────────────┘
```

The lineage graph is interactive — click any node to open that pipeline or table.

---

### 7 — Workspace Settings → Admin

**Without Unity Catalog:**
```
Admin Console → tabs:
  Users, Groups, Service Principals
  Workspace Settings (general)
  Git Credentials
  (no Data/Metastore tab)
```

**With Unity Catalog:**
```
Admin Console → tabs:
  Users, Groups, Service Principals (same as without)
  Workspace Settings (same)
  Data tab (NEW):
    └── Metastore — shows:
          Metastore ID, region (Central India), storage root
          Link to Account Console for full metastore management
  Security tab (NEW):
    └── IP Access List, Token Management
```

---

## Summary: What You CANNOT Do Without Unity Catalog

| Capability | Without UC | With UC |
|---|---|---|
| Browse ADLS files in UI (no notebook) | No | Yes — Volumes in Catalog tab |
| 3-level table namespace | No — `schema.table` only | Yes — `catalog.schema.table` |
| Cross-workspace table sharing | No | Yes — same metastore |
| Column-level security | No | Yes — via GRANT/DENY on columns |
| Row-level security | No | Yes — via row filter functions |
| Data lineage graph | No | Yes — Lineage tab on every table |
| External Location access control | No — storage keys in cluster config | Yes — via Storage Credentials |
| Volume file browser in UI | No | Yes |
| Table permissions in UI | No — command line only | Yes — Permissions tab |
| Test storage connection in UI | No | Yes — "Test connection" button |

---

## Common Confusion: "Why Can't I See Tables from Another Workspace?"

If a colleague opens `workspace-B` and queries `dbw_ev_intelligence_dev.default.pipeline_audit` — they CAN see it, but only if:
1. Both workspaces are attached to the same metastore (same Azure region, same Databricks account)
2. The colleague's user account has been granted SELECT on that table (via Permissions tab or SQL GRANT)

Without Unity Catalog, tables are completely invisible across workspaces — there's no mechanism to share them at all.

---

## Our Project: What UC Enables That We Actively Use

| Feature | Where we use it |
|---|---|
| External Location | Bronze/Silver/Gold ADLS containers registered — notebooks access via `/Volumes/...` |
| Volume | `bronze-volume` — migration notebook writes here; ADF volumes for reference |
| Storage Credential | `cred-ev-intelligence-dev` — Access Connector identity used for ADLS auth |
| 3-level namespace | `dbw_ev_intelligence_dev.default.pipeline_audit` |
| Single User cluster mode | `dev-cluster` — required for UC enforcement |
| File browsing in UI | Browse Bronze Volume in Catalog tab to verify migration job output |
