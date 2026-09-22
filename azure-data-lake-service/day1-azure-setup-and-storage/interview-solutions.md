# Day 1 — Interview Solutions: Azure Setup & Storage

> Complete answers for all 30 questions in `interview-questions.md`.

---

## Concept 1: Azure Account, Subscriptions & Resource Groups

**Q1 — Account vs. subscription**

**Azure account:** Your identity — the email address (Microsoft account or work/school account) you use to sign in to Azure. Analogous to a Gmail account.

**Azure subscription:** The billing and governance boundary. All resources you create live inside a subscription and their costs accrue to it. A subscription has a billing owner (a person or organisation that pays the bill).

**One account, multiple subscriptions:** Yes. A single Azure account can own or be associated with multiple subscriptions. Common patterns:
- One subscription per environment (dev, staging, prod)
- One subscription per business unit (finance, engineering, marketing)
- One subscription per cost centre for chargeback

This allows different billing arrangements, spending limits, and governance policies per subscription while all being managed by the same team.

---

**Q2 — What is a Resource Group**

A **Resource Group** is a logical container inside an Azure subscription that holds related Azure resources. Every Azure resource (storage account, VM, database, Event Hub) must belong to exactly one Resource Group.

**Why put a project's resources in one Resource Group:**

1. **Unified lifecycle:** Deleting the Resource Group deletes all resources inside it. No need to hunt for and delete 20 individual resources at project end.
2. **Unified billing view:** Azure Cost Management shows the combined spend for all resources in a group — easy to see what a project costs.
3. **Unified access control:** Assign an RBAC role on the Resource Group and it applies to all resources inside — one permission grant instead of many.
4. **Unified deployment:** ARM templates and Bicep deploy all resources in a group together as one unit.

---

**Q3 — Organising dev/staging/prod**

**Option A — One Resource Group per environment:**
```
Subscription: "Company Production"
├── rg-datalake-dev
├── rg-datalake-staging
└── rg-datalake-prod
```
- Pro: Clean separation, delete dev group without touching prod
- Con: All environments share the same subscription limits and billing boundary
- Best for: Small-to-medium teams with simple governance needs

**Option B — One subscription per environment:**
```
Account: admin@company.com
├── Subscription: "DL-Dev"    → rg-datalake-dev
├── Subscription: "DL-Staging" → rg-datalake-staging
└── Subscription: "DL-Prod"   → rg-datalake-prod
```
- Pro: Hard isolation — a mistake in dev subscription cannot touch prod; separate spending limits; different policies (e.g., no expensive VMs allowed in dev)
- Con: More overhead to manage multiple subscriptions; cross-subscription networking requires extra configuration
- Best for: Enterprise organisations with strict compliance, regulated industries, large teams

**Senior answer:** Most enterprises use Option B (subscription-per-environment) for production workloads because Azure Policy and spending limits can be applied per subscription independently.

---

**Q4 — No Resource Groups — cleanup problem**

**Problem:** With all resources in the subscription root (technically impossible — all resources must be in some RG, but if someone creates many ad-hoc RGs with one resource each), finding and deleting all "dev" resources requires:
- Manually identifying which resources belong to the dev environment
- Deleting them one by one
- Risk of missing a resource and being charged for months

**Real risk:** A forgotten VM or storage account in a dev environment can accumulate significant costs silently.

**Fix going forward:**
1. Enforce a naming convention: `rg-{project}-{env}` — one RG per project per environment
2. Apply **Azure Policy** to deny resource creation outside an approved RG pattern
3. Use **resource tags** (`Environment: dev`) in addition to RG naming, so cost reports can filter by environment even if RGs are mixed

---

**Q5 — Azure resource tags**

Tags are key-value pairs attached to Azure resources. They appear in billing reports and can be used to filter resources.

**Three useful tags for a storage account:**

| Tag name | Tag value | Why useful |
|---|---|---|
| `Environment` | `dev` / `prod` | Filter all dev resources in cost reports to see dev spend separately |
| `Project` | `datalake-course` | Attribute cost to a specific project for chargeback to the right team |
| `Owner` | `hariom@company.com` | Know who to contact if there's a problem; automatically alert the owner on budget threshold |

Tags do not affect resource behaviour — they are purely for organisation, cost management, and governance.

---

**Q6 — Accidental Resource Group deletion**

**Features that could have prevented data loss:**

1. **Blob soft delete:** Deleted blobs are retained for a configurable period (1–365 days) before permanent deletion. Even if the storage account was deleted, soft-deleted blobs can be recovered — but only if the account itself still exists. Soft delete helps against accidental file deletion inside the account, not deletion of the entire account.

2. **Resource lock (best protection):** Apply a **Delete lock** to the Resource Group or storage account. Any delete attempt — including the Resource Group delete — is blocked with an error. A human must explicitly remove the lock before deletion can proceed.
   ```
   Lock type: Delete
   Applied to: rg-datalake-prod
   Effect: Cannot delete the RG or any resource inside it
   ```

3. **Azure Backup / Operational Backup for Blob Storage:** Point-in-time restore — recover blobs to any state within the configured retention window (up to 365 days). Survives even accidental account deletion if the backup vault is in a separate RG.

4. **Geo-redundant storage (GRS/GZRS):** A secondary copy in a paired region. If the primary region is lost, data can be failed over. Does not protect against logical deletion but protects against regional outage.

**The resource lock is the most direct protection against accidental deletion** and costs nothing.

---

**Q7 — Owner vs. Contributor vs. Reader**

| Role | Can create/modify resources | Can delete Resource Group | Can assign roles to others |
|---|---|---|---|
| Reader | No | No | No |
| Contributor | Yes | No | No |
| Owner | Yes | Yes | Yes |

**Can a Contributor delete the Resource Group?** No. The Contributor role can create, modify, and delete individual resources inside the group — but deleting the Resource Group itself requires the **Owner** or **User Access Administrator** role (which includes write on the RG object itself and all its children).

**Interview nuance:** "Delete lock" overrides even the Owner role — an Owner cannot delete a locked resource until the lock is removed by someone with `Microsoft.Authorization/locks/delete` permission.

---

## Concept 2: Azure Blob Storage

**Q8 — Storage Account, Container, Blob**

**Storage Account:** The top-level Azure resource for all storage services. It has a globally unique name (e.g., `stadlsdev001`). It hosts Blob Storage, File shares, Queues, and Tables — all within one account. You set redundancy, performance tier, and access settings at the account level.

**Container:** A logical grouping of blobs inside a storage account — equivalent to an S3 bucket. Containers have names and access policies. One storage account can have thousands of containers. Common pattern: one container per data zone (`bronze`, `silver`, `gold`).

**Blob:** The actual data object — a file, an image, a Parquet file, a JSON event. Stored inside a container. There are three blob types:
- **Block blob** — for files (most common; Parquet, CSV, images)
- **Append blob** — for log files (data can only be appended, not modified)
- **Page blob** — for virtual machine disks

---

**Q9 — Three access tiers**

| Tier | Storage cost | Access cost | Retrieval time | Use case |
|---|---|---|---|---|
| **Hot** | Highest | Lowest | Immediate | Frequently accessed data (Silver, Gold tables queried daily) |
| **Cool** | Medium | Medium | Immediate | Infrequently accessed (Bronze raw files older than 30 days, monthly reports) |
| **Archive** | Lowest | Highest | Hours (rehydration required) | Rarely accessed (compliance copies, 1+ year old data) |

**Key rule:** You pay more to store in Hot but less to access it. Archive is cheapest to store but expensive and slow to retrieve. Choose based on access frequency, not just storage size.

---

**Q10 — LRS vs. ZRS vs. GRS**

| Redundancy | Copies | Where | SLA | Use case |
|---|---|---|---|---|
| LRS (Locally Redundant) | 3 | Same datacenter | 99.9% | Dev, testing, data that can be regenerated |
| ZRS (Zone-Redundant) | 3 | 3 availability zones in same region | 99.99% | Production workloads where zone failure is a risk |
| GRS (Geo-Redundant) | 6 | 3 local + 3 in a paired region | 99.999% | Disaster recovery; compliance that requires geographic separation |
| GZRS (Geo-Zone-Redundant) | 6 | 3 zones local + paired region | 99.9999% | Mission-critical production; highest durability |

**Recommendation for production data lake:** **ZRS** at minimum. GRS if regulatory compliance requires geographic separation (common in financial services, healthcare). GZRS for the most critical data (payment records, audit logs).

---

**Q11 — Rename folder in Blob Storage with 500 million files**

**How it works internally:** Blob Storage has a **flat namespace** — there are no real folders. `logs/2024/orders.parquet` is just a blob whose name contains slashes. A "folder" is a virtual construct that exists only when a blob name contains that prefix.

**Renaming `logs/` to `raw-logs/`** requires:
1. Listing all 500 million blobs with the `logs/` prefix
2. Copying each blob to a new blob with the `raw-logs/` prefix
3. Deleting the original 500 million blobs

**Performance implication:** This is a copy + delete operation on every single object. At 10 MB/s per blob, 500 million blobs = weeks of work. In practice, this is done programmatically with AzCopy or the Azure SDK in parallel, but it still takes hours to days for large datasets.

**In ADLS Gen2:** The same rename is a single atomic metadata operation — one API call, completed in milliseconds regardless of how many files are in the directory. This is why ADLS Gen2 is essential for data lake patterns like Write-Audit-Publish (atomically swap a staging directory for a production directory).

---

**Q12 — SAS token vs. RBAC role**

**SAS token (Shared Access Signature):** A time-limited, permission-scoped URL that grants temporary access to a specific storage resource. No Azure AD account required.

**RBAC role:** A permanent permission assigned to an Azure AD identity (user, group, service principal). Requires the recipient to have an Azure AD account.

**When to use SAS token:**
- Sharing a file with an external partner who doesn't have an Azure AD account
- Granting a mobile app temporary upload access without giving it full Azure credentials
- Providing a customer a time-limited download link for their report
- CI/CD pipeline in a non-Azure environment that needs to push build artifacts

**When to use RBAC:**
- Your own team accessing their own storage
- Service principals (Azure Functions, Databricks) accessing storage in production
- Any long-lived access that should be centrally managed and auditable

**Production rule:** Prefer RBAC + Managed Identity for service-to-service access. Use SAS tokens only for external parties or temporary one-off access.

---

**Q13 — Access key vs. Azure AD RBAC**

**Access key:**
- A master password — anyone with the key has full unrestricted access to the entire storage account
- Cannot be scoped to a specific container or permission level
- Cannot be rotated automatically
- If leaked, the only remediation is to rotate the key (which breaks all existing connections using that key)
- Appears in plaintext in connection strings — easy to accidentally commit to Git

**Azure AD RBAC:**
- Tied to a specific identity (user, group, or service principal)
- Can be scoped to the storage account, container, or even directory level
- Can be revoked per-identity without affecting other identities
- Integrates with Conditional Access, MFA, and audit logs
- Managed Identities eliminate the need to store any credentials at all

**Which is more secure:** Azure AD RBAC + Managed Identity. No credentials to store, rotate, or accidentally expose. Access can be audited and revoked per identity.

**Rule for production:** Never use access keys for application authentication. Use Managed Identity if the app runs on Azure (AKS, App Service, Azure Functions). Use Service Principal + client secret or certificate for non-Azure applications.

---

**Q14 — Managing storage costs for hot/rare data**

**Solution: Blob Lifecycle Management policy.**

```
Rule: "archive-raw-data"
├── If last modified > 30 days:  move to Cool tier
├── If last modified > 90 days:  move to Archive tier
└── If last modified > 365 days: delete (if business allows)
```

This is configured in the portal under **Data management → Lifecycle management** and runs automatically daily.

**Estimated cost savings:** Hot = ~$0.02/GB/month. Cool = ~$0.01/GB/month. Archive = ~$0.002/GB/month. For 100 TB of data distributed across tiers, lifecycle management can reduce storage costs by 50–80% vs. keeping everything in Hot.

**Additional lever:** Enable **Blob versioning** only on critical containers — versioning keeps copies of every change, which multiplies storage cost if applied indiscriminately.

---

## Concept 3: ADLS Gen2

**Q15 — Hierarchical Namespace (HNS)**

**Hierarchical Namespace (HNS)** is a setting enabled at the storage account level that changes the internal data model from a flat key-value store to a true directory tree.

**What it changes:**

| | HNS Disabled (Blob Storage) | HNS Enabled (ADLS Gen2) |
|---|---|---|
| Folder structure | Virtual (prefix in blob name) | Real directories with metadata |
| Rename directory | Copy all blobs + delete (slow) | Single atomic metadata operation (instant) |
| Delete directory | Delete each blob individually | Single operation |
| File-level permissions | No (container level only) | Yes (POSIX ACLs per file/directory) |
| Optimised for | General file storage | Big data analytics (Spark, Databricks, Synapse) |

Once HNS is enabled, it **cannot be disabled** on that storage account. To revert, you must create a new account.

---

**Q16 — Azure services that work with ADLS Gen2**

1. **Azure Databricks** — reads/writes Delta Lake tables on ADLS Gen2; uses ABFS (Azure Blob File System) driver; requires HNS for atomic directory operations
2. **Azure Synapse Analytics** — natively integrates with ADLS Gen2 for data lake queries via Synapse Serverless SQL and Spark pools
3. **Azure Data Factory (ADF)** — uses ADLS Gen2 as both source and sink for pipeline data movement; supports folder-level copy, HNS directory management

**Why they prefer ADLS Gen2 over Blob Storage:**
- **Performance:** The ABFS driver is optimised for hierarchical directory traversal; Blob's WASB driver treats directories as prefix scans (slow for millions of files)
- **Atomic rename:** Write-Audit-Publish pattern (used by Delta Lake for ACID transactions) requires atomic directory rename — only possible with HNS
- **Security:** POSIX ACLs allow fine-grained data access (analyst can read `gold/`, but not `bronze/`) without creating separate storage accounts per role

---

**Q17 — Rename 10 million files: ADLS Gen2 vs. Blob**

**ADLS Gen2:** The rename is a single atomic metadata operation on the directory node. The 10 million files do not move — only the directory pointer is updated. Time: **milliseconds**.

**Blob Storage:** Rename requires copying every blob to a new key and deleting the old one. For 10 million files at 1 MB average:
- 10 million copy operations
- 10 million delete operations
- At 10,000 operations/second → 2,000 seconds ≈ **30+ minutes**
- During this time, the directory is in a partially renamed state — not atomic

**Why this matters in practice:**

Delta Lake uses a Write-Audit-Publish (WAP) pattern:
1. Write new data to `_delta_staging/`
2. Validate the new data
3. Atomically rename `_delta_staging/` to the final table directory

Step 3 only works correctly with ADLS Gen2. On Blob Storage, the "rename" is a copy + delete — if it is interrupted, the table is in a corrupt half-renamed state. This is why Delta Lake on Azure requires ADLS Gen2.

---

**Q18 — Read-only on gold, no access to bronze/silver**

**Solution: RBAC scoped to the container level.**

```
Role: Storage Blob Data Reader
Scope: stadlsdev001/gold (container level)
Member: analyst@company.com
```

**Steps in the portal:**
1. Go to `stadlsdev001` → Containers → `gold`
2. Click **Access Control (IAM)** on the container (not the storage account)
3. Add role assignment: **Storage Blob Data Reader** → analyst's email

This gives the analyst read access to `gold/` only. They cannot see or access `bronze/` or `silver/` containers.

**For Spark service principals:** Use the same pattern with a **Service Principal** instead of a user. Assign `Storage Blob Data Contributor` to the Spark service principal on `bronze/` and `silver/`, and `Storage Blob Data Reader` on `gold/`.

**Alternative — POSIX ACLs (ADLS Gen2 only):** ACLs can grant access at the directory or file level within a container — even more granular than container-level RBAC.

---

**Q19 — Bronze / Silver / Gold (Medallion Architecture)**

The Medallion Architecture organises a data lake into three quality layers:

| Layer | Container | Contents | Quality |
|---|---|---|---|
| **Bronze** | `bronze/` | Raw ingested data — exactly as received from the source system, no transformation | Untrusted, may have duplicates, nulls, wrong types |
| **Silver** | `silver/` | Cleaned, deduplicated, validated data — typed correctly, bad rows quarantined | Trusted, queryable, still at raw grain |
| **Gold** | `gold/` | Business-level aggregations — daily revenue by store, monthly active users, KPIs | Trusted, business-ready, optimised for dashboards |

**Raw ingested data → Bronze. Business-ready aggregated data → Gold.**

Bronze is append-only (never modify source data). Silver is MERGE/upsert. Gold is rebuilt (overwrite) on each pipeline run.

---

**Q20 — Enabling HNS on an existing Blob Storage account**

**Short answer: Not easily.**

As of 2024, Microsoft provides a migration tool (preview) to enable HNS on an existing account — but:
- It is a one-way operation (cannot be reversed)
- Migration can take hours for large accounts
- During migration, the storage account is not accessible
- Not all features are supported during migration
- The tool is still in preview, not recommended for production

**Correct approach for a production system:**
1. Create a new storage account with HNS enabled from the start
2. Use AzCopy or Azure Data Factory to copy data from the old Blob account to the new ADLS Gen2 account
3. Update application connection strings to point to the new account
4. Verify all data is present in the new account
5. Delete the old account

**The lesson:** Enable HNS at account creation. It cannot be retrofitted cleanly. This is one of the most common architectural mistakes in Azure data lake projects.

---

**Q21 — Vendor read-only access to a specific directory**

**Solution using POSIX ACLs (ADLS Gen2 only):**

```
Container: bronze
Directory: bronze/orders/2024/

ACL on bronze/orders/2024/:
  User: vendor-service-principal → Read + Execute
  (Execute on parent directories is required to traverse into the directory)

ACL on bronze/                  → Execute only (traverse permission, no read)
ACL on bronze/orders/           → Execute only
ACL on bronze/orders/2024/      → Read + Execute
```

**Why Execute on parent directories?** POSIX ACLs require Execute permission on every directory in the path to traverse it — just like Linux file permissions. Without Execute on `bronze/` and `bronze/orders/`, the vendor cannot navigate to `bronze/orders/2024/` even if they have Read there.

**Alternative — SAS token scoped to the path:**
Generate a SAS token with Read + List permissions scoped to `bronze/orders/2024/` with an expiry. Share the URL with the vendor. No Azure AD account needed.

---

**Q22 — POSIX ACLs: Access ACL vs. Default ACL**

**Access ACL:** Defines who can read/write/execute the specific file or directory it is applied to. Like the permissions on an existing file in Linux.

**Default ACL:** Only applies to **directories**. Defines the ACL that new files and subdirectories inside the directory will **inherit** when created. Like `setgid` or `umask` in Linux.

**Why Default ACLs matter for Spark:**

When Spark writes a Parquet file to `silver/orders/`, it creates a new file. Without a Default ACL, the new file inherits only the owner's permissions. The analytics team's service principal may have Read permission on the directory, but the newly created file may have no permission for them.

With a Default ACL on `silver/orders/`:
```
Default ACL: analytics-group → Read
```
Every new file Spark creates inside `silver/orders/` automatically has Read permission for the analytics group — no manual permission grants required after each pipeline run.

**The interview answer:** Always set Default ACLs on data lake directories where multiple teams need ongoing access. Without them, you will spend time manually fixing permissions every time a pipeline writes new data.

---

## Mixed / Senior-Level Questions

**Q23 — Migrating from Blob Storage to Databricks**

**Why ADLS Gen2 is better for Databricks:**

1. **Atomic directory rename:** Delta Lake's transaction log relies on atomic rename to commit transactions. On Blob Storage, Delta Lake uses a workaround (polling + file locking) that is slower and less reliable.
2. **ABFS driver performance:** Databricks uses the ABFS (Azure Blob File System) driver for ADLS Gen2, which is ~10× faster for metadata-heavy operations (listing files, directory traversal) than the WASB driver for Blob Storage.
3. **Fine-grained permissions:** POSIX ACLs allow different teams to access different directories without separate storage accounts.
4. **Capture + replay:** ADLS Gen2 integrates with Event Hubs Capture for streaming ingestion directly to the data lake.

**First step in migration:**
1. Create a new ADLS Gen2 storage account with HNS enabled
2. Use AzCopy to copy all existing data: `azcopy copy 'https://stblobdev001.blob.core.windows.net/raw-data/*' 'https://stadlsdev001.dfs.core.windows.net/bronze/' --recursive`
3. Update Databricks cluster configurations (mount points or credential passthrough) to point to the new account
4. Run both accounts in parallel briefly to validate, then cut over

---

**Q24 — Financial services requirements**

| Requirement | Azure feature | Configuration |
|---|---|---|
| All data encrypted at rest | Azure Storage encryption | Enabled by default (AES-256). Optionally use Customer-Managed Keys in Azure Key Vault for full control. |
| All data in Australia | Region selection | Australia East (primary). Australia Southeast (paired region for GRS). |
| 99.99% availability SLA | Redundancy | ZRS (Zone-Redundant Storage) — 99.99% availability SLA. |
| Recover from accidental deletion within 7 days | Soft delete + Point-in-time restore | Enable Blob soft delete with 7-day retention. Enable Container soft delete. Enable Point-in-time restore for up to 7 days. |

**Storage account configuration:**
- Region: Australia East
- Redundancy: ZRS (or GZRS for 99.9999% durability with geo-failover)
- Blob soft delete: Enabled, 7 days
- Container soft delete: Enabled, 7 days
- Encryption: Microsoft-managed keys (or Customer-Managed Keys for highest compliance)
- HNS: Enabled (ADLS Gen2) for data lake workloads

---

**Q25 — Blob Storage "works fine with Spark" — when is this true?**

**When Blob Storage is genuinely fine:**
- Reading static data (one-time reads, no concurrent writes)
- Small datasets where directory listing performance is not a bottleneck (< 10,000 files)
- Simple pipeline with no atomic rename requirement (batch overwrite only)
- Delta Lake in append-only mode on small tables

**When they will hit problems with Blob Storage:**

1. **Delta Lake ACID transactions:** Delta Lake's atomic commit requires renaming `_delta_log/*.json.tmp` → `_delta_log/*.json`. On Blob, this is copy+delete — if the Spark job crashes mid-rename, the log is corrupted. On ADLS Gen2, this is atomic.

2. **Directory listing performance:** Spark reads partition discovery by listing all files in a directory. On Blob, listing 1 million files requires 1000 API calls (1,000 results per page). On ADLS Gen2, hierarchical listing is a tree traversal — orders of magnitude faster.

3. **Concurrent writers:** Two Spark jobs writing to the same directory simultaneously can produce duplicate or lost files on Blob Storage (no locking). ADLS Gen2 with Delta Lake provides optimistic concurrency control.

4. **Write-Audit-Publish pattern:** Cannot be done atomically on Blob. The "publish" step (rename staging → prod) takes minutes on Blob vs. milliseconds on ADLS Gen2 — leaving consumers reading incomplete data during the window.

---

**Q26 — Blob Lifecycle Management for a data lake**

```
Rule: "datalake-lifecycle"

Condition 1: Bronze data older than 30 days
Action: Move to Cool tier

Condition 2: Bronze data older than 365 days (1 year)
Action: Move to Archive tier

Condition 3: Bronze data older than 2555 days (7 years)
Action: Delete permanently

Applies to: Blobs in the "bronze" container only
```

In the portal (JSON representation):
```json
{
  "rules": [{
    "name": "bronze-lifecycle",
    "type": "Lifecycle",
    "definition": {
      "filters": { "blobTypes": ["blockBlob"], "prefixMatch": ["bronze/"] },
      "actions": {
        "baseBlob": {
          "tierToCool":    { "daysAfterModificationGreaterThan": 30 },
          "tierToArchive": { "daysAfterModificationGreaterThan": 365 },
          "delete":        { "daysAfterModificationGreaterThan": 2555 }
        }
      }
    }
  }]
}
```

---

**Q27 — Authentication vs. authorisation in Azure Storage**

**Authentication:** Proving who you are. Azure Storage supports:
- Access keys (password-based — the storage account key)
- Azure AD token (OAuth 2.0 — proving you are a specific identity)
- SAS token (pre-signed, time-limited proof of intent)

**Example:** When your Spark job presents an OAuth token from its Managed Identity to Azure Storage, Azure verifies "this token is really from the Databricks cluster's Managed Identity" — that is authentication.

**Authorisation:** Determining what you are allowed to do, after authentication. Azure Storage uses:
- RBAC roles (Storage Blob Data Reader, Contributor, etc.)
- POSIX ACLs (ADLS Gen2 only, at file/directory level)

**Example:** After authentication, Azure checks "does this Managed Identity have the Storage Blob Data Contributor role on the `silver` container?" — that is authorisation.

**The two must both succeed.** Authentication proves identity; authorisation enforces permission. A valid authenticated user with no RBAC role gets a 403 Forbidden.

---

**Q28 — Cross-region storage and Spark cluster**

**Problem:** Network egress between Azure regions (East US to Australia East) is:
1. **Slow:** Cross-region bandwidth is much lower than intra-region bandwidth. A 100 GB read that takes 2 minutes within a region can take 20+ minutes cross-region.
2. **Expensive:** Azure charges for outbound data transfer across regions (typically $0.02–$0.08 per GB depending on regions).

**For a Spark cluster reading 10 TB of data:** Cross-region transfer can cost hundreds of dollars per job run and make jobs 10× slower.

**Fix:** Always provision your storage account and compute in the **same Azure region**. This is a hard rule in Azure architecture:
- Storage: Australia East → Compute (Databricks, Synapse, ADF): Australia East
- The network between them is free and at datacenter speeds

---

**Q29 — Soft delete vs. versioning**

**Soft delete:** When a blob is deleted, it is marked as "deleted" but retained for a configurable number of days (1–365). You can undelete it within that window. After the window, it is permanently gone.

**Versioning:** Every time a blob is modified (overwritten), Azure automatically keeps the previous version. You can restore any previous version of any blob at any time (as long as versioning is enabled).

| | Soft delete | Versioning |
|---|---|---|
| Protects against | Accidental deletion | Accidental overwrite AND deletion |
| Keeps how many copies | 1 (the deleted version) | All previous versions |
| Storage cost | Low (only deleted blobs) | High (every version of every blob) |
| Restore operation | Undelete the blob | Select a specific version to restore |

**When to use soft delete:** Protect against accidental container or file deletion. Low cost, simple to implement. Use on all production containers.

**When to use versioning:** When data files are frequently overwritten and you need to audit or roll back to any previous state (compliance, legal hold, debugging). Be aware of cost — on a busy Silver table overwritten daily, versioning can 30× your storage cost.

**Data lake recommendation:** Enable soft delete on all containers (low cost, high protection). Enable versioning only on critical metadata files (Delta log, schema definitions) where rollback is essential.

---

**Q30 — Fintech startup storage architecture**

```
Layer        Container    Account         Tier    Redundancy  Who has access
─────────────────────────────────────────────────────────────────────────────────
Raw events   bronze       stadlsprod001   Cool    ZRS         Ingestion SPN: Contributor
(permanent)                                                   Data engineers: Contributor
                                                              Compliance: Reader

Daily clean  silver       stadlsprod001   Hot     ZRS         Spark SPN: Contributor
dataset                                                       Analysts: Reader
                                                              Data engineers: Contributor

Real-time    gold/fraud   stadlsprod001   Hot     ZRS         Fraud service SPN: Contributor
fraud scores                                                  Risk team: Reader

ML training  ml-data      stadlsprod001   Cool    LRS         DS team SPN: Contributor
data                                                          Data engineers: Contributor
(rebuilt monthly — LRS acceptable because it can be rebuilt)

Checkpoints  checkpoints  stadlsprod001   Hot     ZRS         Spark SPN: Contributor
```

**Key decisions:**

1. **One storage account, multiple containers** — simpler to manage, RBAC applied per container
2. **Bronze in Cool tier** — permanently retained but rarely queried directly (Spark reads it, not analysts); Cool saves ~50% vs. Hot
3. **Silver and Gold in Hot** — actively queried by dashboards and APIs; latency matters
4. **ML data in LRS** — it is rebuilt from Silver monthly; if lost, can be regenerated; LRS saves cost
5. **ZRS for everything production** — 99.99% availability; no geo-replication needed since Australia data residency is required
6. **Managed Identities for all service principals** — no connection strings, no keys, automatic rotation
7. **Resource locks on the storage account** — prevent accidental deletion
8. **Blob soft delete: 30 days** — recovery window for accidental deletion
9. **Lifecycle policy:** Bronze blobs older than 2 years move to Archive tier (compliance retention at minimal cost)
