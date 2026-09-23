# Day 2 — Interview Solutions: ADLS Gen2 Access & SAS Tokens

---

## Concept 1: SAS Tokens

**Q1 — What is a SAS token? How is it different from an access key?**

A **SAS (Shared Access Signature) token** is a cryptographically signed URL that grants specific, time-limited permissions on a specific Azure Storage resource — without sharing the master account key.

| | Access Key | SAS Token |
|---|---|---|
| Scope | Full account — all containers, all operations | Scoped: specific container, file, permissions, and expiry |
| Expiry | Never expires (until rotated) | Always has an expiry time |
| Revocation | Rotate the key — breaks everything signed by it | Let it expire, or rotate the signing key |
| Who gets it | Internal services only | Can be shared with vendors, external systems |
| Risk if leaked | Attacker has full account access forever | Attacker has scoped access until expiry |

The access key is like handing someone the master key to your house. A SAS token is like a temporary pass that only opens one door and expires tonight.

---

**Q2 — Three types of SAS tokens. Which is most secure?**

| Type | Scope | Signed by |
|---|---|---|
| Account SAS | Entire storage account — any container, any service | Account access key |
| Service SAS | One specific container or blob | Account access key |
| User Delegation SAS | One specific container or blob | Azure AD identity (your user or Service Principal) |

**Most secure: User Delegation SAS**

Why:
- Signed by your Azure AD identity — not the account key
- If your Azure AD account is revoked, all User Delegation SAS tokens issued by that account immediately become invalid (even before their expiry)
- No need to handle or distribute the storage account key
- Azure AD audit logs record that you generated the SAS — full traceability

Generate it with: `az storage fs generate-sas --auth-mode login`

---

**Q3 — `sp=rl` — what permissions does this grant?**

`sp` stands for "signed permissions". Each letter maps to a permission:

| Letter | Permission |
|---|---|
| r | Read |
| l | List |

So `sp=rl` grants **Read + List** only.

The holder can:
- Read (download) any blob/file the SAS applies to
- List the contents of a container

The holder CANNOT:
- Write, create, delete, or append any files
- Access containers or paths not covered by the SAS scope

---

**Q4 — Account SAS with full permissions, 1-year expiry, shared with a vendor for one file — three problems**

1. **Over-permissioned:** The vendor has Write and Delete permissions — they could overwrite or delete data they should only be reading. Should be `sp=rl` (read + list only).

2. **Over-scoped:** An Account SAS covers the entire storage account. If the vendor's system is compromised, the attacker can read or write to every container — not just the one CSV file. Should be a Service SAS scoped to `bronze/` or a specific directory.

3. **Expiry too long:** 1-year SAS is essentially permanent from a security standpoint. If the token is leaked (logged, cached, committed to code), the attacker has a year to exploit it. Should be 24–48 hours maximum for a vendor data share.

**Correct approach:** Service SAS, `sp=rl`, expiry 48 hours, scoped to the specific blob path.

---

**Q5 — What happens when Access Key 1 is rotated?**

All SAS tokens that were **signed using Key 1** immediately become invalid — even if their expiry date is in the future.

SAS tokens signed using **Key 2** are NOT affected.

This is why:
- Azure provides two keys (Key 1 and Key 2) — you can rotate one at a time
- Generate SAS tokens with Key 2, rotate Key 1 (invalidates all old Key 1 tokens), then switch new tokens to Key 1, rotate Key 2
- This gives you zero-downtime key rotation with zero valid "old" tokens

**Key rotation is your emergency stop button** — if a SAS token is leaked, rotate the signing key to invalidate all tokens signed by it.

---

**Q6 — Developer commits SAS token to public GitHub — immediate steps**

1. **Rotate the signing key immediately:**
   - Go to portal → storage account → Access keys → **Rotate Key 1**
   - All SAS tokens signed by Key 1 are now invalid — attacker's access is cut off

2. **Check the audit logs:**
   - Go to portal → storage account → **Monitoring → Diagnostic settings** (if enabled) or **Azure Monitor Storage logs**
   - Look for any unexpected reads, writes, or deletes since the token was committed

3. **Remove the token from GitHub:**
   - Delete the file from the repository AND purge it from git history (`git filter-branch` or `git filter-repo`)
   - GitHub's secret scanning may have already flagged it — check repository security alerts
   - Even after deletion, assume the token was scraped (GitHub is indexed by bots within minutes)

4. **Assess the damage:**
   - What permissions did the token have? Read-only vs. write/delete?
   - What data is in the containers the token could access?
   - If PII was accessible, follow your company's data breach notification process

5. **Prevent recurrence:**
   - Add pre-commit hooks that scan for Azure SAS tokens and access key patterns
   - Use Azure Key Vault for all secrets — never hardcode in code files

---

**Q7 — What is a User Delegation SAS?**

A User Delegation SAS is a SAS token signed using an **Azure AD OAuth token** from a specific user or Service Principal — not the storage account key.

Differences:
| | Account SAS / Service SAS | User Delegation SAS |
|---|---|---|
| Signing identity | Storage account key | Azure AD user or Service Principal |
| Revocation | Rotate the account key | Revoke the Azure AD account or Service Principal |
| Audit | Storage account logs | Azure AD sign-in logs + storage logs — full trail |
| Max expiry | Up to 7 days | Up to 7 days |

Why more secure:
- The account key never leaves Azure — no risk of it being copied
- The SAS is tied to an Azure AD identity — if that identity is disabled, SAS is immediately invalid
- Azure AD provides MFA, conditional access, and identity risk policies on top

Generate with Azure CLI:
```bash
az storage fs generate-sas \
  --account-name stadlsdev001 \
  --name bronze \
  --permissions rl \
  --expiry 2024-01-17T00:00:00Z \
  --auth-mode login
```

---

**Q8 — Third-party vendor needs read-only access to `bronze/vendor-xyz/` for 48 hours**

**Exact SAS configuration:**

| Setting | Value | Reason |
|---|---|---|
| Type | Service SAS (Container-scoped) | Limits to `bronze` only — not other containers |
| Container | `bronze` | Scoped to bronze only |
| Path prefix | `vendor-xyz/` | Further scope — vendor cannot list other directories |
| Permissions | `r` + `l` (read + list) | Read-only — no write, no delete |
| Expiry | 48 hours from now | Minimum needed, auto-expires |
| Protocol | HTTPS only | Prevents token from being intercepted over HTTP |
| IP restriction | Add vendor's IP range if known | Blocks token from being used from other networks |

**Portal path:** Storage account → Containers → `bronze` → `...` → Generate SAS → fill in above settings.

For even tighter scope, use POSIX ACLs (ADLS Gen2 feature) to grant the vendor's identity read permission on `bronze/vendor-xyz/` only — then use a User Delegation SAS for that identity.

---

## Concept 2: Python SDK

**Q9 — Python package for ADLS Gen2. Why preferred over `azure-storage-blob`?**

Package: `azure-storage-file-datalake`

Why preferred over `azure-storage-blob`:

| Capability | `azure-storage-blob` | `azure-storage-file-datalake` |
|---|---|---|
| Read/write files | Yes | Yes |
| Create real directories (HNS) | No — only virtual prefix | Yes — `create_directory()` |
| Atomic directory rename | No | Yes — `rename_directory()` |
| POSIX ACL management | No | Yes — `set_access_control()` |
| Path-level permissions | No | Yes |

`azure-storage-blob` works for basic file I/O but it treats ADLS Gen2 like flat blob storage — it cannot leverage hierarchical namespace features like atomic directory operations or per-directory permissions.

---

**Q10 — `DataLakeServiceClient` vs `FileSystemClient` vs `DataLakeFileClient`**

```
DataLakeServiceClient          → represents the whole storage account
    │
    └── FileSystemClient       → represents one container (called "file system" in ADLS Gen2)
            │
            └── DataLakeFileClient   → represents one file
            └── DataLakeDirectoryClient → represents one directory
```

| Client | What it does |
|---|---|
| `DataLakeServiceClient` | Account-level: list containers, create/delete file systems |
| `FileSystemClient` | Container-level: list paths, create directories, get files |
| `DataLakeFileClient` | File-level: create, read, append, flush, delete |

You always start with `DataLakeServiceClient` and navigate down to the specific file or directory you need.

---

**Q11 — What is `DefaultAzureCredential`? Authentication order?**

`DefaultAzureCredential` is a convenience credential from `azure-identity` that automatically tries multiple authentication methods in sequence — the first one that succeeds is used.

**Order of methods tried:**

1. **EnvironmentCredential** — checks environment variables: `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID` (Service Principal)
2. **WorkloadIdentityCredential** — Kubernetes workload identity
3. **ManagedIdentityCredential** — checks if running on Azure infrastructure (VM, Databricks, Functions, AKS)
4. **SharedTokenCacheCredential** — reads cached Azure CLI or Visual Studio tokens
5. **VisualStudioCodeCredential** — reads VS Code Azure account extension token
6. **AzureCliCredential** — uses token from `az login`
7. **AzurePowerShellCredential** — uses token from `Connect-AzAccount`

**In practice:**
- Local development: `az login` first → `AzureCliCredential` picks it up
- Azure VM / Databricks: `ManagedIdentityCredential` picks it up automatically — no code change needed
- Same code works in both environments — zero-config production deployment

---

**Q12 — Hardcoded SAS token in script — how to rewrite for Databricks production?**

**Problem:** Hardcoded SAS token in code is a secret — it can be leaked via git, logs, or job output. SAS tokens also expire, causing production failures.

**Production approach: Use Managed Identity + RBAC**

On Databricks, the cluster has a **Managed Identity** (or the workspace is configured with a **Service Principal**). Assign the `Storage Blob Data Contributor` RBAC role on the ADLS Gen2 account to this identity.

```python
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential

# No secrets in code — Managed Identity provides the credential automatically
credential = DefaultAzureCredential()

service_client = DataLakeServiceClient(
    account_url="https://stadlsprod001.dfs.core.windows.net",
    credential=credential
)
```

Alternatively in Databricks, use the native `abfss://` path with cluster-attached service principal:
```python
df = spark.read.parquet("abfss://bronze@stadlsprod001.dfs.core.windows.net/payments/")
```
Databricks automatically uses the attached Service Principal/Managed Identity — no explicit credentials in code.

---

**Q13 — Why `append_data()` AND `flush_data()` separately?**

ADLS Gen2 (unlike Blob Storage) uses an **append + commit** model for file writes, based on the HDFS/Hadoop filesystem model:

1. **`create_file()`** — creates an empty file, opens a write lease
2. **`append_data(data, offset, length)`** — stages a block of data at a specified offset (data is buffered, not committed to the file yet)
3. **`flush_data(position)`** — commits all appended data up to `position`, closing the write and making the file readable

**If you skip `flush_data()`:**
- The file exists (was created by `create_file()`)
- The data blocks were staged (via `append_data()`)
- But the file is still in an **uncommitted state** — readers see an empty file or the previous committed state
- Other jobs reading the file get empty or stale data with no error

This is by design — it allows atomic multi-part writes (write 1 GB in 100 MB chunks, all or nothing). `flush_data()` is the commit point.

---

**Q14 — Silently ignoring `ResourceExistsError` — good pattern?**

For directory creation it is generally acceptable because:
- Creating a directory that already exists is idempotent — the outcome is the same either way
- In concurrent pipelines, two jobs might try to create the same directory simultaneously — the second one gets `ResourceExistsError` and continuing is correct

**When it CAN cause a bug:**
```python
try:
    file_client.create_file()   # Creates a NEW empty file
except ResourceExistsError:
    pass
```
If you silently ignore `ResourceExistsError` on **file creation** and then call `append_data(offset=0)`, you will overwrite the first `len(data)` bytes of an existing file — potentially corrupting it. Always check whether you intend to create-or-overwrite vs. create-only.

**Better pattern:**
```python
# Explicitly overwrite: use overwrite=True
file_client.upload_data(data=encoded, overwrite=True)
```

---

**Q15 — `get_paths(recursive=False)` vs `recursive=True`**

Directory structure:
```
orders/2024/01/15/file1.parquet
orders/2024/01/15/file2.parquet
orders/2024/01/16/file1.parquet
```

**`recursive=False` with `path="orders/2024/01"`:**
Lists only the **immediate children** of `orders/2024/01/` — which are the subdirectories:
```
orders/2024/01/15   (is_directory=True)
orders/2024/01/16   (is_directory=True)
```
Files inside those subdirectories are NOT listed.

**`recursive=True` with `path="orders/2024/01"`:**
Lists everything recursively:
```
orders/2024/01/15              (is_directory=True)
orders/2024/01/15/file1.parquet (is_directory=False)
orders/2024/01/15/file2.parquet (is_directory=False)
orders/2024/01/16              (is_directory=True)
orders/2024/01/16/file1.parquet (is_directory=False)
```

Use `recursive=False` when building a directory tree UI or when you only want to iterate over date partitions. Use `recursive=True` when you want every single file to pass to Spark or a copy operation.

---

**Q16 — ABFS URL format used by Spark and Databricks**

ABFS = **Azure Blob File System** — the protocol driver that lets Spark/Hadoop talk to ADLS Gen2 using native filesystem semantics.

```
abfss://bronze@stadlsdev001.dfs.core.windows.net/orders/2024/01/15/

Parts:
abfss://          = Azure Blob File System Secure (HTTPS)
bronze            = container (file system) name
@stadlsdev001     = storage account name
.dfs.core.windows.net = ADLS Gen2 endpoint (dfs = data lake service)
/orders/2024/01/15/   = path within the container
```

In Spark:
```python
df = spark.read.parquet("abfss://bronze@stadlsdev001.dfs.core.windows.net/orders/")
df.write.parquet("abfss://silver@stadlsdev001.dfs.core.windows.net/orders/cleaned/")
```

---

**Q17 — Databricks Mount vs. `abfss://` with Managed Identity**

| | Mount (`/mnt/bronze/`) | `abfss://` with Managed Identity |
|---|---|---|
| Setup | One-time mount command with credentials | Cluster IAM / Managed Identity — zero credentials |
| Security | Credentials stored in Databricks secret scope (still a secret) | No credentials — Azure handles auth via identity |
| Multi-workspace | Mount is per workspace — must be recreated | Same `abfss://` path works in any workspace with the right identity |
| Unity Catalog | Not compatible — Unity Catalog requires `abfss://` | Required for Unity Catalog |
| Recommended | Legacy approach — being phased out | Modern recommended approach |

**Recommendation:** Use `abfss://` with Managed Identity and Unity Catalog. Databricks has officially deprecated mount points in Unity Catalog environments. Managed Identity eliminates secret management entirely.

---

## Concept 3: AzCopy

**Q18 — What is AzCopy? Two scenarios where you prefer it over Python SDK.**

AzCopy is a command-line tool for high-speed, parallelised data transfers to and from Azure Storage. It uses optimised multi-threaded transfers and supports resumable copies.

**Scenarios where AzCopy is preferred:**

1. **Bulk initial data lake seeding** — uploading 50 TB of historical data from an on-premises server. AzCopy uses parallel threads and saturates the network connection far more efficiently than a Python SDK loop.

2. **Server-to-server copies** — copying data between two Azure storage accounts. AzCopy transfers data directly in Azure's backbone network — no traffic goes through your machine. Python SDK would download to your machine then re-upload.

3. (Bonus) **Simple scripting without Python** — a bash/PowerShell script that runs as a scheduled task and moves files to ADLS Gen2 without requiring a Python environment.

---

**Q19 — `azcopy copy` vs. `azcopy sync`**

| | `azcopy copy` | `azcopy sync` |
|---|---|---|
| What it does | Copies all source files to destination | Copies only files that are new or changed |
| Existing files | Overwrites them | Skips files that are already up to date |
| Deleted source files | Does not delete from destination | Optionally deletes from destination if removed from source (`--delete-destination`) |
| Use case | First-time migration | Ongoing incremental updates |

**When to use sync:** Daily incremental updates where most files haven't changed. Running `copy` on 100,000 files daily re-uploads all 100,000 even if only 10 are new — expensive and slow. `sync` only touches the 10 new ones.

---

**Q20 — Server-to-server transfer — what does it mean?**

When you copy from one Azure storage account to another using AzCopy, the data does NOT travel through your local machine:

```
Normal copy (through local machine):
Azure Account A → download → your laptop → upload → Azure Account B
(bandwidth limited by your internet connection — e.g., 100 Mbps)

Server-to-server copy (AzCopy):
Azure Account A ─────────────────────────────→ Azure Account B
              (directly in Azure's network — no local download)
(bandwidth: Azure backbone — potentially 10+ Gbps)
```

Your machine just sends the instruction — AzCopy tells Azure "copy from here to there" and Azure's infrastructure handles the data movement. This is orders of magnitude faster for large datasets and costs no egress bandwidth on your end.

---

**Q21 — 50 TB migration from on-premises to ADLS Gen2 in 48 hours via 1 Gbps**

**Calculation:**
- 50 TB = 50,000 GB
- 1 Gbps = 125 MB/s = 0.45 TB/hour
- Raw transfer time: 50 TB ÷ 0.45 TB/h ≈ 111 hours — cannot finish in 48 hours!

**Solutions:**

1. **AzCopy with maximum parallel threads:** AzCopy saturates the link and uses up to 32 parallel connections by default. On a dedicated 1 Gbps uplink with no other traffic you can approach theoretical maximum.

2. **Azure Data Box:** Microsoft ships a physical appliance (80 TB or 100 TB) to your office. You copy data to it locally, ship it back, and Microsoft loads it into Azure. Bypasses the internet link entirely. Best for > 10 TB migrations or slow internet.

3. **ExpressRoute or VPN:** Set up a dedicated private circuit between your network and Azure for high-throughput migration.

4. **Split and parallel:** If data is organised by year/month, run 4 AzCopy processes in parallel (one per year) — each on a different machine — to multiply throughput.

For 50 TB in 48 hours with only 1 Gbps, **Azure Data Box** is the recommended solution.

---

**Q22 — AzCopy fails halfway — does it restart or resume?**

**It resumes** from where it stopped.

AzCopy automatically maintains a **job journal** — a log file stored locally (on Windows: `%USERPROFILE%\.azcopy\`) that tracks every file transferred in the current job. Each job gets a unique Job ID.

When you rerun the same AzCopy command, it:
1. Detects the previous job journal for that source/destination combination
2. Skips files that are already marked as transferred in the journal
3. Resumes from the first un-transferred file

You can also explicitly resume a specific job:
```bash
azcopy jobs resume <job-id>
```

This makes AzCopy safe for large migrations — a network blip at file 60,000 does not mean starting over.

---

**Q23 — Running `azcopy copy` again after 3 new files are added**

Running `azcopy copy` again:
- AzCopy will re-upload ALL files — including the 100,000 already uploaded
- It overwrites them with identical content (no harm done, but wasteful)
- The 3 new files will be uploaded too

**More efficient command:**
```bash
azcopy sync "C:\data\" "https://stadlsprod.dfs.core.windows.net/bronze/?sv=..." --recursive
```

`sync` compares source and destination. Files already in ADLS Gen2 that are identical to the local copy are skipped. Only the 3 new files are uploaded. Much faster and cheaper.

---

## Mixed / Senior-Level Questions

**Q24 — Three risks of access keys instead of SAS tokens for external sharing**

1. **Scope — access keys are all-or-nothing.** An access key grants full control of the entire storage account — all containers, all operations. A vendor who only needs to read one CSV file gets the ability to delete all your data, overwrite all your containers, or enumerate all your sensitive files. There is no way to scope an access key to a subset.

2. **No expiry — access keys never expire.** A SAS token you forget about expires and becomes useless. A leaked access key remains valid forever until you explicitly rotate it. In practice, keys shared "temporarily" with a vendor often stay active for years.

3. **No revocation without downtime.** To revoke an access key, you must rotate it — which immediately breaks every service currently using that key (your own pipelines, backup jobs, monitoring scripts). With SAS tokens, you can let them expire naturally without disrupting anything.

---

**Q25 — Principle of least privilege — three role examples**

**Principle of least privilege:** Give each identity only the permissions it needs to do its job — nothing more.

| Identity | Access needed | Least-privilege implementation |
|---|---|---|
| Ingestion pipeline (ADF) | Write to `bronze/`, read from `bronze/` | Managed Identity with **Storage Blob Data Contributor** on `bronze` container only — not `silver` or `gold` |
| Data analyst | Read Gold layer only — no raw PII | **Storage Blob Data Reader** scoped to `gold/` container. ADLS Gen2 POSIX ACL: Execute on all parent directories, Read on `gold/` |
| Vendor | Read specific directory for 48 hours | Service SAS: `sp=rl`, scoped to `bronze/vendor-data/`, 48-hour expiry |

The key principle: scope to the **smallest resource** (file/directory > container > account) with the **minimum permissions** (read vs. read+write vs. full) for the **shortest time** needed.

---

**Q26 — Analyst access to Gold but not Bronze — with audit trail**

**Implementation:**

1. **RBAC — Storage Blob Data Reader on `gold` container only:**
   - Go to `gold` container → Access Control (IAM) → Add role assignment
   - Role: Storage Blob Data Reader
   - Member: analyst's Azure AD user or group
   - This allows reading `gold/` but grants zero access to `bronze/` or `silver/`

2. **POSIX ACLs (ADLS Gen2) for fine-grained directory control:**
   - On `gold/`: Access ACL = `read + execute` for the analyst group
   - On `bronze/` and `silver/`: no ACL entry for the analyst group
   - Analyst's requests to `bronze/` return `403 Forbidden`

3. **Audit trail — Azure Monitor / Diagnostic Logs:**
   - Storage account → Diagnostic settings → enable "StorageRead" logs → send to Log Analytics
   - Every read operation is logged with: who (Azure AD identity), what (path), when, from where (IP)
   - Query in Log Analytics: `StorageBlobLogs | where identity contains "analyst@company.com"`

4. **Microsoft Purview (optional):** Scans ADLS Gen2 and classifies PII data automatically — you can enforce that classified PII containers have no analyst access.

---

**Q27 — Authentication vs. authorisation in ADLS Gen2**

**Authentication** = proving your identity — "Who are you?"
**Authorisation** = checking what you're allowed to do — "Are you allowed to do this?"

| Concept | Azure ADLS Gen2 example |
|---|---|
| Authentication | Presenting a valid Azure AD token (from `az login` or Managed Identity) — Azure verifies the token is genuine and hasn't expired |
| Authorisation | RBAC role check or POSIX ACL check — Azure checks whether the authenticated identity has permission to read/write the requested path |

You can be successfully **authenticated** (valid token) but **unauthorised** (403 Forbidden) if you don't have the right RBAC role or ACL on the resource. Both checks must pass.

---

**Q28 — Reading a 10 GB file with `readall()` — problem and fix**

**Problem:** `readall()` loads the entire 10 GB into your Python process's memory at once. This will:
- Crash the process with `MemoryError` if the machine has less than 10 GB of free RAM
- Even if it doesn't crash, it pins 10 GB of RAM while you process the file

**Fix — Stream the download in chunks:**

```python
download = file_client.download_file()

# Stream in 4 MB chunks
with open("local_output.parquet", "wb") as local_file:
    for chunk in download.chunks():
        local_file.write(chunk)
```

**Better fix — Don't download at all:**
For large Parquet files, read directly into a DataFrame using Spark or pandas with the ABFS path — the compute engine handles streaming and parallelism:

```python
# Databricks / PySpark — never downloads to driver
df = spark.read.parquet("abfss://silver@stadlsdev001.dfs.core.windows.net/payments/")

# pandas — streams efficiently
import pandas as pd
df = pd.read_parquet("abfss://silver@stadlsdev001.dfs.core.windows.net/payments/file.parquet")
```

---

**Q29 — Access key in `spark.conf.set()` — problem and alternative**

**Problem with access key in `spark.conf.set()`:**

```python
spark.conf.set(
    "fs.azure.account.key.stadlsprod001.dfs.core.windows.net",
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx=="
)
```

1. **Secret in code** — the key is visible in the notebook, in job logs, in git history, and to anyone who can view the cluster configuration
2. **All-or-nothing access** — this key grants the cluster full access to the entire storage account, not just the containers it needs
3. **Cluster-level scope** — if the key is set on the cluster, every user and job on that cluster can access the storage, bypassing any per-user access control
4. **No expiry** — access keys don't expire; a leaked key stays valid until manually rotated

**Azure-recommended alternative:**

**Option 1 — Managed Identity (recommended):**
- Assign the `Storage Blob Data Contributor` RBAC role to the Databricks cluster's Managed Identity on the specific ADLS Gen2 storage account
- No credentials in code at all:
  ```python
  df = spark.read.parquet("abfss://bronze@stadlsdev001.dfs.core.windows.net/payments/")
  ```

**Option 2 — Service Principal via Databricks Secret Scope:**
- Store the Service Principal client secret in Azure Key Vault
- Reference via Databricks secret scope — never appears in code:
  ```python
  spark.conf.set("fs.azure.account.oauth2.client.secret...", dbutils.secrets.get("kv-scope", "sp-secret"))
  ```

---

**Q30 (System design) — Complete ADLS Gen2 access control architecture**

| Team | Identity type | Auth method | RBAC role | Scope |
|---|---|---|---|---|
| Ingestion pipeline (ADF) | Managed Identity (ADF's system-assigned identity) | Azure AD token (automatic) | Storage Blob Data Contributor | `bronze` container only |
| Transform pipeline (Databricks) | Managed Identity (Databricks cluster) | Azure AD token (automatic) | Storage Blob Data Contributor | `silver` and `gold` containers; Storage Blob Data Reader on `bronze` |
| Data analysts | Azure AD User (or group) | Browser login / Power BI OAuth | Storage Blob Data Reader | `gold` container only |
| Data science team | Azure AD Group (DS team) | Azure AD token | Storage Blob Data Reader | `silver` and `gold`; Storage Blob Data Contributor on `ml-data` container |
| External vendor | SAS Token | Append to URL | N/A (SAS is self-contained) | Service SAS: `sp=rl`, scoped to `bronze/vendor-data/`, 7-day expiry |

**Additional security layers:**

- **POSIX ACLs** (ADLS Gen2): Set default ACLs on each container so new files inherit the right permissions automatically — prevents analysts from accidentally accessing newly created Bronze files
- **Azure Defender for Storage**: Detects unusual access patterns (bulk downloads, access from unexpected geographies) and alerts the security team
- **Diagnostic logs → Log Analytics**: All read/write operations logged with identity, path, timestamp, IP — full audit trail for compliance
- **Private endpoint**: Disable public internet access to the storage account; allow only Azure-internal traffic via private endpoint in the VNet — SAS tokens only work from within the VNet

**Key design principle:** Managed Identities for all Azure services — zero secrets in code. SAS token only for the one external party that cannot be given an Azure identity.
