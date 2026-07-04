# Writing Standards — EV Intelligence Azure Course

These are the rules followed when writing every `.md` guide and every `.ipynb` notebook in this project.
Reference this file before writing any new day's content.

---

## 1. General Principles

- Every guide is written for a student who has zero Azure experience but can follow exact steps.
- Nothing is assumed — every acronym, every button name, every CLI flag is explained.
- UI steps and CLI steps are always both provided. Student can choose one.
- Each step is atomic: one action per line, one outcome per step.

---

## 2. `.md` Guide Files — Rules

### Structure of every day's main file (e.g. `DAY2_BRONZE_INGESTION_ADF_SETUP.md`)

```
# Day N — <Title>
**Session:** <duration> | **Goal:** <one line>

> Region note (if applicable)
> Free credit note (if applicable)

## Glossary — What Is Each Azure Service?
(table: Term | Plain English Definition)

## What You Will Have at the End of Day N
(bullet list of tangible outputs)

## Part 1 — <First Resource>  (10 min)
> **Cost: ₹X** — explanation of why

### What is <Service>?
(3–5 line plain English explanation)

### N.1 Via Azure Portal
(numbered steps, exact button names in bold)

### N.2 Via CLI
> CMD / PowerShell note before code
Single line version (CMD / PowerShell — labeled)
Multi-line version (bash / Git Bash only — labeled)

## Part N — ...

## Day N Cost Summary
(table: Resource | Cost)

## End of Session — STOP THE CLUSTER
(always included for any day that uses Databricks)

## Day N Checklist
(checkbox list — one item per thing created or verified)

## Common Errors on Day N
(table: Error | Fix)
```

### Cost blocks
Every Part heading must have a cost callout:
```
> **Cost: ₹X** — explanation
```
Include minimum-cost config tips when there is a cheaper vs more expensive option.

### CLI blocks
- Always provide both single-line (CMD / PowerShell) and multi-line (bash) versions
- Always add this note before the first CLI block in any Part that has a multi-line example:
  > **CMD / PowerShell users:** The `\` line continuation below is bash syntax and will break in CMD/PowerShell. Use the single-line version to copy-paste directly.
- Label each code block clearly:
  ```
  **Single line (CMD / PowerShell — copy-paste this):**
  ```bash
  az ...
  ```
  **Multi-line (bash / Git Bash only):**
  ```bash
  az ... \
    --flag value
  ```

### Tables
- Every service creation step includes: prerequisites, what you get, what it costs.
- Every error table has 3 columns: `Error | Cause | Fix`.

### Naming conventions used in this project

| Resource Type | Name Pattern | Example |
|---|---|---|
| Resource Group | `rg-ev-intelligence-dev` | — |
| ADLS Gen2 | `evdatalakedev` | globally unique, lowercase |
| Key Vault | `kv-ev-intelligence-dev` | globally unique |
| Databricks Workspace | `dbw-ev-intelligence-dev` | — |
| ADF Instance | `adf-ev-intelligence-dev` | — |
| Service Principal | `sp-ev-intelligence-dev` | — |
| Secret Scope | `kv-ev-scope` | fixed — used in all notebooks |
| Databricks Cluster | `dev-cluster` | — |
| Region | `Central India` (`centralindia`) | cheapest India region |

---

## 3. Notebook `.ipynb` Files — Rules

### Cell structure
Every notebook follows this alternating pattern:
```
[markdown cell]  — heading + explanation of what the next code cell does
[code cell]      — clean runnable code only
[markdown cell]  — heading + explanation
[code cell]      — clean runnable code only
...
```

### Markdown cells (above each code cell)
- Start with `## Cell N — <Short Title>`
- Explain WHAT the cell does in 1–2 sentences
- Explain each line / key concept in plain English
- List expected output
- List common errors in a table if relevant
- NO code snippets inside markdown cells — all code goes in code cells

### Code cells — strict rules
- **Zero inline comments** (`#`) inside code cells
- All explanations live in the markdown cell ABOVE the code cell
- Variable names must be self-documenting (no abbreviations)
- `print()` statements must show the student that the step worked — every code cell prints something
- Always use `dbutils.secrets.get(scope=SCOPE, key=...)` — never hardcode any credential

### First cell of every notebook (markdown)
```markdown
# NN — <Notebook Title>
**Day N | Part N.N**

What this notebook does: <2–3 sentences>

**Prerequisites:**
- <notebook or step that must run first>
- <secrets that must exist in Key Vault>
```

### Auth patterns — which to use

| Scenario | Protocol | Auth method |
|---|---|---|
| Your ADLS Gen2 (Bronze/Silver/Gold) | `abfss://` | SP OAuth via `spark.conf.set()` |
| External blob with SAS token | `wasbs://` | SAS token via `spark.conf.set()` |
| VoltGrid API | `requests.post/get` | `Authorization: Token <value>` |
| Key Vault secrets | `dbutils.secrets.get()` | secret scope `kv-ev-scope` |

**Never use:**
- `dbutils.fs.mount()` — deprecated, blocked on Standard/Shared/Serverless clusters
- `/mnt/` paths — legacy mount syntax, do not use
- Hardcoded credentials — all secrets from Key Vault only

### SP OAuth config pattern (use in every notebook that touches ADLS)
```python
SCOPE = "kv-ev-scope"
storage_account  = dbutils.secrets.get(scope=SCOPE, key="adls-account-name")
sp_client_id     = dbutils.secrets.get(scope=SCOPE, key="sp-client-id")
sp_client_secret = dbutils.secrets.get(scope=SCOPE, key="sp-client-secret")
sp_tenant_id     = dbutils.secrets.get(scope=SCOPE, key="sp-tenant-id")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", sp_client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", sp_client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{sp_tenant_id}/oauth2/token")

def abfss(container, path=""):
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base
```

### SAS token config pattern (use when reading external blob)
```python
source_account = dbutils.secrets.get(scope=SCOPE, key="source-storage-account")
source_container = dbutils.secrets.get(scope=SCOPE, key="source-container")
sas_token = dbutils.secrets.get(scope=SCOPE, key="source-sas-token")

spark.conf.set(
    f"fs.azure.sas.{source_container}.{source_account}.blob.core.windows.net",
    sas_token
)
```

### VoltGrid API response structure
```json
{
  "data": [ { ...record... } ],
  "pagination": {
    "page": 1,
    "page_size": 5,
    "total": 125430,
    "total_pages": 25086
  }
}
```
- Records are under `"data"` — NOT `"results"` (common mistake from other frameworks)
- `pagination.total_pages` drives the pagination loop

### Delta Lake write patterns
```python
# Bronze append (API pagination loop — each page)
df_page.write.format("delta").mode("append").save(abfss("bronze", "api/payments/"))

# Bronze append partitioned (blob sessions)
df.write.format("delta").mode("append").partitionBy("ingestion_date", "ingestion_hour").save(abfss("bronze", "blob/iot_sessions/"))

# External Delta table (production pattern — DROP TABLE leaves ADLS files intact)
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.payments
    USING DELTA LOCATION 'abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/'
""")

# Internal Delta table (learning comparison — DROP TABLE deletes files)
df.write.format("delta").mode("overwrite").saveAsTable("bronze.payments_internal")
```

---

## 4. Companion `.md` Guide Files (per notebook)

Every notebook has a companion `NN_NOTEBOOK_NAME.md` guide in the day's folder. It mirrors the notebook cell-by-cell with the same structure:

```
# NN — <Title>
**Day N | Part N.N**

## What is this notebook doing?
(3–5 line summary)

---

## Key Terms — Read This Before Running
(glossary of terms used in this notebook — explain each CLI/API/Spark concept)

---

## Prerequisites
(table: Secret Name | Value | Notes)

---

## Cell 1 — <Title>
**What it does:** ...
**Line by line:**
- `variable_name` — what it stores and why

```python
clean code here — zero inline comments
```

**Expected output:**
```
output here
```

**Errors:**
| Error | Cause | Fix |
|---|---|---|
```

### Rules for companion `.md` files
- Code blocks contain **zero inline `#` comments** — all explanation is above the block as plain text
- Every `**Line by line:**` section explains every non-obvious line using `-  \`code\` — explanation` format
- Every code block is followed by `**Expected output:**` and `**Errors:**` table
- All code must be clean copy-pasteable — no placeholder comments inside

---

## 5. What Never Goes in This Project

| Don't do this | Do this instead |
|---|---|
| `dbutils.fs.mount()` | `spark.conf.set()` + `abfss://` |
| `/mnt/bronze/` paths | `abfss://bronze@evdatalakedev.dfs.core.windows.net/` |
| Hardcoded credentials | `dbutils.secrets.get(scope="kv-ev-scope", key=...)` |
| `data.get("results", [])` | `data.get("data", [])` — VoltGrid uses `"data"` key |
| Inline `#` comments in code cells | Move to markdown cell above as plain text |
| Inline `#` comments in `.md` code blocks | Move to plain text above the code block |
| Generic variable names (`df1`, `x`, `temp`) | Self-documenting names (`df_payments_page`, `watermark_value`) |
| All logic in one cell | Split: one concept per cell, markdown header above each |
| Skipping error tables | Every cell must have an Errors table |

---

## 6. Day File Map

| Day | Main file | Topic |
|---|---|---|
| 1 | `day_1_kickoff_architecture_azure_setup/DAY1_AZURE_SETUP.md` | Resource provisioning, SP, Key Vault, Databricks, storage connect |
| 2 | `day_2_bronze_ingestion_adf_setup/DAY2_BRONZE_INGESTION_ADF_SETUP.md` | ADF provisioning, linked services, API + blob pipelines, Bronze Delta tables |
