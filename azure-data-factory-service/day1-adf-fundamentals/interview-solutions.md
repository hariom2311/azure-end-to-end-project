# Day 1 — Interview Solutions: ADF Core Concepts, Linked Services, Datasets & Copy Activity

> Complete answers for all 30 questions in `interview-questions.md`.

---

## Concept 1: What is Azure Data Factory

**Q1 — What is ADF, three things Python script cannot do out of the box**

**Azure Data Factory (ADF)** is Microsoft's managed, cloud-native data integration and orchestration service. It provides a visual, no-code/low-code environment to build, schedule, monitor, and manage data pipelines that move and transform data across 90+ supported connectors.

Three things ADF does that a Python script with a scheduler cannot without significant custom work:

1. **Built-in monitoring and run history:** ADF provides a Monitor panel with a full history of every pipeline run — success/failure, duration, activity-level breakdown, input/output JSON, retry history — all without writing a single line of logging code. A Python script needs a database, a logging framework, and a dashboard to match this.

2. **Managed retry and fault tolerance:** ADF Copy Activity has configurable retry policies (retry count, retry interval) and fault tolerance (skip incompatible rows, log skipped rows to a file). A Python script must implement all of this from scratch.

3. **Built-in connectors and Integration Runtime:** ADF ships with connectors for 90+ sources (Salesforce, SAP, Oracle, SQL Server, Event Hubs, S3, Snowflake, and more). Connecting to an on-premises SQL Server requires only deploying a Self-Hosted Integration Runtime agent — no firewall changes, no VPN configuration in code.

---

**Q2 — ETL vs. ELT; which is more common with ADF + Databricks**

**ETL (Extract, Transform, Load):**
- Extract raw data from source
- Transform it in a middle-tier compute engine (clean, join, aggregate)
- Load the clean, shaped result into the destination

**ELT (Extract, Load, Transform):**
- Extract raw data from source
- Load it as-is into the destination (data lake / data warehouse)
- Transform it inside the destination using its own compute (Spark, SQL)

| | ETL | ELT |
|---|---|---|
| Where transformation runs | Middle tier (SSIS, Informatica, ADF Data Flow) | Inside destination (Databricks Spark, Synapse SQL) |
| Destination receives | Clean, shaped data | Raw data |
| Best for | Traditional data warehouse (rigid schema-on-write) | Cloud data lake (schema-on-read, flexible) |
| Scalability | Bounded by the transform tier | Scales with the destination compute (Databricks auto-scale) |

**In ADF + Databricks architectures: ELT is more common.** ADF uses its Copy Activity to land raw data into ADLS Gen2 Bronze (fast, cheap, no transformation). Databricks then transforms Bronze → Silver → Gold using Spark. ADF orchestrates the chain (trigger Databricks notebooks after Copy completes). This pattern is preferred because:
- ADF Copy Activity is optimised for data movement, not transformation
- Databricks is far more powerful for complex transformations (joins, window functions, ML features)
- Decoupling ingest from transform makes each layer independently scalable and debuggable

---

**Q3 — The four ADF Studio panels**

| Panel | Icon | What you do here |
|---|---|---|
| **Author** | Pencil | Build and edit pipelines. Drag activities onto the canvas. Configure Source, Sink, Mapping tabs. Create Datasets. Write ADF expressions. Design Data Flows. |
| **Monitor** | Monitor | Observe every pipeline run, activity run, trigger run, and debug run. Filter by status, date, pipeline name. Drill into individual activity outputs. Set up alerts. |
| **Manage** | Toolbox | Create and edit Linked Services (connections). Configure Integration Runtimes. Create and manage Triggers. Connect to Git repository. Manage credentials and Key Vault references. |
| **Learn** | Graduation cap | Template gallery with pre-built pipeline templates for common patterns. ADF documentation links. Quick-start tutorials. Useful for finding a starting point before building a custom pipeline. |

**Development lifecycle usage:**
- Day 1 of a new pipeline: **Manage** (create Linked Services) → **Author** (build pipeline, datasets, activities)
- Testing: **Author** (run Debug) → **Monitor** (inspect Debug run output)
- Scheduling: **Manage** (create trigger) → **Author** (attach trigger to pipeline) → **Publish All**
- Operations: **Monitor** (daily monitoring of production runs, alerts, reruns)

---

**Q4 — Why migrate from Python + cron VM to ADF**

Three specific, concrete reasons:

**1. Centralised monitoring and alerting without custom code:**
With a Python/cron approach, monitoring requires building and maintaining a custom dashboard, log aggregation, and alerting system. ADF provides this out of the box — every run is logged, failures trigger configurable alerts (email, webhook, Azure Monitor), and 45 days of run history is accessible in the Monitor panel. When a pipeline fails at 3 AM, an alert fires automatically.

**2. Managed retries and fault tolerance:**
A Python script that fails mid-run requires someone to investigate, clean up partial state, and re-run manually. ADF Copy Activity has a configurable retry policy (e.g., retry 3 times with 30-second intervals). For transient failures (network timeout, rate limit), ADF retries automatically without human intervention.

**3. Connectivity to on-premises and cloud sources without managing VM infrastructure:**
The Python VM approach ties ingestion to the availability and configuration of one VM. If the VM is patched, restarted, or scaled, the scheduler is disrupted. ADF's Self-Hosted Integration Runtime runs as a lightweight agent that is separate from the orchestration layer. For cloud sources, ADF's Azure IR is fully serverless — there is no VM to manage, patch, or pay for when pipelines are not running (you pay per activity run, not per hour of VM time).

---

**Q5 — Integration Runtime: Azure IR vs. Self-Hosted IR**

An **Integration Runtime (IR)** is the compute infrastructure that ADF uses to perform data movement, activity dispatch, and SSIS package execution. It is the "engine" that actually runs the Copy Activity or sends a command to a Databricks cluster.

| | Azure Integration Runtime | Self-Hosted Integration Runtime |
|---|---|---|
| Where it runs | Microsoft-managed Azure cloud infrastructure | A server or VM you own (on-premises or cloud) |
| Network access | Public internet only | Private networks, on-premises databases, behind firewalls |
| Setup required | None — it is built-in (AutoResolveIntegrationRuntime) | Install the Self-Hosted IR agent on your server |
| Maintenance | None — Microsoft manages it | You manage the agent, updates, and host machine |
| Cost | Per activity run (DIUs used) | IR node cost + your VM/server cost |
| Use case | Copying data between cloud services (HTTP, ADLS, SQL MI on public endpoint) | On-premises Oracle, SQL Server, SAP; databases inside a VNet not exposed to internet |

**When you need a Self-Hosted IR:** Any source or destination that is:
- Inside a corporate network not accessible from the public internet
- An on-premises database (Oracle, SQL Server, SAP HANA)
- A cloud resource inside a private VNet without a public endpoint
- Behind a firewall that blocks inbound connections from Azure

---

**Q6 — On-premises Oracle ingestion into ADLS Gen2**

**Integration Runtime needed:** Self-Hosted Integration Runtime (SHIR).

**Steps:**

1. **Install the SHIR agent on-premises:**
   - In ADF Studio → Manage → Integration runtimes → **"+ New"**
   - Select **"Self-Hosted"** → give it a name: `ir-onprem-oracle`
   - ADF generates an authentication key
   - Download and install the SHIR agent on a Windows Server or VM inside the corporate network that can reach the Oracle database
   - Enter the authentication key during installation to link it to your ADF instance

2. **Create an Oracle Linked Service:**
   - In Manage → Linked services → **"+ New"** → search **"Oracle"**
   - Set the **Integration runtime** to `ir-onprem-oracle` (not AutoResolveIntegrationRuntime)
   - Enter the Oracle connection string, username, and password (or Key Vault reference)

3. **Data flow:**
   - Oracle (on-premises, private network) → SHIR agent (also on-premises, reads from Oracle) → SHIR pushes data out to Azure via HTTPS → ADF receives data → writes to ADLS Gen2

The SHIR initiates outbound HTTPS connections to Azure — no inbound firewall rules are required on the corporate network. Only port 443 (HTTPS) outbound must be allowed.

---

**Q7 — Debug run vs. Trigger run**

| | Debug run | Trigger run |
|---|---|---|
| How triggered | Manual click of "Debug" button in ADF Studio | Schedule, event, tumbling window, or manual trigger |
| Uses published version? | No — uses the current **draft** (unsaved/unpublished) version of the pipeline | Yes — uses the **last published** version of the pipeline |
| Compute used | Same Azure IR or SHIR compute (same cost per activity run) | Same |
| Appears in Monitor? | Yes — under **"Debug runs"** tab in Monitor | Yes — under **"Pipeline runs"** tab |
| Parameters | You enter parameter values in a prompt dialog | Parameters come from the trigger definition or pipeline defaults |
| Purpose | Test pipeline changes without publishing; iterate quickly on a draft | Production execution on a schedule or event |

**Key implication:** If you make changes to a pipeline and want to test them, use Debug — you do not need to Publish first. Debug runs your current draft. But when a trigger fires at 6:00 AM, it runs whatever was last **Published** — your draft changes are invisible to triggers until you Publish All.

---

**Q8 — Pipeline vs. Activity**

A **pipeline** is the container — a logical grouping of one or more activities with a defined execution flow (sequence, branching, loops). A pipeline has parameters, triggers, and a canvas where activities are connected.

An **activity** is a single unit of work within a pipeline — a Copy, a Databricks Notebook run, a Stored Procedure call, a Lookup, a Web request, or a Wait.

**Example: Three-activity pipeline `pl_daily_payment_ingest`**

```
pl_daily_payment_ingest
  │
  ├── [1] LookupLatestSourceFile     (Lookup Activity)
  │       Reads config file from ADLS Gen2 to find which HTTP URL to pull today's file from
  │
  ├── [2] CopyPaymentsHttpToBronze   (Copy Activity)
  │       Reads CSV from HTTP, writes Parquet to ADLS Gen2 bronze/payments/{run_date}/
  │       (connected after [1] with success dependency)
  │
  └── [3] NotifyOnSuccess            (Web Activity)
          Sends a POST request to a Teams webhook to notify the team the copy succeeded
          (connected after [2] with success dependency)
```

Activity [1] outputs the source URL dynamically. Activity [2] uses that URL from the Lookup output (`@activity('LookupLatestSourceFile').output.firstRow.source_url`). Activity [3] only runs if Activity [2] succeeds.

---

**Q9 — Diagnosing a pipeline failure at 7:30 AM**

**Step 1 — Open the Monitor panel**
- In ADF Studio, click the monitor icon
- Go to **"Pipeline runs"**
- Filter by: date range (today), status (Failed)
- Find the Wednesday run of `pl_ingest_http_to_bronze` — it shows "Failed" with a red icon

**Step 2 — Drill into the pipeline run**
- Click the pipeline name row to open the run details
- This shows all activities in the pipeline with their individual status (Succeeded / Failed / Skipped)
- Find the activity row marked "Failed"

**Step 3 — View activity error details**
- Click the **glasses icon** (View details) on the failed activity row
- The detail pane shows:
  - **Error code** (e.g., `UserErrorInvalidColumnMappingColumnNotFound`)
  - **Error message** (human-readable description of what went wrong)
  - **Failure type** (User error vs. System error vs. Dependency failure)
  - **Input** — the exact parameters and dataset references the activity was given
  - **Output** — what was returned before the failure

**Step 4 — Check logs if fault tolerance is enabled**
- If the Copy Activity has logging enabled (fault tolerance → skip incompatible rows → log to ADLS Gen2), navigate to the log path in `stadlsdev001` to see which rows were skipped and why

---

**Q10 — What is "Publish All" and draft vs. published version**

**Publish All** saves all authored changes — pipelines, datasets, Linked Services, triggers — from the ADF Studio editing session into the live, persisted state of the Data Factory. Before Publish, changes exist only as unsaved drafts in the browser session.

**What happens without Publish All:**
- The draft exists in ADF Studio's in-memory state for your session
- Other team members opening ADF Studio will not see your draft changes
- Any trigger that fires will use the **last published version** of the pipeline — your unpublished changes are completely invisible to it

**Analogy:** Publishing in ADF is like committing and pushing code to a Git repository. Working in the draft is like having local uncommitted changes. Triggers (like a CI/CD deployment) only see what has been "pushed" (published).

**With Git integration enabled (production setup):**
- Instead of "Publish All", changes are committed to a feature branch in Azure DevOps or GitHub
- A PR/merge to the `main` (collaboration) branch publishes to the Data Factory
- This gives you full version history, code review, and rollback capability

---

## Concept 2: Linked Services and Datasets

**Q11 — What is a Linked Service, what does it store, two examples**

A **Linked Service** is a saved, reusable connection definition in ADF. It stores the metadata ADF needs to connect to an external data store or compute resource: the endpoint URL, authentication method, and credentials (or a Key Vault reference for credentials).

A Linked Service does **not** contain data — it is just the connection details.

**Two examples for an HTTP → ADLS Gen2 pipeline:**

```
ls_http_source:
  Type:           HTTP
  Base URL:       https://raw.githubusercontent.com
  Auth:           Anonymous

ls_adls_gen2:
  Type:           Azure Data Lake Storage Gen2
  Storage account: stadlsdev001.dfs.core.windows.net
  Auth:           Account key (stored as Key Vault secret reference in production)
```

These two Linked Services are created once in the Manage panel. Every pipeline and dataset that needs HTTP or ADLS Gen2 access reuses these definitions — no credentials are re-entered per pipeline.

---

**Q12 — Linked Service vs. Dataset, with analogy**

| | Linked Service | Dataset |
|---|---|---|
| What it represents | The connection | The data location and format |
| What it stores | Endpoint URL, auth credentials | Container/path, file format, schema, parameters |
| Analogy | A saved contact in your phone | A shipping label on a parcel |
| Created where | Manage panel | Author panel |
| How many per source | One (shared by all datasets and pipelines) | One per data location/format combination |

**Analogy explained:**

A Linked Service is like a saved contact in your phone — "Bank: +61 2 9999 0000". You do not re-type the phone number every time you call the bank. Similarly, `ls_adls_gen2` stores the connection details once, and every Dataset that reads from ADLS Gen2 references it.

A Dataset is like a shipping label on a parcel — it says "Pick up from: bronze container, payments/2026-09-25/ folder" and "Format: Parquet, Snappy compressed". The Linked Service (saved contact) tells ADF how to authenticate; the Dataset (shipping label) tells ADF where to look and what the data looks like.

---

**Q13 — One Linked Service per source system, not per pipeline**

**The rule:** Create one Linked Service for `stadlsdev001`, regardless of how many pipelines or datasets use it.

**Maintenance benefit — credential rotation example:**

```
Bad pattern (one Linked Service per pipeline):
  ls_pipeline1_adls   ← stores the storage account key
  ls_pipeline2_adls   ← stores the same key (duplicate)
  ls_pipeline3_adls   ← stores the same key (duplicate)
  ...15 more pipelines...

When the access key is rotated: update 18 Linked Services manually
Risk: missing one → 18th pipeline breaks silently

Good pattern (one Linked Service per source):
  ls_adls_gen2        ← stores the key once

When the access key is rotated: update 1 Linked Service
All 18 pipelines pick up the new key automatically
```

Additional benefits:
- **Testing:** Test the connection once — if `ls_adls_gen2` passes, all pipelines using it can connect
- **Key Vault migration:** Change from account key to Managed Identity in one place — all datasets and pipelines benefit immediately
- **Governance:** A central list of Linked Services shows you exactly which external systems your Data Factory connects to

---

**Q14 — One storage account, one Linked Service, two Datasets**

**Answer: 1 Linked Service, 2 Datasets.**

```
ls_adls_gen2          ← 1 Linked Service (connection to stadlsdev001)

ds_adls_source_csv    ← Dataset 1: source location and format
  Linked Service: ls_adls_gen2
  Container: bronze
  Path: payments/2026-09-24/payments_raw.parquet
  Format: Parquet (source)

ds_adls_sink_parquet  ← Dataset 2: sink location and format
  Linked Service: ls_adls_gen2
  Container: silver
  Path: payments/2026-09-24/payments_clean.parquet
  Format: Parquet (sink)
```

**Reasoning:** The Linked Service is the connection — it is the same storage account, so one Linked Service serves both. Datasets are different because the source and sink have different containers, paths, and potentially different schemas or formats. The Copy Activity needs a separate source Dataset and sink Dataset to know where to read from and where to write to.

---

**Q15 — Dataset parameters, example with expression syntax**

A **Dataset parameter** is a variable defined on a Dataset that allows the dataset's configuration (file path, container, file name) to be dynamic at runtime. The pipeline passes values for these parameters when it references the Dataset.

**Example: `ds_adls_bronze_parquet` with `run_date` parameter**

Dataset parameter definition (in the Parameters tab):
```
Name:     run_date
Type:     String
Default:  (blank)
```

Dataset Connection tab (folder path uses the parameter):
```
Container:  bronze
Directory:  payments/@{dataset().run_date}
File:       payments_raw.parquet
```

Pipeline Copy Activity Sink tab (passes the pipeline parameter to the dataset parameter):
```
Sink dataset:              ds_adls_bronze_parquet
Dataset property run_date: @pipeline().parameters.run_date
```

**Runtime resolution for `run_date = 2026-09-25`:**
```
Dataset path resolves to: bronze/payments/2026-09-25/payments_raw.parquet
```

Without parameters, you would need a separate Dataset for every date partition — an impractical approach for a daily pipeline.

---

**Q16 — Resolved path and what happens with no value**

**Resolved path:** If the pipeline passes `run_date = 2026-09-25`:

```
Container:  bronze
Directory:  payments/2026-09-25
File:       payments_raw.parquet

Full path:  bronze/payments/2026-09-25/payments_raw.parquet
```

**If the pipeline does not pass a value for `run_date`:**

ADF uses the **default value** specified in the Dataset's Parameters tab.

- If a default is set (e.g., `2026-01-01`): ADF uses that default — the path resolves to `bronze/payments/2026-01-01/payments_raw.parquet`
- If no default is set (blank): The expression `@{dataset().run_date}` evaluates to an empty string — the path resolves to `bronze/payments//payments_raw.parquet` — the double slash may cause ADF to throw an error or write to an unexpected location

**Best practice:** Always set a meaningful default value on Dataset parameters, and validate that the pipeline always passes the parameter explicitly. Use ADF validation (the checkmark icon on the pipeline canvas) to catch missing parameter mappings before publishing.

---

**Q17 — 15 pipelines, access key rotated — how many updates, and how to eliminate the problem**

**With Linked Services correctly set up (one `ls_adls_gen2`):**
- Update **1 Linked Service** — all 15 pipelines pick up the new key automatically

**With the bad pattern (one Linked Service per pipeline):**
- Update 15+ Linked Services manually — risk of missing one and breaking a pipeline silently

**The feature that eliminates the problem entirely: Azure Key Vault integration.**

When ADF is connected to Azure Key Vault:
1. The storage account key is stored in Key Vault as a secret: `adls-dev-account-key`
2. The Linked Service stores a **Key Vault reference** instead of the key itself:
   ```
   Auth: Account key
   Key: (Key Vault reference)
       Key Vault: kv-datalake-dev
       Secret name: adls-dev-account-key
   ```
3. When the key is rotated, update the secret in Key Vault only
4. ADF reads the new key from Key Vault at runtime — no changes to ADF required at all

**Additional benefit: Managed Identity (no key needed at all):**
If ADF's Managed Identity is granted the `Storage Blob Data Contributor` role on the ADLS Gen2 account:
- No key to rotate — ever
- No secret in Key Vault
- Authentication uses the ADF service's Azure AD identity
- This is the production best practice

---

**Q18 — Parquet Dataset vs. DelimitedText Dataset; Bronze/Silver/Gold usage**

| | DelimitedText (CSV) | Parquet |
|---|---|---|
| Format | Human-readable text rows | Binary columnar format |
| Schema enforcement | None — all values are strings | Strong typing per column |
| Compression | None (or GZIP) | Built-in Snappy/GZIP (much smaller) |
| Read performance | Row scan (read entire row to get one column) | Columnar scan (read only the columns you need) |
| ADF write speed | Faster for small files | Slightly slower (encoding overhead) |
| Analytics query speed | Slow at scale | Very fast for aggregations (Spark, Synapse) |

**Layer usage:**

| Layer | Format | Reason |
|---|---|---|
| **Bronze (source CSV → ADF Copy)** | Source: DelimitedText (if source is CSV), Sink: Parquet | Preserve source format on read; land as Parquet for efficient downstream processing |
| **Silver (Spark transform)** | Parquet or Delta Lake (Parquet under the hood) | Typed, structured data; Spark reads Parquet 10× faster than CSV for large datasets |
| **Gold (BI queries)** | Parquet or Delta Lake | Aggregated data queried by dashboards — columnar format dramatically reduces query cost and time |

**Rule of thumb:** Convert to Parquet at the earliest opportunity. ADF's Copy Activity can convert CSV → Parquet in a single step with zero extra code.

---

**Q19 — Schema evolution: new columns added to source**

**What happens by default:**

When you import schema into a Dataset, ADF stores the column list statically. If the source adds three new columns:

- **ADF Copy Activity with explicit Mapping tab:** The new columns are ignored (mapping only maps the defined columns). The copy succeeds but the new columns do not appear in the output.
- **ADF Copy Activity with auto-mapping (no Mapping tab defined):** ADF copies all source columns — including the new ones — to the sink. This is schema drift handling at the Copy level.
- **ADF Data Flow:** Has explicit schema drift handling settings (allow schema drift = true/false). With drift allowed, new columns pass through automatically.

**How to handle schema evolution properly:**

1. **For Copy Activity:** Do not use the Mapping tab if you want all columns to flow through automatically. Use Mapping only when you need column renames or type changes on specific known columns.

2. **Enable schema drift in Datasets:** In the Dataset properties, check "Allow schema drift" — ADF will not fail if the source has more or fewer columns than the stored schema.

3. **Bronze layer philosophy:** In the Bronze layer, copy everything — do not drop columns at ingest. Schema enforcement belongs in the Silver layer. If you filter columns at the Copy Activity Mapping tab, you may lose data you need later.

4. **Automated schema monitoring:** Use ADF's Data flow validation, or write an Azure Monitor alert that fires when a Copy Activity produces a different row structure than expected.

---

**Q20 — Azure Key Vault integration in ADF**

**Step 1 — Create an ADF Linked Service for Key Vault**
- In Manage → Linked services → **"+ New"** → search **"Azure Key Vault"**
- Name: `ls_keyvault`
- Select your Key Vault instance
- Authentication: **System-assigned Managed Identity** (ADF's managed identity)

**Step 2 — Grant ADF's Managed Identity access to Key Vault**
- In the Azure portal → your Key Vault → **Access Control (IAM)**
- Add role assignment: **Key Vault Secrets User**
- Member: your ADF instance's Managed Identity (search by your ADF name)

**How the Linked Service looks after integration:**

Instead of:
```
Auth: Account key
Account key: DefaultEndpointsProtocol=https;AccountName=stadlsdev001;AccountKey=XXXXXXX
```

It becomes:
```
Auth: Account key
Account key: (Azure Key Vault)
  AKV linked service: ls_keyvault
  Secret name:        adls-dev-account-key
  Secret version:     (latest)
```

At runtime, ADF fetches the current value of `adls-dev-account-key` from Key Vault and uses it to authenticate — the actual secret value is never stored in ADF itself. This means:
- Rotating the secret in Key Vault is the only step required
- No ADF Linked Service update needed
- The secret is never visible in ADF Studio, Git history, or ADF logs

---

## Concept 3: Copy Activity, DIUs, Fault Tolerance & Mixed Senior

**Q21 — What does Copy Activity do, four configuration tabs**

A **Copy Activity** is ADF's data-mover activity. It reads data from a source (a Dataset), optionally transforms column names and types, and writes the data to a sink (another Dataset). It handles format conversion (CSV → Parquet), compression, column mapping, and fault tolerance — all in one activity without writing code.

**Four main configuration tabs:**

| Tab | What you configure |
|---|---|
| **Source** | Which source Dataset to read. Request method (GET/POST for HTTP). Partition options (reading SQL data in parallel partitions). Additional filters or queries. |
| **Sink** | Which sink Dataset to write to. Write behaviour (overwrite, append, merge). Dataset parameter values (e.g., `run_date`). |
| **Mapping** | Column name mapping (rename source columns to sink column names). Data type conversion (String → Timestamp). Column exclusion (omit columns from the output). |
| **Settings** | DIU count (Auto or manual). Degree of copy parallelism. Fault tolerance (fail / skip incompatible rows). Logging (write skipped rows to ADLS Gen2). Retry policy. |

---

**Q22 — DIUs, what is Auto, when to override manually**

**DIU (Data Integration Unit):** A combined unit of CPU, memory, and network throughput that ADF allocates to run a Copy Activity. More DIUs = more parallel processing = faster copy = higher cost per run.

**Auto setting:** ADF analyses the source and sink types, the data volume, and the file structure, and automatically picks the optimal DIU count. For most scenarios, Auto is correct.

| Data size | Auto typically picks |
|---|---|
| < 100 MB | 2 DIUs |
| 1–10 GB | 4–8 DIUs |
| 10–100 GB | 16–32 DIUs |
| > 100 GB (parallel sources) | Up to 256 DIUs |

**When to override with a manual value:**

1. **Cost predictability:** You need to guarantee that a Copy Activity never uses more than 4 DIUs (to control cost). Set a manual cap.

2. **Performance SLA:** A Copy Activity must complete in under 30 minutes for a 50 GB file. Auto may pick too few DIUs. Set a higher manual value (e.g., 32) and measure.

3. **Throttling a low-priority pipeline:** A nightly archive copy should not compete with a high-priority real-time pipeline for compute. Set the archive copy to 2 DIUs manually.

4. **Source throttling:** The HTTP source rate-limits to 2 concurrent connections. Setting DIUs high would cause 429 errors. Set DIUs manually low (2–4) to stay within the source limit.

---

**Q23 — Three things you can configure in the Mapping tab beyond name matching**

1. **Column renaming:** Map a source column name to a different sink column name. For example, `cust_id → customer_id` or `ts → event_timestamp`. This standardises naming conventions between the source system and the data lake schema without writing code.

2. **Data type conversion:** Change the type of a column between source and sink. For example:
   - `timestamp` (String in CSV) → `Timestamp` (proper Timestamp type in Parquet)
   - `amount` (String in CSV) → `Decimal` (Decimal type in Parquet)
   This ensures the Parquet file has correct column types for Spark/SQL queries, rather than landing everything as strings.

3. **Column exclusion (drop columns):** Delete a mapping row to exclude that column from the sink entirely. Useful for removing PII columns (e.g., `ssn`, `credit_card_number`) before landing in a shared Bronze layer, or for excluding internal system columns that have no analytical value.

**Bonus — a fourth thing:** Map a constant value to a sink column that does not exist in the source. For example, add a `pipeline_run_id` column in the sink with value `@pipeline().RunId` — useful for data lineage tracking.

---

**Q24 — Improving Copy Activity throughput for 500 GB CSV → Parquet**

Two (or more) settings to change:

**1. Increase DIU count (most impactful):**
- Change from Auto (may be 4 DIUs) to a manual value: 32 or 64 DIUs
- More DIUs = more parallel threads reading from the source and writing to the sink
- Expected improvement: 2× to 8× throughput, depending on source and sink limits

**2. Enable parallel copy (degree of copy parallelism):**
- In Settings tab, increase **"Degree of copy parallelism"** from Auto to a higher number (e.g., 8–16)
- This controls how many files are copied in parallel when the source has multiple files
- For 500 GB split across many CSV files, parallel copy means all files are copied simultaneously rather than one at a time

**3. Use Parquet at the source (if possible):**
- CSV → Parquet conversion in ADF is CPU-intensive. If the source can provide Parquet instead of CSV, skip the conversion cost

**4. Enable staging:**
- For cross-region copies, enable **"Enable staging"** in Settings — data is staged in a nearby Blob Storage account before the final write, improving throughput and reducing inter-region transfer costs

**Combined effect:** With 32 DIUs + 8 parallel copies, a 3-hour copy can often be reduced to 30–45 minutes, meeting the under-1-hour SLA.

---

**Q25 — Fault tolerance: default behaviour and how to configure skip + log**

**Default behaviour (no fault tolerance configured):**
ADF fails the Copy Activity on the first incompatible row encountered. The pipeline run shows "Failed", and zero or partial data may be written to the sink — depending on whether the error occurred at the start or end of the copy.

**Incompatible row example:** A CSV column `amount` is expected to be `Decimal` in the Parquet sink, but one row contains the value `"N/A"`. ADF cannot convert `"N/A"` to Decimal → failure.

**How to configure skip + log:**

In the Copy Activity → **Settings** tab:

| Setting | Value |
|---|---|
| Fault tolerance | Skip incompatible rows |
| Enable logging | Checked (On) |
| Log settings — Linked service | `ls_adls_gen2` |
| Log settings — Folder path | `logs/copy-activity-errors/@{pipeline().RunId}/` |
| Log level | Warning |

**What happens at runtime:**
- ADF processes all rows
- Rows that cannot be converted (the incompatible ones) are skipped
- Skipped rows are written to a CSV log file at the specified log path in ADLS Gen2
- The Copy Activity completes with status "Succeeded" (not failed) — but the output details show "Rows skipped: N"
- The log file contains the raw source row and the error message for each skipped row

**Best practice:** Always enable logging when using fault tolerance. A pipeline that silently drops rows without logging is worse than a pipeline that fails loudly.

---

**Q26 — Diagnostic process for 1,500 missing rows**

**Diagnostic process:**

**Step 1 — Check the Copy Activity run details:**
- Monitor panel → Pipeline runs → find the run → glasses icon on `CopyPaymentsHttpToBronze`
- Check the "Rows copied" vs. "Rows read" counts:
  ```
  Rows read:    10,000,000
  Rows copied:   9,998,500
  Rows skipped:      1,500
  ```
  If `Rows skipped: 1,500` — fault tolerance was triggered and 1,500 rows were incompatible

**Step 2 — Read the skip log file:**
- Navigate in ADLS Gen2 to the log path configured in Settings: `logs/copy-activity-errors/{RunId}/`
- Open the CSV log file — it contains columns like:
  ```
  source_row | error_code | error_message
  ...row content...  | TypeConversionFailure | Cannot convert 'N/A' to type Decimal
  ```
- The error message tells you exactly which column and which value caused each skip

**Step 3 — Identify the root cause pattern:**
- If all 1,500 skipped rows have the same error (e.g., `amount = 'N/A'`): the source has a data quality issue in a specific column
- If skipped rows span many error types: there may be a schema mismatch between the source and the defined mapping

**Step 4 — Fix the root cause:**
- If the source data has `'N/A'` in numeric columns: add a **Mapping data flow** or pre-processing step to replace `'N/A'` with `null` before the Copy Activity
- If the schema changed: update the Dataset schema and Mapping tab
- If the skipped rows are acceptable (true junk data): document the skip rate in your data quality monitoring

---

**Q27 — Copy Activity vs. Data Flow**

| | Copy Activity | Mapping Data Flow |
|---|---|---|
| Purpose | Data movement — copy from A to B | Data transformation — join, aggregate, deduplicate, pivot |
| Transformation capability | Column rename, type cast, column exclusion only | Full Spark-based transformations (join, group by, window, filter, derived column, conditional split) |
| Execution engine | ADF's proprietary copy engine (DIUs) | Apache Spark (runs on Spark clusters) |
| Performance for movement | Very fast — optimised for bulk copy | Slower start (Spark cluster warm-up ~1–2 min) but scales to billions of rows |
| Code required | None | None (visual designer) but complex logic is easier to reason about |
| Cost | Per DIU-hour (cheap for simple copies) | Per Spark vCore-hour (more expensive; justified for complex transforms) |
| Debug mode | Instant (no warm-up) | Data Flow debug mode requires a live Spark cluster (takes 1–2 min to start) |

**For deduplication + join during ingestion:** Use a **Mapping Data Flow**. Copy Activity cannot join two datasets or deduplicate records — it can only move data with light column-level changes. A Data Flow can express: "join HTTP CSV with a reference table from ADLS Gen2, deduplicate on `transaction_id`, and write the result to Silver Parquet" — all in a visual, no-code canvas backed by Spark.

**Performance note:** For pure data movement (no join, no aggregate, no dedup), always use Copy Activity — it is 2–5× faster than a Data Flow for the same volume because it does not pay the Spark cluster warm-up cost.

---

**Q28 — Only start pipeline if new source file exists**

**Implementation: Validation Activity + Web Activity (or Get Metadata Activity)**

**Pattern 1 — Get Metadata Activity + If Condition:**

```
Pipeline: pl_ingest_http_to_bronze
│
├── [1] GetMetadataSourceFile    (Get Metadata Activity)
│       Dataset: ds_http_source_csv
│       Field list: ["exists"]
│       ↓ outputs: { "exists": true } or { "exists": false }
│
├── [2] CheckFileExists          (If Condition Activity)
│       Expression: @activity('GetMetadataSourceFile').output.exists
│       ↓ If True:
│           └── [3] CopyPaymentsHttpToBronze  (Copy Activity)
│       ↓ If False:
│           └── [4] LogNoFile               (Web Activity — POST to Teams webhook)
│                   "No source file found for @{pipeline().parameters.run_date} — skipping"
```

**Pattern 2 — Validation Activity (simpler for ADLS Gen2 sources):**
- Add a **Validation Activity** before the Copy Activity
- Configure: Dataset = source file, Timeout = 1 minute, Sleep = 10 seconds
- If the file does not appear within the timeout, the Validation Activity fails with "Timeout exceeded" — but set it to a short timeout so the pipeline exits quickly rather than waiting
- This works well when the file is expected to arrive within a known time window

**Key detail:** For HTTP sources, `Get Metadata` checks if the URL returns a 200 status code. If it returns 404, `exists` is `false`. The pipeline then exits the `If Condition` false branch — returning "Succeeded" status overall (it was a valid business outcome, not a failure).

---

**Q29 — ADF expressions: `@pipeline().parameters.run_date` and two scenarios**

**`@pipeline().parameters.run_date`** is an ADF expression that reads the value of a pipeline parameter named `run_date` at runtime. ADF expressions use the `@` prefix for single-value references or `@{...}` for expressions embedded in strings.

**Where you can use it:**
- Dataset property (sink folder path, file name)
- Activity configurations (Linked Service name override, HTTP relative URL)
- If Condition expressions
- ForEach Items expressions
- Web Activity URL and body

**Scenario 1 — Sink folder path includes the run date:**

```
Expression:
@{concat('bronze/payments/', pipeline().parameters.run_date, '/')}

For run_date = 2026-09-25, resolves to:
bronze/payments/2026-09-25/
```

In the Dataset Connection tab, the Directory field is set to:
```
payments/@{dataset().run_date}
```
And the pipeline passes: `run_date = @pipeline().parameters.run_date`

**Scenario 2 — Use different storage account based on `env` parameter:**

```
Expression (Linked Service name override):
@if(equals(pipeline().parameters.env, 'prod'), 'ls_adls_gen2_prod', 'ls_adls_gen2_dev')
```

Or in the Dataset Connection tab (storage account name override):
```
@if(equals(pipeline().parameters.env, 'prod'), 'stadlsprod001', 'stadlsdev001')
```

This allows a single pipeline to target dev or prod storage depending on which environment trigger runs it — without duplicating the pipeline.

---

**Q30 — Complete multi-source ingestion architecture for EV charging company**

**Requirements recap:**
- HTTP API → daily payment CSV files (20 tables)
- On-premises SQL Server → session data (not internet accessible)
- Azure Event Hubs → real-time charger telemetry stream
- All data → ADLS Gen2 Bronze, date-partitioned
- Failed runs → Teams/email alert
- Daily batch at 5:00 AM
- 20 payment tables via metadata-driven approach
- Idempotent (re-running same date does not duplicate data)

---

**Source 1: HTTP API — 20 Payment Tables (Metadata-Driven)**

| Component | Choice | Detail |
|---|---|---|
| Linked Service | `ls_http_source` (HTTP) | Base URL: payment provider API endpoint |
| Integration Runtime | Azure IR (AutoResolve) | HTTP API is publicly accessible |
| Dataset | `ds_http_payments_csv` (parameterised: `table_name`, `run_date`) | DelimitedText source |
| Sink Dataset | `ds_adls_bronze_parquet` (parameterised: `table_name`, `run_date`) | Parquet sink: `bronze/{table_name}/{run_date}/` |
| Activity type | ForEach → Copy Activity (metadata-driven) | |
| Config file | `bronze/config/payment_tables.json` — lists 20 tables with source paths | |
| Trigger | Schedule Trigger at 5:00 AM daily | Pass `run_date = @formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')` |
| Idempotency | Sink Copy behaviour: **Overwrite** — re-running the same date writes to the same path and replaces the file | |
| Failure alert | Web Activity on pipeline failure branch → POST to Teams webhook | |

**Metadata config file (`bronze/config/payment_tables.json`):**
```json
[
  { "table_name": "payments",      "source_path": "/api/v1/payments.csv" },
  { "table_name": "settlements",   "source_path": "/api/v1/settlements.csv" },
  { "table_name": "chargebacks",   "source_path": "/api/v1/chargebacks.csv" }
]
```

**Pipeline structure (`pl_ingest_http_payments`):**
```
[1] LookupPaymentTableConfig    ← reads payment_tables.json from ADLS Gen2
[2] ForEachTable                ← iterates over 20 table config entries (batch count: 5 for parallelism)
      └── [3] CopyTableHttpToBronze ← Copy Activity with @item().table_name and @item().source_path
[4] NotifyOnFailure             ← Web Activity on pipeline failure → Teams alert (on failure dependency)
```

---

**Source 2: On-Premises SQL Server — Session Data**

| Component | Choice | Detail |
|---|---|---|
| Linked Service | `ls_sql_sessions` (Azure SQL / SQL Server) | Connection string to on-prem SQL Server |
| Integration Runtime | Self-Hosted IR `ir-onprem-sql` | Installed on a VM in the corporate network with access to SQL Server |
| Dataset | `ds_sql_sessions_source` | SQL Server table or query |
| Sink Dataset | `ds_adls_sessions_parquet` | Parquet, `bronze/sessions/{run_date}/sessions.parquet` |
| Activity type | Copy Activity | Source: SQL query with date filter `WHERE session_date = '@run_date'` |
| Idempotency | SQL query filters by `session_date` — same date always returns same rows. Sink: Overwrite mode. | |
| Trigger | Same Schedule Trigger at 5:00 AM (or a separate trigger after the SQL Server window opens) | |
| Failure alert | Same Web Activity on failure branch | |

---

**Source 3: Azure Event Hubs — Real-Time Charger Telemetry**

| Component | Choice | Detail |
|---|---|---|
| Linked Service | Not ADF — use Event Hubs Capture | ADF is not designed for real-time streaming ingest |
| Integration Runtime | N/A (Event Hubs Capture is built-in) | |
| Mechanism | **Event Hubs Capture** | Auto-captures telemetry to ADLS Gen2 as Avro files in real time |
| Output path | `bronze/telemetry/{Year}/{Month}/{Day}/{Hour}/{Minute}/` | Event Hubs Capture writes partitioned Avro files automatically |
| ADF role | A separate ADF pipeline runs hourly to convert Avro → Parquet in Bronze | Copy Activity: Avro source → Parquet sink; triggered by Storage Event Trigger (file created) |
| Failure alert | Azure Monitor alert on Event Hubs capture failure → Action Group → email | |

---

**Failure alerting — unified pattern across all sources:**

```
For each pipeline:
├── [last activity] NotifyOnFailure
│     Activity type: Web Activity
│     Dependency on previous activity: "Failed" or "Skipped"
│     Method: POST
│     URL: https://prod.teams.microsoft.com/webhooks/{channel-id}/...
│     Body: {
│       "text": "Pipeline @{pipeline().Pipeline} failed on @{pipeline().parameters.run_date}.\nError: @{activity('CopyActivity').error.message}"
│     }
```

---

**Idempotency summary across all sources:**

| Source | Idempotency mechanism |
|---|---|
| HTTP payments (20 tables) | Sink write mode = Overwrite; same `run_date` → same path → file is replaced |
| SQL sessions | Source query filters `WHERE session_date = @run_date`; sink Overwrite mode |
| Telemetry (Event Hubs Capture) | Event Hubs Capture is append-only by hour; ADF Avro→Parquet pipeline uses `run_hour` partition; Overwrite mode per hour |

---

**Architecture diagram (text):**

```
5:00 AM Schedule Trigger
│
├── pl_ingest_http_payments (Azure IR)
│     LookupConfig → ForEach(20 tables) → CopyHttpToBronze
│     On Failure → WebActivity → Teams Alert
│     Output: bronze/payments/{table}/{run_date}/
│
├── pl_ingest_sql_sessions (Self-Hosted IR: ir-onprem-sql)
│     CopySqlSessionsToBronze
│     On Failure → WebActivity → Teams Alert
│     Output: bronze/sessions/{run_date}/
│
Event Hubs Capture (continuous, no ADF trigger)
│     Output: bronze/telemetry/{Year}/{Month}/{Day}/{Hour}/
│
Storage Event Trigger (fires on new Avro file in bronze/telemetry/)
│
└── pl_convert_telemetry_avro_to_parquet (Azure IR)
      CopyAvrotoParquet
      On Failure → WebActivity → Teams Alert
      Output: bronze/telemetry_parquet/{Year}/{Month}/{Day}/{Hour}/
```

**All three pipelines land raw data in Bronze. No transformation occurs in ADF.** Downstream Databricks notebooks read from Bronze, apply business rules, and write Silver and Gold. ADF orchestrates the ingestion layer; Databricks owns the transformation layer.
