# Day 1 — Practice Exercises: ADF Core Concepts, Linked Services, Datasets & Copy Activity

> All exercises are done in the **Azure Portal** (`https://portal.azure.com`) and **ADF Studio** (`https://adf.azure.com`).  
> Complete them in order — each exercise builds on the previous one.  
> You must have `rg-datalake-dev` and `stadlsdev001` (ADLS Gen2) already created from ADLS Day 1.  
> Estimated total time: 90–120 minutes.

---

## Exercise 1 — Create Your ADF Instance and Explore the 4 Studio Panels

**Concept:** What is Azure Data Factory, ADF Studio UI

**Tasks:**

**1a. Create the ADF instance**
- In the Azure portal, search for **"Data factories"**
- Click **"+ Create"**
- Fill in:

| Field | Value |
|---|---|
| Resource group | `rg-datalake-dev` |
| Region | Australia East |
| Name | `adf-datalake-dev-001` (append digits if taken) |
| Version | V2 |

- Git configuration: **"Configure Git later"**
- Click **"Review + create"** → **"Create"**
- After deployment, click **"Go to resource"** → **"Launch Studio"**

**1b. Explore the Author panel**
- Click the pencil icon (Author) in the left sidebar
- Observe the Factory Resources pane on the left (Pipelines, Datasets, Data flows, etc.)
- The canvas in the centre is where you build pipelines
- The Activities pane on the left of the canvas contains all available activity types
- Answer: what is the difference between "Data flows" and "Pipelines" in the Author panel?

**1c. Explore the Monitor panel**
- Click the monitor icon (Monitor) in the left sidebar
- Observe the sections: Pipeline runs, Activity runs, Trigger runs, Debug runs
- Note the date filter at the top — runs older than 45 days are not shown by default
- Answer: what information would you find in "Activity runs" that you cannot see in "Pipeline runs"?

**1d. Explore the Manage panel**
- Click the toolbox icon (Manage) in the left sidebar
- Observe: Linked services, Integration runtimes, Triggers, Git configuration
- Under **"Integration runtimes"**, find the default **AutoResolveIntegrationRuntime**
- Answer: what is an Integration Runtime in ADF? What is the difference between Azure IR and Self-Hosted IR?

**1e. Explore the Learn panel**
- Click the graduation cap icon (Learn) in the left sidebar
- Browse the template gallery
- Find the **"Copy from HTTP to ADLS Gen2"** template (or similar HTTP ingestion template)
- Do not deploy it — just note what steps it includes

**1f. Tag your ADF resource**
- Go back to the Azure portal → `adf-datalake-dev-001` → **Tags**
- Add:

| Name | Value |
|---|---|
| Environment | dev |
| Project | datalake-course |
| Owner | your-name |

---

## Exercise 2 — Create the ADLS Gen2 Linked Service and HTTP Linked Service, Test Both Connections

**Concept:** Linked Services

**Tasks:**

**2a. Create the ADLS Gen2 Linked Service**
- In ADF Studio → Manage → Linked services → **"+ New"**
- Search for and select **"Azure Data Lake Storage Gen2"**
- Configure:

| Field | Value |
|---|---|
| Name | `ls_adls_gen2` |
| Description | Connects to stadlsdev001 ADLS Gen2 data lake |
| Authentication method | Account key |
| Storage account name | `stadlsdev001` |

- Click **"Test connection"** — confirm you see **"Connection successful"**
- Click **"Create"**

**2b. Create the HTTP Linked Service**
- In Linked services → **"+ New"**
- Search for and select **"HTTP"**
- Configure:

| Field | Value |
|---|---|
| Name | `ls_http_source` |
| Description | Generic HTTP connector for raw CSV source data |
| Base URL | `https://raw.githubusercontent.com` |
| Authentication type | Anonymous |

- Click **"Test connection"** — confirm success
- Click **"Create"**

**2c. Verify both Linked Services**
- Your Linked Services list should now show two entries: `ls_adls_gen2` and `ls_http_source`
- Both should have a green dot or "Succeeded" status after connection test

**2d. Understand the naming convention**
Answer the following based on what you have built:
1. Why is the Linked Service named `ls_adls_gen2` instead of `ls_pipeline1_adls` or `ls_copy_adls`?
2. If you had three pipelines all reading from the same ADLS Gen2 account, how many Linked Services would you create for it?
3. What would happen if the ADLS Gen2 account key was rotated and you needed to update it — how many places in ADF would you need to update?

**2e. Explore authentication options**
- Edit `ls_adls_gen2` (click the pencil icon next to it)
- Click on the **"Authentication method"** dropdown
- Note the available options: Account key, Service Principal, System-assigned Managed Identity, User-assigned Managed Identity
- Answer: in a production environment with no secrets to manage, which authentication method would you choose? Why?
- Close without saving

---

## Exercise 3 — Create the Source Dataset (HTTP CSV) and Sink Dataset (ADLS Gen2 Parquet)

**Concept:** Datasets

**Tasks:**

**3a. Create the source Dataset `ds_http_source_csv`**
- In the Author panel, click **"+"** next to "Datasets" → **"New dataset"**
- Search for **"HTTP"** → select **"HTTP"** → Continue
- Select format: **"DelimitedText"** → Continue
- Configure:

| Field | Value |
|---|---|
| Name | `ds_http_source_csv` |
| Linked service | `ls_http_source` |
| Relative URL | `/hariom2311/azure-ev-end-to-end-project/main/azure-data-factory-service/day1-adf-fundamentals/data/payments.csv` |
| First row as header | Checked |
| Import schema | From connection/store |

- Click **"OK"**

**3b. Inspect the Schema tab of `ds_http_source_csv`**
- Click on `ds_http_source_csv` to open it
- Go to the **"Schema"** tab
- Click **"Import schema"**
- Confirm that column names are imported from the CSV file
- Write down the column names you see — you will use these in the Mapping tab in Exercise 4

**3c. Create the sink Dataset `ds_adls_bronze_parquet`**
- In the Author panel, click **"+"** next to "Datasets" → **"New dataset"**
- Search for **"Azure Data Lake Storage Gen2"** → select it → Continue
- Select format: **"Parquet"** → Continue
- Configure:

| Field | Value |
|---|---|
| Name | `ds_adls_bronze_parquet` |
| Linked service | `ls_adls_gen2` |
| File path — Container | `bronze` |
| File path — Directory | `payments/@{dataset().run_date}` |
| File path — File name | `payments_raw.parquet` |
| Import schema | None |

- Click **"OK"**

**3d. Add a Dataset parameter to `ds_adls_bronze_parquet`**
- Open `ds_adls_bronze_parquet`
- Go to the **"Parameters"** tab
- Click **"+ New"**
- Name: `run_date`, Type: `String`, Default value: leave blank
- Go to the **"Connection"** tab and confirm the directory path shows `payments/@{dataset().run_date}` (the parameter reference)

**3e. Understand the Dataset structure**
Answer:
1. What is the directory path that will be used if the pipeline passes `run_date = 2026-09-25`? Write the full path including container name.
2. Why is the sink Dataset format Parquet instead of CSV? What are two advantages of Parquet for a Bronze data layer?
3. If you wanted to write to a different container (e.g., `silver`) using the same Linked Service, would you need a new Linked Service or just a new Dataset? Why?

**3f. Click "Publish All"**
- Click **"Publish All"** in the top bar
- Confirm the diff shows: `ls_adls_gen2`, `ls_http_source`, `ds_http_source_csv`, `ds_adls_bronze_parquet`
- Click **"Publish"**

---

## Exercise 4 — Build `pl_ingest_http_to_bronze`, Add Copy Activity, Debug Run, Verify File

**Concept:** Copy Activity, Pipeline Parameters, Debug Run

**Tasks:**

**4a. Create the pipeline**
- In the Author panel, click **"+"** next to "Pipelines" → **"New pipeline"**
- In the Properties pane (right side), set:
  - Name: `pl_ingest_http_to_bronze`
  - Description: Ingests raw payment CSV from HTTP source into ADLS Gen2 bronze/payments/ as Parquet

**4b. Add the `run_date` pipeline parameter**
- At the bottom of the canvas, click the **"Parameters"** tab
- Click **"+ New"**
- Name: `run_date`, Type: `String`, Default value: `2026-09-25`

**4c. Add a Copy Activity**
- In the Activities pane (left of canvas), expand **"Move & transform"**
- Drag **"Copy data"** onto the canvas
- In the General tab at the bottom, set Name: `CopyPaymentsHttpToBronze`

**4d. Configure the Source tab**
- Click the **"Source"** tab in the activity properties panel

| Field | Value |
|---|---|
| Source dataset | `ds_http_source_csv` |
| Request method | GET |

**4e. Configure the Sink tab**
- Click the **"Sink"** tab

| Field | Value |
|---|---|
| Sink dataset | `ds_adls_bronze_parquet` |
| Dataset property — run_date | `@pipeline().parameters.run_date` |

This expression passes the pipeline parameter into the Dataset parameter, which substitutes it into the folder path.

**4f. Click Debug**
- Click **"Debug"** in the top bar
- In the parameter prompt, set `run_date`: `2026-09-25`
- Click **"OK"**
- Wait for the pipeline to complete (watch the activity turn green)

**4g. Inspect the Copy Activity output**
- At the bottom of the canvas, in the **"Output"** tab, find the run row
- Click the **glasses icon** (View details) on the `CopyPaymentsHttpToBronze` row
- Record the following from the details pane:
  - Data read (MB)
  - Data written (MB)
  - Rows copied
  - Duration (seconds)
  - DIUs used
  - Throughput (MB/s)

**4h. Verify the Parquet file in the Azure portal**
- Open a new browser tab
- Go to `https://portal.azure.com` → `stadlsdev001` → Containers → `bronze`
- Navigate to: `payments/2026-09-25/`
- Confirm that `payments_raw.parquet` exists
- Note the file size — compare it to the original CSV (Parquet with Snappy compression should be smaller)

**4i. Publish All**
- Back in ADF Studio, click **"Publish All"**
- Confirm `pl_ingest_http_to_bronze` appears in the diff
- Click **"Publish"**

Answer the following:
1. The pipeline wrote to `bronze/payments/2026-09-25/`. If you run the same Debug again with `run_date = 2026-09-26`, what path will the output file be written to?
2. The Copy Activity Details showed "DIUs used: 2". What would happen if you manually set the DIU count to 32 for this small file? Would the copy be faster?
3. You ran Debug but did not Publish. A colleague opens ADF Studio — will they see `pl_ingest_http_to_bronze` in the Author panel?

---

## Exercise 5 — Add Schema Mapping (Column Rename), Re-Run Debug, Verify Mapping Applied

**Concept:** Schema Mapping, column rename in Copy Activity

**Tasks:**

**5a. Open `pl_ingest_http_to_bronze` and select the Copy Activity**
- In the Author panel, click on `pl_ingest_http_to_bronze` to open it
- Click on the `CopyPaymentsHttpToBronze` activity to select it

**5b. Open the Mapping tab**
- Click the **"Mapping"** tab in the activity properties at the bottom
- Click **"Import schemas"**
- ADF reads both source (HTTP CSV) and sink (Parquet) schemas and displays them side by side

**5c. Rename a column in the sink**
- Find the source column `cust_id` (or the closest equivalent column from your schema — e.g., a customer or transaction ID column)
- Under the **"Destination"** column name for that row, click on the field to edit it
- Change the value to `customer_id`
- Confirm the mapping row now shows: `cust_id → customer_id`

**5d. Add a type conversion**
- Find the `timestamp` column (or your date/time column)
- In the **"Type"** dropdown for the destination column, change `String` to `Timestamp`
- This converts the ISO string to a proper Timestamp type in the Parquet output

**5e. Re-run Debug with mapping applied**
- Click **"Debug"** → set `run_date`: `2026-09-25`
- Click **"OK"**
- Wait for the pipeline to complete

**5f. Verify the column rename was applied**
Since Parquet files are binary, use one of these methods to verify:
- **Method A (Portal preview):** In `stadlsdev001` → `bronze/payments/2026-09-25/` → click `payments_raw.parquet` → **"Edit"** — the portal shows the first few rows with column names
- **Method B (ADF Mapping output):** In the Copy Activity Details (glasses icon), expand the "Column mapping" section — it should list `cust_id → customer_id` and `timestamp → event_timestamp`

**5g. Remove a column from the mapping**
- Go back to the Mapping tab of the Copy Activity
- Find any column you want to exclude from the output (e.g., an internal system column)
- Delete the mapping row for that column by clicking the trash icon on the right
- Re-run Debug → confirm the excluded column does not appear in the output

**5h. Publish All**
- Click **"Publish All"** → **"Publish"**
- Confirm that the pipeline with updated mapping is saved

Answer the following:
1. If the HTTP source CSV gains a new column next month, will ADF automatically include it in the Parquet output? Why or why not? (Hint: think about schema drift and the Mapping tab.)
2. What is the difference between changing a column name in the Mapping tab vs. renaming it in the sink Dataset schema?
3. You changed `timestamp` to `Timestamp` type in the mapping. What happens if the source data contains a row where the timestamp value is `null`? What about `not-a-date`?

---

## Bonus Challenge — Design a Metadata-Driven Pipeline for 20 Source Tables

You are building an ingestion layer for an EV charging company. There are 20 source tables that all need to be copied from HTTP endpoints into ADLS Gen2 bronze as Parquet. Writing 20 separate Copy Activity pipelines is inefficient to build and maintain.

**Design a metadata-driven pipeline using Lookup + ForEach:**

**Approach:**
1. Store a JSON or CSV configuration file in `bronze/config/table_config.json` that lists all 20 tables:
   The config file is already provided in this repo at `data/table_config.json`. Upload it to `bronze/config/table_config.json` in your ADLS Gen2 account.

   Content of `data/table_config.json`:
   ```json
   [
     {
       "table_name": "payments",
       "source_path": "/hariom2311/azure-ev-end-to-end-project/main/azure-data-factory-service/day1-adf-fundamentals/data/payments.csv",
       "run_date": "2026-01-15"
     },
     {
       "table_name": "sessions",
       "source_path": "/hariom2311/azure-ev-end-to-end-project/main/azure-data-factory-service/day1-adf-fundamentals/data/sessions.csv",
       "run_date": "2026-01-15"
     },
     {
       "table_name": "chargers",
       "source_path": "/hariom2311/azure-ev-end-to-end-project/main/azure-data-factory-service/day1-adf-fundamentals/data/chargers.csv",
       "run_date": "2026-01-15"
     }
   ]
   ```
2. Use a **Lookup Activity** to read this JSON file from ADLS Gen2 into memory
3. Use a **ForEach Activity** to iterate over each row of the Lookup output
4. Inside ForEach, use a **Copy Activity** with parameterised source path and sink directory:
   - Source relative URL: `@item().source_path`
   - Sink directory: `@{concat('bronze/', item().table_name, '/', item().run_date, '/')}`

**Your tasks:**
1. Create the config JSON file in `bronze/config/table_config.json` in your ADLS Gen2 account (with at least 3 entries)
2. Create a new pipeline `pl_metadata_driven_ingest`
3. Add a Lookup Activity — configure it to read `bronze/config/table_config.json` from `ds_adls_bronze_parquet`'s Linked Service (create a new JSON Dataset `ds_adls_config_json` for this)
4. Add a ForEach Activity connected after Lookup — set its Items to `@activity('LookupTableConfig').output.value`
5. Inside ForEach, add a Copy Activity configured with `@item().source_path` and `@item().table_name`
6. Debug the pipeline — confirm it runs the inner Copy Activity once per config entry (3 times for 3 entries)

**Bonus questions:**
- What is the `Sequential` vs. `Batch count` setting on the ForEach Activity? When would you use Sequential mode?
- If one of the 20 tables fails (e.g., the HTTP endpoint is down), does the ForEach stop or continue with the remaining tables?
- How would you add error handling so that a failure in one table sends an email alert but does not fail the entire pipeline?

---

## Summary Checklist

Before moving to Day 2, confirm you have completed all of the following:

- [ ] ADF instance `adf-datalake-dev-001` created in `rg-datalake-dev` (Australia East)
- [ ] All 4 ADF Studio panels explored: Author, Monitor, Manage, Learn
- [ ] Linked Service `ls_adls_gen2` created — connection test succeeded
- [ ] Linked Service `ls_http_source` created — connection test succeeded
- [ ] Dataset `ds_http_source_csv` created — schema imported (columns visible in Schema tab)
- [ ] Dataset `ds_adls_bronze_parquet` created — `run_date` parameter added, path uses `@{dataset().run_date}`
- [ ] Pipeline `pl_ingest_http_to_bronze` created with `run_date` pipeline parameter
- [ ] Copy Activity `CopyPaymentsHttpToBronze` configured — source + sink wired up
- [ ] Debug run completed successfully — `payments_raw.parquet` exists in `bronze/payments/2026-09-25/`
- [ ] Schema mapping applied — `cust_id` renamed to `customer_id` in the Mapping tab
- [ ] Debug re-run confirmed mapping applied
- [ ] Publish All completed — all resources saved to the live Data Factory
- [ ] Bonus: metadata-driven pipeline designed (or attempted)
