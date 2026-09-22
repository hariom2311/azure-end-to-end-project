# Day 1 — Practice Exercises: Azure Setup & Storage

> All exercises are done in the **Azure Portal** (`https://portal.azure.com`).  
> Complete them in order — each exercise builds on the previous one.  
> Estimated total time: 60–90 minutes.

---

## Exercise 1 — Create Your Azure Environment

**Concept:** Azure Account & Resource Groups

**Tasks:**

**1a. Create your free Azure account**
- Go to `https://azure.microsoft.com/free`
- Complete the sign-up (Microsoft account + phone verification + credit card for identity)
- Confirm you land on the Azure Portal with an active "Free Trial" subscription

**1b. Create a Resource Group**
- Name: `rg-datalake-dev`
- Region: **Australia East**
- Confirm it appears in your Resource Groups list with status "Succeeded"

**1c. Explore the portal**
- Find the **Cost Management + Billing** section in the portal
- Note where you can see your current spend (should be $0.00 if you just created the account)
- Find the **Subscriptions** page — record your Subscription ID (you will need it later)

**1d. Tag your Resource Group**
- Go to `rg-datalake-dev` → **Tags**
- Add the following tags:

| Name | Value |
|---|---|
| Environment | dev |
| Project | datalake-course |
| Owner | your-name |

- Save the tags
- Explain: why would tags be useful in a real company with many teams?

---

## Exercise 2 — Create Blob Storage and Explore the Flat Namespace

**Concept:** Azure Blob Storage

**Tasks:**

**2a. Create a standard Blob Storage account**
- Name: `stblobdev001` (add numbers at the end if the name is taken)
- Resource group: `rg-datalake-dev`
- Region: Australia East
- Performance: Standard
- Redundancy: LRS
- **Hierarchical namespace: DISABLED**

**2b. Create containers**
Inside `stblobdev001`, create three containers:
- `raw-data` (Private access)
- `processed-data` (Private access)
- `archive` (Private access)

**2c. Upload files and observe the flat namespace**
- Create a folder on your local computer called `test-data`
- Create three files inside it: `orders_2024_01_15.csv`, `orders_2024_01_16.csv`, `orders_2024_01_17.csv`
  (content doesn't matter — they can be empty or have 1 line of text)
- In the portal, go to the `raw-data` container
- Click **Upload** → select all three files → In "Advanced" options, set "Upload to folder" = `orders/2024/01`
- Upload

**2d. Observe the result**
- After upload, you see the files listed under a virtual path `orders/2024/01/`
- Click on the "folder" `orders` — notice it doesn't really exist as a directory; it's just a prefix
- Answer: what would happen if you tried to rename the `orders` folder in Blob Storage when it contains 1 million files?

**2e. Find your storage account's connection string**
- Go to `stblobdev001` → **Security + networking → Access keys**
- Click **Show keys**
- Copy the connection string for key1 — save it in a text file (do NOT commit to Git)

---

## Exercise 3 — Create ADLS Gen2 and Explore the Hierarchical Namespace

**Concept:** ADLS Gen2

**Tasks:**

**3a. Create an ADLS Gen2 storage account**
- Name: `stadlsdev001` (add numbers if taken)
- Resource group: `rg-datalake-dev`
- Region: Australia East
- Redundancy: LRS
- **Advanced tab → Hierarchical namespace: ENABLED** ← critical step

**3b. Create the data lake container structure**
Create these containers:
- `bronze`
- `silver`
- `gold`
- `checkpoints`

**3c. Create real directories inside bronze**
Inside the `bronze` container:
- Create directory: `orders`
- Inside `orders`, create directory: `2024`
- Inside `2024`, create directory: `01`
- Inside `01`, create directory: `15`
- Upload any small CSV file into `15`

Your path should be: `bronze/orders/2024/01/15/yourfile.csv`

**3d. Compare with Blob Storage**
Answer the following:
1. In ADLS Gen2, click on the `orders` directory — does it exist as a real object?
2. What would happen if you renamed the `orders` directory to `orders_backup` in ADLS Gen2 vs. Blob Storage?
3. Why does this matter when Spark writes 10,000 Parquet files to a staging directory and then atomically promotes them to the final location?

**3e. Test the POSIX permissions**
- Go to `bronze/orders/2024/01/15/` → click on your file → **Manage ACL**
- Observe the Access Control List (ACL) entries
- Note that ADLS Gen2 shows individual permissions (Read, Write, Execute) per user/group — Blob Storage cannot do this at file level

---

## Exercise 4 — Set Up Secure Access with RBAC

**Concept:** Access Control

**Tasks:**

**4a. Add the "Storage Blob Data Contributor" role**
- Go to `stadlsdev001` → **Access Control (IAM)**
- Click **+ Add role assignment**
- Role: `Storage Blob Data Contributor`
- Member: your own email address
- Assign

**4b. Verify your role assignment**
- Go to the **Role assignments** tab
- Confirm your email appears under "Storage Blob Data Contributor"

**4c. Understand the difference between roles**
Research and fill in this table (use the Azure portal "Add role assignment" wizard to browse roles):

| Role | Can read data? | Can write data? | Can manage access? |
|---|---|---|---|
| Storage Blob Data Reader | ? | ? | ? |
| Storage Blob Data Contributor | ? | ? | ? |
| Storage Blob Data Owner | ? | ? | ? |
| Owner | ? | ? | ? |

**4d. Create a SAS token (Shared Access Signature)**
- Go to `stadlsdev001` → **Security + networking → Shared access signature**
- Configure:
  - Allowed services: Blob
  - Allowed resource types: Container, Object
  - Allowed permissions: Read, List
  - Expiry: 1 hour from now
- Click **Generate SAS and connection string**
- Copy the **Blob service SAS URL**
- Paste it in your browser — what do you see?
- Explain: when would you give someone a SAS token instead of an RBAC role?

---

## Exercise 5 — Explore Redundancy, Tiers & Lifecycle Management

**Concept:** Storage reliability and cost management

**Tasks:**

**5a. Understand redundancy options**
In the Azure portal, go to `stadlsdev001` → **Redundancy** (under "Data management"):
- Your current setting is LRS — note what "Locally Redundant Storage" means (3 copies in one datacenter)
- Browse the other options: ZRS, GRS, GZRS
- Answer: in a production data lake for a bank, which redundancy would you choose and why?

**5b. Set blob access tiers**
- Go to `stblobdev001` → `archive` container → upload a small file
- Click on the uploaded file → **Change tier**
- Change the tier from "Hot" to "Cool"
- Answer: when would you move a blob from Hot to Cool? From Cool to Archive?

**5c. Create a Lifecycle Management rule**
- Go to `stblobdev001` → **Data management → Lifecycle management**
- Click **+ Add rule**
- Rule name: `archive-old-raw-data`
- Rule scope: Apply to all blobs in the storage account
- Blob type: Block blobs
- Conditions:
  - If base blobs were last modified **more than 30 days** ago → move to **Cool** storage
  - If base blobs were last modified **more than 90 days** ago → move to **Archive** storage
- Save the rule

This simulates a real production pattern where raw data automatically moves to cheaper tiers as it ages.

---

## Bonus Challenge — Design the Storage Architecture

You are building a data lake for a ride-sharing company. The company needs to store:

1. **Trip events** — every GPS ping from every driver (10 million events/day, kept for 2 years)
2. **Driver documents** — licence photos, insurance docs (50,000 drivers, rarely accessed after onboarding)
3. **Processed trip summaries** — daily aggregated stats used by the BI dashboard (queried daily)
4. **ML training data** — features extracted from 6 months of trips (rebuilt monthly)

**Your tasks:**
1. Which of the four datasets should use **Blob Storage** and which should use **ADLS Gen2**? Justify each choice.
2. Draw the container and directory structure for your ADLS Gen2 data lake (Bronze/Silver/Gold layers).
3. Which **access tier** (Hot/Cool/Archive) would you assign to each dataset?
4. What **redundancy option** (LRS/ZRS/GRS) would you choose for the processed trip summaries, and why?
5. Create the ADLS Gen2 storage account and directory structure in the Azure portal that matches your design.
