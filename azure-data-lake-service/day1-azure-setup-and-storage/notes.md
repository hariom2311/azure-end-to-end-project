# Day 1 — Azure Account Setup, Resource Groups & Storage

## Overview

Before writing a single line of data pipeline code, you need a working Azure environment. Day 1 covers everything from creating your free Azure account to understanding the difference between Azure Blob Storage and Azure Data Lake Storage Gen2 (ADLS Gen2) — and hands-on steps to create and test both in the Azure portal UI.

**The 3 concepts:**
1. Azure Account & Subscription — what they are and how to set them up
2. Resource Groups — how Azure organises resources and why it matters
3. Azure Blob Storage vs. ADLS Gen2 — the difference, when to use which, and step-by-step setup

---

## Concept 1: Azure Account & Subscription

### What is an Azure account?

An **Azure account** is your identity in Azure — the email address you use to sign in. Think of it like a Gmail account.

A **subscription** is the billing container that sits under your account. All resources you create (virtual machines, storage accounts, databases) live inside a subscription and their costs are billed to it. One account can have multiple subscriptions (e.g., one for dev, one for prod).

```
Azure Account (your email)
└── Subscription: "Pay-As-You-Go" or "Free Trial"
        ├── Resource Group: "rg-dev"
        │       ├── Storage Account: "stdatalakedev"
        │       └── Event Hub: "evh-payments"
        └── Resource Group: "rg-prod"
                └── Storage Account: "stdatalakeprod"
```

### Free Trial vs. Pay-As-You-Go

| Option | What you get | Best for |
|---|---|---|
| Free Trial (12 months) | $200 USD credit for 30 days + free services for 12 months | Learning, this course |
| Pay-As-You-Go | No upfront cost, pay only what you use | After trial expires |
| Azure for Students | $100 USD credit, no credit card needed | Students with .edu email |

---

### Step-by-step: Create your free Azure account

**Step 1 — Go to the Azure free account page**
- Open your browser and go to: `https://azure.microsoft.com/free`
- Click **"Start free"**

**Step 2 — Sign in with a Microsoft account**
- If you have an Outlook, Hotmail, or Xbox account — use that
- Otherwise click **"Create one"** and create a new Microsoft account
- Enter your email address and set a password

**Step 3 — Verify your identity**
- Enter your phone number
- Azure will send a verification code via SMS
- Enter the code to confirm

**Step 4 — Enter credit card details (for identity verification only)**
- Azure requires a card to prevent abuse of free trials
- You will **not be charged** during the free trial unless you explicitly upgrade
- Enter your card number, expiry, and CVC

**Step 5 — Agree to terms and finish**
- Check "I agree to the subscription agreement..."
- Click **"Sign up"**
- Wait 1–2 minutes for Azure to provision your account

**Step 6 — Arrive at the Azure Portal**
- You will be redirected to `https://portal.azure.com`
- You should see "Welcome to Microsoft Azure" with your subscription active
- At the top right, confirm your email is shown

> **Checkpoint:** You should see "Free Trial" subscription in the portal. If you see it, you're ready for Step 2.

---

## Concept 2: Resource Groups

### What is a Resource Group?

A **Resource Group** is a logical container for Azure resources. It groups everything that belongs to the same project or environment. Think of it like a **project folder** on your computer.

**Why resource groups matter:**
- **Delete everything at once:** When you delete a resource group, all resources inside it are deleted too — great for cleaning up after this course
- **Access control:** You can give a team member permission to one resource group without giving them access to everything
- **Cost tracking:** Azure shows you the cost of everything inside a resource group, so you can see what a specific project is spending
- **Deployment:** You can deploy all resources for an app together and tear them down together

### Naming conventions

Azure resource names must be globally unique (for storage accounts) or at least unique within your subscription. Use a consistent pattern:

```
rg-{project}-{environment}      # Resource group
st{project}{environment}001     # Storage account (no dashes, 3–24 chars)
evh-{project}-{environment}     # Event Hub

Examples:
rg-datalake-dev
stdatalakedev001
evh-payments-dev
```

---

### Step-by-step: Create a Resource Group

**Step 1 — Open the Azure Portal**
- Go to `https://portal.azure.com` and sign in

**Step 2 — Search for Resource Groups**
- In the top search bar, type **"Resource groups"**
- Click **"Resource groups"** in the results (under Services)

**Step 3 — Create a new Resource Group**
- Click **"+ Create"** (top left)

**Step 4 — Fill in the Basics tab**

| Field | Value to enter |
|---|---|
| Subscription | Your "Free Trial" subscription |
| Resource group name | `rg-datalake-dev` |
| Region | **(Asia Pacific) Australia East** |

- Click **"Review + create"**

**Step 5 — Review and Create**
- Azure shows a summary. Check that region = Australia East
- Click **"Create"**
- Wait 5–10 seconds

**Step 6 — Confirm it was created**
- Click **"Go to resource group"** or go back to the Resource Groups list
- You should see `rg-datalake-dev` with status **"Succeeded"**

> **Checkpoint:** You should see your resource group listed. The "Location" column should show "Australia East".

---

## Concept 3: Azure Blob Storage vs. ADLS Gen2

### The core question: what is the difference?

Both are Microsoft Azure storage services that store files (blobs). But they serve different purposes.

**Simple analogy:**

| | Blob Storage | ADLS Gen2 |
|---|---|---|
| Think of it as | A Google Drive for apps — great for storing any file | A proper data warehouse filesystem — built for big data analytics |
| Folder structure | Flat — no real folders, just "/" in the name | True hierarchical directories — real folders with permissions |
| Performance | Good for individual file access | Optimised for reading millions of files in parallel (Spark, Databricks) |
| Security | Container-level or SAS token access | File and folder-level permissions (POSIX ACLs) |
| Cost | Slightly cheaper | Slightly more expensive but much faster for analytics workloads |

**The key technical difference:**

Blob Storage uses a **flat namespace** — every file is just a blob with a name like `2024/01/15/orders.parquet`. The "/" is part of the name, not a real folder. Deleting the "folder" means deleting each file one by one (slow for millions of files).

ADLS Gen2 uses a **hierarchical namespace (HNS)** — `2024/01/15/` is a real directory. Renaming or deleting a folder is one atomic operation (instant, regardless of how many files are inside).

```
Blob Storage (flat namespace):
Container: raw-data
├── 2024/01/15/orders.parquet   ← just a blob named "2024/01/15/orders.parquet"
├── 2024/01/16/orders.parquet
└── 2024/01/17/orders.parquet

ADLS Gen2 (hierarchical namespace):
Container: raw-data
└── 2024/                       ← real directory
    └── 01/                     ← real directory
        ├── 15/                 ← real directory
        │   └── orders.parquet  ← file inside real directory
        └── 16/
            └── orders.parquet
```

### When to use which?

| Use case | Recommendation |
|---|---|
| Storing logs, backups, images, documents | Blob Storage |
| Bronze / Silver / Gold data lake for Spark or Databricks | ADLS Gen2 |
| Serving files to a website or CDN | Blob Storage |
| Delta Lake or Apache Iceberg tables | ADLS Gen2 (required for atomic renames) |
| Archiving old data cheaply | Blob Storage (Archive tier) |
| Azure Synapse Analytics or Azure Databricks data | ADLS Gen2 |

**In this course:** We use **ADLS Gen2** for our Bronze → Silver → Gold data lake layers.

### Key storage concepts

**Storage Account** — the top-level Azure resource. Everything lives inside it. A storage account can have both Blob and ADLS Gen2 containers (HNS is enabled at the account level).

**Container** — like a bucket in AWS S3. You can have many containers inside one storage account (e.g., `bronze`, `silver`, `gold`, `checkpoints`).

**Blob / File** — the actual data object stored inside a container.

**Access tiers:**

| Tier | Use case | Retrieval cost | Storage cost |
|---|---|---|---|
| Hot | Frequently accessed data (Silver, Gold tables) | Low | High |
| Cool | Infrequently accessed (Bronze raw files, 30+ days old) | Medium | Medium |
| Archive | Rarely accessed (compliance copies, 180+ days) | High (hours to retrieve) | Very low |

---

### Step-by-step: Create a Storage Account (Blob Storage)

We will first create a standard Blob Storage account to understand the baseline.

**Step 1 — Search for Storage Accounts**
- In the Azure portal search bar, type **"Storage accounts"**
- Click **"Storage accounts"** under Services

**Step 2 — Click Create**
- Click **"+ Create"**

**Step 3 — Fill in the Basics tab**

| Field | Value |
|---|---|
| Subscription | Free Trial |
| Resource group | `rg-datalake-dev` |
| Storage account name | `stblobdev001` (must be globally unique — if taken, try `stblobdev002`) |
| Region | Australia East |
| Performance | Standard |
| Redundancy | Locally Redundant Storage (LRS) — cheapest for dev |

**Step 4 — Advanced tab (leave defaults for Blob)**
- **Hierarchical namespace: DISABLED** (this is what makes it plain Blob Storage)
- Click **"Next: Networking"**

**Step 5 — Networking tab**
- Connectivity method: **Public endpoint (all networks)**
- Click **"Next: Data protection"**

**Step 6 — Data protection tab (leave defaults)**
- Click **"Next: Encryption"**

**Step 7 — Encryption tab (leave defaults)**
- Click **"Review + create"**

**Step 8 — Review and Create**
- Confirm: Resource group = `rg-datalake-dev`, Location = Australia East
- Click **"Create"**
- Deployment takes about 30 seconds

**Step 9 — Explore your Blob Storage account**
- Click **"Go to resource"**
- In the left menu, click **"Containers"**
- Click **"+ Container"** → Name: `raw-data` → Public access level: **Private** → OK
- Click into `raw-data` → Click **"Upload"** → Select any small file from your computer → Upload
- You should see your file listed in the container

> **Checkpoint:** You can see your uploaded file inside the `raw-data` container. Note the path shows the filename directly — no real folder structure.

---

### Step-by-step: Create an ADLS Gen2 Storage Account

Now create a second storage account with Hierarchical Namespace enabled — this is ADLS Gen2.

**Step 1 — Go back to Storage Accounts and click Create**

**Step 2 — Fill in the Basics tab**

| Field | Value |
|---|---|
| Subscription | Free Trial |
| Resource group | `rg-datalake-dev` |
| Storage account name | `stadlsdev001` (must be globally unique) |
| Region | Australia East |
| Performance | Standard |
| Redundancy | LRS |

**Step 3 — Advanced tab — THIS IS THE KEY STEP**
- Scroll down to **"Data Lake Storage Gen2"**
- **Hierarchical namespace: ENABLE THIS** ← this is what makes it ADLS Gen2
- Everything else: leave defaults
- Click **"Next: Networking"**

**Step 4 — Networking and remaining tabs**
- Connectivity: Public endpoint (all networks)
- Leave all other tabs as defaults
- Click **"Review + create"** → **"Create"**

**Step 5 — Explore your ADLS Gen2 account**
- Click **"Go to resource"**
- In the left menu, look for **"Containers"** (or **"Data storage > Containers"**)
- Click **"+ Container"** → Name: `bronze` → Create
- Repeat: create `silver` and `gold` containers
- Click into `bronze`
- Click **"+ Add Directory"** → Name: `orders` → OK
- Click into `orders` → Click **"+ Add Directory"** → Name: `2024` → OK
- Click **"Upload"** → Upload a small CSV file

> **Checkpoint:** You should see a real folder tree: `bronze/orders/2024/yourfile.csv`. Notice that when you click on the `orders` folder, Azure shows it as a real directory — not just a filename prefix. This is hierarchical namespace in action.

---

### Side-by-side: What you created

| | Blob Storage (`stblobdev001`) | ADLS Gen2 (`stadlsdev001`) |
|---|---|---|
| Hierarchical namespace | Disabled | **Enabled** |
| Container created | `raw-data` | `bronze`, `silver`, `gold` |
| Folder structure | Flat (filename is the path) | True directories |
| Best for | App storage, backups | Data lake (Spark, Databricks, Synapse) |
| Rename/delete folder | Slow (delete each blob) | Instant atomic operation |
| File-level permissions | No | Yes (POSIX ACLs) |

---

### Step-by-step: Set up Access Keys and Connection String

To connect to your storage accounts from code or tools, you need credentials.

**Step 1 — Go to your ADLS Gen2 account (`stadlsdev001`)**

**Step 2 — Find Access Keys**
- In the left menu, click **"Security + networking" → "Access keys"**
- Click **"Show keys"**
- You will see **key1** and **key2** — each has a **Key** and a **Connection string**

**Step 3 — Copy the connection string**
- Click the copy icon next to **"Connection string"** under key1
- This string lets any app authenticate and read/write your storage

**Step 4 — Test with Azure Storage Explorer (optional but recommended)**
- Download Azure Storage Explorer: `https://azure.microsoft.com/features/storage-explorer/`
- Open it → click **"Add an account"** → Use a connection string
- Paste your connection string → Connect
- You should see your containers (`bronze`, `silver`, `gold`) in the left panel
- You can drag and drop files directly

> **Security note:** Never commit your connection string or access keys to Git. Store them in Azure Key Vault or environment variables.

---

### Step-by-step: Assign Role (RBAC) to your account

Rather than using access keys, the production way to access storage is with **Role-Based Access Control (RBAC)**.

**Step 1 — Go to `stadlsdev001`**

**Step 2 — Click "Access Control (IAM)"** in the left menu

**Step 3 — Click "Add role assignment"**

**Step 4 — Select role**
- Search for **"Storage Blob Data Contributor"**
- Select it → Click **"Next"**

**Step 5 — Select member**
- Click **"+ Select members"**
- Search for your own email address → select it → click **"Select"**
- Click **"Review + assign"** → **"Review + assign"** again

**Step 6 — Confirm**
- Go to the **"Role assignments"** tab
- You should see your email listed under **"Storage Blob Data Contributor"**

Now your user account can read and write data to ADLS Gen2 without using access keys.

---

### Step-by-step: Clean up (avoid charges)

When you are done practising, delete everything to avoid any unexpected charges.

**Option A — Delete individual resources:**
- Go to the storage account → **Overview** → **Delete** → Type the account name to confirm

**Option B — Delete the entire Resource Group (recommended):**
- Go to **Resource Groups** → click `rg-datalake-dev`
- Click **"Delete resource group"** at the top
- Type `rg-datalake-dev` to confirm → **Delete**
- This deletes **all resources** inside the group in one step (both storage accounts, all containers, all files)

> This is why resource groups are powerful — one delete clears an entire environment cleanly.

---

## Summary

| Concept | Key takeaway |
|---|---|
| Azure Account | Your identity; subscription is the billing container |
| Resource Group | Logical folder for resources — delete the group to clean up everything |
| Blob Storage | Flat namespace — good for app files, backups, static content |
| ADLS Gen2 | Hierarchical namespace — required for data lake workloads (Spark, Delta Lake) |
| Key difference | HNS = real directories → atomic rename/delete → essential for large-scale analytics |
| Access | Access keys (dev/testing) vs. RBAC roles (production) |
