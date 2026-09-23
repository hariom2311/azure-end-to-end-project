# Day 2 — Accessing ADLS Gen2: SAS Tokens, Python SDK & AzCopy

## Overview

Day 1 gave you an ADLS Gen2 storage account with Bronze, Silver, and Gold containers. Day 2 answers: **how do you actually read and write files in ADLS Gen2?**

There are four ways to access ADLS Gen2:
1. **Access Keys** — master key, full access, for dev/testing only
2. **SAS Tokens** — scoped, time-limited URLs you can share safely
3. **Python SDK (azure-storage-file-datalake)** — programmatic access from code
4. **AzCopy** — command-line tool for bulk file transfers

By the end of today you will be able to read a file from ADLS Gen2 using a SAS token URL in your browser, read and upload files using Python, and bulk-copy files with AzCopy.

**The 3 concepts:**
1. SAS Tokens — what they are, how to generate them in the portal, and how to use them
2. Python SDK — reading, writing, and listing files in ADLS Gen2 from Python code
3. AzCopy — bulk upload and download from the command line

---

## Concept 1: SAS Tokens — Scoped, Time-Limited Access

### What is a SAS Token?

A **Shared Access Signature (SAS) token** is a signed URL that grants specific permissions on a specific resource for a specific time window — without sharing your master access key.

**Real-world analogy:**
Your storage account access key is like the master key to your office building. You would never hand it to a delivery driver. Instead you give them a **temporary access card** that only opens the loading dock, and only works today. That temporary card is the SAS token.

```
SAS Token URL structure:
https://{account}.blob.core.windows.net/{container}/{file}?sv=2022-11-02
    &ss=b
    &srt=sco
    &sp=rl
    &se=2024-01-16T00%3A00%3A00Z
    &sig=abc123...

sv  = storage version
ss  = services (b = blob)
srt = resource type (s = service, c = container, o = object)
sp  = permissions (r = read, l = list)
se  = expiry datetime
sig = cryptographic signature (proves it was signed by the account key)
```

### Types of SAS

| Type | What it is | Use case |
|---|---|---|
| Account SAS | Access to any container/file in the account | Broad — dev and testing only |
| Service SAS | Access to one specific container or blob | Give a vendor access to one folder |
| User Delegation SAS | Signed with your Azure AD identity instead of the account key | Most secure — recommended for production |

### Permissions you can grant in a SAS token

| Permission letter | What it allows |
|---|---|
| r | Read a blob/file |
| w | Write (create/overwrite) a blob/file |
| d | Delete a blob/file |
| l | List blobs in a container |
| c | Create a new blob |
| a | Append to an existing blob |

You combine permissions: `rl` = read + list. `rwdl` = full access.

---

### Step-by-step: Generate an Account SAS Token in the Portal

**Step 1 — Go to your ADLS Gen2 storage account**
- Open `https://portal.azure.com`
- Go to your storage account (`stadlsdev001` from Day 1)

**Step 2 — Navigate to Shared Access Signature**
- In the left menu, click **"Security + networking"** → **"Shared access signature"**

**Step 3 — Configure the SAS**

| Field | Value |
|---|---|
| Allowed services | Blob |
| Allowed resource types | Container, Object |
| Allowed permissions | Read, List |
| Start date/time | Now (default) |
| Expiry date/time | 1 hour from now |
| Allowed IP addresses | Leave blank (open for testing) |
| Allowed protocols | HTTPS only |
| Signing key | Key 1 |

- Click **"Generate SAS and connection string"**

**Step 4 — Copy the SAS token and URL**
You will see three values appear:
- **SAS token** — starts with `?sv=...`
- **Blob service SAS URL** — the full base URL with the token appended
- **Connection string** — for SDK use

Copy the **SAS token** value (the `?sv=...` string) — save it in a text file.

> **Checkpoint:** You have a SAS token string that starts with `?sv=`.

---

### Step-by-step: Use a SAS Token to Read a File in the Browser

**Step 1 — Upload a test file**
- Go to your storage account → **Containers** → `bronze`
- Click **"+ Add Directory"** → name: `test`
- Click into `test` → click **"Upload"**
- Create a small file on your computer called `hello.txt` with content: `Hello from ADLS Gen2`
- Upload it — the path is now `bronze/test/hello.txt`

**Step 2 — Get the file's direct URL**
- Click on `hello.txt` in the portal
- In the file details pane on the right, find the **URL** field
- It looks like: `https://stadlsdev001.dfs.core.windows.net/bronze/test/hello.txt`
- Copy this URL

**Step 3 — Try opening without SAS (should fail)**
- Paste the URL into your browser
- You will get: `AuthenticationRequiredError` — the container is private

**Step 4 — Append the SAS token and try again**
- Add your SAS token to the end of the URL:
  ```
  https://stadlsdev001.dfs.core.windows.net/bronze/test/hello.txt?sv=2022-11-02&ss=b&...
  ```
- Paste this full URL into your browser
- You should see the text: `Hello from ADLS Gen2`

> **Checkpoint:** The file content appears in your browser. You accessed a private ADLS Gen2 file using only a URL + SAS token — no login, no SDK, no code.

---

### Step-by-step: Generate a Service SAS for a Specific Container

An Account SAS covers the whole account. A **Service SAS** is scoped to one container only — safer to share with a vendor or teammate.

**Step 1 — Go to the bronze container**
- Storage account → **Containers** → `bronze`

**Step 2 — Right-click the container → Generate SAS**
- In the `bronze` container view, click **"..."** (more options) at the top right
- Click **"Generate SAS"**

**Step 3 — Configure**

| Field | Value |
|---|---|
| Signing method | Account key |
| Permissions | Read, List |
| Expiry | 24 hours from now |
| Allowed IP addresses | Leave blank |
| Allowed protocols | HTTPS only |

**Step 4 — Generate and copy**
- Click **"Generate SAS token and URL"**
- Copy the **Blob SAS URL** — this URL only works for the `bronze` container, not `silver` or `gold`

**Test it:**
- Paste the SAS URL into your browser — you should see an XML listing of all blobs in the `bronze` container (because you have the `List` permission)

---

### SAS Token Security Rules

1. **Never put a SAS token in code that gets committed to Git** — treat it like a password
2. **Set the shortest possible expiry** — 1 hour for testing, 24 hours max for sharing with a vendor
3. **Scope to the minimum permission** — if the vendor only needs to read, do not give them write
4. **Scope to the minimum resource** — if the vendor only needs the `bronze/orders/` directory, generate a SAS for that path only, not the whole container
5. **Rotate if compromised** — go to the storage account → **Regenerate key 1** — all SAS tokens signed with key 1 immediately become invalid

---

## Concept 2: Python SDK — Programmatic Access to ADLS Gen2

### Why use the Python SDK?

The Azure portal is great for manual exploration. The Python SDK is how your data pipelines, notebooks, and scripts interact with ADLS Gen2:
- Upload processed files to Bronze
- Read Parquet files from Silver
- List all files in a directory to check what landed
- Delete old files as part of cleanup

### Installing the SDK

```bash
pip install azure-storage-file-datalake azure-identity
```

Two packages:
- `azure-storage-file-datalake` — the ADLS Gen2 client (supports hierarchical namespace)
- `azure-identity` — handles authentication (Managed Identity, Service Principal, browser login)

> **Why not `azure-storage-blob`?** That package works too (ADLS Gen2 is blob-compatible), but `azure-storage-file-datalake` supports HNS-specific operations: create real directories, rename directories atomically, and set POSIX ACLs.

---

### Authentication methods in Python

| Method | When to use | How |
|---|---|---|
| Account key | Dev/testing only | Pass key directly in the URL |
| SAS token | Sharing with external scripts or teams | Pass SAS token as credential |
| DefaultAzureCredential | Production: Managed Identity / Service Principal | `from azure.identity import DefaultAzureCredential` |

For this lesson we use **SAS token** (simplest to get started) and also show **DefaultAzureCredential** for production.

---

### Step-by-step: Read a File from ADLS Gen2 Using Python + SAS Token

**Step 1 — Set up your Python environment**

On your local computer:
```bash
pip install azure-storage-file-datalake
```

**Step 2 — Get your account details**
- Storage account name: `stadlsdev001`
- Generate a SAS token (from Concept 1 steps) with **Read + List** permissions
- File to read: `bronze/test/hello.txt` (from Concept 1)

**Step 3 — Write the Python script**

Create a file called `read_from_adls.py`:

```python
from azure.storage.filedatalake import DataLakeServiceClient

# Your storage account details
account_name = "stadlsdev001"
sas_token = "?sv=2022-11-02&ss=b&srt=sco&sp=rl&se=..."  # paste your SAS token here

# Build the account URL
account_url = f"https://{account_name}.dfs.core.windows.net"

# Create the client using SAS token
service_client = DataLakeServiceClient(account_url=account_url, credential=sas_token)

# Get a reference to the container (file system in ADLS Gen2 terminology)
file_system_client = service_client.get_file_system_client(file_system="bronze")

# Get a reference to the file
file_client = file_system_client.get_file_client("test/hello.txt")

# Download and read the file
download = file_client.download_file()
content = download.readall()

print(content.decode("utf-8"))
```

**Step 4 — Run it**
```bash
python read_from_adls.py
```

Expected output:
```
Hello from ADLS Gen2
```

> **Checkpoint:** The file content prints to your terminal. You read a file from ADLS Gen2 using Python + SAS token.

---

### Step-by-step: Upload a File to ADLS Gen2 Using Python

**Step 1 — Generate a SAS token with Write permission**
- Go to the portal → Storage account → Shared access signature
- Permissions: **Read, Write, List, Create**
- Generate and copy the new SAS token

**Step 2 — Write the upload script**

Create `upload_to_adls.py`:

```python
from azure.storage.filedatalake import DataLakeServiceClient

account_name = "stadlsdev001"
sas_token = "?sv=2022-11-02&ss=b&..."  # SAS token with write permission

account_url = f"https://{account_name}.dfs.core.windows.net"

service_client = DataLakeServiceClient(account_url=account_url, credential=sas_token)

# Get the bronze container
file_system_client = service_client.get_file_system_client(file_system="bronze")

# Create directories if they don't exist
dir_client = file_system_client.get_directory_client("orders/2024/01/15")
dir_client.create_directory()

# Create and upload a file
file_client = dir_client.get_file_client("orders_20240115.csv")
data = "order_id,customer_id,amount\n1001,C001,99.50\n1002,C002,249.00\n1003,C001,15.75"

file_client.create_file()
file_client.append_data(data=data.encode(), offset=0, length=len(data.encode()))
file_client.flush_data(len(data.encode()))

print("File uploaded successfully")
print(f"Path: bronze/orders/2024/01/15/orders_20240115.csv")
```

**Step 3 — Run it**
```bash
python upload_to_adls.py
```

**Step 4 — Verify in the portal**
- Go to your storage account → `bronze` container → navigate to `orders/2024/01/15/`
- You should see `orders_20240115.csv`
- Click on it → click **"Edit"** to see the CSV content

> **Checkpoint:** The CSV file appears in ADLS Gen2 with the correct content.

---

### Step-by-step: List Files in a Directory Using Python

```python
from azure.storage.filedatalake import DataLakeServiceClient

account_name = "stadlsdev001"
sas_token = "?sv=2022-11-02&..."  # SAS token with read + list

account_url = f"https://{account_name}.dfs.core.windows.net"
service_client = DataLakeServiceClient(account_url=account_url, credential=sas_token)

file_system_client = service_client.get_file_system_client("bronze")

# List all paths under orders/2024/01/15/
paths = file_system_client.get_paths(path="orders/2024/01/15")

print("Files in bronze/orders/2024/01/15/:")
for path in paths:
    print(f"  {path.name}  ({path.content_length} bytes)  is_directory={path.is_directory}")
```

Output:
```
Files in bronze/orders/2024/01/15/:
  orders/2024/01/15/orders_20240115.csv  (82 bytes)  is_directory=False
```

---

### Production pattern: Using DefaultAzureCredential (no SAS token needed)

In production (Databricks, Azure Functions, Azure VMs), your code runs with a **Managed Identity** — Azure automatically provides credentials without any secret management.

```python
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential

account_name = "stadlsprod001"

# DefaultAzureCredential tries in order:
# 1. Environment variables (Service Principal)
# 2. Managed Identity (when running on Azure VM / Databricks / Azure Functions)
# 3. Azure CLI login (on your local machine: run `az login` first)
# 4. Browser popup login

credential = DefaultAzureCredential()

service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=credential
)

file_system_client = service_client.get_file_system_client("bronze")
# ... rest of your code is identical
```

On your local machine: run `az login` in your terminal first, then `DefaultAzureCredential` picks up your logged-in identity automatically.

---

## Concept 3: AzCopy — Bulk File Transfers from the Command Line

### What is AzCopy?

AzCopy is a command-line utility made by Microsoft for high-speed bulk data transfers to and from Azure Storage. It is:
- **Fast** — uses parallel threads and optimises for large file counts
- **Resumable** — if a transfer is interrupted, it can resume from where it stopped
- **Scriptable** — ideal for automated data loads in shell scripts or CI/CD pipelines

### When to use AzCopy vs Python SDK vs Portal

| Tool | Best for |
|---|---|
| Azure Portal | Manual uploads, browsing, one-off operations |
| Python SDK | Programmatic access from pipelines, notebooks, applications |
| AzCopy | Bulk transfers (thousands of files), initial data lake seeding, cross-account copies |

### Install AzCopy

**Windows:**
Download from: `https://aka.ms/downloadazcopy-v10-windows`
- Extract the ZIP → you get `azcopy.exe`
- Move `azcopy.exe` to a folder in your PATH (e.g., `C:\Windows\System32`) or run it from the download folder

**Mac:**
```bash
brew install azcopy
```

**Linux:**
```bash
wget https://aka.ms/downloadazcopy-v10-linux -O azcopy.tar.gz
tar -xf azcopy.tar.gz
sudo mv azcopy_linux_*/azcopy /usr/local/bin/
```

---

### Step-by-step: Authenticate AzCopy with SAS Token

AzCopy can authenticate with either:
- A SAS token appended to the URL
- `azcopy login` (uses your Azure AD account — recommended for production)

**For this lesson we use a SAS token:**

1. Generate an Account SAS token with **Read, Write, List, Create, Delete** permissions
2. AzCopy uses it by appending it to the storage URL

---

### Step-by-step: Upload a Local File to ADLS Gen2 with AzCopy

**Step 1 — Open a terminal (Command Prompt or PowerShell on Windows)**

**Step 2 — Create a test file locally**
```
echo "payment_id,amount,status" > payments.csv
echo "P001,100.00,settled" >> payments.csv
echo "P002,250.50,pending" >> payments.csv
```

**Step 3 — Upload using AzCopy**

Syntax:
```
azcopy copy <source> <destination-url-with-SAS>
```

Example:
```bash
azcopy copy "payments.csv" "https://stadlsdev001.dfs.core.windows.net/bronze/payments/payments.csv?sv=2022-11-02&ss=b&..."
```

Expected output:
```
INFO: Scanning...
INFO: Any empty folders will not be processed, because source and/or destination doesn't have full folder support

Job 8b123456-...-... has started
Log file is located at: C:\Users\you\.azcopy\...log

100.0 %, 1 Done, 0 Failed, 0 Pending, 0 Skipped, 1 Total

Job 8b123456-... summary
Elapsed Time (Minutes): 0.0167
Number of File Transfers: 1
Number of Folder Property Transfers: 0
Total Number of Transfers: 1
Number of Transfers Completed: 1
Final Job Status: Completed
```

**Step 4 — Verify in the portal**
- Go to storage account → `bronze` → `payments` → you should see `payments.csv`

---

### Step-by-step: Upload an Entire Local Folder (Recursive)

```bash
azcopy copy "C:\data\raw-orders\" "https://stadlsdev001.dfs.core.windows.net/bronze/orders/?sv=2022-11-02&..." --recursive
```

The `--recursive` flag copies all subdirectories and files. This is the standard pattern for seeding a data lake with historical data.

---

### Step-by-step: Download a File from ADLS Gen2

```bash
azcopy copy "https://stadlsdev001.dfs.core.windows.net/bronze/test/hello.txt?sv=2022-11-02&..." "C:\downloads\hello.txt"
```

---

### Step-by-step: Copy Between Two ADLS Gen2 Accounts (Server-to-Server)

One of AzCopy's most powerful features — copying directly between two Azure storage accounts without downloading to your machine:

```bash
azcopy copy \
  "https://stadlsdev001.dfs.core.windows.net/bronze/?sv=...&sas-for-source..." \
  "https://stadlsprod001.dfs.core.windows.net/bronze/?sv=...&sas-for-destination..." \
  --recursive
```

The data flows directly from one Azure account to the other — your computer just initiates the copy. This is called **server-to-server transfer** and is extremely fast (gigabytes per second).

---

### Step-by-step: List Files in ADLS Gen2 with AzCopy

```bash
azcopy list "https://stadlsdev001.dfs.core.windows.net/bronze/orders/?sv=2022-11-02&..."
```

Output:
```
INFO: Listing of folder: https://stadlsdev001.dfs.core.windows.net/bronze/orders/

orders/2024/01/15/orders_20240115.csv; Content Length: 82
orders/2024/01/16/orders_20240116.csv; Content Length: 95
payments/payments.csv; Content Length: 64
```

---

### Step-by-step: Sync (Only Copy Changed Files)

The `sync` command copies only files that have changed or are missing in the destination — like `rsync` for Azure:

```bash
azcopy sync \
  "C:\data\daily-exports\" \
  "https://stadlsdev001.dfs.core.windows.net/bronze/daily-exports/?sv=2022-11-02&..." \
  --recursive
```

Use `sync` for incremental updates — it skips files that are already up to date, saving time and egress costs.

---

## Summary

| Tool | Use case | Auth method |
|---|---|---|
| SAS Token (browser URL) | Share a file with a vendor or student — no code needed | Append `?sv=...` to the file URL |
| SAS Token (Python SDK) | Script reads/writes files programmatically | Pass SAS token as `credential` |
| DefaultAzureCredential (Python SDK) | Production pipelines on Azure VMs, Databricks, Functions | Managed Identity — no secrets in code |
| AzCopy upload | Upload a folder of files to ADLS Gen2 | SAS token in the destination URL |
| AzCopy server-to-server | Copy between two Azure storage accounts | SAS token on both source and destination URLs |
| AzCopy sync | Incremental update — only copy changed files | SAS token |

### Key security rules (repeat from Concept 1 — important enough to say twice)

- Never commit SAS tokens or access keys to Git
- Use the shortest expiry that works for the use case
- Use `DefaultAzureCredential` in production — no SAS tokens, no secrets in code
- Scope SAS tokens to the minimum: least permissions, smallest resource (file → directory → container → account)
