# Day 2 — Practice Exercises: ADLS Gen2 Access & SAS Tokens

> You need your ADLS Gen2 storage account (`stadlsdev001`) and `bronze` container from Day 1.
> Complete exercises in order — each one builds on the previous.
> Estimated total time: 60–90 minutes.

---

## Exercise 1 — Generate SAS Tokens and Read Files in the Browser

**Concept:** SAS Tokens

**Tasks:**

**1a. Upload a test file**
- Go to your storage account (`stadlsdev001`) → **Containers** → `bronze`
- Click **"+ Add Directory"** → name: `day2-test`
- Click into `day2-test` → click **"Upload"**
- Create a file on your computer called `student_data.txt` with this content:
  ```
  Name: John
  Course: Azure Data Engineering
  Day: 2
  Topic: SAS Tokens
  ```
- Upload the file

**1b. Confirm the file is private**
- Click on `student_data.txt` in the portal
- Copy the **URL** from the file details pane (right side)
- Paste it in a new browser tab — you should get an authentication error
- Screenshot or note the error message

**1c. Generate a read-only SAS token**
- Go to your storage account → **"Security + networking"** → **"Shared access signature"**
- Settings:
  - Allowed services: Blob
  - Allowed resource types: Container, Object
  - Allowed permissions: **Read, List only** (uncheck everything else)
  - Expiry: 30 minutes from now
  - Protocol: HTTPS only
- Click **"Generate SAS and connection string"**
- Copy the **SAS token** (the `?sv=...` string)

**1d. Read the file using the SAS token**
- Construct the URL: `<file URL from step 1b> + <SAS token from step 1c>`
- Example: `https://stadlsdev001.dfs.core.windows.net/bronze/day2-test/student_data.txt?sv=2022-11-02&...`
- Paste this full URL in your browser
- Confirm you can read the file content

**1e. Try to write using the read-only SAS token**
Open PowerShell and run:
```powershell
Invoke-WebRequest `
  -Method PUT `
  -Uri "https://stadlsdev001.dfs.core.windows.net/bronze/day2-test/hacked.txt?sv=...your-read-only-sas..." `
  -Headers @{"x-ms-blob-type"="BlockBlob"} `
  -Body "trying to write"
```
Expected: you get a `403 AuthorizationPermissionMismatch` error.

**Answer the questions:**
1. What error message did you get when you tried to read without a SAS token?
2. Why did the PUT request fail even though the SAS token was valid?
3. What would happen if the SAS token expired and someone tried to use the URL?

> **Checkpoint:** You read a private file in the browser using a SAS token URL. The write attempt with a read-only SAS was blocked.

---

## Exercise 2 — Generate a Container-Scoped SAS (Service SAS)

**Concept:** Minimum-scope SAS for sharing with a vendor

**Tasks:**

**2a. Generate a Service SAS for only the `bronze` container**
- Go to: storage account → **Containers** → `bronze`
- Click **"..."** (ellipsis / more options) at the top → click **"Generate SAS"**
- Settings:
  - Signing method: Account key
  - Permissions: Read, List
  - Expiry: 2 hours from now
  - Protocol: HTTPS only
- Click **"Generate SAS token and URL"**
- Copy the **Blob SAS URL**

**2b. List files using the Service SAS URL**
- Append `&restype=container&comp=list` to the SAS URL:
  ```
  https://stadlsdev001.blob.core.windows.net/bronze?sv=...&restype=container&comp=list
  ```
  (Note: use `.blob.core.windows.net` not `.dfs.` for the list operation)
- Paste in your browser — you should see an XML document listing all blobs in `bronze`

**2c. Test that the Service SAS cannot access `silver`**
- Change the URL to target the `silver` container:
  ```
  https://stadlsdev001.blob.core.windows.net/silver?sv=...&restype=container&comp=list
  ```
- Expected: `403 AuthorizationFailure`

**Answer the questions:**
1. What is the difference between an Account SAS and a Service SAS?
2. A vendor needs to upload files to `bronze/vendor-data/` only — no other containers. Which SAS type would you use and what permissions would you give?

---

## Exercise 3 — Read and Write Files Using the Python SDK

**Concept:** Programmatic ADLS Gen2 access

**Prerequisites:**
```bash
pip install azure-storage-file-datalake
```

**Tasks:**

**3a. Read `student_data.txt` using Python**

Create `exercise3a_read.py` on your computer:
```python
from azure.storage.filedatalake import DataLakeServiceClient

account_name = "stadlsdev001"
sas_token = "?sv=..."  # your SAS token with read + list permission

service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=sas_token
)

fs_client = service_client.get_file_system_client("bronze")
file_client = fs_client.get_file_client("day2-test/student_data.txt")

download = file_client.download_file()
content = download.readall()
print("File content:")
print(content.decode("utf-8"))
```

Run it:
```bash
python exercise3a_read.py
```

Expected output: the content of `student_data.txt` printed to your terminal.

**3b. Upload a new file using Python**

Generate a new SAS token with **Read, Write, List, Create** permissions (you need write for this step).

Create `exercise3b_upload.py`:
```python
from azure.storage.filedatalake import DataLakeServiceClient

account_name = "stadlsdev001"
sas_token = "?sv=..."  # SAS token with write permission

service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=sas_token
)

fs_client = service_client.get_file_system_client("bronze")

# Create the directory if needed
dir_client = fs_client.get_directory_client("day2-test/python-uploads")
dir_client.create_directory()

# Upload a CSV
file_client = dir_client.get_file_client("transactions.csv")
csv_data = "tx_id,amount,currency,status\nTX001,100.00,AUD,settled\nTX002,55.50,AUD,pending\nTX003,999.99,AUD,settled"
encoded = csv_data.encode("utf-8")

file_client.create_file()
file_client.append_data(data=encoded, offset=0, length=len(encoded))
file_client.flush_data(len(encoded))

print("Uploaded: bronze/day2-test/python-uploads/transactions.csv")
```

Run it, then verify in the portal that the file appears.

**3c. List all files in `bronze/day2-test/` using Python**

Create `exercise3c_list.py`:
```python
from azure.storage.filedatalake import DataLakeServiceClient

account_name = "stadlsdev001"
sas_token = "?sv=..."  # read + list

service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=sas_token
)

fs_client = service_client.get_file_system_client("bronze")
paths = fs_client.get_paths(path="day2-test", recursive=True)

print("Contents of bronze/day2-test/:")
for path in paths:
    kind = "DIR " if path.is_directory else "FILE"
    print(f"  [{kind}] {path.name}")
```

Expected output:
```
Contents of bronze/day2-test/:
  [FILE] day2-test/student_data.txt
  [DIR ] day2-test/python-uploads
  [FILE] day2-test/python-uploads/transactions.csv
```

**Answer the questions:**
1. What is the difference between `DataLakeServiceClient` and the `azure-storage-blob` SDK?
2. Why do you need to call both `append_data` and `flush_data` when uploading a file?
3. What would you change in the script to use Managed Identity instead of a SAS token?

---

## Exercise 4 — AzCopy: Upload and Download Files

**Concept:** Bulk file transfers from the command line

**Prerequisites:**
- Download AzCopy: `https://aka.ms/downloadazcopy-v10-windows`
- Generate an Account SAS with: Read, Write, List, Create, Delete permissions, 2-hour expiry

**Tasks:**

**4a. Verify AzCopy is installed**
```bash
azcopy --version
```
Expected: `azcopy version 10.x.x`

**4b. Upload a single file**

Create a file `sales_report.csv` on your computer:
```
region,sales,month
Australia,50000,January
Singapore,32000,January
India,45000,January
```

Upload it:
```bash
azcopy copy "sales_report.csv" "https://stadlsdev001.dfs.core.windows.net/bronze/reports/sales_report.csv?sv=..."
```

Verify it appears in the portal: `bronze/reports/sales_report.csv`

**4c. Create and upload a folder of files**

On your computer, create a folder `local-data` with 3 files:
- `orders_jan.csv` — a few lines of made-up order data
- `orders_feb.csv` — a few lines of made-up order data
- `orders_mar.csv` — a few lines of made-up order data

Upload the whole folder:
```bash
azcopy copy "local-data" "https://stadlsdev001.dfs.core.windows.net/bronze/orders/?sv=..." --recursive
```

Verify all 3 files appear in `bronze/orders/local-data/`

**4d. List files in ADLS Gen2 using AzCopy**
```bash
azcopy list "https://stadlsdev001.dfs.core.windows.net/bronze/?sv=..." --recursive
```

How many files are listed? Do you see all the files from previous exercises?

**4e. Download a file from ADLS Gen2**
```bash
azcopy copy "https://stadlsdev001.dfs.core.windows.net/bronze/reports/sales_report.csv?sv=..." "downloaded_sales.csv"
```

Open `downloaded_sales.csv` — does it match what you uploaded?

**Answer the questions:**
1. What does the `--recursive` flag do in AzCopy?
2. When would you use `azcopy sync` instead of `azcopy copy`?
3. You have 10 TB of historical data on an on-premises NAS. You need to load it into ADLS Gen2 once. Would you use the Python SDK or AzCopy? Why?

---

## Exercise 5 — Expire and Revoke a SAS Token

**Concept:** SAS token lifecycle management

**Tasks:**

**5a. Generate a short-lived SAS token (5 minutes)**
- Portal → storage account → Shared access signature
- Expiry: exactly 5 minutes from now
- Permissions: Read, List
- Generate → copy the URL with token

**5b. Use it immediately**
- Construct the full file URL: `https://stadlsdev001.dfs.core.windows.net/bronze/day2-test/student_data.txt?sv=...`
- Open in browser — it works

**5c. Wait 5 minutes, then try again**
- After 5 minutes, refresh the same URL in the browser
- Expected: `403 AuthenticationFailed` — the SAS token has expired

**5d. Revoke all SAS tokens immediately (Key Rotation)**

If a SAS token is leaked and you need to invalidate it immediately:
- Go to storage account → **"Security + networking"** → **"Access keys"**
- Click **"Rotate key"** next to **Key 1**
- Confirm

All SAS tokens signed with Key 1 are now invalid — even tokens with future expiry dates.

**Answer the questions:**
1. After rotating Key 1, are SAS tokens signed with Key 2 still valid?
2. A developer accidentally commits a SAS token to a public GitHub repository. What are the immediate steps you should take?
3. Why is key rotation the nuclear option — what else breaks when you rotate the key?

---

## Bonus Challenge — End-to-End Data Flow Using Python

Build a complete mini data pipeline using only the Python SDK:

**Scenario:** You receive a daily payments file via HTTP. Your pipeline should:
1. Download the CSV from a URL (use Python's `requests` library)
2. Upload the raw CSV to `bronze/payments/YYYY-MM-DD/payments_raw.csv`
3. Read the CSV back from ADLS Gen2
4. Filter only rows where `status == "settled"`
5. Write the filtered data to `silver/payments/YYYY-MM-DD/payments_settled.csv`

**Source data URL:**
Use this URL which returns a sample CSV:
```
https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv
```
(Substitute your own column logic — filter `Survived == 1` instead of `status == settled`)

**Requirements:**
- Use `datetime.date.today()` to build the date-partitioned path automatically
- Handle the case where the `bronze` directory already exists (don't fail on `ResourceExistsError`)
- Print a summary at the end: rows downloaded, rows filtered, rows written to silver

**Starter structure:**
```python
import requests
from datetime import date
from azure.storage.filedatalake import DataLakeServiceClient
import io

account_name = "stadlsdev001"
sas_token = "?sv=..."  # read + write permission

today = date.today().strftime("%Y-%m-%d")
bronze_path = f"payments/{today}/payments_raw.csv"
silver_path = f"payments/{today}/payments_settled.csv"

# Step 1: Download from URL
# Step 2: Upload to bronze
# Step 3: Read from bronze
# Step 4: Filter
# Step 5: Upload to silver
```

Complete the implementation and test it end to end.

---

## Summary Checklist

By the end of today you should be able to:

- [ ] Generate an Account SAS token and a Service (Container) SAS token in the portal
- [ ] Read a private ADLS Gen2 file in the browser using a SAS token URL
- [ ] Explain why write-attempt with a read-only SAS is rejected
- [ ] Read a file from ADLS Gen2 using the Python SDK + SAS token
- [ ] Upload a file to ADLS Gen2 using the Python SDK
- [ ] List files in a directory using the Python SDK
- [ ] Upload a file using AzCopy from the command line
- [ ] Upload a folder recursively using AzCopy
- [ ] Download a file from ADLS Gen2 using AzCopy
- [ ] Revoke all SAS tokens by rotating the access key
- [ ] Explain when to use SAS tokens vs. DefaultAzureCredential (Managed Identity) in production
