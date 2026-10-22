"""
Repartitions Bronze API blobs that are missing run_id= folders.

Current structure (broken):
  api/<entity>/ingestion_date=YYYY-MM-DD/<file>.json

Target structure (correct):
  api/<entity>/ingestion_date=YYYY-MM-DD/run_id=YYYY-MM-DD/<file>.json

The script copies each blob into the run_id= subfolder (using the same date
as ingestion_date). Original files are left in place so nothing is deleted.

Entities fixed: all under api/ where depth-4 has a .json file directly
under ingestion_date= (i.e. no run_id= level in between).

Run:
  pip install azure-storage-blob
  python repartition_bronze.py
"""

import sys
import io
import re
from azure.storage.blob import BlobServiceClient

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# Set your connection string in the environment before running:
#   $env:AZURE_STORAGE_CONN_STR = "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
import os
CONN_STR = os.environ["AZURE_STORAGE_CONN_STR"]
CONTAINER    = "bronze"
ACCOUNT_URL  = "https://evdatalakedev2.blob.core.windows.net"

# Regex: api/<entity>/ingestion_date=<date>/<filename>.json
# Matches blobs that are DIRECTLY under ingestion_date= (no run_id= level)
PATTERN = re.compile(
    r"^(api/[^/]+/ingestion_date=(\d{4}-\d{2}-\d{2}))/"   # group1=prefix, group2=date
    r"(?!run_id=)"                                           # NOT already in run_id= folder
    r"([^/]+\.json)$"                                        # group3=filename
)

client           = BlobServiceClient.from_connection_string(CONN_STR)
container_client = client.get_container_client(CONTAINER)

blobs_to_fix = []
for blob in container_client.list_blobs(name_starts_with="api/"):
    m = PATTERN.match(blob.name)
    if m:
        blobs_to_fix.append({
            "src":      blob.name,
            "prefix":   m.group(1),          # e.g. api/payments/ingestion_date=2026-10-16
            "date":     m.group(2),           # e.g. 2026-10-16
            "filename": m.group(3),           # e.g. payments_page_1.json
        })

print(f"Found {len(blobs_to_fix)} blobs to repartition\n")

copied  = 0
skipped = 0

for item in blobs_to_fix:
    dst_path = f"{item['prefix']}/run_id={item['date']}/{item['filename']}"

    # Skip if destination already exists
    dst_blob = container_client.get_blob_client(dst_path)
    if dst_blob.exists():
        skipped += 1
        continue

    # Server-side copy (no download needed)
    src_url = f"{ACCOUNT_URL}/{CONTAINER}/{item['src']}"
    dst_blob.start_copy_from_url(src_url)
    copied += 1

    if copied % 50 == 0:
        print(f"  Copied {copied} blobs so far...")

print(f"\nDone. Copied: {copied}  |  Already existed (skipped): {skipped}")
print("\nNew structure example:")
if blobs_to_fix:
    ex = blobs_to_fix[0]
    print(f"  {ex['prefix']}/run_id={ex['date']}/{ex['filename']}")
