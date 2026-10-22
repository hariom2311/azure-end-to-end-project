"""
Uploads sample payment_transactions JSON files to the Bronze volume
with proper run_id=YYYY-MM-DD partition structure.

Structure created:
  bronze/api/payments/run_id=2026-10-14/payment_transactions_raw_20261014.json
  bronze/api/payments/run_id=2026-10-15/payment_transactions_raw_20261015.json
  ...up to yesterday

Run from your local machine:
  pip install azure-storage-blob
  python upload_bronze_payments.py
"""

import json
import random
import io
from datetime import date, timedelta
from azure.storage.blob import BlobServiceClient

# Set your connection string in the environment before running:
#   $env:AZURE_STORAGE_CONN_STR = "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
import os
CONN_STR = os.environ["AZURE_STORAGE_CONN_STR"]
CONTAINER = "bronze"

client           = BlobServiceClient.from_connection_string(CONN_STR)
container_client = client.get_container_client(CONTAINER)

# ── config ────────────────────────────────────────────────────────────────────
START_DATE   = date(2026, 10, 1)
END_DATE     = date(2026, 10, 21)   # yesterday relative to today 2026-10-22
RECORDS_PER_DAY = 20

PAYMENT_METHODS = ["credit_card", "debit_card", "direct_debit", "charge_card"]
STATUSES        = ["success", "success", "success", "success", "failed", "pending"]
STATES          = ["NSW", "VIC", "QLD", "WA", "SA"]

customer_pool = [f"CUST-{i:06d}" for i in range(1, 501)]
session_pool  = [f"SES-{i:08d}" for i in range(1, 10001)]

# ── generate + upload ─────────────────────────────────────────────────────────
pay_seq = 1

current = START_DATE
while current <= END_DATE:
    run_id   = current.isoformat()           # e.g. "2026-10-14"
    date_str = current.strftime("%Y%m%d")    # e.g. "20261014"
    blob_path = f"api/payments/run_id={run_id}/payment_transactions_raw_{date_str}.json"

    records = []
    for _ in range(RECORDS_PER_DAY):
        ts_hour   = random.randint(0, 23)
        ts_minute = random.randint(0, 59)
        ts_second = random.randint(0, 59)
        amount    = round(random.uniform(10.0, 350.0), 2)
        status    = random.choice(STATUSES)

        records.append({
            "payment_id":            f"PAY-{date_str}-{pay_seq:06d}",
            "session_id":            random.choice(session_pool),
            "customer_id":           random.choice(customer_pool),
            "amount_aud":            amount,
            "payment_timestamp_utc": f"{run_id}T{ts_hour:02d}:{ts_minute:02d}:{ts_second:02d}Z",
            "payment_method":        random.choice(PAYMENT_METHODS),
            "status":                status,
            "state":                 random.choice(STATES),
            "currency":              "AUD",
        })
        pay_seq += 1

    payload = json.dumps(records, indent=2).encode("utf-8")
    container_client.upload_blob(
        name=blob_path,
        data=io.BytesIO(payload),
        overwrite=True,
    )
    print(f"Uploaded → {blob_path}  ({len(records)} records)")
    current += timedelta(days=1)

print(f"\nDone. Total payment records uploaded: {pay_seq - 1}")
