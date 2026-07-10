"""
full_load_bronze.py
-------------------
Full load for all 14 VoltGrid API entities -> Azure ADLS Gen2 Bronze container.

Run locally ONCE before switching ADF to incremental mode.
ADF handles all future incremental loads automatically.

Output structure (mirrors what ADF v4 produces):
  bronze/<entity_name>/ingestion_date=<yyyy-MM-dd>/page_<N>.json

Usage:
  pip install requests azure-storage-file-datalake azure-identity python-dotenv
  python full_load_bronze.py
"""

import os
import json
import time
import datetime
import concurrent.futures
from dotenv import load_dotenv

import requests
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

API_BASE        = "https://ev-project-navy-mu.vercel.app"
USERNAME        = os.getenv("VOLTGRID_USERNAME")
PASSWORD        = os.getenv("VOLTGRID_PASSWORD")
STORAGE_ACCOUNT = "evdatalakedev"
CONTAINER       = "bronze"
PAGE_SIZE       = 500
MAX_PAGES       = 1000          # hard cap per entity
MAX_WORKERS     = 4             # parallel entities — keep low to avoid API rate limits
RETRY_ATTEMPTS  = 3
RETRY_DELAY     = 5             # seconds between retries

TENANT_ID     = os.getenv("AZURE_TENANT_ID")
CLIENT_ID     = os.getenv("AZURE_PROJECT_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_PROJECT_CLIENT_SECRET")

ENTITIES = [
    {"entity_name": "payments",           "api_path": "/api/db/payments/"},
    {"entity_name": "sessions",           "api_path": "/api/db/sessions/"},
    {"entity_name": "customers",          "api_path": "/api/db/customers/"},
    {"entity_name": "fleet",              "api_path": "/api/db/fleet/"},
    {"entity_name": "chargers",           "api_path": "/api/db/chargers/"},
    {"entity_name": "vehicles",           "api_path": "/api/db/vehicles/"},
    {"entity_name": "stations",           "api_path": "/api/db/stations/"},
    {"entity_name": "complaints",         "api_path": "/api/db/complaints/"},
    {"entity_name": "maintenance_events", "api_path": "/api/db/maintenance-events/"},
    {"entity_name": "energy_prices",      "api_path": "/api/db/energy-prices/"},
    {"entity_name": "tariffs",            "api_path": "/api/db/tariffs/"},
    {"entity_name": "charge_cards",       "api_path": "/api/db/charge-cards/"},
    {"entity_name": "employees",          "api_path": "/api/db/employees/"},
    {"entity_name": "partners",           "api_path": "/api/db/partners/"},
    {"entity_name": "cities",             "api_path": "/api/db/cities/"},
    {"entity_name": "states",             "api_path": "/api/db/states/"},
    {"entity_name": "weather",            "api_path": "/api/db/weather/"},
]

INGESTION_DATE = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
WATERMARK      = "1900-01-01T00:00:00Z"   # full load — fetch everything

# ── Auth helpers ──────────────────────────────────────────────────────────────

def get_api_token():
    resp = requests.post(
        f"{API_BASE}/api/auth/login/",
        json={"username": USERNAME, "password": PASSWORD},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("token")
    if not token:
        raise ValueError(f"No token in login response: {resp.text}")
    print("[auth] Token obtained successfully")
    return token


def get_adls_client():
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )
    account_url = f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=credential)

# ── API fetch with retry ──────────────────────────────────────────────────────

def fetch_page(session, api_path, page, token):
    url = (
        f"{API_BASE}{api_path}"
        f"?page={page}&page_size={PAGE_SIZE}&updated_after={WATERMARK}"
    )
    headers = {"Authorization": f"Token {token}"}
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = session.get(url, headers=headers, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt == RETRY_ATTEMPTS:
                raise
            print(f"  [retry {attempt}/{RETRY_ATTEMPTS}] {e} — retrying in {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)

# ── ADLS Gen2 upload ──────────────────────────────────────────────────────────

def upload_page(adls_client, entity_name, page, data):
    blob_path = f"api/{entity_name}/ingestion_date={INGESTION_DATE}/page_{page}.json"
    content   = json.dumps(data, ensure_ascii=False).encode("utf-8")

    fs     = adls_client.get_file_system_client(CONTAINER)
    # ensure parent directory exists
    dir_path = f"api/{entity_name}/ingestion_date={INGESTION_DATE}"
    fs.get_directory_client(dir_path).create_directory()

    file_client = fs.get_file_client(blob_path)
    file_client.upload_data(content, overwrite=True, length=len(content))

# ── Per-entity full load ──────────────────────────────────────────────────────

def load_entity(entity, token, adls_client):
    name     = entity["entity_name"]
    api_path = entity["api_path"]
    session  = requests.Session()

    try:
        first = fetch_page(session, api_path, 1, token)
    except Exception as e:
        print(f"[{name}] FAILED to fetch page 1: {e}")
        return {"entity": name, "status": "failed", "pages_done": 0, "error": str(e)}

    total_pages   = min(first.get("pagination", {}).get("total_pages", 1), MAX_PAGES)
    total_records = first.get("pagination", {}).get("total", "?")
    print(f"[{name}] {total_pages} pages | ~{total_records} records — starting upload")

    try:
        upload_page(adls_client, name, 1, first)
        print(f"[{name}] page 1/{total_pages} uploaded ({len(first.get('data', first.get('results', [])))} records)")
    except Exception as e:
        print(f"[{name}] FAILED to upload page 1: {e}")
        return {"entity": name, "status": "failed", "pages_done": 0, "error": str(e)}

    for page in range(2, total_pages + 1):
        try:
            data = fetch_page(session, api_path, page, token)
            upload_page(adls_client, name, page, data)
            print(f"[{name}] page {page}/{total_pages} uploaded ({len(data.get('results', []))} records)")
        except Exception as e:
            print(f"[{name}] FAILED on page {page}: {e}")
            return {"entity": name, "status": "failed", "pages_done": page - 1, "error": str(e)}

    return {"entity": name, "status": "succeeded", "pages_done": total_pages, "error": None}

# ── Audit CSV update ──────────────────────────────────────────────────────────

def append_audit_rows(adls_client, results):
    fs         = adls_client.get_file_system_client(CONTAINER)
    audit_path = "audit/pipeline_audit.csv"

    try:
        existing = fs.get_file_client(audit_path).download_file().readall().decode("utf-8")
    except Exception:
        existing = "pipeline_name,entity_name,load_type,watermark_value,ingestion_date,total_pages,status,pipeline_run_id,run_timestamp\n"

    run_ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    run_id = f"local-full-{INGESTION_DATE}"

    new_rows = ""
    for r in results:
        new_rows += (
            f"full_load_bronze.py,{r['entity']},full,{WATERMARK},"
            f"{INGESTION_DATE},{r['pages_done']},{r['status']},{run_id},{run_ts}\n"
        )

    updated = (existing.rstrip("\n") + "\n" + new_rows).encode("utf-8")
    fc = fs.get_file_client(audit_path)
    fc.upload_data(updated, overwrite=True, length=len(updated))
    print(f"\n[audit] pipeline_audit.csv updated with {len(results)} rows")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  VoltGrid Full Load — {INGESTION_DATE}")
    print(f"  Entities : {len(ENTITIES)}")
    print(f"  Max pages: {MAX_PAGES} per entity")
    print(f"  Workers  : {MAX_WORKERS} parallel entities")
    print("=" * 60)

    token       = get_api_token()
    adls_client = get_adls_client()

    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(load_entity, entity, token, adls_client): entity["entity_name"]
            for entity in ENTITIES
        }
        for future in concurrent.futures.as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                results.append(result)
                print(f"[done] {name:25s} — {result['status']} ({result['pages_done']} pages)")
            except Exception as e:
                print(f"[error] {name}: {e}")
                results.append({"entity": name, "status": "failed", "pages_done": 0, "error": str(e)})

    append_audit_rows(adls_client, results)

    print("\n" + "=" * 60)
    succeeded = [r for r in results if r["status"] == "succeeded"]
    failed    = [r for r in results if r["status"] != "succeeded"]
    print(f"  Succeeded: {len(succeeded)}/{len(ENTITIES)}")
    if failed:
        print(f"  Failed   : {len(failed)}")
        for r in failed:
            print(f"    - {r['entity']}: {r['error']}")
    print("=" * 60)
    print("\nFull load complete. Switch ADF pl_bronze_api_master_v4 to incremental mode now.")


if __name__ == "__main__":
    main()
