# 01 — Verify API Auth
**Day 1 | Part 7.3**

Step-by-step guide for notebook `01_verify_api_auth.ipynb`.

---

## What is this notebook doing?

The VoltGrid API uses **token-based authentication**. Every session works like this:

1. Send username + password to `POST /api/auth/login/` → API returns a short-lived token
2. Attach that token to every subsequent API request as a header: `Authorization: Token <value>`
3. When the notebook or pipeline ends, the token is gone — it only ever lived in memory

**Why this is secure:**
- Credentials come from Key Vault — never hardcoded in the notebook
- The token is never written to disk, logs, or storage
- Each run gets a fresh token — no stale credential risk
- If a token leaks, it can be invalidated without touching the password

---

## How this scales across all 18 endpoints in ADF / Databricks pipelines

```
Pipeline starts
  → POST /api/auth/login/         → token stored in pipeline variable (memory only)
  → GET /api/db/payments/         with Authorization: Token <value>
  → GET /api/db/sessions/         with Authorization: Token <value>
  → GET /api/db/customers/        with Authorization: Token <value>
  → ... all 18 endpoints follow the same pattern
Pipeline ends → token discarded automatically
Next run      → fresh login → fresh token
```

---

## Key Terms

### `import requests`

`requests` is a Python library for making HTTP calls. It is pre-installed in Databricks.

- `requests.post(url, json=..., timeout=...)` — sends a POST request. `json=` auto-serialises the dict to JSON and sets the `Content-Type` header.
- `requests.get(url, headers=..., timeout=...)` — sends a GET request. `headers=` lets you attach auth and other headers.
- `timeout=10` — if the server does not respond within 10 seconds, raise a `Timeout` exception instead of hanging forever.
- `.raise_for_status()` — if the response code is 4xx or 5xx, raise an exception immediately. Without this, a failed request looks like success.
- `.json()` — parses the response body as JSON and returns a Python dict.

### `Authorization: Token <value>`

This is the HTTP header the VoltGrid API expects on every authenticated request.

- `Authorization` — the standard HTTP header name for credentials
- `Token` — the auth scheme name (the API defines this, not us)
- `<value>` — the token string returned by the login endpoint

In code: `headers = {"Authorization": f"Token {token}"}`

### `resp.status_code`

The HTTP status code tells you what happened:

| Code | Meaning |
|---|---|
| `200` | Success — request worked |
| `400` | Bad request — something wrong with what you sent |
| `401` | Unauthorized — credentials wrong or token missing/expired |
| `403` | Forbidden — credentials valid but you don't have access |
| `404` | Not found — URL path is wrong |
| `500` | Server error — problem on the API side |

---

## Prerequisites

Before running this notebook, add these 3 secrets to your Key Vault:

```
Portal → Key vaults → kv-ev-intelligence-dev → Secrets → + Generate/Import
```

| Secret Name | Value |
|---|---|
| `voltgrid-api-base-url` | Provided during the session |
| `voltgrid-username` | Provided during the session |
| `voltgrid-password` | Provided during the session |

---

## Cell 1 — Load API credentials from Key Vault

**What it does:** Reads the 3 API credentials from Key Vault. Nothing is hardcoded.

**Line by line:**
- `import requests` — loads the HTTP library. Must be at the top before any API calls.
- `SCOPE` — name of the Databricks secret scope linked to your Key Vault.
- `dbutils.secrets.get(scope, key)` — reads a secret. Value never appears in output — Databricks always shows `[REDACTED]`.
- `api_base_url` — the root URL of the VoltGrid API, e.g. `https://voltgrid-api.example.com`. All endpoint paths are appended to this.
- `username` / `password` — your VoltGrid login credentials, used only in Cell 2 to get a token.

```python
import requests

SCOPE = "kv-ev-scope"

api_base_url = dbutils.secrets.get(scope=SCOPE, key="voltgrid-api-base-url")
username     = dbutils.secrets.get(scope=SCOPE, key="voltgrid-username")
password     = dbutils.secrets.get(scope=SCOPE, key="voltgrid-password")

print(f"API base URL : {api_base_url}")
print(f"Username     : {username}")
print(f"Password     : [REDACTED]")
print("Credentials loaded from Key Vault — OK")
```

**Expected output:**
```
API base URL : https://voltgrid-api.example.com
Username     : your_username
Password     : [REDACTED]
Credentials loaded from Key Vault — OK
```

**Errors:**

| Error | Cause | Fix |
|---|---|---|
| `Secret does not exist` | Secret name typo or not created | Check Key Vault → Secrets — names must match exactly |
| `Secret scope not found` | `kv-ev-scope` not set up | Day 1 Part 6.5 — create the secret scope |

---

## Cell 2 — Login and get a token at runtime

**What it does:** Sends username + password to the login endpoint. API responds with a token. Token is stored in memory only — never written anywhere.

**Line by line:**
- `requests.post(url, json=..., timeout=10)` — sends a POST request to the login endpoint. `json={"username": ..., "password": ...}` sends the credentials as a JSON body.
- `resp.raise_for_status()` — if login fails (e.g. wrong password → 401), stop immediately with an error instead of continuing silently.
- `resp.json()["token"]` — parses the JSON response and extracts the `token` field. This is the value you will use in all future API calls.
- `token[:8]` — prints only the first 8 characters. Never print the full token — it is a live credential.
- `API_HEADERS` — a dict with the `Authorization` header. Passed to every GET request in Cells 3 and 4.

```python
resp = requests.post(
    f"{api_base_url}/api/auth/login/",
    json={"username": username, "password": password},
    timeout=10,
)
resp.raise_for_status()
token = resp.json()["token"]

print(f"Login response status : {resp.status_code}")
print(f"Token acquired        : {token[:8]}...[REDACTED]")
print("API login — OK")

API_TOKEN   = token
API_HEADERS = {"Authorization": f"Token {API_TOKEN}"}
```

**Expected output:**
```
Login response status : 200
Token acquired        : a1b2c3d4...[REDACTED]
API login — OK
```

**Errors:**

| Error | Cause | Fix |
|---|---|---|
| `401 Unauthorized` | Username or password wrong in Key Vault | Check `voltgrid-username` and `voltgrid-password` values |
| `ConnectionError` | `api_base_url` is wrong or unreachable | Check `voltgrid-api-base-url` value in Key Vault |
| `Timeout` | Server did not respond in 10 seconds | Check network / VPN. Retry. |

---

## Cell 3 — Fetch first page of the payments endpoint

**What it does:** Makes one authenticated GET request to the payments endpoint to confirm the token works. Shows total record count and a sample record.

**Line by line:**
- `requests.get(url, headers=API_HEADERS, timeout=10)` — sends a GET with the token in the `Authorization` header.
- `?page=1&page_size=5` — URL query parameters: fetch only page 1 with 5 records. Keeps the response small — we only want to verify access, not download everything.
- `data.get("pagination", {})` — safely gets the `pagination` key from the response dict. Returns an empty dict `{}` if the key does not exist, so the next `.get()` calls don't crash.
- `pg.get("total", "N/A")` — gets the total record count. Falls back to `"N/A"` if missing.
- Looping over `data["results"][0].items()` — prints every key-value pair of the first record so you can see the data shape.

```python
r = requests.get(
    f"{api_base_url}/api/db/payments/?page=1&page_size=5",
    headers=API_HEADERS,
    timeout=10,
)
r.raise_for_status()
data = r.json()

pg = data.get("pagination", {})
print(f"Total records        : {pg.get('total', 'N/A'):,}")
print(f"Total pages          : {pg.get('total_pages', 'N/A'):,}")
print(f"Page size            : {pg.get('page_size', 'N/A')}")
print(f"Records in this page : {len(data.get('results', []))}")

print(f"\nSample record:")
if data.get("results"):
    for k, v in data["results"][0].items():
        print(f"  {k:<25} : {v}")

print("\nPayments API call — OK")
```

**Expected output:**
```
Total records        : 125,430
Total pages          : 25,086
Page size            : 5
Records in this page : 5

Sample record:
  payment_id                : PAY-000001
  amount_aud                : 45.20
  status                    : Success
  ...

Payments API call — OK
```

**Errors:**

| Error | Cause | Fix |
|---|---|---|
| `401 Unauthorized` | Token expired or not set | Re-run Cell 2 to get a fresh token |
| `404 Not Found` | Endpoint URL path is wrong | Check `api_base_url` has no trailing slash issues |

---

## Cell 4 — Scan all 18 API endpoints

**What it does:** Loops through every endpoint, fetches page 1 with 1 record, and prints the total row count and page count for each. This confirms all 18 endpoints are reachable and the token works across all of them.

**Line by line:**
- `ENDPOINTS` — the list of all 18 endpoint names. Each maps to `/api/db/{name}/`.
- `page_size=1` — we only need 1 record per endpoint. The `pagination` response still gives us the full totals.
- `pg.get("total", 0)` — total record count across all pages for that endpoint.
- `endpoint_errors` — collects any endpoint that failed so we can report them all at the end.

```python
ENDPOINTS = [
    "payments", "sessions", "customers", "fleet", "chargers",
    "vehicles", "stations", "complaints", "maintenance_events",
    "energy_prices", "tariffs", "charge_cards", "employees",
    "partners", "cities", "states", "weather", "pipeline_audit"
]

print(f"{'Endpoint':<25} {'Status':>8} {'Total Rows':>12} {'Total Pages':>13}")
print("-" * 65)

endpoint_errors = []
for ep in ENDPOINTS:
    try:
        r = requests.get(
            f"{api_base_url}/api/db/{ep}/?page=1&page_size=1",
            headers=API_HEADERS,
            timeout=10,
        )
        if r.status_code == 200:
            pg    = r.json().get("pagination", {})
            total = pg.get("total", 0)
            pages = pg.get("total_pages", 0)
            print(f"{ep:<25} {'200 OK':>8} {total:>12,} {pages:>13,}")
        else:
            print(f"{ep:<25} {r.status_code:>8} {'ERROR':>12}")
            endpoint_errors.append(ep)
    except Exception as e:
        print(f"{ep:<25} {'FAIL':>8} {str(e)[:30]:>12}")
        endpoint_errors.append(ep)

print("-" * 65)
if endpoint_errors:
    print(f"\nEndpoints with errors: {endpoint_errors}")
    print("Re-run Cell 2 to refresh the token if you see 401 errors.")
else:
    print(f"\nAll {len(ENDPOINTS)} endpoints reachable — API auth verified.")
```

**Expected output:**
```
Endpoint                   Status   Total Rows   Total Pages
-----------------------------------------------------------------
payments                   200 OK      125,430        25,086
sessions                   200 OK       98,210        98,210
customers                  200 OK       12,500         2,500
fleet                      200 OK        3,200         3,200
...
-----------------------------------------------------------------

All 18 endpoints reachable — API auth verified.
```

**Errors:**

| Error | Cause | Fix |
|---|---|---|
| `401` on all endpoints | Token expired | Re-run Cell 2 |
| `401` on specific endpoint | That endpoint needs different permission | Contact instructor |
| `FAIL` with connection error | Network issue | Check connectivity and retry |

---

## Cell 5 — Noise check on payment records

**What it does:** Fetches 500 payment records and checks for intentionally injected data quality issues. These are built into the dataset on purpose — Silver layer (Day 7) will clean them.

**Line by line:**
- `page_size=500` — fetch 500 records in one call for the noise check sample.
- `r.json().get("results", [])` — gets the list of record dicts. Falls back to empty list `[]` if missing so the list comprehensions below don't crash.
- `float(x.get("amount_aud", 0) or 0)` — gets the amount value. The `or 0` handles `None` values (if `amount_aud` is null, `None or 0` gives `0`). Then cast to `float` for comparison.
- List comprehensions — a compact way to filter a list. `[x for x in recs if condition]` returns only the items where condition is True.
- `VALID_STATUS` — the set of known good status values. Using a `set` (not a list) makes the `in` check faster.

```python
r = requests.get(
    f"{api_base_url}/api/db/payments/?page=1&page_size=500",
    headers=API_HEADERS,
    timeout=30,
)
r.raise_for_status()
recs = r.json().get("results", [])

VALID_STATUS = {"Success", "Failed", "Pending", "Retry", "Refunded", "Disputed"}

neg_amount  = [x for x in recs if float(x.get("amount_aud", 0) or 0) < 0]
zero_amount = [x for x in recs if float(x.get("amount_aud", 0) or 0) == 0]
bad_status  = [x for x in recs if x.get("status", "") not in VALID_STATUS]

print(f"Noise check on {len(recs)} payment records:\n")
print(f"  Negative amounts  : {len(neg_amount):>4}  ({len(neg_amount)/len(recs)*100:.1f}%) — expected ~5%")
print(f"  Zero amounts      : {len(zero_amount):>4}  ({len(zero_amount)/len(recs)*100:.1f}%) — expected ~5%")
print(f"  Invalid status    : {len(bad_status):>4}  ({len(bad_status)/len(recs)*100:.1f}%) — expected ~5%")

if neg_amount:
    s = neg_amount[0]
    print(f"\n  Sample negative: payment_id={s.get('payment_id')}, amount={s.get('amount_aud')}")
if bad_status:
    s = bad_status[0]
    print(f"  Sample bad status: payment_id={s.get('payment_id')}, status='{s.get('status')}'")

print("\nNoise check complete — Silver layer will clean these in Day 7.")
```

**Expected output:**
```
Noise check on 500 payment records:

  Negative amounts  :   26  (5.2%) — expected ~5%
  Zero amounts      :   24  (4.8%) — expected ~5%
  Invalid status    :   23  (4.6%) — expected ~5%

  Sample negative: payment_id=PAY-000043, amount=-12.50
  Sample bad status: payment_id=PAY-000078, status='UNKNOWN'

Noise check complete — Silver layer will clean these in Day 7.
```

These numbers are **intentional** — the dataset is designed with ~5% noise in each category.

---

## After Every Cluster Restart

Re-run **Cell 1 and Cell 2** — Cell 1 reloads the credentials, Cell 2 gets a fresh token. Cells 3–5 can then run as normal.

---

## Quick Reference — Token Auth Flow

```
Key Vault
  └── voltgrid-username    ──┐
  └── voltgrid-password    ──┼──→  POST /api/auth/login/  →  token (in memory)
  └── voltgrid-api-base-url ─┘          │
                                         ▼
                              Authorization: Token <value>
                                         │
                             ┌───────────┼───────────┐
                             ▼           ▼           ▼
                        /payments/  /sessions/  /customers/  ... all 18 endpoints
```

Token lives only in the notebook session. Cleared when cluster restarts or notebook detaches.
