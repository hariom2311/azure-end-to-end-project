# pl_bronze_api_payments — Pipeline Explained

---

## What this pipeline does in one line

> Fetches credentials from Key Vault → logs in to VoltGrid API → copies one page of payment records → stores raw JSON in ADLS Gen2 Bronze layer.

---

## Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    pl_bronze_api_payments                       │
│                                                                 │
│  INPUT PARAMETERS                                               │
│  ┌─────────────────┐  ┌─────────────────┐                      │
│  │   p_page        │  │   p_page_size   │                      │
│  │   default: 1    │  │   default: 100  │                      │
│  └────────┬────────┘  └────────┬────────┘                      │
│           │                    │                                │
│           └────────┬───────────┘                                │
│                    │ passed to Copy Activity                     │
│                    ▼                                            │
│  ┌─────────────────────────────┐                                │
│  │  act_get_username           │  Web Activity                  │
│  │  GET Key Vault secret       │  Auth: Managed Identity        │
│  │  → voltgrid-username        │                                │
│  └──────────────┬──────────────┘                                │
│                 │ on Success                                     │
│                 ▼                                               │
│  ┌─────────────────────────────┐                                │
│  │  act_get_password           │  Web Activity                  │
│  │  GET Key Vault secret       │  Auth: Managed Identity        │
│  │  → voltgrid-password        │                                │
│  └──────────────┬──────────────┘                                │
│                 │ on Success                                     │
│                 ▼                                               │
│  ┌─────────────────────────────┐                                │
│  │  act_api_login              │  Web Activity                  │
│  │  POST /api/auth/login/      │  Method: POST                  │
│  │  body: {username, password} │  Returns: { token: "abc..." }  │
│  └──────────────┬──────────────┘                                │
│                 │ on Success                                     │
│                 ▼                                               │
│  ┌─────────────────────────────┐                                │
│  │  act_set_token              │  Set Variable                  │
│  │  v_token =                  │                                │
│  │  activity('act_api_login')  │                                │
│  │  .output.token              │                                │
│  └──────────────┬──────────────┘                                │
│                 │ on Success                                     │
│                 ▼                                               │
│  ┌─────────────────────────────┐                                │
│  │  act_copy_payments          │  Copy Activity                 │
│  │                             │                                │
│  │  SOURCE                     │  SINK                          │
│  │  ds_voltgrid_payments_src   │  ds_bronze_payments_sink       │
│  │  REST GET                   │  ADLS Gen2 JSON                │
│  │  /api/db/payments/          │  bronze/api/payments/          │
│  │  ?page=1&page_size=100      │  raw/payments.json             │
│  │  Header:                    │                                │
│  │  Authorization: Token abc.. │                                │
│  └─────────────────────────────┘                                │
└─────────────────────────────────────────────────────────────────┘
```

---

## Activity-by-Activity Breakdown

### 1. act_get_username
```
Type        : Web Activity
Method      : GET
URL         : https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-username/?api-version=7.0
Auth        : Managed Identity (ADF's identity, no password needed)
Output used : activity('act_get_username').output.value  →  "your-username"
```

### 2. act_get_password
```
Type        : Web Activity
Method      : GET
URL         : https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-password/?api-version=7.0
Auth        : Managed Identity
Output used : activity('act_get_password').output.value  →  "your-password"
```

### 3. act_api_login
```
Type        : Web Activity
Method      : POST
URL         : https://ev-project-navy-mu.vercel.app/api/auth/login/
Body        : { "username": "<from step 1>", "password": "<from step 2>" }
Response    : { "token": "abc123xyz..." }
Output used : activity('act_api_login').output.token  →  "abc123xyz..."
```

### 4. act_set_token
```
Type        : Set Variable
Variable    : v_token
Value       : activity('act_api_login').output.token
Purpose     : Store token so the Copy Activity can use it in the Authorization header
```

### 5. act_copy_payments
```
Type        : Copy Activity

SOURCE (ds_voltgrid_payments_src)
  Linked service : ls_voltgrid_api  (base URL: https://ev-project-navy-mu.vercel.app)
  Relative URL   : /api/db/payments/?page=1&page_size=100
  Header         : Authorization: Token abc123xyz...
  Response       : { "data": [...100 payment records...], "pagination": {...} }

SINK (ds_bronze_payments_sink)
  Linked service : ls_adls_bronze  (evdatalakedev, Managed Identity)
  Path           : bronze/api/payments/raw/payments.json
  Format         : JSON
```

---

## Data Flow

```
Key Vault                    VoltGrid API                  ADLS Gen2
─────────────────────        ──────────────────────        ────────────────────────────
voltgrid-username   ──┐
                      ├──►  POST /api/auth/login/
voltgrid-password   ──┘         │
                                │  token
                                ▼
                         GET /api/db/payments/      ──►   bronze/
                             ?page=1                        api/
                             &page_size=100                   payments/
                             Authorization: Token               raw/
                                                                 payments.json
```

---

## Parameters — What to enter when you trigger

| Parameter | Type | Default | What it does |
|---|---|---|---|
| `p_page` | int | 1 | Which page of results to fetch. Page 1 = first 100 records. |
| `p_page_size` | int | 100 | How many records per page. Max 100. |

**Example trigger values:**

| Goal | p_page | p_page_size |
|---|---|---|
| First 100 records | 1 | 100 |
| Records 101–200 | 2 | 100 |
| First 10 records (quick test) | 1 | 10 |

---

## Output File

After a successful run, one file appears in ADLS:

```
evdatalakedev
└── bronze/
    └── api/
        └── payments/
            └── raw/
                └── payments.json   ← raw API response, exactly as returned
```

Contents of `payments.json`:
```json
{
  "data": [
    {
      "payment_id": "PAY-001",
      "session_id": "SES-001",
      "customer_id": "CUST-001",
      "amount_aud": 25.50,
      "status": "completed",
      ...
    },
    ...99 more records...
  ],
  "pagination": {
    "page": 1,
    "page_size": 100,
    "total": 12500,
    "total_pages": 125
  }
}
```

---

## Why Managed Identity for Key Vault?

```
Without Managed Identity:
  You → store username/password in ADF → security risk, rotation headache

With Managed Identity:
  ADF identity → asks Azure "give me a token for Key Vault"
  Azure        → checks: does ADF have Key Vault Secrets User role? Yes
  Azure        → returns short-lived OAuth token
  ADF          → calls Key Vault REST API with that token
  Key Vault    → returns secret value
  
  No password stored anywhere in ADF. Token auto-rotates. Nothing to manage.
```

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `act_get_username` fails with 403 | ADF Managed Identity missing `Key Vault Secrets User` role | Portal → Key Vault → IAM → assign role to ADF MI |
| `act_api_login` fails with 401 | Wrong username or password stored in Key Vault | Check `voltgrid-username` and `voltgrid-password` values in Key Vault |
| `act_copy_payments` fails with 401 | Token not stored correctly in `v_token` | Check `act_api_login` output — confirm `.output.token` exists |
| `act_copy_payments` fails with 403 | ADF MI missing `Storage Blob Data Contributor` on `evdatalakedev` | Portal → Storage account → IAM → assign role |
| Output file is empty | API returned 0 records | Check API directly — try page 1 with page_size 10 first |
