# Topic 08 — Access Control, Security & Compliance
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 3 Questions | 2–8 years experience | Scenario-based

---

### Q20. A new data analyst joins the team and needs access to Gold layer data for Power BI dashboards but must NOT see raw customer PII or charge card numbers. Walk me through how you set this up end-to-end.

**Answer:**

**Access granted — what the analyst CAN see:**

| Layer | Access | Reason |
|---|---|---|
| Bronze | None | Raw data, PII present |
| Silver | None | `sl_customers` has email hashes; `sl_charge_cards` has last4 — still restricted |
| Gold | Read | PII fully masked; card numbers are last4 only |
| Power BI | Report Viewer | RLS filters by their assigned state/franchise |

**Step-by-step setup:**

**Step 1 — Azure Entra ID group:**
Create AAD group `grp-voltgrid-analysts`. Add the new analyst's account.

**Step 2 — ADLS RBAC on Gold container only:**
```
Azure Portal → evdatalakedev → Access Control (IAM) → Add role assignment
Role: Storage Blob Data Reader
Scope: gold container (not the entire storage account)
Assignee: grp-voltgrid-analysts
```
Bronze and Silver containers are not in this assignment — the group gets no access to them.

**Step 3 — Unity Catalog permissions:**
```sql
GRANT SELECT ON SCHEMA dbw_ev_intelligence_dev.gold TO `grp-voltgrid-analysts`;
-- Do NOT grant bronze or silver schema access
```

**Step 4 — Azure Synapse RLS:**
```sql
CREATE FUNCTION dbo.fn_analyst_filter(@state VARCHAR(3))
RETURNS TABLE
AS RETURN
SELECT 1 AS access
WHERE @state = (SELECT assigned_state FROM analyst_permissions WHERE user_name = USER_NAME())
   OR USER_NAME() IN (SELECT user_name FROM executive_group);

ALTER TABLE FactChargingSession
ADD SECURITY POLICY analyst_rls
ADD FILTER PREDICATE dbo.fn_analyst_filter(state_code) ON FactChargingSession;
```

**Step 5 — Power BI RLS:**
In the Power BI data model:
- Create role `StateAnalyst` with DAX filter: `DimState[state_name] = USERPRINCIPALNAME()`
- Assign the analyst's email to the `StateAnalyst` role in Power BI workspace.
- They only see data for their state in all 14 dashboards.

**Step 6 — Audit logging:**
Azure Monitor diagnostic settings on the `gold` ADLS container log all read operations. If the analyst somehow attempts to read `bronze/crm/charge_cards_raw/`, the access is denied (no RBAC) and logged — security team alert fires.

---

### Q21. Explain the difference between a Service Principal, a Managed Identity, and an Access Connector in this project. When would you use each?

**Answer:**

**Service Principal (`sp-ev-intelligence-dev`):**

What it is: An application identity in Azure Entra ID. It has a `client_id` + `client_secret` (or certificate) pair. You manually create it, and you are responsible for rotating the secret.

When used in this project:
- Databricks notebooks that need to read/write ADLS directly via `spark.conf.set("fs.azure.account.oauth2.client.secret...", secret)`.
- The secret is stored in Key Vault, never in notebook code.

Risk: If `client_secret` leaks (e.g., accidentally logged), the SP can be used by anyone until the secret is rotated. **Secret must be rotated every 90 days.**

---

**Managed Identity (`mi-ev-intelligence-dev`):**

What it is: An identity whose credentials Azure manages automatically — no client secret, no rotation needed. Bound to a specific Azure resource (e.g., an ADF instance or VM).

When used in this project:
- ADF pipelines authenticate to ADLS and Key Vault using the ADF Managed Identity.
- No human ever sees or manages a password.

Why better than SP for ADF: ADF is a long-running managed service — you don't want to rotate an SP secret and break all ADF pipelines. Managed Identity rotates automatically, transparently.

---

**Access Connector (`ac-ev-intelligence-dev`):**

What it is: A dedicated Azure resource specifically designed to connect Databricks Unity Catalog to external storage. It has a System-Assigned Managed Identity.

When used in this project:
- Unity Catalog's Storage Credentials reference this Access Connector.
- When a Databricks cluster accesses `/Volumes/dbw_ev_intelligence_dev/bronze/bronze_volume/`, Unity Catalog uses the Access Connector's identity to authenticate to ADLS.
- Notebooks never need to configure SAS tokens or SP credentials for Unity Catalog paths.

---

**Decision rule summary:**

| Use case | Identity type |
|---|---|
| Human user needing portal/CLI access | AAD user account |
| Databricks notebooks needing ADLS access | Service Principal (Key Vault-backed) |
| ADF pipelines needing ADLS/Key Vault access | Managed Identity |
| Unity Catalog volumes needing ADLS access | Access Connector |

---

**RBAC assignments in this project:**

| Identity | Role | Resource |
|---|---|---|
| `sp-ev-intelligence-dev` | Storage Blob Data Contributor | `evdatalakedev` |
| `sp-ev-intelligence-dev` | Key Vault Secrets User | `kv-ev-intelligence-dev` |
| `AzureDatabricks` (enterprise app) | Key Vault Secrets User | `kv-ev-intelligence-dev` |
| `mi-ev-intelligence-dev` | Storage Blob Data Contributor | `evdatalakedev` |
| `mi-ev-intelligence-dev` | Key Vault Secrets User | `kv-ev-intelligence-dev` |
| `ac-ev-intelligence-dev` | Storage Blob Data Contributor | `evdatalakedev` |

---

### Q22. What is the Australian Privacy Act 1988 requirement and how did the data platform comply with it?

**Answer:**

**Key APP (Australian Privacy Principles) requirements relevant to this platform:**

**APP 11 — Security of personal information:**
Personal information must be protected from misuse, interference, loss, and unauthorised access.

Compliance in VoltGrid:
- Customer email hashed with SHA-256 in Silver/Gold — not stored in plain text.
- PAN (card number) masked to last 4 digits — full PAN never leaves the restricted Bronze zone.
- CVV discarded at Bronze → Silver transition — never stored in the lakehouse.
- `bronze/crm/charge_cards_raw/` isolated with RBAC restricted reader role.
- All data encrypted at rest (ADLS Gen2 Microsoft-managed encryption) and in transit (HTTPS/TLS 1.2+).

**APP 12 — Access to personal information:**
An individual can request access to the personal information an organisation holds about them.

Compliance: With Delta Lake time travel and the structured Silver schema, we can query: "What data do we hold about customer C-10042 at any point in time?" — answerable in a single SQL query without manual file scanning.

**APP 3 — Collection of solicited personal information:**
Only collect what is necessary.

Compliance: CVV is collected at payment point but immediately discarded — never stored in the data platform. The `card_token` (from payment gateway) is sufficient for all downstream analytics.

**Audit trail:**
- `_ingestion_ts`, `_source_file`, `_pipeline_run_id` on every Bronze record — satisfies "when was this collected and from where."
- Azure Monitor logs all reads of restricted zones.
- The `gold/data_quality_audit/` table logs every DQ check run — demonstrates active data governance to regulators.

**Security rules applied across the platform:**

| Rule | Why |
|---|---|
| No passwords, keys, or tokens in notebook cells | If the notebook is exported or shared, credentials leak |
| No credentials in git | git history is permanent — deleted creds are still in history |
| All secrets in Key Vault — read via `dbutils.secrets.get()` | Single source of truth. Rotating a secret = one Key Vault update |
| SP client secret rotated every 90 days | Limits exposure window if secret is ever compromised |
| SAS token for external users = `sp=rl` (read + list only) | External users cannot write, delete, or overwrite source data |
| Cluster access mode = Dedicated | Standard mode blocks `dbutils.secrets.get()` — secrets won't work |
| RBAC model on Key Vault (not Access Policies) | Access Policies are legacy — RBAC is the current standard and auditable |
