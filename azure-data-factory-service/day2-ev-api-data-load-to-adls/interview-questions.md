# Day 2 — Interview Questions: EV API Data Load to ADLS

> 30 questions covering Bronze ingestion patterns, ADF REST pipelines, Key Vault integration, and ADLS Gen2 write. Attempt your own answer first, then check `interview-solutions.md`.

---

## Concept 1: Bronze Ingestion Pattern

**Q1 (Warm-up)**
What is the Bronze layer in a lakehouse architecture? Why do you store raw, unmodified data there instead of cleaning it before storage?

---

**Q2 (Warm-up)**
What is the difference between a full load and an incremental load? In Day 2, which one are we doing and why?

---

**Q3 (Conceptual)**
The VoltGrid API requires a Bearer token obtained via `POST /api/auth/login/`. Why can't ADF handle this automatically in the Linked Service, and how do we work around it?

---

**Q4 (Scenario)**
A junior engineer says: "I'll just hardcode the VoltGrid username and password directly in the Web Activity body — it's faster." What are the two biggest risks of this approach, and what is the correct solution?

---

**Q5 (Tricky)**
The `ls_voltgrid_api` linked service uses **Anonymous** authentication even though the API requires a token. Is this a security misconfiguration? Explain why Anonymous is correct here.

---

**Q6 (Conceptual)**
What is ADF Managed Identity? How does it differ from a Service Principal with a client secret? Which is better for production and why?

---

**Q7 (Scenario)**
You assign `Storage Blob Data Contributor` to ADF's Managed Identity on `evdatalakedev` and immediately run the pipeline. The Copy Activity still fails with 403. What is the most likely cause and how long do you wait?

---

**Q8 (Warm-up)**
In the pipeline `pl_bronze_api_payments`, why is there a **Set Variable** activity (`act_set_token`) between the login Web Activity and the Copy Activity? Why not use `activity('act_api_login').output.token` directly in the Copy Activity header?

---

**Q9 (Conceptual)**
The `ds_voltgrid_payments_src` dataset has two parameters: `p_page` and `p_page_size`. What would happen if you hardcoded `?page=1&page_size=100` directly in the dataset's Relative URL instead of using parameters?

---

**Q10 (Tricky)**
A colleague runs the pipeline with `p_page=1` and `p_page_size=100` and gets 100 records. She then reruns it with the same parameters — what happens to the `payments.json` file in Bronze? Is this a problem?

---

## Concept 2: ADF Components

**Q11 (Warm-up)**
What is the difference between a Linked Service and a Dataset? Give one example of each using the Day 2 VoltGrid pipeline.

---

**Q12 (Conceptual)**
In the pipeline, `act_get_username` and `act_get_password` are two separate Web Activities — one per secret. Why not combine them into a single Web Activity that returns both values?

---

**Q13 (Scenario)**
The pipeline has this dependency chain: `act_get_username` → `act_get_password` → `act_api_login`. The engineer says `act_get_username` and `act_get_password` don't actually depend on each other — why were they chained sequentially here, and how could you make them run in parallel?

---

**Q14 (Conceptual)**
What does the ADF expression `@concat('Token ', variables('v_token'))` produce? Why is the prefix `Token ` (with a space) required?

---

**Q15 (Scenario)**
You're debugging the pipeline in ADF Studio. `act_api_login` succeeds but `act_set_token` fails with: `"The variable 'v_token' is not found."` What did you forget to do?

---

**Q16 (Tricky)**
The Copy Activity source uses `additionalHeaders` to pass the Authorization header. Why is the Authorization header set on the **Copy Activity** and not on the **Linked Service** `ls_voltgrid_api`?

---

**Q17 (Conceptual)**
What is the `filePattern: setOfObjects` setting on the JSON sink dataset? What would the output file look like without it?

---

**Q18 (Scenario)**
You run the pipeline. `act_copy_payments` shows `rowsRead: 100` but `rowsCopied: 0`. What does this mean and what would you check first?

---

**Q19 (Conceptual)**
What RBAC role does ADF Managed Identity need on the Key Vault, and what RBAC role does it need on ADLS Gen2? What is the minimum permission for each — and why not use Owner for both to keep it simple?

---

**Q20 (Tricky)**
If you click **Debug** on the pipeline, does ADF use the published linked services and datasets, or the draft (unpublished) versions? What about when a scheduled trigger fires?

---

## Concept 3: Hands-on / Senior

**Q21 (Scenario)**
The VoltGrid API token expires after 1 hour. Day 2's pipeline fetches 1 page and finishes in 15 seconds, so token expiry is not a problem. In Day 3 (all pages, hundreds of iterations), the pipeline takes 3 hours. How would you handle token expiry in the pagination loop?

---

**Q22 (Conceptual)**
The Bronze sink path is `bronze/api/payments/raw/payments.json` — a fixed file. In a production pipeline, what path structure would you use instead, and why?

---

**Q23 (Scenario)**
The VoltGrid API returns paginated results: each page has a `next` URL if more pages exist, and `null` when the last page is reached. The Day 2 pipeline fetches only page 1. Describe the ADF control flow activities you would add to fetch all pages automatically.

---

**Q24 (Tricky)**
The Web Activity that calls Key Vault uses:
```
Authentication: System Assigned Managed Identity
Resource: https://vault.azure.net
```
What does the **Resource** field tell ADF? What happens if you set Resource to `https://storage.azure.com` instead?

---

**Q25 (Scenario)**
Your team adds a 6th endpoint (`/api/db/energy-prices/`) to the VoltGrid ingestion. Currently, adding it requires: (1) a new dataset, (2) a new Copy Activity, (3) new sink dataset. How would you redesign the pipeline so that adding a new endpoint requires changing only a configuration file and nothing else in ADF?

---

**Q26 (Conceptual)**
Compare these two approaches for the VoltGrid auth step:
- **Approach A:** Store the username and password in ADF Pipeline Parameters (filled at trigger time)
- **Approach B:** Store them in Azure Key Vault and read with Web Activity at runtime

What are the security and operational trade-offs of each?

---

**Q27 (Scenario)**
You are asked to add a daily **Schedule Trigger** to `pl_bronze_api_payments` that runs at 02:00 UTC every night. The next day, the data team says: "We didn't get Thursday's data — the trigger failed and no one was notified." How would you: (1) add the trigger, (2) ensure failed runs generate an alert?

---

**Q28 (Tricky)**
The Key Vault REST API endpoint used in the Web Activity is:
```
https://key-vault-session-ded.vault.azure.net/secrets/voltgrid-username/?api-version=7.0
```
The response contains many fields: `id`, `value`, `attributes`, `tags`. Why do you reference `.output.value` to get the secret? If the Key Vault secret name was `my-secret/2` (with a version), how would the URL change?

---

**Q29 (Scenario)**
The pipeline runs successfully every night for 2 weeks. On week 3, the API team rotates the VoltGrid password. The pipeline starts failing with 401. What is the minimum number of changes needed to fix this, and where do you make them?

---

**Q30 (System design)**
Design a complete ADF Bronze ingestion solution for all 5 VoltGrid endpoints (payments, sessions, customers, stations, vehicles). Requirements:
- All 5 endpoints must run nightly at 01:00 UTC
- All 5 must use the same auth token (one login call per run, not one per endpoint)
- If any single endpoint fails, the others must still complete
- Failed endpoints must be retried once before the pipeline fails
- Data lands at `bronze/api/{endpoint_name}/{run_date}/page_{n}.json`
- Adding a 6th endpoint in future should require no pipeline structure changes

Name every ADF component you would use and why.
