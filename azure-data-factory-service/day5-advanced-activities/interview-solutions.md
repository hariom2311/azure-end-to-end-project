# Day 5 — Interview Solutions: Remaining ADF Activities

---

## Lookup

**A1**
Lookup reads rows from a source dataset and returns them as a JSON object in memory.

- **First row only: Yes** → Returns `{ "firstRow": { ... } }` — use when you want a single config value (e.g., a watermark, a base URL, a flag)
- **First row only: No** → Returns `{ "count": N, "value": [ {...}, {...} ] }` — use when you need all rows to pass to a ForEach or Filter

---

**A2**
Index `[4]` returns the fifth row (0-based indexing) — i.e., the last of 5 rows.

If you write `[5]` on a 5-row result, ADF throws a runtime expression error: **index out of range**. The valid indices are `[0]` through `[4]` for a 5-row result.

Always guard against this by checking `@activity('lkp').output.count` before indexing, or use First row only: No and ForEach instead of hardcoded indices.

---

**A3**
When the table is empty, `output.firstRow` is `null`. If a downstream expression tries to access `@activity('lkp').output.firstRow.last_run`, ADF throws a null reference error.

Handle it with the `if()` function:
```
@if(empty(activity('lkp').output.firstRow),
    '1900-01-01T00:00:00Z',
    activity('lkp').output.firstRow.last_run)
```
This returns a safe default (distant past = full load) when the table has no rows.

---

**A4**
A metadata-driven pipeline is one where the pipeline logic is controlled by configuration data (a file or table), not hardcoded activity settings. Adding or removing a data source requires only changing the config — not the pipeline itself.

With `tables.json`:
```json
[
  { "table": "payments", "active": true },
  { "table": "sessions", "active": true }
]
```

The Lookup Activity reads this file → ForEach loops over the result → Copy Activity inside ForEach uses `@item().table` as the source. To add a 6th table, edit `tables.json`. Zero pipeline changes needed.

---

## Delete

**A5**
The activity **succeeds**. Delete does not throw an error on a missing file — it silently skips it.

Output tab:
```json
{ "filesDeleted": 0, "filesSkipped": 1, "dataRead": 0 }
```

This is by design — it makes Delete safe to run idempotently (running it multiple times has the same effect as running it once).

---

**A6**
Set **Recursive: true** and point the dataset to the folder path `bronze/landing/payments/` without specifying a file name. This deletes all files inside the folder recursively.

Note: In ADLS Gen2, deleting a folder's contents with Recursive does not remove the folder itself if it still has the folder entry — the empty folder may remain. To delete the folder too, the dataset path must point to the folder and Recursive must be true.

---

**A7**
The risk: ADLS Gen2 uses **append-only semantics** on some configurations, and even in standard mode, if the Copy Activity writes less data than the previous run (e.g., the API returned fewer records), the old file is overwritten with the smaller file — data is lost silently, with no warning in Monitor.

Delete is safer because:
- It makes the cleanup explicit and auditable (you can see `filesDeleted: 1` in Monitor)
- It prevents stale data from persisting if the new write fails — if Copy fails after Delete, the old file is gone; that signals a real problem rather than silently serving stale data
- With "Enable logging" on Delete, you have a record of exactly what was removed and when

---

**A8**
"Enable logging" writes a CSV log file to an ADLS Gen2 path you specify. The log records every file that was deleted or skipped, including the file path, size, and deletion timestamp.

It is useful for:
- Compliance audits (prove what was deleted and when)
- Debugging (identify which files were removed during a problematic run)
- Retention policy pipelines (keep a record of old partitions that were cleaned up)

---

## Script

**A9**
| | Script Activity | Stored Procedure Activity |
|---|---|---|
| SQL location | Inline in the ADF pipeline | Pre-defined stored procedure in the database |
| Flexibility | Any SQL — DDL, DML, SELECT | Only calls the named procedure |
| Best for | Quick inline ops, DDL, one-off SQL | Reusable complex logic, upserts |

Choose Script over Stored Procedure when:
- You need to run DDL (`TRUNCATE`, `CREATE TABLE`, `ALTER TABLE`) — stored procedures can do this but the Script Activity is simpler for ad hoc DDL
- The SQL is simple and doesn't need to be reused across multiple pipelines
- You want the SQL visible in the pipeline config without opening the database

---

**A10**
The Script Activity with type `Query` returns results in `output.resultSets`:

```
output.resultSets[0]         → first result set (array of row objects)
output.resultSets[0][0]      → first row of first result set
output.resultSets[0][0].total → the 'total' column value
```

Full expression:
```
@activity('act_script').output.resultSets[0][0].total
```

---

**A11**
The student raises a valid concern but is partially wrong. ADF dynamic content expressions (`@{...}`) are string interpolations — the final SQL string is built by ADF and sent to the database. If `v_schema` contained malicious SQL like `dbo; DROP TABLE payments--`, the resulting string would be:

```sql
TRUNCATE TABLE dbo; DROP TABLE payments--.payments
```

which could be destructive.

The safer approach is to validate `v_schema` before use — add an If Condition or Switch that only allows known schema names (`stg`, `dbo`, `prod`) and fails on anything else. This is a whitelist approach and is the standard defence against injection via pipeline variables.

---

**A12**
Yes. A Script Activity with type `Query` returns rows that downstream activities can reference.

Expression syntax:
```
@activity('act_script').output.resultSets[0][0].column_name
```

Example: if the script returns `SELECT 42 AS answer`, then:
```
@activity('act_script').output.resultSets[0][0].answer  → 42
```

If you need a NonQuery result (rows affected), use:
```
@activity('act_script').output.recordsAffected  → integer
```

---

## Wait

**A13**
Wait Activity pauses the pipeline for a fixed number of seconds before the next activity starts. No data is moved or processed during the wait.

Maximum wait time: **604,800 seconds** (7 days). In practice, the pipeline's own timeout (default 12 hours) usually limits how long a Wait can effectively run.

---

**A14**
The Wait adds **60 seconds total**, not 600 seconds.

With `Is Sequential: false` and `Batch count: 10`, all 10 iterations run at the same time (in parallel). Each iteration has its own Wait of 60 seconds, but since they all run concurrently, the wall-clock wait is still 60 seconds — not 60 × 10.

If `Is Sequential: true`, the total would be 60 × 10 = 600 seconds.

---

**A15**
For simple rate limiting, **Wait Activity is better**:
- One activity, one setting (seconds), zero logic
- Predictable — always pauses exactly N seconds
- No variables, no counter, no condition to maintain

A Until loop polling every 5 seconds would be appropriate when you need to wait for a **condition** to be true (e.g., "wait until the file appears" or "wait until status = ready"). For a fixed-time pause, Until is over-engineered and harder to read.

---

## WebHook

**A16**
| | Web Activity | WebHook Activity |
|---|---|---|
| Call | Fires HTTP request, gets immediate response | Fires HTTP request, then **pauses and waits** |
| Pipeline state | Continues immediately after HTTP response | Paused — waiting for external callback |
| Use when | Synchronous calls (Key Vault, login, REST APIs) | Async jobs that take minutes to hours |

WebHook Activity pauses the pipeline. Web Activity does not.

---

**A17**
The WebHook Activity **fails** with a timeout error. The pipeline reports the activity as Failed with message indicating the callback was not received within the timeout period. All downstream activities depending on this activity with On Success condition are Skipped.

If you need 35 minutes, set the timeout to at least `0.00:40:00` to give a safe buffer.

---

**A18**
`callBackUri` is the ADF-generated URL that the external service must POST back to in order to resume the pipeline. Without it in the request body, the external service has no way to notify ADF that it is done — the pipeline would sit paused until it times out.

The expression `@activity('act_webhook').output.callBackUri` is self-referential — it is evaluated at runtime by ADF and inserted into the outgoing request body so the external system receives the correct callback address for this specific pipeline run.

---

**A19**
The WebHook Activity **fails**. When the external service returns `"statusCode": "500"` in the callback body, ADF treats this as a failure and marks the activity as Failed in Monitor with the error message from the callback body. Downstream activities with On Success dependency are Skipped.

For the pipeline to resume successfully, the callback must return `"statusCode": "200"`.

---

## Filter

**A20**
Filter Activity takes an input array and returns only elements where the condition expression evaluates to `true`.

`@activity('act_filter').output.filterCount` returns the **number of items that passed the filter** — i.e., the length of the output array. It is useful for checking "did any items pass?" before wiring to a ForEach:
```
@equals(activity('act_filter').output.filterCount, 0)  → true if nothing passed
```

---

**A21**
The ForEach processes **3 items** — the 3 that passed the filter (`size > 1,000,000`). The other 5 were dropped by the Filter. ForEach always iterates over `@activity('act_filter').output.value`, which contains only the filtered results.

---

**A22**
Those rows are **dropped**. The condition `@equals(item().status, 'active')` is case-sensitive. `'Active'` ≠ `'active'` in ADF expression evaluation.

Fix: use `equalsIgnoreCase`:
```
@equalsIgnoreCase(item().status, 'active')
```
Or normalise with `lower()`:
```
@equals(toLower(item().status), 'active')
```

---

## ForEach

**A23**
- **Is Sequential: true** — items processed one at a time, in order. Item 2 does not start until Item 1 finishes. Total time = sum of all item times.
- **Is Sequential: false** — items run in parallel up to `Batch count`. Total time = time of the slowest item (not the sum).

Maximum batch count: **50**.

---

**A24**
During the second iteration, `@item()` returns `"sessions"` (a string, not an object). The array is `["payments", "sessions", "customers"]` — plain strings, not objects — so `@item()` is the string itself, not an object with fields.

If the array were `[{"name":"payments"}, {"name":"sessions"}]`, then `@item()` would be the object and `@item().name` would be `"sessions"`.

---

**A25**
Likely cause: **race condition with Append Variable in parallel ForEach**.

When ForEach runs in parallel, multiple iterations execute at the same time. If two iterations try to append to the same array variable simultaneously, only one write "wins" — the other is silently lost. This is a known ADF limitation: variables are not thread-safe in parallel ForEach.

Fix options:
1. Set `Is Sequential: true` — iterations run one at a time, no race condition
2. Don't use Append Variable inside parallel ForEach — instead, write results to ADLS (one file per iteration) and collect them after the loop

---

**A26**
Yes, you can nest a ForEach inside another ForEach.

Limitation: **you cannot use `@item()` from the outer ForEach inside the inner ForEach's expression**. Each ForEach has its own `@item()` scope — the inner `@item()` always refers to the inner loop's current element. To pass the outer item into the inner loop, you must first store it in a pipeline variable using a Set Variable activity before entering the inner ForEach.

---

## Switch

**A27**
- **If Condition** evaluates a boolean (`true`/`false`) and routes to one of two branches (True or False)
- **Switch** evaluates a string and matches it against named case values — any number of cases plus a Default

Switch is clearly better when you have 3+ branches on the same value. Example: routing by environment (`dev`, `staging`, `prod`, `dr`) — using nested If Conditions for this would be unreadable and hard to maintain.

---

**A28**
The **Default branch** runs. Switch matching is **case-sensitive** — `"Prod"` does not match the case `"prod"`. Since no case matches, the Default branch executes.

If the Default contains a Fail Activity, the pipeline fails with the custom error. This is the correct behaviour — it surfaces the misconfigured parameter value immediately.

---

**A29**
No. Only one branch runs per execution. Switch evaluates the expression once, finds the matching case (or Default), runs that branch's inner activities, and exits. Other branches are skipped entirely. There is no way for two branches to run in the same pipeline execution.

---

## Until

**A30**
The student is **correct** — the single Set Variable `v_count = @string(add(int(variables('v_count')), 1))` will throw a runtime error: **"The variable 'v_count' is being both read and set in the same Set Variable activity."** ADF explicitly prohibits a variable from referencing itself in the value expression.

**Correct implementation using two variables:**

```
Variables:
  v_count  (String, default "0")
  v_temp   (String)

Until Expression: @greaterOrEquals(int(variables('v_count')), 5)

Inner canvas:

  Activity A: act_set_temp
    Variable: v_temp
    Value:    @string(add(int(variables('v_count')), 1))
    (reads v_count, writes to v_temp — different variables, allowed)

  Activity B: act_set_count  (depends on act_set_temp)
    Variable: v_count
    Value:    @variables('v_temp')
    (reads v_temp, writes to v_count — different variables, allowed)
```

Result: v_count increments by 1 per iteration. After 5 iterations, condition becomes true and the loop exits.
