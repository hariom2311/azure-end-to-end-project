# Day 6 — Demo Pipeline: End-to-End Activity Walkthrough

> **Goal:** Build a complete demo pipeline from scratch that chains SetVariable → Filter → ForEach → Until → Switch together using only in-memory arrays — no dataset, no linked service, no external API required.
> The corrected pipeline JSON is in `pl_demo_pipeline.json` in this folder.

---

## What the Pipeline Does — Full Flow

```
pl_demo_pipeline

Step 1 — Set Run ID
  Capture the pipeline's run ID into a variable for reference.

Step 2 — Filter Items
  Start with an array: ["Apple", "Banana", "Mango", "Orange"]
  Keep everything EXCEPT "Banana"
  Result: ["Apple", "Mango", "Orange"]

Step 3 — ForEach Item
  Loop over the filtered array ["Apple", "Mango", "Orange"] sequentially.
  In each iteration, append the current item to v_items_array.
  Result: v_items_array = ["Apple", "Mango", "Orange"]

Step 4 — Init Until Counter
  Reset v_until_counter to "0" before entering the Until loop.

Step 5 — Until 3 Items
  Loop until v_until_counter reaches 3.
  Each iteration:
    a. Append "Iteration-<counter>" to v_until_array
    b. Set v_temp_counter = counter + 1   (intermediate step)
    c. Set v_until_counter = v_temp_counter (increment)
  Result: v_until_array = ["Iteration-0", "Iteration-1", "Iteration-2"]

Step 6 — Switch Environment
  Read v_environment variable (default: "dev").
  "dev"  → v_switch_result = "Development selected"
  "prod" → v_switch_result = "Production selected"
  other  → v_switch_result = "Unknown environment"
```

**Visual pipeline flow:**

```
[Set Run ID]
     │ On Success
[Filter Items]
  Input:  ["Apple","Banana","Mango","Orange"]
  Filter: keep where item != "Banana"
  Output: ["Apple","Mango","Orange"]
     │ On Success
[ForEach Item]  (sequential)
  Iteration 1: item = "Apple"  → Append "Apple"   to v_items_array
  Iteration 2: item = "Mango"  → Append "Mango"   to v_items_array
  Iteration 3: item = "Orange" → Append "Orange"  to v_items_array
     │ On Success
[Init Until Counter]
  v_until_counter = "0"
     │ On Success
[Until 3 Items]
  condition: v_until_counter >= 3 → stop
  ┌─────────────────────────────────────────────────────────┐
  │ Iteration 1: counter="0"                                │
  │   Append "Iteration-0" → v_until_array                 │
  │   v_temp_counter = "1"                                  │
  │   v_until_counter = "1"  → check: 1 >= 3? No → repeat  │
  │ Iteration 2: counter="1"                                │
  │   Append "Iteration-1" → v_until_array                 │
  │   v_temp_counter = "2"                                  │
  │   v_until_counter = "2"  → check: 2 >= 3? No → repeat  │
  │ Iteration 3: counter="2"                                │
  │   Append "Iteration-2" → v_until_array                 │
  │   v_temp_counter = "3"                                  │
  │   v_until_counter = "3"  → check: 3 >= 3? Yes → EXIT   │
  └─────────────────────────────────────────────────────────┘
     │ On Success
[Switch Environment]
  v_environment = "dev" → Dev Case → v_switch_result = "Development selected"
```

---

## Bugs in the Original Pipeline — What Was Wrong

The original pipeline had **4 bugs**. Here is each one explained with the fix.

---

### Bug 1 — Until: Self-Reference in AppendVariable (Runtime Error)

**Original code:**
```json
{
    "name": "Append Until Value",
    "type": "AppendVariable",
    "typeProperties": {
        "variableName": "untilArray",
        "value": "@concat('Iteration-', string(length(variables('untilArray'))))"
    }
}
```

**Problem:** The `value` expression reads `variables('untilArray')` — the same variable being written to by AppendVariable. ADF explicitly prohibits this: you **cannot read and write the same variable in one AppendVariable activity**. This throws a runtime error:

```
The variable 'untilArray' is being both read and set in the same activity.
```

**Fix:** Use a separate counter variable (`v_until_counter`) that tracks the iteration number independently. The append value reads from `v_until_counter` (a different variable), not from `untilArray` itself:

```json
{
    "name": "Append Until Value",
    "type": "AppendVariable",
    "typeProperties": {
        "variableName": "v_until_array",
        "value": "@concat('Iteration-', variables('v_until_counter'))"
    }
}
```

---

### Bug 2 — Until: No Counter Increment (Infinite Loop)

**Original pipeline:** The Until loop had only one inner activity (`Append Until Value`). There was nothing that changed `untilArray` length beyond appending — and since Bug 1 made the append fail, the loop would never terminate even if Bug 1 were fixed differently.

More fundamentally: **the original pipeline had no mechanism to increment any counter**. The Until condition `@equals(length(variables('untilArray')), 3)` required `untilArray` to reach length 3, but inside the loop there was only one `AppendVariable` that crashed due to Bug 1. Even if the crash were somehow bypassed, the Until would loop forever because nothing incremented a counter safely.

**Fix:** Add a dedicated counter variable (`v_until_counter`) and two SetVariable activities inside the loop to safely increment it using the two-variable workaround:

```
Append Until Value          → appends "Iteration-<counter>" to v_until_array
     │
Set Temp Counter            → v_temp_counter = counter + 1
     │
Increment Counter           → v_until_counter = v_temp_counter
```

The two-step increment is required because ADF also prohibits `v_until_counter = add(variables('v_until_counter'), 1)` (self-reference in SetVariable).

---

### Bug 3 — Until Condition: `equals` Instead of `greaterOrEquals`

**Original condition:**
```
@equals(length(variables('untilArray')), 3)
```

**Problem:** `equals(..., 3)` is fragile — it only stops if the length is **exactly** 3. If due to a bug or race condition the array somehow skips from length 2 to length 4, the condition never becomes true and the loop runs until timeout (1 hour in this pipeline).

**Fix:** Use `greaterOrEquals` which stops as soon as the counter reaches **or exceeds** 3 — much safer:

```
@greaterOrEquals(int(variables('v_until_counter')), 3)
```

This guarantees the loop always terminates even if the counter overshoots.

---

### Bug 4 — Variable Names Without Prefix Convention

**Original names:** `runid`, `itemsArray`, `untilArray`, `environment`, `switchResult`

**Problem:** Not a runtime error, but an ADF best-practice issue. In production pipelines with many variables, unprefixed names make it impossible to distinguish variables from parameters at a glance, especially in complex dynamic content expressions.

**Fix:** All variable names are prefixed with `v_` and use snake_case:

| Original | Fixed |
|---|---|
| `runid` | `v_run_id` |
| `itemsArray` | `v_items_array` |
| `untilArray` | `v_until_array` |
| `environment` | `v_environment` |
| `switchResult` | `v_switch_result` |
| *(missing)* | `v_until_counter` |
| *(missing)* | `v_temp_counter` |

---

## Variables Declared in the Corrected Pipeline

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `v_run_id` | String | — | Stores `@pipeline().RunId` |
| `v_items_array` | Array | `[]` | Collects filtered items via ForEach |
| `v_until_array` | Array | `[]` | Collects iteration labels via Until |
| `v_until_counter` | String | `"0"` | Counter for the Until loop |
| `v_temp_counter` | String | `"0"` | Temp variable for safe counter increment |
| `v_environment` | String | `"dev"` | Drives Switch routing |
| `v_switch_result` | String | — | Stores the Switch output message |

---

## Step-by-Step: Create the Pipeline in ADF Studio

### Prerequisites
- Access to `adf-datalake-dev-ded` → Author view
- No datasets or linked services needed — this pipeline uses only in-memory variables

---

### Step 1 — Create the Pipeline

1. **Author** → **Pipelines** → **+** → **New pipeline**
2. Name: `pl_demo_pipeline`
3. Click empty canvas area → **Variables** tab → add all 7 variables:

   | Name | Type | Default value |
   |---|---|---|
   | `v_run_id` | String | *(empty)* |
   | `v_items_array` | Array | `[]` |
   | `v_until_array` | Array | `[]` |
   | `v_until_counter` | String | `0` |
   | `v_temp_counter` | String | `0` |
   | `v_environment` | String | `dev` |
   | `v_switch_result` | String | *(empty)* |

---

### Step 2 — Add Set Variable (Capture Run ID)

1. Activities panel → **General** → drag **Set variable** → rename to `Set Run ID`
2. **Settings tab:**
   - Variable: `v_run_id`
   - Value → **Add dynamic content**: `@pipeline().RunId`
3. This will be the first activity (no dependency arrows coming in)

---

### Step 3 — Add Filter Activity

1. Drag **Filter** → rename to `Filter Items`
2. **Wire it:** `Set Run ID` → `Filter Items` (On Success)
3. **Settings tab:**
   - **Items** → **Add dynamic content**:
     ```
     @createArray('Apple','Banana','Mango','Orange')
     ```
   - **Condition** → **Add dynamic content**:
     ```
     @not(equals(item(), 'Banana'))
     ```

**What this does:** Creates an inline array of 4 fruits and keeps all items where the item is NOT equal to `'Banana'`. Output: `["Apple","Mango","Orange"]`.

---

### Step 4 — Add ForEach Activity

1. Drag **ForEach** → rename to `ForEach Item`
2. **Wire it:** `Filter Items` → `ForEach Item` (On Success)
3. **Settings tab:**
   - **Items** → **Add dynamic content**:
     ```
     @activity('Filter Items').output.value
     ```
   - **Is Sequential:** `true` (process one item at a time)

4. Click the **pencil icon** on `ForEach Item` to open inner canvas
5. Drag **Append Variable** → rename to `Append Item`
6. **Settings:**
   - Variable: `v_items_array`
   - Value → **Add dynamic content**: `@item()`
7. Click the pipeline canvas background to exit inner canvas

**What this does:** Loops over `["Apple","Mango","Orange"]` one by one. Each iteration appends the current fruit string to `v_items_array`. After the loop: `v_items_array = ["Apple","Mango","Orange"]`.

---

### Step 5 — Add Init Until Counter

1. Drag **Set variable** → rename to `Init Until Counter`
2. **Wire it:** `ForEach Item` → `Init Until Counter` (On Success)
3. **Settings tab:**
   - Variable: `v_until_counter`
   - Value: `0`

**Why this step?** Always reset the counter before an Until loop. If the pipeline ever re-runs or the loop is inside a larger ForEach, a stale counter value would cause the loop to skip entirely or never run.

---

### Step 6 — Add Until Activity

1. Drag **Until** → rename to `Until 3 Items`
2. **Wire it:** `Init Until Counter` → `Until 3 Items` (On Success)
3. **Settings tab:**
   - **Expression** → **Add dynamic content**:
     ```
     @greaterOrEquals(int(variables('v_until_counter')), 3)
     ```
   - **Timeout:** `0.01:00:00` (1 hour safety cap)

4. Click the **pencil icon** on `Until 3 Items` to open inner canvas

**Add 3 activities inside the Until in this exact order:**

**Activity A — Append Until Value:**
- Drag **Append Variable** → rename to `Append Until Value`
- Variable: `v_until_array`
- Value → **Add dynamic content**:
  ```
  @concat('Iteration-', variables('v_until_counter'))
  ```
- No dependency (first activity in inner canvas)

**Activity B — Set Temp Counter:**
- Drag **Set variable** → rename to `Set Temp Counter`
- **Wire it:** `Append Until Value` → `Set Temp Counter` (On Success)
- Variable: `v_temp_counter`
- Value → **Add dynamic content**:
  ```
  @string(add(int(variables('v_until_counter')), 1))
  ```

**Activity C — Increment Counter:**
- Drag **Set variable** → rename to `Increment Counter`
- **Wire it:** `Set Temp Counter` → `Increment Counter` (On Success)
- Variable: `v_until_counter`
- Value → **Add dynamic content**:
  ```
  @variables('v_temp_counter')
  ```

5. Click the pipeline canvas background to exit inner canvas

**Why three activities?**
- `Append Until Value` appends `"Iteration-0"`, `"Iteration-1"`, `"Iteration-2"` using the current counter value (reads `v_until_counter`, writes to `v_until_array` — **different variables**, allowed)
- `Set Temp Counter` computes `counter + 1` and writes to `v_temp_counter` (reads `v_until_counter`, writes `v_temp_counter` — **different variables**, allowed)
- `Increment Counter` copies `v_temp_counter` into `v_until_counter` (reads `v_temp_counter`, writes `v_until_counter` — **different variables**, allowed)

**Why not do `v_until_counter = add(variables('v_until_counter'), 1)` directly?**
ADF throws a runtime error — you cannot read and write the same variable in a single SetVariable activity. The two-variable pattern is the standard workaround.

---

### Step 7 — Add Switch Activity

1. Drag **Switch** → rename to `Switch Environment`
2. **Wire it:** `Until 3 Items` → `Switch Environment` (On Success)
3. **Settings tab:**
   - **On** → **Add dynamic content**: `@variables('v_environment')`

4. Click **+ Add case** twice:

**Case `dev`:**
- Click pencil on `dev` → inner canvas
- Drag **Set variable** → rename to `Dev Case`
- Variable: `v_switch_result`
- Value: `Development selected`

**Case `prod`:**
- Click pencil on `prod` → inner canvas
- Drag **Set variable** → rename to `Prod Case`
- Variable: `v_switch_result`
- Value: `Production selected`

**Default:**
- Click pencil on Default → inner canvas
- Drag **Set variable** → rename to `Default Case`
- Variable: `v_switch_result`
- Value: `Unknown environment`

5. **Publish all**

---

### Step 8 — Run and Verify

1. Click **Debug** (top toolbar) — no parameters needed
2. Monitor → watch each activity run in sequence:

```
Set Run ID          Succeeded   ~0s
Filter Items        Succeeded   ~0s
ForEach Item        Succeeded   ~1s
  └─ Append Item (Apple)   Succeeded
  └─ Append Item (Mango)   Succeeded
  └─ Append Item (Orange)  Succeeded
Init Until Counter  Succeeded   ~0s
Until 3 Items       Succeeded   ~1s
  └─ Iteration 1:  Append Until Value → Set Temp Counter → Increment Counter
  └─ Iteration 2:  Append Until Value → Set Temp Counter → Increment Counter
  └─ Iteration 3:  Append Until Value → Set Temp Counter → Increment Counter
Switch Environment  Succeeded   ~0s
  └─ Dev Case      Succeeded
```

**Verify each activity output in Monitor:**

| Activity | Click → Output tab | Expected value |
|---|---|---|
| Set Run ID | Output | `{ "variableName": "v_run_id", "value": "<guid>" }` |
| Filter Items | Output | `{ "value": ["Apple","Mango","Orange"], "filterCount": 3 }` |
| ForEach Item | — | 3 child iterations listed |
| Until 3 Items | — | 3 iterations listed |
| Switch Environment | — | `Dev Case` inner activity ran |

**To test other Switch branches:**
- Change `v_environment` default value to `prod` → Publish → Debug → `Prod Case` runs
- Change to `staging` → Debug → `Default Case` runs → `v_switch_result = "Unknown environment"`

---

## Corrected Pipeline JSON

The full corrected JSON is in `pl_demo_pipeline.json` in this folder. Key differences from the original:

```
Original                          →  Corrected
─────────────────────────────────────────────────────────────────────
Bug 1: AppendVariable reads       →  AppendVariable reads v_until_counter
       untilArray (self-ref)             (different variable — allowed)

Bug 2: No counter increment       →  Added Set Temp Counter + Increment
       → infinite loop                   Counter activities in Until body

Bug 3: @equals(length(...), 3)    →  @greaterOrEquals(int(...), 3)
       → fragile, loops forever          → safe, always terminates
       if count skips 3

Bug 4: No variable name prefix    →  All variables prefixed v_,
       → hard to read in                 snake_case convention
         expressions

Missing: Init counter before      →  Added "Init Until Counter"
         Until loop                      SetVariable activity
```

### Import the JSON into ADF

1. **Author** → **Pipelines** → **+** → **Import from pipeline template** (or use the `{}` code view)
2. In ADF Studio, click any existing pipeline → top-right **{}** icon (code view) → paste the JSON
3. Or: create the pipeline manually following the steps above — both produce the same result

---

## Expression Reference — All Expressions Used

| Activity | Expression | What it does |
|---|---|---|
| Set Run ID | `@pipeline().RunId` | Gets the unique GUID for this pipeline run |
| Filter Items — Items | `@createArray('Apple','Banana','Mango','Orange')` | Creates an inline string array |
| Filter Items — Condition | `@not(equals(item(), 'Banana'))` | Returns true for all items except Banana |
| ForEach — Items | `@activity('Filter Items').output.value` | Reads the filtered array |
| ForEach — Value | `@item()` | Current element in the loop |
| Until — Expression | `@greaterOrEquals(int(variables('v_until_counter')), 3)` | Stop when counter ≥ 3 |
| Append Until Value | `@concat('Iteration-', variables('v_until_counter'))` | Builds "Iteration-0", "Iteration-1", etc. |
| Set Temp Counter | `@string(add(int(variables('v_until_counter')), 1))` | Safely computes counter + 1 |
| Increment Counter | `@variables('v_temp_counter')` | Copies temp value to real counter |
| Switch — On | `@variables('v_environment')` | Reads environment variable for routing |
