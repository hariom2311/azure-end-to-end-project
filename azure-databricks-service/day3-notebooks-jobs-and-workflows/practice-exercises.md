# Day 3 — Practice Exercises: Notebooks, Jobs & Workflows

> **Goal:** Explore the Databricks workspace, create notebooks with simple code, and build jobs — no external storage, no linked services, no project setup required.
> Everything runs on a fresh Databricks cluster with built-in data only.

---

## Before You Start

You need:
- An Azure Databricks workspace (any workspace, any cluster)
- A running cluster attached to your notebook (All-Purpose, any size)
- Nothing else — all exercises use inline data created inside the notebook

---

## Exercise 1 — Explore the Workspace Sidebar

**Goal:** Navigate every section of the workspace so you know what each one does.

### Steps

1. Open your Databricks workspace → look at the **left sidebar**

2. Click **Home** → note: recently opened items, quick create buttons

3. Click **Workspace** → expand **Shared** and **Users/your-email** — this is the folder tree where notebooks live

4. Click **Data** (or Catalog) → browse available databases or catalogs (may be empty in a fresh workspace — that is fine)

5. Click **Compute** → you should see your running cluster here

6. Click **Workflows** → this is where jobs will appear after Exercise 4

7. Click **Settings** (gear icon at bottom-left):
   - Go to **Developer** → **Access tokens** → **Generate new token**
   - Name: `day3-explore-token`, Lifetime: 7 days
   - Copy and save the token value (used later for CLI/API — for now just note it exists)
   - Click **Done**

8. Back in **Workspace** → right-click **Shared** → **Create** → **Folder**
   - Name the folder: `day3-practice`

**What to verify:** You can navigate all sections without errors. The `Shared/day3-practice` folder is visible in the tree.

---

## Exercise 2 — Create a Notebook and Run Basic Commands

**Goal:** Create a notebook, attach it to a cluster, and run Python and SQL cells.

### Steps

1. **Workspace** → right-click `Shared/day3-practice` → **Create** → **Notebook**
   - Name: `notebook_basics`
   - Default language: `Python`
   - Attach to: your running cluster
   - Click **Create**

2. **Cell 1** — Markdown header:
   ```python
   %md
   # My First Databricks Notebook
   Exploring basic Spark commands — no external data needed.
   ```
   Press `Shift + Enter` — the cell renders as a formatted heading.

3. **Cell 2** — Check Spark is running:
   ```python
   print("Spark version:", spark.version)
   print("Hello from Databricks!")
   ```
   Expected output: `Spark version: 3.5.x`

4. **Cell 3** — Create a small DataFrame from a Python list:
   ```python
   data = [
       (1, "Alice", 30, "Engineering"),
       (2, "Bob",   25, "Marketing"),
       (3, "Carol", 35, "Engineering"),
       (4, "David", 28, "HR"),
       (5, "Eve",   32, "Marketing"),
   ]
   columns = ["id", "name", "age", "department"]

   df = spark.createDataFrame(data, columns)
   df.show()
   ```

5. **Cell 4** — Print the schema:
   ```python
   df.printSchema()
   ```

6. **Cell 5** — Register as a temp view and query with SQL:
   ```python
   df.createOrReplaceTempView("employees")
   ```

7. **Cell 6** — SQL magic cell:
   ```sql
   %sql
   SELECT department, COUNT(*) AS headcount, AVG(age) AS avg_age
   FROM employees
   GROUP BY department
   ORDER BY headcount DESC
   ```

8. **Cell 7** — Shell magic to see the driver hostname:
   ```sh
   %sh
   echo "Driver node: $(hostname)"
   ```

9. Click **Run All** in the toolbar.

**What to verify:** All cells show a green check. The SQL cell shows a table grouped by department. No errors anywhere.

---

## Exercise 3 — Try All Magic Commands

**Goal:** Use `%md`, `%sql`, `%sh`, `%fs`, and `%pip` in separate cells.

### Steps

1. In `Shared/day3-practice`, create a new notebook: `magic_commands`

2. **Cell 1** — Markdown:
   ```python
   %md
   ## Magic Commands Demo
   Each cell below uses a different `%` prefix to switch the language or tool.
   ```

3. **Cell 2** — Python (default):
   ```python
   numbers = [1, 2, 3, 4, 5]
   total = sum(numbers)
   print("Sum:", total)
   ```

4. **Cell 3** — SQL:
   ```sql
   %sql
   SELECT 1 + 1 AS two, 'hello' AS greeting, current_date() AS today
   ```

5. **Cell 4** — Shell:
   ```sh
   %sh
   echo "Disk space on driver:"
   df -h /
   ```

6. **Cell 5** — DBFS listing:
   ```python
   %fs ls dbfs:/
   ```
   This lists the root of the Databricks File System. You will see folders like `FileStore`, `user`, `tmp`.

7. **Cell 6** — Install a library for this session:
   ```python
   %pip install faker
   ```
   Wait for it to finish, then in the next cell:

8. **Cell 7** — Use the installed library:
   ```python
   from faker import Faker
   fake = Faker()

   for _ in range(5):
       print(fake.name(), "|", fake.email(), "|", fake.city())
   ```
   This prints 5 randomly generated fake names, emails, and cities.

9. **Run All**

**What to verify:** Every cell runs. The `%fs` cell shows DBFS folders. The faker cell prints 5 rows of fake data.

---

## Exercise 4 — Use dbutils

**Goal:** Practice the most common `dbutils` functions.

### Steps

1. Create a new notebook: `dbutils_explore`

2. **Cell 1** — Write a file to DBFS:
   ```python
   dbutils.fs.put("dbfs:/tmp/day3_test.txt", "Hello from dbutils!", overwrite=True)
   print("File written.")
   ```

3. **Cell 2** — Read it back:
   ```python
   content = dbutils.fs.head("dbfs:/tmp/day3_test.txt")
   print("File content:", content)
   ```

4. **Cell 3** — List the tmp folder to confirm the file exists:
   ```python
   files = dbutils.fs.ls("dbfs:/tmp/")
   for f in files:
       print(f.name, "-", f.size, "bytes")
   ```

5. **Cell 4** — Add a widget (creates a text box at the top of the notebook):
   ```python
   dbutils.widgets.text("city", "Mumbai", "Your City")
   city = dbutils.widgets.get("city")
   print(f"Selected city: {city}")
   ```
   After running: a text box appears at the top. Change `Mumbai` to any city and re-run Cell 4 — the output changes.

6. **Cell 5** — Dropdown widget:
   ```python
   dbutils.widgets.dropdown("colour", "Blue", ["Red", "Green", "Blue", "Yellow"], "Favourite Colour")
   colour = dbutils.widgets.get("colour")
   print(f"You chose: {colour}")
   ```

7. **Cell 6** — Exit value:
   ```python
   dbutils.notebook.exit(f"Done! City={city}, Colour={colour}")
   ```
   This prints the exit string below the cell. In a job run, this value is captured in the run output.

8. **Cell 7** — Clean up the test file:
   ```python
   dbutils.fs.rm("dbfs:/tmp/day3_test.txt")
   print("File deleted.")
   ```

9. **Run All**

**What to verify:** File is written, read back, listed, then deleted. Two widget boxes appear at the top of the notebook. Exit value prints at the end.

---

## Exercise 5 — Create a Single-Task Job

**Goal:** Schedule `notebook_basics` to run automatically.

### Steps

1. **Workflows** → **+ Create job**

2. Click the name `Untitled` at the top → type: `job_notebook_basics`

3. Configure the task:

   | Field | Value |
   |---|---|
   | Task name | `run_basics` |
   | Type | `Notebook` |
   | Source | `Workspace` |
   | Path | Browse to `Shared/day3-practice/notebook_basics` |
   | Cluster | `New job cluster` |

4. Configure the job cluster (click **Edit** next to the cluster):

   | Field | Value |
   |---|---|
   | Databricks runtime | `15.4 LTS` (or latest LTS) |
   | Worker type | `Standard_D4s_v3` |
   | Workers | `1` |

   Click **Confirm**

5. Add a schedule:
   - Click **Add trigger** → Type: `Scheduled`
   - Every day at `09:00 AM`
   - Timezone: `UTC`
   - Click **Save**

6. Click **Create** (or **Save job**)

7. Click **Run now** to test immediately — do not wait for the schedule

8. Go to the **Runs** tab → wait for the run to appear → click the run row → click the task → open **Logs** tab

**What to verify:** Run shows **SUCCEEDED**. The Logs tab shows the Spark version print and the SQL department table output from all cells in `notebook_basics`.

---

## Exercise 6 — Configure Retries and Notifications

**Goal:** Add retry logic and email alerts to the job.

### Steps

1. Open `job_notebook_basics` → click **Edit**

2. Click the task `run_basics` to open task settings on the right

3. Find **Retries** → set:
   - Max retries: `2`
   - Retry interval: `2 minutes`

4. Scroll down to **Notifications** → **Add notification**:
   - Trigger: `On failure`
   - Email: your email address
   - Click **Add**

5. **Save** the job

6. To see retries in action — temporarily break the notebook:
   - Open `notebook_basics` → add a new cell at the very end:
     ```python
     raise Exception("Intentional failure to test retries")
     ```
   - Go back to the job → **Run now**
   - Watch the **Runs** tab — you will see the run fail, wait 2 min, fail again, wait 2 min, fail again → marked **FAILED** (3 total attempts)

7. **Important — remove the broken cell:**
   - Open `notebook_basics` → click the exception cell → press `DD` to delete it
   - **Run now** again → confirm it succeeds

**What to verify:** The failed run shows 3 attempts in the run history. After removing the exception cell, a fresh run succeeds.

---

## Exercise 7 — Create a Multi-Task Pipeline Job

**Goal:** Build a 3-task job where each notebook passes a result to the next.

### Steps

#### Prepare three simple notebooks

1. In `Shared/day3-practice`, create notebook: `task_one`

   Paste this content (replace any existing cells):
   ```python
   %md
   ## Task One — Generate Numbers
   ```
   ```python
   numbers = list(range(1, 11))   # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
   print(f"Task One: generated {len(numbers)} numbers")
   print("Numbers:", numbers)
   dbutils.notebook.exit("Task One done")
   ```

2. Create notebook: `task_two`
   ```python
   %md
   ## Task Two — Filter and Square
   ```
   ```python
   numbers = list(range(1, 11))
   even_numbers = [n for n in numbers if n % 2 == 0]
   squared = [n ** 2 for n in even_numbers]
   print(f"Task Two: even numbers = {even_numbers}")
   print(f"Task Two: squared = {squared}")
   dbutils.notebook.exit("Task Two done")
   ```

3. Create notebook: `task_three`
   ```python
   %md
   ## Task Three — Summary
   ```
   ```python
   numbers = list(range(1, 11))
   total = sum(numbers)
   average = total / len(numbers)
   maximum = max(numbers)
   minimum = min(numbers)

   print("Task Three: Summary Report")
   print(f"  Total   : {total}")
   print(f"  Average : {average}")
   print(f"  Max     : {maximum}")
   print(f"  Min     : {minimum}")
   dbutils.notebook.exit("Task Three done")
   ```

#### Create the job

4. **Workflows** → **+ Create job**

5. Name: `job_three_tasks`

6. **Task 1:**

   | Field | Value |
   |---|---|
   | Task name | `task_one` |
   | Type | `Notebook` |
   | Path | `Shared/day3-practice/task_one` |
   | Cluster | New job cluster, D4s_v3, 1 worker, DBR 15.4 LTS |

7. **Add Task 2:** click **+ Add task**

   | Field | Value |
   |---|---|
   | Task name | `task_two` |
   | Type | `Notebook` |
   | Path | `Shared/day3-practice/task_two` |
   | Depends on | `task_one` |
   | Cluster | New job cluster, same config |

8. **Add Task 3:** click **+ Add task**

   | Field | Value |
   |---|---|
   | Task name | `task_three` |
   | Type | `Notebook` |
   | Path | `Shared/day3-practice/task_three` |
   | Depends on | `task_two` |
   | Cluster | New job cluster, same config |

9. Verify the visual DAG: `task_one → task_two → task_three`

10. **Save job** → **Run now**

11. Monitor → **Runs** tab → click the run → watch all 3 tasks turn green one by one

12. Click each task → **Logs** tab → see each notebook's print output

**What to verify:**
- All 3 tasks show SUCCEEDED
- Each task's Logs tab shows its own printed output
- `task_two` did not start until `task_one` finished (check start times in the run detail)
- Each task ran on its own separate cluster (cluster IDs are different)

---

## Exercise 8 — Pass Parameters from Job to Notebook

**Goal:** Configure a job to inject parameters into a notebook via widgets.

### Steps

1. Create a new notebook: `param_notebook`

   ```python
   # Cell 1 — define widgets first (always before get)
   dbutils.widgets.text("name",    "World",    "Your Name")
   dbutils.widgets.text("country", "India",    "Country")
   dbutils.widgets.text("number",  "5",        "How Many Lines")
   ```

   ```python
   # Cell 2 — read the widget values
   name    = dbutils.widgets.get("name")
   country = dbutils.widgets.get("country")
   number  = int(dbutils.widgets.get("number"))

   print(f"Hello, {name} from {country}!")
   for i in range(1, number + 1):
       print(f"  Line {i} of {number}")

   dbutils.notebook.exit(f"Ran {number} lines for {name}")
   ```

2. Run the notebook manually first — change the widget values in the UI and re-run Cell 2 to see the output change.

3. **Workflows** → **+ Create job** → name: `job_with_params`

4. Task:

   | Field | Value |
   |---|---|
   | Task name | `run_param_notebook` |
   | Type | `Notebook` |
   | Path | `Shared/day3-practice/param_notebook` |
   | Cluster | New job cluster, D4s_v3, 1 worker, DBR 15.4 LTS |

5. In the task settings → **Parameters** → **Add parameter**:

   | Key | Value |
   |---|---|
   | `name` | `VoltGrid Student` |
   | `country` | `Australia` |
   | `number` | `3` |

6. **Save** → **Run now**

7. Open the run → click the task → **Logs** tab

   Expected output:
   ```
   Hello, VoltGrid Student from Australia!
     Line 1 of 3
     Line 2 of 3
     Line 3 of 3
   ```

8. Go back to the job → **Edit** → change `number` to `7` → **Save** → **Run now** — the output now shows 7 lines.

**What to verify:** The job-supplied values override the widget defaults. Changing parameters in the job config changes the notebook output without touching the notebook code.

---

## Final Summary — What You Created

```
Workspace: Shared/day3-practice/
  ├── notebook_basics       ← Exercise 2 (DataFrame, SQL magic, shell)
  ├── magic_commands        ← Exercise 3 (all magic commands + faker)
  ├── dbutils_explore       ← Exercise 4 (fs, widgets, exit)
  ├── task_one              ← Exercise 7 Task 1
  ├── task_two              ← Exercise 7 Task 2
  ├── task_three            ← Exercise 7 Task 3
  └── param_notebook        ← Exercise 8 (widgets + job parameters)

Workflows (Jobs):
  ├── job_notebook_basics   ← Exercise 5 (single task, daily schedule)
  ├── job_three_tasks       ← Exercise 7 (3-task pipeline DAG)
  └── job_with_params       ← Exercise 8 (parameter injection)
```

---

## Quick Verification Checklist

| Task | How to verify |
|---|---|
| Workspace folder created | Workspace → Shared → day3-practice folder visible |
| Notebook runs all cells | Run All → every cell shows green check |
| SQL magic works | %sql cell returns a result table |
| %pip install works | `faker` installed, fake names printed |
| dbutils.fs write/read | File created and read back from dbfs:/tmp/ |
| Widget text box appears | Text box visible at top of notebook after widget cell runs |
| Single-task job runs | Workflows → job_notebook_basics → Runs → SUCCEEDED |
| Retry config visible | Task settings → Retries = 2, interval = 2 min |
| 3-task DAG job runs | job_three_tasks → all 3 tasks SUCCEEDED in order |
| Parameters injected | job_with_params → Logs show job-supplied values, not defaults |
