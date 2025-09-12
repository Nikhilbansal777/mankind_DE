# MKM Cleaning — How to Run (All the Useful Ways)

This doc explains **every practical way** to run the MKM cleaning jobs — from a single command to full orchestration — so teammates can pick what fits their workflow.

> TL;DR: **Recommended** = run the *orchestrator* (`clean_all.py`). For one-click runs on Windows, use **`mkm_clean_runner.ps1`**. For scheduled pipelines, use **Airflow** (sample DAG provided).


---

## 0) Prereqs (one-time)

- **Activate venv**: `Data_cleaning/mankind_env`  
- **Spark**: 3.2.4 (Python ≤ 3.10; Java 8 or 11 works best)  
- **Windows only**: Set up Hadoop native shims (winutils & hadoop.dll).  
  See: `Data_cleaning/README_Windows_Spark_Hadoop.md`

- **.env**: present under `Data_cleaning/.env` with DB_URL, DB_USERNAME, DB_PASSWORD, etc.


---

## 1) Orchestrator (recommended)

**Script**: `Data_cleaning/MKM_Data_Validation_and_cleaning/cleaners/clean_all.py`  
This is the *one place* to run everything or a subset. Internally it calls `run_cleaning_common.py` per table.

```powershell
# Clean everything in the default set
python Data_cleaning\MKM_Data_Validation_and_cleaning\cleaners\clean_all.py

# Clean a subset
python Data_cleaning\...\clean_all.py --tables users,products

# Skip one
python Data_cleaning\...\clean_all.py --exclude order_status_history

# Choose output format
python Data_cleaning\...\clean_all.py --format parquet

# Stop on first error (default continues)
python Data_cleaning\...\clean_all.py --stop-on-error
```

**Default tables** handled by the orchestrator:

```
users, products,
orders, order_items, order_payments, order_status_history,
payment,
cart_item, wishlist
```

**Outputs land in**:  
`Data_cleaning\MKM_Data_Validation_and_cleaning\reports\cleaned_outputs\`  
- `<table>_cleaned.<ext>`
- `<table>_cleaned.<ext>.cleaning_audit.json`


---

## 2) Per-table scripts (optional convenience)

If you prefer quick, focused runs, each table has a tiny wrapper like:

- `Data_cleaning/MKM_Data_Validation_and_cleaning/cleaners/clean_users.py`
- `clean_products.py`, `clean_orders.py`, …

Run one:
```powershell
python Data_cleaning\MKM_Data_Validation_and_cleaning\cleaners\clean_users.py
```

> These wrappers simply call `run_clean_table("users")`. You can keep them for convenience or rely only on the orchestrator.


---

## 3) One-click PowerShell runner (Windows)

We ship a ready-made runner: **`mkm_clean_runner.ps1`** (place it at repo root).  
Download provided by the team. Usage:

```powershell
# everything
.\mkm_clean_runner.ps1

# subset
.\mkm_clean_runner.ps1 -Tables users,products

# skip one
.\mkm_clean_runner.ps1 -Exclude order_status_history

# change format
.\mkm_clean_runner.ps1 -Format parquet

# stop on first error
.\mkm_clean_runner.ps1 -StopOnError
```

> Tip: The script auto-uses your venv python if found at `Data_cleaning\mankind_env\Scripts\python.exe`. You can also uncomment the `HADOOP_HOME` lines inside the script to set Hadoop vars automatically.


---

## 4) VS Code “Run and Debug” (quality-of-life)

Add this file as `.vscode/launch.json` at repo root (download provided).  
Then **Run → Run and Debug** in VS Code and pick a config (all / subset / CSV-parquet, etc.). No typing — just click ▶️.


---

## 5) Run as a Python module (optional)

If you prefer `-m` style (and you’re running from repo root):

```powershell
python -m Data_cleaning.MKM_Data_Validation_and_cleaning.cleaners.clean_all --tables users,products
```

> Note: module runs work cleanly if you use the repo-root bootstrap header in entry scripts (already added), or if you later package the repo (see next).


---

## 6) Packaged CLI (next sprint)

If/when we package and do an editable install (`pyproject.toml` placed under `Data_cleaning/`):

```powershell
# from: Data_cleaning/
pip install -e .

# now you get CLIs
mkm-clean                  # same as clean_all.py default run
mkm-clean --tables users   # subset
mkm-clean --format parquet # change format
```

> This is the long-term, team-friendly approach (no path hacks, neat CLIs). We’ve pinned `pyspark==3.2.4` in `pyproject.toml`.


---

## 7) Airflow (production orchestration)

We provide a sample DAG: **`airflow_dag_mkm_clean.py`** (download provided).

**How to use (typical)**
- Run Airflow in **Linux** (Docker/WSL2).  
- Mount this repo into the Airflow worker at `/opt/mkm_repo`.  
- Ensure a Python venv inside the container at `/opt/mkm_repo/Data_cleaning/mankind_env/` (or change the path in the DAG).  
- Set env inside the task (HADOOP_HOME, PATH, HADOOP_TMP_DIR).

**What it does**
- Task 1: `users,products` → Parquet
- Task 2: orders family (`orders, order_items, order_payments, order_status_history`)
- Task 3: `cart_item, wishlist`
- Scheduled daily at 02:00 (cron: `0 2 * * *`)

> Airflow is ideal when you need retries, SLAs, email alerts, and dependency graphs.


---

## 8) dbt or other orchestrators?

- **dbt**: best for **SQL transforms** after data is landed in the warehouse (e.g., Redshift). You can run our Spark cleaners *before* dbt in Airflow/Prefect, and then chain a `dbt run` task.
- **Prefect/Dagster**: nice Python-native orchestrators. If needed, we can add a Prefect flow that calls `clean_all.py` with parameters — similar to the Airflow DAG but easier to run locally.


---

## 9) Troubleshooting (quick)

- **Windows: `NativeIO$Windows.access0` / write failures**
  → Follow `README_Windows_Spark_Hadoop.md` (winutils + hadoop.dll + env vars).

- **Output path shows `Data_cleaning\Data_cleaning` twice**
  → We adjusted `src/utils/path_utils.py` so it builds paths as:  
  `Data_cleaning\MKM_Data_Validation_and_cleaning\reports\...` (no duplicate segment).

- **Schema notices** during pre-validate (`Unresolved columns`)
  → Add missing columns to expected YAML (`expected_schemas/latest`) or relax checks for new columns (merge mode).


---

## 10) File map (for reference)

```
Data_cleaning/
  MKM_Data_Validation_and_cleaning/
    cleaners/
      run_cleaning_common.py     # shared logic
      clean_all.py               # orchestrator (primary entry)
      clean_users.py             # optional tiny wrappers (per-table)
      clean_products.py
      ...

    validators/
      pre_validations/
        run_prevalidate_common.py
        users_pre_validate.py     # same pattern exists for all tables
        ...

  README_Windows_Spark_Hadoop.md  # Windows-specific Spark/Hadoop setup
```

---

## 11) Examples (copy/paste)

```powershell
# Everything (JSON)
python Data_cleaning\MKM_Data_Validation_and_cleaning\cleaners\clean_all.py

# Subset (Parquet)
python Data_cleaning\MKM_Data_Validation_and_cleaning\cleaners\clean_all.py --tables users,products --format parquet

# Skip one (CSV)
python Data_cleaning\MKM_Data_Validation_and_cleaning\cleaners\clean_all.py --exclude order_status_history --format csv

# One-click runner (PowerShell)
.\mkm_clean_runner.ps1 -Tables users,products -Format parquet

# Airflow task (inside Linux/WSL worker)
python /opt/mkm_repo/Data_cleaning/MKM_Data_Validation_and_cleaning/cleaners/clean_all.py --tables cart_item,wishlist --format parquet
```
