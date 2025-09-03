# 📊 MKM Data Profiling

This folder contains profiling scripts for each table in the MKA database (e.g., `users`, `products`, `wishlist`, etc.).

The goal is to analyze nulls, distinct counts, frequent values, basic statistics, and data types before performing any validation or cleaning.

---

## 📁 Folder Structure

```
MKM_Data_Profiling/
├── profilers_common/            # Reusable profiling logic
├── profiling_reports/profiling/ # Output folder for profiling JSON files
├── users_profile.py             # Per-table controller (repeatable)
├── products_profile.py          # Another per-table controller
└── ...
```

---

## 🔁 Sample Reusable Script: `users_profile.py`

Use this template for profiling any table. Just change the table name and paths as needed.

```python
# ✅ File: MKM_Data_Profiling/users_profile.py

from profilers_common.run_common_profilers import run_all_profilers
from src.utils.db_connection import spark_session_for_JDBC
from src.utils.project_bootstrap import get_project_root
import os
from dotenv import load_dotenv

# Load .env for JDBC configs
load_dotenv()
project_root = get_project_root()

# Step 1: Start Spark session
spark = spark_session_for_JDBC()

# Step 2: Table to Profile
table_name = "users"   # 🔄 Change this to your table name

# Step 3: Read Data from MySQL
df = spark.read \
    .format("jdbc") \
    .option("url", os.getenv("DB_URL")) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", table_name) \
    .option("user", os.getenv("DB_USERNAME")) \
    .option("password", os.getenv("DB_PASSWORD")) \
    .load()

# Step 4: Run Common Profilers
output_path = os.path.join(project_root, "MKM_Data_Profiling", "profiling_reports", "profiling", f"{table_name}_profile.json")
run_all_profilers(df, table_name, output_path)
```

---

## 🧠 Notes

- You can copy this script for any table: `products_profile.py`, `wishlist_profile.py`, etc.
- Only the `table_name` and possibly the output filename need to be updated.
- The script is fully modular and uses Spark to connect via JDBC to the MySQL database.

---

## 📤 Output

Each script produces a JSON report here:
```
MKM_Data_Profiling/profiling_reports/profiling/<table_name>_profile.json
```
You can use this in downstream schema or cleaning rule generation.
