# 🧰 MKA Data Engineering - Developer Onboarding Guide

Welcome to the **Mankind Matrix E-commerce** Data Engineering project!

This repository follows a structured, modular architecture where every major data task (profiling, validation, cleaning, transformation) is split per table using repeatable, DRY-style scripts.

---

## 📁 Folder Overview

| Folder                               | Purpose                                        |
|--------------------------------------|------------------------------------------------|
| `MKM_Data_Profiling/`                | Profiling raw tables (nulls, types, stats)     |
| `MKM_Data_Validation_and_cleaning/`  | Validating and cleaning tables using YAML      |
| `MKM_Glue_tranformations/`           | Transforming and joining cleaned datasets      |

---

## 📊 1. Profiling (Before Cleaning)

Use this pattern for each table: `users_profile.py`, `products_profile.py`, etc.

```python
# File: MKM_Data_Profiling/users_profile.py

from profilers_common.run_common_profilers import run_all_profilers
from src.utils.db_connection import spark_session_for_JDBC
from src.utils.project_bootstrap import get_project_root
import os
from dotenv import load_dotenv

load_dotenv()
spark = spark_session_for_JDBC()
project_root = get_project_root()
table_name = "users"  # Change this for your table

df = spark.read.format("jdbc") \
    .option("url", os.getenv("DB_URL")) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", table_name) \
    .option("user", os.getenv("DB_USERNAME")) \
    .option("password", os.getenv("DB_PASSWORD")) \
    .load()

output_path = os.path.join(project_root, "MKM_Data_Profiling", "profiling_reports", "profiling", f"{table_name}_profile.json")
run_all_profilers(df, table_name, output_path)
```

---

## 🧪 2. Pre-Cleaning Validation (Optional)

```python
# File: validators/validate_users_pre.py

from validators.validators_common.validation_checks import run_basic_schema_checks
from src.utils.db_connection import spark_session_for_JDBC
from src.utils.project_bootstrap import get_project_root
import os
from dotenv import load_dotenv

load_dotenv()
spark = spark_session_for_JDBC()
table_name = "users"

df = spark.read.format("jdbc") \
    .option("url", os.getenv("DB_URL")) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", table_name) \
    .option("user", os.getenv("DB_USERNAME")) \
    .option("password", os.getenv("DB_PASSWORD")) \
    .load()

run_basic_schema_checks(df, table_name)
```

---

## 🧹 3. Cleaning Script

```python
# File: cleaners/clean_users.py

from src.utils.cleaning_rules import clean_dataframe_with_rules, load_cleaning_config
from src.utils.db_connection import spark_session_for_JDBC
from src.utils.project_bootstrap import get_project_root
import os
from dotenv import load_dotenv

load_dotenv()
spark = spark_session_for_JDBC()
project_root = get_project_root()
table_name = "users"

rules = load_cleaning_config(table_name)

df = spark.read.format("jdbc") \
    .option("url", os.getenv("DB_URL")) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", table_name) \
    .option("user", os.getenv("DB_USERNAME")) \
    .option("password", os.getenv("DB_PASSWORD")) \
    .load()

cleaned_df = clean_dataframe_with_rules(df, rules)

output_path = os.path.join(project_root, "MKM_Data_Validation_and_cleaning", "cleaned_outputs", f"{table_name}_cleaned.json")
cleaned_df.write.mode("overwrite").json(output_path)
```

---

## ✅ 4. Post-Cleaning Validation

```python
# File: validators/validate_users_post.py

from validators.validators_common.validation_checks import run_post_clean_validations
from src.utils.db_connection import spark_session_for_JDBC
from src.utils.project_bootstrap import get_project_root
import os
from dotenv import load_dotenv

load_dotenv()
spark = spark_session_for_JDBC()
project_root = get_project_root()
table_name = "users"

input_path = os.path.join(project_root, "MKM_Data_Validation_and_cleaning", "cleaned_outputs", f"{table_name}_cleaned.json")
df = spark.read.json(input_path)

run_post_clean_validations(df, table_name)
```

---

## 🔄 5. Transformations (Optional Per Join/Stage)

```python
# File: MKM_Glue_tranformations/src/jobs/join_enrich_transform.py

# Sample logic for joining users and products — customize as needed
from src.utils.spark_helpers import enrich_and_join
from src.utils.project_bootstrap import get_project_root
from src.utils.db_connection import spark_session_for_JDBC
import os

spark = spark_session_for_JDBC()
project_root = get_project_root()

users_df = spark.read.json(os.path.join(project_root, "MKM_Data_Validation_and_cleaning", "cleaned_outputs", "users_cleaned.json"))
products_df = spark.read.json(os.path.join(project_root, "MKM_Data_Validation_and_cleaning", "cleaned_outputs", "products_cleaned.json"))

final_df = enrich_and_join(users_df, products_df)

output_path = os.path.join(project_root, "MKM_Glue_tranformations", "output", "users_products_enriched.json")
final_df.write.mode("overwrite").json(output_path)
```

---

## 🎯 Summary

| Phase           | Reusable Script Template             |
|----------------|---------------------------------------|
| Profiling       | `table_profile.py` using `run_all_profilers()` |
| Pre-Validation  | `validate_<table>_pre.py`            |
| Cleaning        | `clean_<table>.py`                   |
| Post-Validation | `validate_<table>_post.py`           |
| Transformation  | `join_enrich_transform.py`           |

Just copy-paste these and change the table name to create new pipelines fast!

Happy coding! 🚀
