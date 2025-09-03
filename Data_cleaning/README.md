
# MKA Data Engineering Project – Profiling, Cleaning, and Transformation

## Overview
This project focuses on building a modular, scalable, and team-friendly data engineering pipeline for the MKA e-commerce platform. 
The pipeline covers **data profiling**, **data cleaning**, **validation**, and **transformations** for multiple tables in a MySQL RDS instance.

Our aim is to maintain **reusable code**, **consistent folder structure**, and **version-controlled configurations**, so that any team member can pick up where another left off without confusion.

---

## Folder Structure

```
project_root/
│
├── MKM_Data_Profiling/
│   ├── profilers_common/        # Reusable profiling scripts (null counts, value frequencies, stats, etc.)
│   ├── profiling_reports/       # JSON profiling outputs for each table
│   ├── /             # Table-specific profiling scripts (e.g., users_profile.py)
│
├── MKM_Data_Validation_and_cleaning/
│   ├── validators/              # Post-cleaning validation scripts
│   ├── cleaned_outputs/         # Final cleaned JSON/CSV outputs
│   ├── src/
│   │   ├── config/              # YAML cleaning rules
│   │   ├── utils/               # Common utility scripts (db connections, cleaning rules, renaming, etc.)
│   ├── reports/                 # Schema summaries and enriched reports
│
├── MKM_Glue_transformations/
│   ├── src/jobs/                 # Glue/PySpark transformation scripts
│
├── README.md                    # This file
```

---

## Process Flow

1. **Data Profiling Phase**
   - Run table-specific profiling scripts from `MKM_Data_Profiling/` (e.g., `users_profile.py`).
   - These use functions from `profilers_common/` to get null counts, distinct values, and type stats.
   - Output JSON profiling reports to `profiling_reports/`.

2. **Schema Discovery & Rule Generation**
   - Run `explore_and_enrich_schema.py` to create `latest_mysql_schema_summary.json`.
   - Run `generate_master_cleaning_template.py` to generate YAML rules in `src/config/master_schema_cleaning_rules.yaml`.

3. **Data Cleaning Phase**
   - Use cleaning scripts like `clean_users.py` to:
     - Apply renaming rules
     - Convert data types
     - Replace nulls
     - Standardize formats and units
   - Save cleaned output to `cleaned_outputs/`.

4. **Post-Cleaning Validation**
   - Use validators in `validators_common/` to ensure cleaned data meets expected criteria.
   - Example: compare wishlist product names against products table.

5. **Transformation Phase**
   - Use Glue/PySpark scripts from `MKM_Glue_transformations/src/jobs/` to join, enrich, and prepare data for analytics or ML pipelines.

---

## Common Utility Scripts

- **file_io.py** – For file reading/writing operations.
- **jdbc_reader.py** – For reading data from MySQL RDS via JDBC.
- **log_utils.py** – For consistent logging format across scripts.
- **spark_session.py** – For creating Spark sessions with pre-configured settings.

---

## Best Practices

- Keep configurations in YAML/JSON, not hardcoded in scripts.
- Always commit profiling reports and cleaning rules to Git for traceability.
- Use consistent naming for scripts (`<table>_profile.py`, `clean_<table>.py`).
- Run profiling before cleaning to ensure rule accuracy.
- Validate after cleaning to catch any mismatches.

---

## How to Run

1. **Activate environment**  
   ```bash
   source mankind_env/bin/activate  # Linux/Mac
   mankind_env\Scripts\activate   # Windows
   ```

2. **Run profiling**  
   ```bash
   python MKM_Data_Profiling/controllers/users_profile.py
   ```

3. **Run schema enrichment & cleaning rules generation**  
   ```bash
   python MKM_Data_Validation_and_cleaning/utilities/explore_and_enrich_schema.py
   python MKM_Data_Validation_and_cleaning/utilities/generate_master_cleaning_template.py
   ```

4. **Run cleaning**  
   ```bash
   python MKM_Data_Validation_and_cleaning/controllers/clean_users.py
   ```

5. **Run post-cleaning validation**  
   ```bash
   python MKM_Data_Validation_and_cleaning/controller/run_all_post_validations.py
   ```

---

## Authors
Team MKA Data Engineering – Maintaining data quality, one pipeline at a time.
