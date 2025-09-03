# Data Cleaning & Validation with Versioning

This README describes the workflow, folder structure, and steps for schema validation, cleaning, and versioning in **Data_Cleaning/MKM_Data_Validation_and_cleaning**.

---

## 📂 Folder Structure

```
Data_cleaning/
│
├── MKM_Data_Validation_and_cleaning/
│   ├── cleaners/                     # Cleaning scripts
│   ├── validators/                   # Pre & post validation scripts
│   │   ├── pre_validations/
│   │   │   └── users_pre_validate.py
│   │   ├── post_validations/
│   │   └── validators_common/        # Shared checks
│   ├── metadata/
│   │   └── expected_schemas/         # Table schema YAMLs (latest + versions)
│   ├── reports/
│   │   ├── profiling/
│   │   ├── validation_reports/
│   │   │   ├── pre_cleaning/
│   │   │   └── post_cleaning/
│   │   └── cleaning/
│   ├── config/
│   │   └── master_schema_cleaning_rules.yaml
│   ├── utilities/
│   │   ├── explore_and_enrich_schema.py
│   │   ├── generate_master_cleaning.py
│   │   └── generate_expected_schemas.py
│   └── controller/
│       ├── run_cleaning_pipeline.py
│       ├── run_pre_validations.py
│       ├── run_post_validations.py
│       └── __init__.py
```

---

## ⚙️ Workflow Steps

### **1. Generate Schema Summary**
```bash
python Data_cleaning/MKM_Data_Validation_and_cleaning/utilities/explore_and_enrich_schema.py
```
- Produces: `reports/latest_mysql_schema_summary.json`

---

### **2. Generate Expected Schemas**
```bash
python Data_cleaning/MKM_Data_Validation_and_cleaning/utilities/generate_expected_schemas.py --mode write --versioned --retain 5
```
- Creates YAMLs under `metadata/expected_schemas/latest/`
- Snapshots under `metadata/expected_schemas/versions/`

---

### **3. Generate Master Cleaning Rules**
```bash
python Data_cleaning/MKM_Data_Validation_and_cleaning/utilities/generate_master_cleaning.py
```
- Output: `config/master_schema_cleaning_rules.yaml`

---

### **4. Run Pre-Validations**
```bash
python Data_cleaning/MKM_Data_Validation_and_cleaning/validators/pre_validations/users_pre_validate.py
```
- Uses schema YAMLs + `validators_common/validation_checks.py`
- Produces JSON under `reports/validation_reports/pre_cleaning/`

---

### **5. Run Cleaning Pipeline**
```bash
python Data_cleaning/MKM_Data_Validation_and_cleaning/controller/run_cleaning_pipeline.py
```
- Applies transformations per `master_schema_cleaning_rules.yaml`

---

### **6. Run Post-Validations**
```bash
python Data_cleaning/MKM_Data_Validation_and_cleaning/controller/run_post_validations.py
```
- Validates cleaned data
- Reports to `reports/validation_reports/post_cleaning/`

---

## 🛠 Dependencies
- `src/utils/config_loader.py` → loads `.env`
- `src/connections/db_connections.py` → JDBC Spark session
- `src/utils/path_utils.py` → report file paths

---

## 📊 Validation Checks
Located in `validators_common/validation_checks.py`
- `check_not_null`
- `check_unique`
- `check_numeric_range`
- `check_format_alphanumeric`
- `check_timestamp_castable`

---

## 📂 Versioning Strategy
- `latest/` → always overwritten, used in pipeline
- `versions/` → keeps timestamped snapshots (optional for audit)
- Git tracks schema drift, `.bak` auto-saves before overwriting

---

## ✅ End Goal
By following this workflow:
1. Schema drifts are detected and versioned
2. Cleaning is rule-driven (YAML-controlled)
3. Pre & post validations ensure consistency
4. Data is **ready for transformation pipelines**
