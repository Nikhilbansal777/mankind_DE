import os
import yaml

def validate_schema(df, table_name, schema_folder="Data_cleaning/MKM_Data_Validation_and_cleaning/metadata/expected_schemas/latest"):
    """
    Validates the schema of the given Spark DataFrame against an expected schema defined in a YAML file.

    Parameters:
    - df: Spark DataFrame to validate.
    - table_name: Name of the table (used to locate the YAML file).
    - schema_folder: Folder path containing the schema YAML files.
    """

    schema_file = os.path.join(schema_folder, f"{table_name}_schema.yaml")

    if not os.path.exists(schema_file):
        raise FileNotFoundError(f"[ERROR] Schema file not found: {schema_file}")

    with open(schema_file, "r", encoding="utf-8") as f:
        schema = yaml.safe_load(f) or {}

    required   = schema.get("required", []) or []
    optional   = schema.get("optional", []) or []
    disallowed = schema.get("disallowed", []) or []

    df_columns = set(df.columns)

    # 1) Required present?
    missing_required = sorted([col for col in required if col not in df_columns])

    # 2) Disallowed absent?
    present_disallowed = sorted([col for col in disallowed if col in df_columns])

    # 3) Required-but-nullable? (Spark-native; no UDFs)
    #    If a required column is nullable in DF schema, flag it: it's a common upstream drift.
    nullable_by_col = {f.name: bool(f.nullable) for f in df.schema.fields}
    nullable_required = sorted([c for c in required if nullable_by_col.get(c, True)])

    # 4) New/unexpected columns (notice only, non-fatal)
    expected = set(required + optional + disallowed)
    unexpected_cols = sorted(list(df_columns - expected))

    # ---- Decide pass/fail (keep your original behavior; unexpected = notice only) ----
    errors = []
    if missing_required:
        errors.append(f"Missing required columns: {missing_required}")
    if present_disallowed:
        errors.append(f"Disallowed columns present: {present_disallowed}")
    if nullable_required:
        errors.append(f"Required columns are nullable in DF schema: {nullable_required}")

    # If you ever want strict mode, uncomment the next two lines:
    # if unexpected_cols:
    #     errors.append(f"Unexpected columns (not in required|optional|disallowed): {unexpected_cols}")

    if errors:
        # Include unexpected as a trailing hint (still non-fatal in current behavior)
        if unexpected_cols:
            errors.append(f"Notice - unexpected columns detected: {unexpected_cols}")
        raise ValueError(f"[SCHEMA ERROR] {table_name}: " + " | ".join(errors))

    if unexpected_cols:
        print(f"[SCHEMA NOTICE] New/unexpected columns detected: {unexpected_cols}")

    print(f"[SCHEMA VALIDATION] ✅ {table_name}: PASSED")
