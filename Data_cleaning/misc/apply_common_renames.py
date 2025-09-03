import os
import yaml

def apply_column_renames(df, table_name, rename_config_path):
    """
    Renames columns in a Spark DataFrame based on a YAML config.

    Args:
        df (DataFrame): Spark DataFrame to rename.
        table_name (str): Table name key to look up in YAML.
        rename_config_path (str): Path to the YAML file.

    Returns:
        DataFrame: Updated DataFrame with renamed columns (if any).
    """
    if not os.path.exists(rename_config_path):
        raise FileNotFoundError(f"Rename config file not found: {rename_config_path}")

    with open(rename_config_path, "r") as f:
        rename_map = yaml.safe_load(f)

    if table_name not in rename_map:
        # No rename rules for this table — return as is
        return df

    table_renames = rename_map[table_name]

    for old_col, new_col in table_renames.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    return df
