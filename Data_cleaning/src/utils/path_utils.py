# src/utils/path_utils.py

import os

def get_project_root():
    """
    Walk upward from this file until a folder containing .env is found.
    Falls back to two-levels-up if not found (old behavior).
    """
    current = os.path.abspath(os.path.dirname(__file__))
    for _ in range(10):
        if os.path.exists(os.path.join(current, ".env")):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    # fallback: preserve your old assumption
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# def get_project_root():
#     return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# def get_local_output_path(folder_name: str = "profiling_reports", sub_dir: str = "") -> str:
#     """
#     Returns an absolute output path like: MKM_Data_Profiling/profiling_reports/profiling/
#     """
#     base = os.path.abspath(
#         os.path.join(os.path.dirname(__file__), "..", "..", "MKM_Data_Profiling", folder_name)
#     )
#     final_path = os.path.join(base, sub_dir) if sub_dir else base
#     os.makedirs(final_path, exist_ok=True)
#     return final_path

def get_local_output_path(folder_name: str = "profiling_reports", sub_dir: str = "") -> str:
    """
    Returns an absolute output *path* under MKM_Data_Profiling/<folder_name>/<sub_dir>.
    If final path looks like a file path, only ensure its parent exists.
    """
    root = get_project_root()
    base = os.path.join(root, "MKM_Data_Profiling", folder_name)
    final_path = os.path.join(base, sub_dir) if sub_dir else base

    # Create parent dir only (works for file or subdir notation)
    parent = os.path.dirname(final_path) if os.path.splitext(final_path)[1] else final_path
    os.makedirs(parent, exist_ok=True)
    return final_path

# def get_local_output_path(folder_name: str = "profiling_reports", sub_dir: str = "") -> str:
#     """
#     Returns an absolute output file path like:
#     MKM_Data_Profiling/profiling_reports/profiling/orders_profile.json
#     """
#     base = os.path.abspath(
#         os.path.join(os.path.dirname(__file__), "..", "..", "MKM_Data_Profiling", folder_name)
#     )
#     final_path = os.path.join(base, sub_dir) if sub_dir else base

#     # ✅ Only make parent folder, not the full path (if it's a file)
#     os.makedirs(os.path.dirname(final_path), exist_ok=True)

#     return final_path


def cleaning_output_paths(table_name: str, file_format: str = "csv") -> str:
    """
    Returns a path like:
    MKM_Data_Validation_and_cleaning/cleaned_outputs/users_cleaned.csv
    """
    root = get_project_root()
    folder = os.path.join(root, "Data_cleaning", "MKM_Data_Validation_and_cleaning", "reports", "cleaned_outputs")
    os.makedirs(folder, exist_ok=True)

    full_path = os.path.join(folder, f"{table_name}_cleaned.{file_format}")
    return full_path

def get_validation_report_path(stage: str, filename: str) -> str:
    """
    stage: 'pre_cleaning' or 'post_cleaning'
    filename: the JSON filename like 'cart_item_validation_pre.json'

    Returns full path to:
    MKM_Data_Validation_and_cleaning/reports/validation_reports/{stage}/{filename}
    """
    root = get_project_root()
    folder = os.path.join(root, "MKM_Data_Validation_and_cleaning", "reports", "validation_reports", stage)
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, filename)
