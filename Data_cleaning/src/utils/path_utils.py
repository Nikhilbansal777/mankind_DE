# src/utils/path_utils.py


# src/utils/path_utils.py
"""
Path utilities for MKM pipelines.

✅ What goes where (authoritative):
- PROFILING JSON (profilers)        -> MKM_Data_Profiling/profiling_reports/profiling/...
- VALIDATION REPORTS (pre/post)     -> MKM_Data_Validation_and_cleaning/reports/validation_reports/<stage>/...
- CLEANED OUTPUTS (CSV/Parquet)     -> MKM_Data_Validation_and_cleaning/reports/cleaned_outputs/...
- SEMANTIC DATA (rollups/enrich)    -> transformed_outputs/semantic/<...>/...
- SEMANTIC LINEAGE (audit/metrics)  -> outputs/lineage/semantic/<...>.json

You can override the roots via .env:
  OUT_ROOT_PROFILING
  OUT_ROOT_VALIDATION
  OUT_ROOT_CLEANED
  OUT_ROOT_SEMANTIC
  OUT_ROOT_LINEAGE
"""

import os
from typing import Optional

# Optional: allow env-based overrides without importing global settings at import time
try:
    # Local import to avoid circular deps in early bootstrap
    from .config_loader import load_env_and_get
except Exception:  # pragma: no cover
    def load_env_and_get(key: str, default: Optional[str] = None):
        return os.environ.get(key, default)


# --------------------------------------------------------------------------------------
# Root resolution
# --------------------------------------------------------------------------------------
def get_project_root() -> str:
    """
    Walk upward from this file until a folder containing .env is found.
    Falls back to two-levels-up if not found (legacy behavior).
    """
    current = os.path.abspath(os.path.dirname(__file__))
    for _ in range(10):
        if os.path.exists(os.path.join(current, ".env")):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    # Fallback: preserve previous assumption
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _norm_join(*parts: str) -> str:
    """Join and normalize a path, ignoring empty segments."""
    return os.path.normpath(os.path.join(*[p for p in parts if p]))


def _ensure_dir_for(final_path: str) -> None:
    """
    Create the directory that should contain final_path.
    If final_path looks like a directory (no extension), create it;
    if it looks like a file, create its parent.
    """
    # If it has an extension, assume it's a file path
    parent = os.path.dirname(final_path) if os.path.splitext(final_path)[1] else final_path
    os.makedirs(parent, exist_ok=True)


# --------------------------------------------------------------------------------------
# Canonical roots (with .env overrides)
# --------------------------------------------------------------------------------------
def _root_profiling() -> str:
    # Default matches your current structure
    default = _norm_join(get_project_root(), "MKM_Data_Profiling", "profiling_reports", "profiling")
    return load_env_and_get("OUT_ROOT_PROFILING", default)

def _root_validation() -> str:
    default = _norm_join(get_project_root(), "MKM_Data_Validation_and_cleaning", "reports", "validation_reports")
    return load_env_and_get("OUT_ROOT_VALIDATION", default)

def _root_cleaned() -> str:
    default = _norm_join(get_project_root(), "MKM_Data_Validation_and_cleaning", "reports", "cleaned_outputs")
    return load_env_and_get("OUT_ROOT_CLEANED", default)

def _root_semantic() -> str:
    default = _norm_join(get_project_root(), "transformed_outputs", "semantic")
    return load_env_and_get("OUT_ROOT_SEMANTIC", default)

def _root_lineage() -> str:
    # Keep your historical location but allow override
    default = _norm_join(get_project_root(), "outputs", "lineage")
    return load_env_and_get("OUT_ROOT_LINEAGE", default)


# --------------------------------------------------------------------------------------
# PROFILING (Profilers only)
# --------------------------------------------------------------------------------------
def get_profiling_output_path(*subpaths: str) -> str:
    """
    Root for profiling JSON outputs.
    Example:
      out = get_profiling_output_path()  # .../MKM_Data_Profiling/profiling_reports/profiling
      out = get_profiling_output_path("orders_profile_20250910.json")
    """
    final_path = _norm_join(_root_profiling(), *subpaths)
    _ensure_dir_for(final_path)
    return final_path


# --------------------------------------------------------------------------------------
# VALIDATION (pre/post) + CLEANED OUTPUTS
# --------------------------------------------------------------------------------------
def get_validation_report_path(stage: str, filename: str) -> str:
    """
    stage: 'pre_cleaning' or 'post_cleaning'
    filename: e.g. 'orders_validation_pre.json'
    Returns:
      .../MKM_Data_Validation_and_cleaning/reports/validation_reports/<stage>/<filename>
    """
    final_path = _norm_join(_root_validation(), stage, filename)
    _ensure_dir_for(final_path)
    return final_path


def cleaning_output_paths(table_name: str, file_format: str = "csv") -> str:
    """
    Cleaned outputs (table-level artifacts).
    Example:
      path = cleaning_output_paths("users", "csv")
      # .../MKM_Data_Validation_and_cleaning/reports/cleaned_outputs/users_cleaned.csv
    """
    final_path = _norm_join(_root_cleaned(), f"{table_name}_cleaned.{file_format}")
    _ensure_dir_for(final_path)
    return final_path


# --------------------------------------------------------------------------------------
# SEMANTIC (enrichments/joins/rollups) — DATA PRODUCTS
# --------------------------------------------------------------------------------------
def get_semantic_output_path(*subpaths: str) -> str:
    """
    Root for SEMANTIC data products (what BI/Redshift should read).
    Typical layout:
      transformed_outputs/semantic/<domain>/<dataset>/run_id=<UTCSTAMP>/data
    Usage:
      data_dir = get_semantic_output_path("rollups", "daily_sales", "run_id=20250910T204705Z")
      df.write.parquet(os.path.join(data_dir, "data"))
    """
    final_path = _norm_join(_root_semantic(), *subpaths)
    _ensure_dir_for(final_path)
    return final_path


def get_semantic_dataset_path(dataset: str, *subpaths: str) -> str:
    """
    Convenience helper pinned to a dataset:
      get_semantic_dataset_path("order_status_by_order", "run_id=...", "data")
    """
    final_path = _norm_join(_root_semantic(), dataset, *subpaths)
    _ensure_dir_for(final_path)
    return final_path


def get_semantic_run_path(dataset: str, run_id: str, *subpaths: str) -> str:
    """
    Convenience helper that enforces run_id folder convention.
    Example:
      p = get_semantic_run_path("rollups/order_status_by_order", run_id, "data")
      -> transformed_outputs/semantic/rollups/order_status_by_order/run_id=<run_id>/data
    """
    final_path = _norm_join(_root_semantic(), dataset, f"run_id={run_id}", *subpaths)
    _ensure_dir_for(final_path)
    return final_path


# --------------------------------------------------------------------------------------
# LINEAGE / AUDIT (metrics only, no raw data)
# --------------------------------------------------------------------------------------
def get_lineage_output_path(stage: str, *subpaths: str) -> str:
    """
    Root for lineage/QA/metrics.

    'stage' can be:
      - 'profiling'      (if you ever add meta about profiling runs)
      - 'validation'     (validators)
      - 'cleaning'       (cleaning QA)
      - 'semantic'       (rollups/enrichments audits)
      - 'transformations' (generic)
    Usage:
      m = get_lineage_output_path("semantic", "daily_sales", "metrics_20250910.json")
    """
    final_path = _norm_join(_root_lineage(), stage, *subpaths)
    _ensure_dir_for(final_path)
    return final_path


# --------------------------------------------------------------------------------------
# BACKWARD COMPAT (legacy helper) — prefer the dedicated helpers above
# --------------------------------------------------------------------------------------
def get_local_output_path(folder_name: str = "profiling_reports", sub_dir: str = "") -> str:
    """
    ⚠️ Legacy helper kept for backward compatibility.
    Historically used like:
      get_local_output_path("profiling_reports", "profiling")
    Prefer:
      - get_profiling_output_path()
      - get_validation_report_path(...)
      - get_semantic_output_path(...)
      - get_lineage_output_path(...)

    This will resolve under: <project_root>/MKM_Data_Profiling/<folder_name>/<sub_dir>
    """
    base = _norm_join(get_project_root(), "MKM_Data_Profiling", folder_name)
    final_path = _norm_join(base, sub_dir) if sub_dir else base
    _ensure_dir_for(final_path)
    return final_path


# --------------------------------------------------------------------------------------
# Simple guards (optional, use in critical scripts)
# --------------------------------------------------------------------------------------
def assert_is_semantic(path: str) -> None:
    """Raise if path does not clearly point to semantic outputs."""
    if "semantic" not in path.replace("\\", "/"):
        raise ValueError(f"[path_utils] Expected a semantic path, got: {path}")

def assert_is_profiling(path: str) -> None:
    """Raise if path does not clearly point to profiling outputs."""
    if "/profiling" not in path.replace("\\", "/"):
        raise ValueError(f"[path_utils] Expected a profiling path, got: {path}")


# --------------------------------------------------------------------------------------
# Usage guide (for humans) — which scripts should call which helper?
# --------------------------------------------------------------------------------------
USAGE_MAP = {
    "profilers": "get_profiling_output_path()",
    "validators_pre_post": "get_validation_report_path(stage, filename)",
    "cleaners": "cleaning_output_paths(table_name, file_format)",
    "semantic_rollups_data": "get_semantic_output_path(...) or get_semantic_run_path(dataset, run_id, 'data')",
    "semantic_rollups_lineage": "get_lineage_output_path('semantic', <dataset>, <file>)",
}






















# import os

# def get_project_root():
#     """
#     Walk upward from this file until a folder containing .env is found.
#     Falls back to two-levels-up if not found (old behavior).
#     """
#     current = os.path.abspath(os.path.dirname(__file__))
#     for _ in range(10):
#         if os.path.exists(os.path.join(current, ".env")):
#             return current
#         parent = os.path.dirname(current)
#         if parent == current:
#             break
#         current = parent
#     # fallback: preserve your old assumption
#     return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# # def get_project_root():
# #     return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# # def get_local_output_path(folder_name: str = "profiling_reports", sub_dir: str = "") -> str:
# #     """
# #     Returns an absolute output path like: MKM_Data_Profiling/profiling_reports/profiling/
# #     """
# #     base = os.path.abspath(
# #         os.path.join(os.path.dirname(__file__), "..", "..", "MKM_Data_Profiling", folder_name)
# #     )
# #     final_path = os.path.join(base, sub_dir) if sub_dir else base
# #     os.makedirs(final_path, exist_ok=True)
# #     return final_path

# def get_local_output_path(folder_name: str = "profiling_reports", sub_dir: str = "") -> str:
#     """
#     Returns an absolute output *path* under MKM_Data_Profiling/<folder_name>/<sub_dir>.
#     If final path looks like a file path, only ensure its parent exists.
#     """
#     root = get_project_root()
#     base = os.path.join(root, "MKM_Data_Profiling", folder_name)
#     final_path = os.path.join(base, sub_dir) if sub_dir else base

#     # Create parent dir only (works for file or subdir notation)
#     parent = os.path.dirname(final_path) if os.path.splitext(final_path)[1] else final_path
#     os.makedirs(parent, exist_ok=True)
#     return final_path

# # def get_local_output_path(folder_name: str = "profiling_reports", sub_dir: str = "") -> str:
# #     """
# #     Returns an absolute output file path like:
# #     MKM_Data_Profiling/profiling_reports/profiling/orders_profile.json
# #     """
# #     base = os.path.abspath(
# #         os.path.join(os.path.dirname(__file__), "..", "..", "MKM_Data_Profiling", folder_name)
# #     )
# #     final_path = os.path.join(base, sub_dir) if sub_dir else base

# #     # ✅ Only make parent folder, not the full path (if it's a file)
# #     os.makedirs(os.path.dirname(final_path), exist_ok=True)

# #     return final_path


# def cleaning_output_paths(table_name: str, file_format: str = "csv") -> str:
#     """
#     Returns a path like:
#     MKM_Data_Validation_and_cleaning/cleaned_outputs/users_cleaned.csv
#     """
#     root = get_project_root()
#     folder = os.path.join(root, "MKM_Data_Validation_and_cleaning", "reports", "cleaned_outputs")
#     os.makedirs(folder, exist_ok=True)

#     full_path = os.path.join(folder, f"{table_name}_cleaned.{file_format}")
#     return full_path

# def get_validation_report_path(stage: str, filename: str) -> str:
#     """
#     stage: 'pre_cleaning' or 'post_cleaning'
#     filename: the JSON filename like 'cart_item_validation_pre.json'

#     Returns full path to:
#     MKM_Data_Validation_and_cleaning/reports/validation_reports/{stage}/{filename}
#     """
#     root = get_project_root()
#     folder = os.path.join(root, "MKM_Data_Validation_and_cleaning", "reports", "validation_reports", stage)
#     os.makedirs(folder, exist_ok=True)
#     return os.path.join(folder, filename)

# #  Lineage path helper (top-level lineage folder, not under reports/)
# def get_lineage_path(filename: str = "") -> str:
#     """
#     Returns a path under:
#       <root>/MKM_Data_Validation_and_cleaning/lineage[/<filename>]
#     Ensures the lineage directory exists. If filename is "", returns the directory path.
#     """
#     root = get_project_root()
#     folder = os.path.join(root, "MKM_Data_Validation_and_cleaning", "lineage")
#     os.makedirs(folder, exist_ok=True)
#     return os.path.join(folder, filename) if filename else folder