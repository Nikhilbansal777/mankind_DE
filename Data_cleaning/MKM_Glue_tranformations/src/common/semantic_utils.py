# semantic_utils.py

import json, os
from pathlib import Path
from typing import Iterable, Set, Dict
from pyspark.sql import DataFrame
from src.utils.path_utils import get_lineage_path

# Columns we usually don't want duplicated without a prefix
DEFAULT_COLLISION_CANDIDATES: Set[str] = {
    "name", "brand", "status", "description", "title", "__event_ts"
}

# Keys we should never rename (keep stable for joins)
NEVER_RENAME: Set[str] = {
    "id", "users_id", "user_id", "products_id", "product_id",
    "orders_id", "order_id", "order_items_id", "wishlist_id"
}

def _lineage_columns(table: str) -> Set[str]:
    """Load set of columns for a table from lineage JSON; fallback to empty set."""
    try:
        path = os.path.join(get_lineage_path(), f"{table}_lineage.json")
        if not os.path.exists(path):
            return set()
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        cols = obj.get("columns") or obj.get("schema") or []
        return set(cols)
    except Exception:
        return set()

def _suggest_renames(
    left_cols: Iterable[str],
    right_cols: Iterable[str],
    right_table: str,
    join_key_names: Iterable[str],
    only_candidates: Set[str]
) -> Dict[str, str]:
    left_set, right_set = set(left_cols), set(right_cols)
    overlap = (left_set & right_set) - set(join_key_names) - NEVER_RENAME
    if only_candidates:
        overlap = {c for c in overlap if c in only_candidates}

    rename_map: Dict[str, str] = {}
    for c in overlap:
        # keep double-underscore shape for __event_ts -> users__event_ts
        if c == "__event_ts":
            tgt = f"{right_table}__event_ts"
        else:
            tgt = f"{right_table}_{c}"
        rename_map[c] = tgt
    return rename_map

def _write_rename_audit(left_table: str, right_table: str, rename_map: Dict[str, str]) -> None:
    """Persist what we renamed for traceability under lineage/join_renames/"""
    if not rename_map:
        return
    folder = Path(get_lineage_path("join_renames"))
    folder.mkdir(parents=True, exist_ok=True)
    out = folder / f"{left_table}_vs_{right_table}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump({
            "left_table": left_table,
            "right_table": right_table,
            "renamed_columns": rename_map
        }, f, indent=2)

def safe_join_with_lineage(
    left: DataFrame,
    right: DataFrame,
    left_table: str,
    right_table: str,
    on_expr,
    how: str = "left",
    join_key_names: Iterable[str] = (),
    only_candidates: Set[str] = DEFAULT_COLLISION_CANDIDATES
) -> DataFrame:
    """
    - Loads lineage columns (fallback to df.columns)
    - Renames overlapping 'generic' columns on the RIGHT side
    - Writes a small audit JSON
    - Returns the joined DataFrame
    """
    # union lineage with real columns so synthetic cols like __event_ts are seen
    lcols = _lineage_columns(left_table) | set(left.columns)
    rcols = _lineage_columns(right_table) | set(right.columns)
    
    # lcols = _lineage_columns(left_table) or set(left.columns)
    # rcols = _lineage_columns(right_table) or set(right.columns)

    rename_map = _suggest_renames(lcols, rcols, right_table, join_key_names, only_candidates)

    # apply renames on right only
    for src, tgt in rename_map.items():
        if src in right.columns and tgt not in right.columns:
            right = right.withColumnRenamed(src, tgt)

    _write_rename_audit(left_table, right_table, rename_map)
    return left.join(right, on_expr, how)























# import json, os
# from typing import Iterable, Set, Dict
# from pyspark.sql import DataFrame
# from pyspark.sql import functions as F
# from src.utils.path_utils import get_lineage_path

# # Columns we usually don't want duplicated without a prefix
# DEFAULT_COLLISION_CANDIDATES: Set[str] = {
#     "name", "brand", "status", "description", "title"
# }

# # Keys we should never rename (keep stable for joins)
# NEVER_RENAME: Set[str] = {
#     "id", "users_id", "user_id", "products_id", "product_id",
#     "orders_id", "order_id", "order_items_id", "wishlist_id"
# }

# def _lineage_columns(table: str) -> Set[str]:
#     """Load the set of columns for a table from lineage JSON; fallback to empty set."""
#     try:
#         path = os.path.join(get_lineage_path(), f"{table}_lineage.json")
#         with open(path, "r", encoding="utf-8") as f:
#             obj = json.load(f)
#         cols = obj.get("columns") or obj.get("schema") or []
#         return set(cols)
#     except Exception:
#         return set()

# def _suggest_renames(
#     left_cols: Iterable[str],
#     right_cols: Iterable[str],
#     right_table: str,
#     only_candidates: Set[str] = DEFAULT_COLLISION_CANDIDATES
# ) -> Dict[str, str]:
#     left_set, right_set = set(left_cols), set(right_cols)
#     overlap = (left_set & right_set) - NEVER_RENAME
#     if only_candidates:
#         overlap = {c for c in overlap if c in only_candidates}
#     # prefix only the right side to keep left stable
#     return {c: f"{right_table}_{c}" for c in overlap}

# def safe_join_with_lineage(
#     left: DataFrame,
#     right: DataFrame,
#     left_table: str,
#     right_table: str,
#     on_expr,
#     how: str = "left",
#     only_candidates: Set[str] = DEFAULT_COLLISION_CANDIDATES
# ) -> DataFrame:
#     """
#     1) Loads lineage columns for both tables (fallback to actual df.columns if lineage missing).
#     2) Renames overlapping 'generic' columns on the RIGHT side (e.g., name -> products_name).
#     3) Executes the join.
#     """
#     lcols = _lineage_columns(left_table) or set(left.columns)
#     rcols = _lineage_columns(right_table) or set(right.columns)
#     rename_map = _suggest_renames(lcols, rcols, right_table, only_candidates)

#     for src, tgt in rename_map.items():
#         if src in right.columns and tgt not in right.columns:
#             right = right.withColumnRenamed(src, tgt)

#     return left.join(right, on_expr, how)
