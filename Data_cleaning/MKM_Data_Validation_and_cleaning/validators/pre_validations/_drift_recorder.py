# MKM_Data_Validation_and_cleaning/validators/pre_validations/_drift_recorder.py


# _drift_recorder.py — minimal, zero-deps, append-only JSONL writers
import json, os
from datetime import datetime, timezone
from pathlib import Path

# Compute <repo-root>/reports/logs from this file's location
# this file: .../Data_cleaning/MKM_Data_Validation_and_cleaning/validators/pre_validations/_drift_recorder.py
ROOT = Path(__file__).resolve().parents[3]   # -> .../Data_cleaning
LOG_DIR = ROOT / "reports" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

DRIFT_JSONL = LOG_DIR / "schema_drift.log.jsonl"
UNRES_JSONL = LOG_DIR / "validation_unresolved.log.jsonl"

def _utc_ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _append_jsonl(path: Path, payload: dict):
    with open(path, "a", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
        f.write("\n")

def record_schema_drift(*, table: str, new_columns=None, removed_columns=None, type_changes=None, run_id: str | None = None):
    """
    Persist a single drift event (append-only).
    Any of new_columns / removed_columns / type_changes can be None/[].
    """
    payload = {
        "ts": _utc_ts(),
        "run_id": run_id,
        "table": table,
        "event": "schema_drift",
        "new_columns": new_columns or [],
        "removed_columns": removed_columns or [],
        "type_changes": type_changes or {},
    }
    _append_jsonl(DRIFT_JSONL, payload)

def record_unresolved_checks(*, table: str, not_null=None, unique=None, run_id: str | None = None):
    """
    Persist unresolved column checks (names we asked to validate but couldn't map).
    """
    payload = {
        "ts": _utc_ts(),
        "run_id": run_id,
        "table": table,
        "event": "unresolved_checks",
        "not_null": not_null or [],
        "unique": unique or [],
    }
    _append_jsonl(UNRES_JSONL, payload)



















# # MKM_Data_Validation_and_cleaning/validators/pre_validations/_drift_recorder.py
# from __future__ import annotations
# import json, os
# from pathlib import Path
# from datetime import datetime, timezone

# # We reuse your log_utils so this also goes to rotating <root>/reports/logs/*.log
# from src.utils.log_utils import get_logger

# _LOGGER = get_logger("validation.drift")

# def _utc_now() -> str:
#     return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# def _logs_dir() -> Path:
#     """
#     Mirrors log_utils default without importing its private helpers.
#     <project-root> is .../Data_cleaning
#     """
#     # pre_validations -> validators -> MKM_Data_Validation_and_cleaning -> Data_cleaning
#     root = Path(__file__).resolve().parents[3]
#     base = Path(os.getenv("LOG_DIR") or (root / "reports" / "logs"))
#     base.mkdir(parents=True, exist_ok=True)
#     return base

# _JSONL = _logs_dir() / "schema_drift.log.jsonl"   # structured, append-only ledger

# def _append_jsonl(obj: dict) -> None:
#     try:
#         with _JSONL.open("a", encoding="utf-8") as f:
#             f.write(json.dumps(obj, ensure_ascii=False) + "\n")
#     except Exception as e:
#         # Don't crash validation runs on IO; just note it.
#         _LOGGER.warning(f"Failed writing drift JSONL: {e!r}")

# def _resolve_run_id(run_id: str | None) -> str:
#     # prefer explicit; else env RUN_ID (set once by pre_validate_all); else a timestamp
#     return run_id or os.getenv("RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# def record_schema_drift(
#     *,
#     table: str,
#     new_columns: list[str] | None = None,
#     removed_columns: list[str] | None = None,
#     type_changes: dict[str, str] | None = None,
#     run_id: str | None = None,
#     source: str = "pre_validation",
# ) -> None:
#     """Record schema drifts (new/removed/type-changed columns). Only writes if something is non-empty."""
#     payload = {
#         "ts": _utc_now(),
#         "event": "schema_drift",
#         "table": table,
#         "run_id": _resolve_run_id(run_id),
#         "source": source,
#     }
#     wrote = False
#     if new_columns:
#         payload["new_columns"] = new_columns; wrote = True
#     if removed_columns:
#         payload["removed_columns"] = removed_columns; wrote = True
#     if type_changes:
#         payload["type_changes"] = type_changes; wrote = True

#     if not wrote:
#         return

#     # human-readable line (rotating .log)
#     _LOGGER.info(
#         f"schema_drift table={table} new={new_columns or []} removed={removed_columns or []} type_changes={type_changes or {}}",
#         extra={"table": table, "run_id": payload["run_id"]},
#     )
#     # structured line (append-only .jsonl)
#     _append_jsonl(payload)

# def record_unresolved_checks(
#     *,
#     table: str,
#     not_null: list[str] | None = None,
#     unique: list[str] | None = None,
#     foreign_keys: list[str] | None = None,
#     run_id: str | None = None,
#     source: str = "pre_validation",
# ) -> None:
#     """Record unresolved validation items (e.g., constraints you flagged after the run)."""
#     payload = {
#         "ts": _utc_now(),
#         "event": "unresolved_checks",
#         "table": table,
#         "run_id": _resolve_run_id(run_id),
#         "source": source,
#     }
#     wrote = False
#     if not_null:
#         payload["not_null"] = not_null; wrote = True
#     if unique:
#         payload["unique"] = unique; wrote = True
#     if foreign_keys:
#         payload["foreign_keys"] = foreign_keys; wrote = True

#     if not wrote:
#         return

#     _LOGGER.info(
#         f"unresolved_checks table={table} not_null={not_null or []} unique={unique or []} fks={foreign_keys or []}",
#         extra={"table": table, "run_id": payload["run_id"]},
#     )
#     _append_jsonl(payload)
