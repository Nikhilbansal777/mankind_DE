# --- bootstrap (keep at very top) ---
import sys, json, os
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# ------------------------------------

from datetime import datetime, timezone
from typing import Any, Dict, List
from pyspark.sql import Row
from src.connections.db_connections import spark_session_for_JDBC
from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import get_project_root

# Where your mismatch JSONs live today (you told me this path)
def _mismatch_dir() -> Path:
    root = Path(get_project_root())
    return root / "MKM_Data_Validation_and_cleaning" / "reports" / "validation_reports" / "mismatches"

# Where we will store normalized lineage events (Parquet)
def _lineage_out_dir() -> Path:
    root = Path(get_project_root())
    return root / "MKM_Glue_tranformations" / "lineage" / "quality_issues"

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _safe_read_json(p: Path) -> Any:
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        return {"_read_error": str(e)}

def _flatten_events(obj: Any, src_file: str, run_id: str, table_hint: str | None) -> List[Dict[str, Any]]:
    """
    Very defensive extractor:
    - Recognizes common shapes from our post-validation outputs and mismatch drops
    - Emits a list of generic lineage-quality events with a consistent schema
    """
    events: List[Dict[str, Any]] = []

    def emit(rule_type: str, details: Dict[str, Any], table: str | None = None):
        evt = {
            "run_id": run_id,
            "event_ts": _now_iso(),
            "source_file": src_file.replace("\\", "/"),
            "rule_type": rule_type,            # e.g., 'foreign_key', 'not_null', 'unique', 'cast', 'regex', 'enum', 'range'
            "table": table or table_hint,      # where possible
            "details": details,                # free-form JSON map for drill
        }
        events.append(evt)

    def walk(x: Any, parent_table: str | None):
        # Pattern 1: single FK mismatch object
        if isinstance(x, dict) and all(k in x for k in ["fk_col", "ref_col", "unmatched_count"]):
            emit("foreign_key", {
                "fk_col": x.get("fk_col"),
                "ref_col": x.get("ref_col"),
                "ref_table": x.get("ref_table"),
                "unmatched_count": x.get("unmatched_count"),
                "samples": x.get("sample_fk", []),
            }, parent_table)
            return

        # Pattern 2: not-null result, e.g. {"col": {"null_count": N, "samples": [...]}, ...}
        if isinstance(x, dict) and all(isinstance(v, dict) and "null_count" in v for v in x.values()):
            for col, res in x.items():
                emit("not_null", {"col": col, "null_count": res.get("null_count", 0), "samples": res.get("samples", [])}, parent_table)
            return

        # Pattern 3: unique check result
        if isinstance(x, dict) and "dup_groups" in x and "columns" in x:
            emit("unique", {"columns": x.get("columns"), "dup_groups": x.get("dup_groups"), "samples": x.get("samples", [])}, parent_table)
            return

        # Pattern 4: cast failures
        if isinstance(x, dict) and "cast_failures" in x and "target" in x and "col" in x:
            emit("cast", {"col": x.get("col"), "target": x.get("target"), "cast_failures": x.get("cast_failures"), "samples": x.get("samples", [])}, parent_table)
            return

        # Pattern 5: enum / allowed values violations
        if isinstance(x, dict) and {"col", "allowed", "violations"} <= set(x.keys()):
            emit("enum", {"col": x.get("col"), "violations": x.get("violations"), "allowed": x.get("allowed"), "samples": x.get("samples", [])}, parent_table)
            return

        # Pattern 6: range violations
        if isinstance(x, dict) and {"col", "min", "max", "violations"} <= set(x.keys()):
            emit("range", {"col": x.get("col"), "min": x.get("min"), "max": x.get("max"), "violations": x.get("violations"), "samples": x.get("samples", [])}, parent_table)
            return

        # Pattern 7: regex violations
        if isinstance(x, dict) and {"col", "pattern", "violations"} <= set(x.keys()):
            emit("regex", {"col": x.get("col"), "pattern": x.get("pattern"), "violations": x.get("violations"), "samples": x.get("samples", [])}, parent_table)
            return

        # Otherwise recurse into dict/list
        if isinstance(x, dict):
            # capture table hint if present
            t_hint = x.get("table") or parent_table
            for v in x.values():
                walk(v, t_hint)
        elif isinstance(x, list):
            for v in x:
                walk(v, parent_table)

    walk(obj, table_hint)
    return events

def main():
    load_env_and_get()
    spark = spark_session_for_JDBC(app_name="promote_mismatches_to_lineage")
    try:
        src_dir = _mismatch_dir()
        if not src_dir.exists():
            print(f"[LINEAGE] No mismatch dir found: {src_dir}")
            return

        out_dir = _lineage_out_dir()
        out_dir.mkdir(parents=True, exist_ok=True)

        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        all_events: List[Dict[str, Any]] = []

        files = sorted([p for p in src_dir.glob("**/*.json") if p.is_file()])
        if not files:
            print(f"[LINEAGE] No mismatch JSONs found under {src_dir}")
            return

        print(f"[LINEAGE] Scanning {len(files)} mismatch files…")
        for fp in files:
            payload = _safe_read_json(fp)
            table_hint = None
            if isinstance(payload, dict):
                table_hint = payload.get("table")
            events = _flatten_events(payload, str(fp), run_id, table_hint)
            if events:
                all_events.extend(events)

        if not all_events:
            print("[LINEAGE] No recognizable mismatch patterns found; nothing to write.")
            return

        # Write as Parquet (append)
        df = spark.createDataFrame([Row(**e) for e in all_events])
        df.write.mode("append").parquet(str(out_dir))
        print(f"[LINEAGE] ✅ wrote {df.count()} events -> {out_dir}")

        # Optional: quick CSV summary per run_id
        summary = (
            df.groupBy("run_id", "rule_type", "table")
              .count()
              .orderBy("rule_type", "table")
        )
        summary_path = out_dir / f"summary_{run_id}.csv"
        (summary.coalesce(1)
                .write.mode("overwrite")
                .option("header", True)
                .csv(str(summary_path)))
        print(f"[LINEAGE] 🧾 summary CSV -> {summary_path}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
