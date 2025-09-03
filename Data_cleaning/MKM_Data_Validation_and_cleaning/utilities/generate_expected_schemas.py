import os
import sys
import yaml
import argparse
from pathlib import Path
from datetime import datetime, timezone

# --- Bootstrap project path (so imports work no matter where you run this) ---
project_root = Path(__file__).resolve().parents[2]  # .../Data_cleaning
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# ---------------------------------------------------------------------------

from pyspark.sql import SparkSession
from src.utils.config_loader import load_env_and_get
from src.connections.db_connections import spark_session_for_JDBC

# Layout:
# Data_cleaning/MKM_Data_Validation_and_cleaning/metadata/expected_schemas/latest/<table>.yaml
# Data_cleaning/MKM_Data_Validation_and_cleaning/metadata/expected_schemas/versions/vYYYYMMDD_HHMMSSZ/<table>.yaml
BASE_EXPECTED = Path("Data_cleaning") / "MKM_Data_Validation_and_cleaning" / "metadata" / "expected_schemas"
LATEST_DIR    = BASE_EXPECTED / "latest"
VERSIONS_DIR  = BASE_EXPECTED / "versions"

# Global: when True, we skip writing .bak files in latest/
VERSIONED_RUN = False


def _utc_ts_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _get_spark() -> SparkSession:
    return spark_session_for_JDBC(app_name="GenerateExpectedSchemas")


def _list_tables(spark: SparkSession, jdbc_url: str, props: dict, only_lower: bool = True):
    q = "(SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()) as t"
    df = spark.read.jdbc(url=jdbc_url, table=q, properties=props)
    names = [r[0] for r in df.collect() if isinstance(r[0], str)]
    if only_lower:
        names = [n for n in names if n.islower()]
    return sorted(names)


def _schema_to_buckets(df):
    """Split columns into required/optional using Spark's nullable flag."""
    required, optional = [], []
    for f in df.schema.fields:
        (optional if f.nullable else required).append(f.name)
    return sorted(required), sorted(optional)


def _write_yaml(path: Path, required, optional, disallowed=None, meta=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    doc = {
        "generated_at": _utc_ts_compact(),
        "required": required or [],
        "optional": optional or [],
        "disallowed": disallowed or [],
    }
    if meta:
        doc["_meta"] = meta
    with path.open("w", encoding="utf-8") as f:
        yaml.dump(doc, f, sort_keys=False, allow_unicode=True)


def _read_yaml_safe(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _backup_existing(path: Path):
    # If we’re doing a versioned snapshot this run, don’t clutter latest/ with .bak.
    if VERSIONED_RUN:
        return
    if path.exists():
        bak = path.with_suffix(path.suffix + f".{_utc_ts_compact()}.bak")
        bak.write_bytes(path.read_bytes())
        print(f"🗃️  Backup created: {bak}")


def _prune_versions(retain: int):
    """
    Keep only the newest 'retain' snapshot folders in versions/.
    """
    if retain <= 0:
        return
    if not VERSIONS_DIR.exists():
        return
    # Consider only subdirectories like vYYYYMMDD_HHMMSSZ
    dirs = [p for p in VERSIONS_DIR.iterdir() if p.is_dir() and p.name.startswith("v")]
    dirs_sorted = sorted(dirs, key=lambda p: p.name, reverse=True)
    to_delete = dirs_sorted[retain:]
    for d in to_delete:
        # remove tree
        for p in sorted(d.rglob("*"), reverse=True):
            if p.is_file():
                p.unlink(missing_ok=True)
            else:
                p.rmdir()
        d.rmdir()
        print(f"🧹 Pruned old snapshot: {d}")


def _merge_latest_with_new(latest_path: Path, required_new, optional_new):
    """
    Merge mode:
      - read existing latest YAML if present.
      - do not remove any existing required/optional/disallowed.
      - add any newly discovered columns into 'optional'.
    """
    existing = _read_yaml_safe(latest_path)
    ex_req = set(existing.get("required", []))
    ex_opt = set(existing.get("optional", []))
    ex_dis = set(existing.get("disallowed", []))

    discovered = set(required_new) | set(optional_new)
    known = ex_req | ex_opt | ex_dis
    new_cols = sorted(discovered - known)

    merged_req = sorted(ex_req)
    merged_opt = sorted(ex_opt | set(new_cols))
    merged_dis = sorted(ex_dis)

    return merged_req, merged_opt, merged_dis, new_cols


def main():
    parser = argparse.ArgumentParser(
        description="Generate/refresh expected schema YAMLs per table with latest/ + versions/ structure."
    )
    parser.add_argument(
        "--tables",
        help="Comma-separated list of tables to process (default: all lowercase tables).",
        default=None,
    )
    parser.add_argument(
        "--mode",
        choices=["write", "merge"],
        default="write",
        help="write = overwrite latest YAML (with optional .bak); merge = keep existing and add new columns to optional.",
    )
    parser.add_argument(
        "--versioned",
        action="store_true",
        help="Also write a full snapshot under versions/vYYYYMMDD_HHMMSSZ/",
    )
    parser.add_argument(
        "--retain",
        type=int,
        default=5,
        help="How many versioned snapshots to retain (only applies with --versioned). Default: 5",
    )

    args = parser.parse_args()

    # Load env & bring up Spark
    load_env_and_get()
    spark = _get_spark()

    # Make version snapshot folder if needed
    global VERSIONED_RUN
    VERSIONED_RUN = bool(args.versioned)
    snapshot_dir = None
    if args.versioned:
        snapshot_dir = VERSIONS_DIR / f"v{_utc_ts_compact()}"
        snapshot_dir.mkdir(parents=True, exist_ok=True)

    try:
        jdbc_url = load_env_and_get("DB_URL")
        props = {
            "user": load_env_and_get("DB_USERNAME"),
            "password": load_env_and_get("DB_PASSWORD"),
            "driver": "com.mysql.cj.jdbc.Driver",
        }

        if args.tables:
            tables = [t.strip() for t in args.tables.split(",") if t.strip()]
        else:
            tables = _list_tables(spark, jdbc_url, props, only_lower=True)

        print(f"🔎 Tables to process ({len(tables)}): {tables}")

        for tbl in tables:
            print(f"📦 Inspecting: {tbl}")
            df = spark.read.jdbc(url=jdbc_url, table=tbl, properties=props)
            required, optional = _schema_to_buckets(df)

            latest_path = LATEST_DIR / f"{tbl}_schema.yaml"

            if args.mode == "write":
                _backup_existing(latest_path)
                _write_yaml(
                    latest_path,
                    required=required,
                    optional=optional,
                    disallowed=[],
                    meta={"source": "live_mysql", "strategy": "nullable->optional"},
                )
                print(f"✅ latest -> {latest_path}")
            else:
                # MERGE
                merged_req, merged_opt, merged_dis, new_cols = _merge_latest_with_new(
                    latest_path, required_new=required, optional_new=optional
                )
                _backup_existing(latest_path)
                _write_yaml(
                    latest_path,
                    required=merged_req,
                    optional=merged_opt,
                    disallowed=merged_dis,
                    meta={"source": "merge", "new_columns_added_to_optional": new_cols},
                )
                print(f"🧩 merged latest -> {latest_path} (+{len(new_cols)} new optional)")

            # Versioned snapshot (copy the just-written latest into snapshot dir)
            if snapshot_dir:
                ver_path = snapshot_dir / f"{tbl}_schema.yaml"
                # Write exactly the same structure as latest
                doc = _read_yaml_safe(latest_path)
                _write_yaml(ver_path, doc.get("required"), doc.get("optional"), doc.get("disallowed"), doc.get("_meta"))
                print(f"🗂️  versioned -> {ver_path}")

        # Prune old snapshots
        if snapshot_dir:
            _prune_versions(args.retain)

        print("✅ Done.")

    finally:
        spark.stop()
        print("🛑 Spark session closed.")


if __name__ == "__main__":
    main()








# import os
# import sys
# import yaml
# import argparse
# from datetime import datetime, timezone

# # --- Bootstrap project path so imports work no matter where you run this ---
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)
# from project_bootstrap import bootstrap_project_paths
# bootstrap_project_paths(__file__)

# from pyspark.sql import SparkSession
# from src.utils.config_loader import load_env_and_get
# from src.connections.db_connections import spark_session_for_JDBC

# EXPECTED_DIR = os.path.join(
#     "Data_cleaning",
#     "MKM_Data_Validation_and_cleaning",
#     "metadata",
#     "expected_schemas",
# )

# def _utc_ts():
#     return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# def _get_spark() -> SparkSession:
#     return spark_session_for_JDBC(app_name="GenerateExpectedSchemas")

# def _list_tables(spark, jdbc_url, props, only_lower: bool = True):
#     q = "(SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()) as t"
#     df = spark.read.jdbc(url=jdbc_url, table=q, properties=props)
#     names = [r[0] for r in df.collect() if isinstance(r[0], str)]
#     if only_lower:
#         names = [n for n in names if n.islower()]
#     return sorted(names)

# def _schema_to_buckets(df):
#     """Split columns into required/optional using Spark's nullable flag."""
#     required, optional = [], []
#     for f in df.schema.fields:
#         (optional if f.nullable else required).append(f.name)
#     return sorted(required), sorted(optional)

# def _write_yaml(path, required, optional, disallowed=None, meta=None):
#     os.makedirs(os.path.dirname(path), exist_ok=True)
#     doc = {
#         "generated_at": _utc_ts(),
#         "required": required or [],
#         "optional": optional or [],
#         "disallowed": disallowed or [],
#     }
#     if meta:
#         doc["_meta"] = meta
#     with open(path, "w", encoding="utf-8") as f:
#         yaml.dump(doc, f, sort_keys=False, allow_unicode=True)

# def _backup_existing(path):
#     if os.path.exists(path):
#         bak = f"{path}.{_utc_ts()}.bak"
#         with open(path, "rb") as src, open(bak, "wb") as dst:
#             dst.write(src.read())
#         print(f"🗃️  Backup created: {bak}")

# def main():
#     parser = argparse.ArgumentParser(
#         description="Generate/refresh expected schema YAMLs per table."
#     )
#     parser.add_argument(
#         "--tables",
#         help="Comma-separated list of tables to process (default: all lowercase tables).",
#         default=None,
#     )
#     parser.add_argument(
#         "--mode",
#         choices=["write", "merge"],
#         default="write",
#         help="write = overwrite YAML (with backup); merge = keep existing keys (required/optional/disallowed) and only add new columns into 'optional'.",
#     )
#     args = parser.parse_args()

#     load_env_and_get()  # ensure .env loaded
#     spark = _get_spark()

#     try:
#         jdbc_url = load_env_and_get("DB_URL")
#         props = {
#             "user": load_env_and_get("DB_USERNAME"),
#             "password": load_env_and_get("DB_PASSWORD"),
#             "driver": "com.mysql.cj.jdbc.Driver",
#         }

#         if args.tables:
#             tables = [t.strip() for t in args.tables.split(",") if t.strip()]
#         else:
#             tables = _list_tables(spark, jdbc_url, props, only_lower=True)

#         print(f"🔎 Tables to process ({len(tables)}): {tables}")

#         for tbl in tables:
#             print(f"📦 Inspecting: {tbl}")
#             df = spark.read.jdbc(url=jdbc_url, table=tbl, properties=props)
#             required, optional = _schema_to_buckets(df)
#             out_path = os.path.join(EXPECTED_DIR, f"{tbl}_schema.yaml")

#             if args.mode == "write":
#                 _backup_existing(out_path)
#                 _write_yaml(
#                     out_path,
#                     required=required,
#                     optional=optional,
#                     disallowed=[],
#                     meta={"source": "live_mysql", "strategy": "nullable->optional"},
#                 )
#                 print(f"✅ Wrote: {out_path}")
#             else:
#                 # merge mode: read existing; add any new columns into optional
#                 existing = {"required": [], "optional": [], "disallowed": []}
#                 if os.path.exists(out_path):
#                     with open(out_path, "r", encoding="utf-8") as f:
#                         existing.update(yaml.safe_load(f) or {})

#                 known = set(existing.get("required", [])) | set(existing.get("optional", [])) | set(existing.get("disallowed", []))
#                 new_cols = [c for c in (required + optional) if c not in known]
#                 merged_optional = sorted(set(existing.get("optional", [])) | set(new_cols))

#                 _backup_existing(out_path)
#                 _write_yaml(
#                     out_path,
#                     required=sorted(existing.get("required", [])),
#                     optional=merged_optional,
#                     disallowed=sorted(existing.get("disallowed", [])),
#                     meta={"source": "merge", "new_columns_added_to_optional": new_cols},
#                 )
#                 print(f"🧩 Merged: {out_path} (+{len(new_cols)} new optional)")
#     finally:
#         spark.stop()
#         print("🛑 Spark session closed.")

# if __name__ == "__main__":
#     main()
