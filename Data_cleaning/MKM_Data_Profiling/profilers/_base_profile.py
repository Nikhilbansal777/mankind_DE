import sys, os, socket, json, math
from urllib.parse import urlparse
from datetime import datetime, timezone

# --- Project bootstrapping for src/* imports ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --- End bootstrapping ---

from src.connections.db_connections import spark_session_for_JDBC
from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import get_local_output_path
from src.utils.log_utils import get_logger
from src.utils import file_io

# Your existing helpers
from MKM_Data_Profiling.profilers.all_common_profilers import (
    run_common_profilers, sanitize_summary
)

# --------------------------
# JDBC URL + preflight
# --------------------------
def _build_mysql_jdbc_url() -> str:
    raw = (load_env_and_get("DB_URL", default="") or "").strip()
    if raw.startswith("jdbc:mysql://"):
        return raw
    host = load_env_and_get("DB_HOST").strip()
    port = (load_env_and_get("DB_PORT", "3306") or "3306").strip()
    db   = load_env_and_get("DB_NAME").strip()
    return (
        f"jdbc:mysql://{host}:{port}/{db}"
        "?useSSL=true&sslMode=REQUIRED&enabledTLSProtocols=TLSv1.2,TLSv1.3"
        "&allowPublicKeyRetrieval=true&serverTimezone=UTC"
        "&connectTimeout=5000&socketTimeout=15000"
    )

def _parse_host_port_from_jdbc(jdbc_url: str):
    u = urlparse(jdbc_url.replace("jdbc:", "", 1))
    return u.hostname, (u.port or 3306), u.path.lstrip("/")

def _preflight_mysql(host: str, port: int, timeout: float = 3.0) -> None:
    socket.gethostbyname(host)                     # DNS
    s = socket.socket(); s.settimeout(timeout)     # Port
    try:
        s.connect((host, port))
    finally:
        s.close()

# --------------------------
# Config helpers
# --------------------------
def _deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out

DEFAULT_OPTIONS = {
    "version": 1,
    "mode": "standard",                 # 'standard' | 'light'
    "value_frequencies": {
        "enabled": True,                # heavy on wide tables
        "sample_fraction": None,        # e.g., 0.2 to cheapen
        "only_types": ["string","boolean","tinyint","smallint","int","bigint"],
        "columns_include": None,        # whitelist
        "columns_exclude": [],          # blacklist (PII, long text)
    },
    # leave other profilers always-on via your existing functions
}

def _filter_df_for_freq(df, opts_vf, logger):
    if not opts_vf.get("enabled", True):
        logger.info("freq disabled via config")
        return None

    # Column type filtering
    allowed_types = set([t.lower() for t in (opts_vf.get("only_types") or [])])
    dtypes = {c.lower(): t.lower() for c, t in df.dtypes}

    # Include/Exclude columns
    include = opts_vf.get("columns_include")
    exclude = set([(x or "").lower() for x in (opts_vf.get("columns_exclude") or [])])

    cols = df.columns
    if include:
        inc_set = set([c.lower() for c in include])
        cols = [c for c in df.columns if c.lower() in inc_set]

    # Drop excluded and non-allowed types
    cols = [
        c for c in cols
        if c.lower() not in exclude and (not allowed_types or dtypes.get(c.lower()) in allowed_types)
    ]

    if not cols:
        logger.info("freq columns resolved to empty after filters")
        return None

    df_freq = df.select(*cols)

    frac = opts_vf.get("sample_fraction")
    if isinstance(frac, (int, float)) and 0.0 < float(frac) < 1.0:
        logger.info("freq sampling applied", extra={"fraction": float(frac)})
        df_freq = df_freq.sample(withReplacement=False, fraction=float(frac), seed=42)

    return df_freq

# --------------------------
# Primary entry
# --------------------------
def profile_once(table_name: str, logger_name: str = "profilers.generic", options: dict | None = None) -> str:
    """
    Profiles `table_name` using your common profilers. Returns output JSON path.
    `options` lets you slim down value frequencies, exclude PII columns, etc.
    """
    logger = get_logger(logger_name)
    opts = _deep_merge(DEFAULT_OPTIONS, options or {})

    load_env_and_get()                 # loads .env and logs its path
    spark = spark_session_for_JDBC()   # Spark + JDBC JAR

    jdbc_url = _build_mysql_jdbc_url()
    host, port, db = _parse_host_port_from_jdbc(jdbc_url)
    logger.info("jdbc preflight", extra={"host": host, "port": port, "db": db})
    _preflight_mysql(host, port)

    props = {
        "user": (load_env_and_get("DB_USERNAME") or "").strip(),
        "password": (load_env_and_get("DB_PASSWORD") or "").strip(),
        "driver": "com.mysql.cj.jdbc.Driver",
    }

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    try:
        logger.info("starting profiling", extra={"run_id": run_id, "table": table_name})
        # Fast TLS/cred smoke test:
        spark.read.jdbc(url=jdbc_url, table="(select 1) t", properties=props).collect()

        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=props)
        rows = df.count()
        logger.info("loaded table", extra={"table": table_name, "rows": rows})

        # Optional cheapening for value frequencies:
        vf_df = _filter_df_for_freq(df, opts.get("value_frequencies", {}), logger)
        # Run your existing profilers on either the full df or a pair (df, vf_df) via options
        summary = run_common_profilers(
            df,
            table_name=table_name,
            options={"value_frequencies_df": vf_df}  # consumed in updated all_common_profilers below
        )
        summary["run_id"] = run_id
        summary["config_version"] = opts.get("version", 1)
        summary = sanitize_summary(summary)

        out_dir = get_local_output_path("profiling_reports", "profiling")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{table_name}_profile_{run_id}.json")

        file_io.write_json(summary, out_path)
        logger.info("profiling saved", extra={"table": table_name, "path": out_path, "run_id": run_id})
        return out_path

    except Exception as e:
        logger.error(f"profiling failed: {e}", extra={"table": table_name, "run_id": run_id})
        raise
    finally:
        spark.stop()
        logger.info("spark session stopped", extra={"table": table_name, "run_id": run_id})
