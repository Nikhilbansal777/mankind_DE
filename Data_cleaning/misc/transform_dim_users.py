# --- bootstrap ---
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# -----------------

from pyspark.sql import functions as F
from src.utils.config_loader import load_env_and_get
from .transforms_common import get_spark, read_silver, write_gold, safe_select, with_surrogate_key

def run(format: str = "parquet"):
    load_env_and_get()
    spark = get_spark("gold_dim_users")
    try:
        users = read_silver(spark, "users")
        # expected cleaned cols: users_id, email, created_at, is_active (CDC flag)
        dim = safe_select(users, ["users_id", "email", "created_at", "is_active"]).dropDuplicates(["users_id"])
        # optional SK (string hash)
        dim = with_surrogate_key(dim, "users_id", "sk_user")
        return write_gold(dim, "dim_users", fmt=format)
    finally:
        spark.stop()

if __name__ == "__main__":
    run()
