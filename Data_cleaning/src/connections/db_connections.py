# src/connections/db_connections.py

import os
from pathlib import Path
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from src.utils.log_utils import get_logger

# initialize logger for this module
logger = get_logger("connections.jdbc")

# ---------- helpers ----------

def _find_env_path(start_file: Path, max_up: int = 8) -> Path:
    """
    Walk upward from this file to find the nearest .env (works when .env is under Data_cleaning).
    """
    cur = start_file.resolve().parent
    for _ in range(max_up):
        env_candidate = cur / ".env"
        if env_candidate.exists():
            return env_candidate
        cur = cur.parent
    # Fallback: repo root guess (may not exist)
    return start_file.resolve().parent / ".env"

def _require_java_home() -> None:
    """
    Team-safe: require JAVA_HOME. Do not mutate env. Fail fast if invalid.
    """
    jh = os.environ.get("JAVA_HOME")
    if not jh:
        raise RuntimeError(
            "JAVA_HOME is not set. Set JAVA_HOME to your JDK11 folder and add %JAVA_HOME%\\bin to PATH.\n"
            "Example (PowerShell):\n"
            "  $env:JAVA_HOME='C:\\Program Files\\Java\\jdk-11'\n"
            "  $env:PATH=\"$env:JAVA_HOME\\bin;$env:PATH\""
        )
    java_exe = Path(jh) / "bin" / "java.exe"
    if not java_exe.exists():
        raise RuntimeError(
            f"JAVA_HOME is set to '{jh}', but '{java_exe}' does not exist.\n"
            "Point JAVA_HOME to the actual JDK folder (not JRE) that contains bin\\java.exe."
        )

def _resolve_jar_from_env(env_path: Path) -> tuple[str, str]:
    """
    Read JDBC_PATH from the loaded .env and resolve it to:
      - jar_uri (file:///...) for spark.jars
      - jar_path (absolute filesystem path) for extraClassPath
    If JDBC_PATH is relative, resolve it relative to the .env directory.
    """
    jdbc_path = os.getenv("JDBC_PATH")
    if not jdbc_path:
        raise ValueError("[ERROR] JDBC_PATH not found in .env")

    jar_path = Path(jdbc_path)
    if not jar_path.is_absolute():
        jar_path = (env_path.parent / jar_path).resolve()

    if not jar_path.exists():
        raise FileNotFoundError(f"[ERROR] JDBC driver JAR not found at: {jar_path}")

    jar_uri = jar_path.as_uri()  # file:///E:/.../mysql-connector-j-8.0.33.jar
    print(f"[INFO] JDBC JAR -> {jar_uri}")
    return jar_uri, str(jar_path)

# ---------- public API ----------

def spark_session_for_JDBC(app_name: str = "MKM_DB_Connections") -> SparkSession:
    """
    Create a Spark session with JDBC driver configured from .env.
    - Loads nearest .env by walking up from this file.
    - Requires a valid JAVA_HOME (team-safe, no env mutation).
    - Resolves JDBC_PATH relative to .env.
    - Uses Windows-safe Spark configs.
    """
    # 1) Load .env
    here = Path(__file__)
    env_path = _find_env_path(here)
    load_dotenv(env_path)
    print(f"[SUCCESS] .env loaded from: {env_path}")
    logger.info("Environment variables loaded", extra={"path": str(env_path)})

    # 2) Require JAVA_HOME (deterministic)
    _require_java_home()

    # 3) Resolve JDBC driver jar
    jar_uri, jar_path = _resolve_jar_from_env(env_path)

    # 4) Build Spark session
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", jar_uri)                    # accepts file:// URI
        .config("spark.driver.extraClassPath", jar_path)  # filesystem path
        .config("spark.executor.extraClassPath", jar_path)
        # Windows-safe settings
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
        .config(
            "spark.sql.sources.commitProtocolClass",
            "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",
        )
        .getOrCreate()
    )
    print("[INFO] Spark session created with JDBC driver")
    logger.info("Spark session created with JDBC driver", extra={"app_name": app_name})
    return spark

# if __name__ == "__main__":
#     spark_session_for_JDBC()









# # src/connections/db_connections.py

# import os
# from pyspark.sql import SparkSession
# # from dotenv import get_key
# from dotenv import load_dotenv

# # def spark_session_for_JDBC(env_path=".env") -> SparkSession:
# #     """
# #     Creates a Spark session with JDBC driver configured from .env.
# #     Includes Windows-safe configs to avoid native I/O crashes.

# #     """
# #     jdbc_path = get_key(env_path, "JDBC_PATH")

# def spark_session_for_JDBC() -> SparkSession:
#     """
#     Creates a Spark session with JDBC driver configured from .env.
#     Includes Windows-safe configs to avoid native I/O crashes.

#     """
#     # Load .env manually from the correct location
#     env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
#     load_dotenv(env_path)

#     jdbc_path = os.getenv("JDBC_PATH")
#     if not jdbc_path:
#         raise ValueError("[ERROR] JDBC_PATH not found in .env")

#     # 🔧 Resolve relative path based on project root
#     project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
#     jdbc_abs_path = jdbc_path
#     if not os.path.isabs(jdbc_path):
#         jdbc_abs_path = os.path.abspath(os.path.join(project_root, jdbc_path))
    

#     # Normalize path (especially for local dev)
#     # 🔃 Convert to file:/// URI
#     if not jdbc_abs_path.lower().startswith("file:///"):
#         jdbc_abs_path = "file:///" + jdbc_abs_path.replace("\\", "/")

#     print(f"[INFO] Final JDBC JAR path: {jdbc_abs_path}")

#     # if not jdbc_path.lower().startswith("file:///"):
#     #     jdbc_path = "file:///" + os.path.abspath(jdbc_path).replace("\\", "/")

#     spark = (
#         SparkSession.builder
#         .appName("MKM_DB_Connections")
#         .config("spark.jars", jdbc_abs_path)
#         .config("spark.driver.extraClassPath", jdbc_abs_path)
#         .config("spark.executor.extraClassPath", jdbc_abs_path)

#         # ✅ Windows-safe Spark configs to bypass NativeIO crash
#         .config("spark.hadoop.io.native.lib.available", "false")
#         .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
#         .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapRedCommitProtocol")
        

#         .getOrCreate()
#     )

#     print("[INFO] Spark session created with JDBC driver")
#     return spark



# # # To test the function, you can uncomment the following lines:
# # if __name__ == "__main__":
# #     spark_session_for_JDBC()