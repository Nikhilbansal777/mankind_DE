# src/connections/db_connections.py

import os
import shutil
import subprocess
from pathlib import Path
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# --- allow running this file directly or via import (src.* always works) ---
try:
    from src.utils.log_utils import get_logger
except ModuleNotFoundError:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # .../Data_cleaning
    from src.utils.log_utils import get_logger

logger = get_logger("connections.jdbc")


# ---------- helpers ----------

def _find_env_path(start_file: Path, max_up: int = 8) -> Path:
    """Walk upward from this file to find the nearest .env (when .env is under Data_cleaning)."""
    cur = start_file.resolve().parent
    for _ in range(max_up):
        cand = cur / ".env"
        if cand.exists():
            return cand
        cur = cur.parent
    return start_file.resolve().parent / ".env"  # best-effort fallback


def _require_java_home() -> None:
    """Require JAVA_HOME to exist and point to a JDK."""
    jh = os.environ.get("JAVA_HOME")
    if not jh:
        raise RuntimeError(
            "JAVA_HOME is not set. Set JAVA_HOME to your JDK11 folder and add %JAVA_HOME%\\bin to PATH.\n"
            "Example (PowerShell):\n"
            "  $env:JAVA_HOME='C:\\Program Files\\Java\\jdk-11'\n"
            "  $env:PATH=\"$env:JAVA_HOME\\bin;$env:PATH\""
        )
    java_bin = Path(jh) / "bin" / ("java.exe" if os.name == "nt" else "java")
    if not java_bin.exists():
        raise RuntimeError(
            f"JAVA_HOME is set to '{jh}', but '{java_bin}' does not exist.\n"
            "Point JAVA_HOME to the actual JDK folder (not JRE) that contains bin\\java(.exe)."
        )


def _ensure_java_access() -> None:
    """
    Make sure 'java' is invokable by this Python process.
    - If on PATH -> ok.
    - Else, if JAVA_HOME/bin/java exists -> prepend that bin to PATH (in-process only).
    - Else -> error.
    """
    if shutil.which("java"):
        return
    jh = os.environ.get("JAVA_HOME")
    if jh:
        cand = Path(jh) / "bin" / ("java.exe" if os.name == "nt" else "java")
        if cand.exists():
            os.environ["PATH"] = str(cand.parent) + os.pathsep + os.environ.get("PATH", "")
            logger.info(f"JAVA not on PATH; using JAVA_HOME -> {cand}")
            return
    raise RuntimeError(
        "Java not found (neither on PATH nor via JAVA_HOME). "
        "Install JDK 11+ and set JAVA_HOME, or add java(.exe) to PATH."
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
    - Requires a valid JAVA_HOME and ensures 'java' is invokable.
    - Self-heals SPARK_HOME/ PATH if stale.
    - Resolves JDBC_PATH relative to .env.
    - Uses Windows-safe Spark configs.
    """
    # 1) Load .env (walk upward from this file)
    here = Path(__file__)
    env_path = _find_env_path(here)
    load_dotenv(env_path)
    print(f"[SUCCESS] .env loaded from: {env_path}")
    logger.info("Environment variables loaded", extra={"path": str(env_path)})

    # 2) Java sanity
    _require_java_home()
    _ensure_java_access()

    # 2.5) Self-heal SPARK_HOME & PATH (force to THIS interpreter's pyspark)
    import sys, os, os.path as p
    import pyspark

    current_pyspark_home = p.dirname(p.abspath(pyspark.__file__))  # venv's pyspark
    os.environ["SPARK_HOME"] = current_pyspark_home

    spark_bin = p.join(current_pyspark_home, "bin")
    # Hard-prepend our bin so it wins over any wrapper on PATH
    os.environ["PATH"] = spark_bin + os.pathsep + os.environ.get("PATH", "")

    # Ensure PySpark uses THIS interpreter
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Sanity: fail fast if somehow broken
    if not p.exists(p.join(spark_bin, "spark-submit.cmd")):
        raise RuntimeError(
            f"spark-submit.cmd not found under {spark_bin}. "
            "Is pyspark installed in this venv?"
        )

    logger.info(
        "SPARK_HOME + PATH self-healed",
        extra={"SPARK_HOME": current_pyspark_home, "spark_bin": spark_bin, "python": sys.executable},
    )


    # 3) JDBC jar
    jar_uri, jar_path = _resolve_jar_from_env(env_path)

    # 4) Spark session
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


if __name__ == "__main__":
    # Minimal smoke if you run this file directly
    s = spark_session_for_JDBC()
    print("[SMOKE] Spark version:", s.version)
    s.stop()
    print("[SMOKE] Spark stopped.")











# # src/connections/db_connections.py

# import os
# import shutil
# import subprocess
# from pathlib import Path
# from pyspark.sql import SparkSession
# from dotenv import load_dotenv
# # from src.utils.log_utils import get_logger

# # --- allow running this file directly or via import (src.* always works) ---
# try:
#     from src.utils.log_utils import get_logger
# except ModuleNotFoundError:
#     import sys
#     sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # .../Data_cleaning
#     from src.utils.log_utils import get_logger

# # initialize logger for this module
# logger = get_logger("connections.jdbc")

# # ---------- helpers ----------

# def _find_env_path(start_file: Path, max_up: int = 8) -> Path:
#     """
#     Walk upward from this file to find the nearest .env (works when .env is under Data_cleaning).
#     """
#     cur = start_file.resolve().parent
#     for _ in range(max_up):
#         env_candidate = cur / ".env"
#         if env_candidate.exists():
#             return env_candidate
#         cur = cur.parent
#     # Fallback: repo root guess (may not exist)
#     return start_file.resolve().parent / ".env"


# def _require_java_home() -> None:
#     """
#     Team-safe: require JAVA_HOME to exist and point to a JDK.
#     """
#     jh = os.environ.get("JAVA_HOME")
#     if not jh:
#         raise RuntimeError(
#             "JAVA_HOME is not set. Set JAVA_HOME to your JDK11 folder and add %JAVA_HOME%\\bin to PATH.\n"
#             "Example (PowerShell):\n"
#             "  $env:JAVA_HOME='C:\\Program Files\\Java\\jdk-11'\n"
#             "  $env:PATH=\"$env:JAVA_HOME\\bin;$env:PATH\""
#         )
#     java_bin = Path(jh) / "bin" / ("java.exe" if os.name == "nt" else "java")
#     if not java_bin.exists():
#         raise RuntimeError(
#             f"JAVA_HOME is set to '{jh}', but '{java_bin}' does not exist.\n"
#             "Point JAVA_HOME to the actual JDK folder (not JRE) that contains bin\\java(.exe)."
#         )


# # this is the new version that forces prepending JAVA_HOME/bin but it didn't worked out and it was done because i got win error 2.
# # def _ensure_java_access(force_prepend: bool = False) -> None:
# #     """
# #     Ensure 'java' is invokable by THIS Python process.
# #     - If force_prepend=True and JAVA_HOME/bin/java exists -> prepend it to PATH for this process.
# #     - Else, if 'java' already on PATH -> ok.
# #     - Else, try JAVA_HOME/bin/java -> prepend.
# #     - Else -> raise with a clear message.
# #     Also runs a quick 'java -version' via subprocess to mirror how PySpark will spawn it.
# #     """
# #     jh = os.environ.get("JAVA_HOME")
# #     cand = Path(jh) / "bin" / ("java.exe" if os.name == "nt" else "java") if jh else None

# #     if force_prepend and cand and cand.exists():
# #         os.environ["PATH"] = str(cand.parent) + os.pathsep + os.environ.get("PATH", "")
# #         logger.info("Prepended JAVA_HOME/bin to PATH", extra={"java": str(cand)})
# #     elif shutil.which("java"):
# #         logger.info("java found on PATH", extra={"java": shutil.which("java")})
# #     elif cand and cand.exists():
# #         os.environ["PATH"] = str(cand.parent) + os.pathsep + os.environ.get("PATH", "")
# #         logger.info("JAVA not on PATH; using JAVA_HOME/bin", extra={"java": str(cand)})
# #     else:
# #         raise RuntimeError(
# #             "Java not found (neither on PATH nor via JAVA_HOME). "
# #             "Install JDK 11+ and set JAVA_HOME, or add java.exe to PATH."
# #         )

# #     # sanity: ensure this process can spawn java (same mechanism Spark uses)
# #     try:
# #         subprocess.run(["java", "-version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# #     except FileNotFoundError:
# #         raise RuntimeError("java still not spawnable by subprocess after PATH fix")
# #     except subprocess.CalledProcessError as e:
# #         # Non-zero exit is fine for -version on some JVMs; keep going but log
# #         try:
# #             stderr_txt = e.stderr.decode(errors="ignore")
# #         except Exception:
# #             stderr_txt = "<unavailable>"
# #         logger.warning("java -version returned non-zero", extra={"stderr": stderr_txt})




# def _ensure_java_access() -> None:
#     """
#     Make sure 'java' is invokable by this Python process.
#     - If 'java' already on PATH -> ok.
#     - Else, if JAVA_HOME/bin/java exists -> prepend its bin to PATH (in-process only).
#     - Else -> raise with a clear message.
#     """
#     if shutil.which("java"):
#         return

#     jh = os.environ.get("JAVA_HOME")
#     if jh:
#         cand = Path(jh) / "bin" / ("java.exe" if os.name == "nt" else "java")
#         if cand.exists():
#             os.environ["PATH"] = str(cand.parent) + os.pathsep + os.environ.get("PATH", "")
#             logger.info(f"JAVA not on PATH; using JAVA_HOME -> {cand}")
#             return

#     raise RuntimeError(
#         "Java not found (neither on PATH nor via JAVA_HOME). "
#         "Install JDK 11+ and set JAVA_HOME, or add java.exe to PATH."
#     )


# def _resolve_jar_from_env(env_path: Path) -> tuple[str, str]:
#     """
#     Read JDBC_PATH from the loaded .env and resolve it to:
#       - jar_uri (file:///...) for spark.jars
#       - jar_path (absolute filesystem path) for extraClassPath
#     If JDBC_PATH is relative, resolve it relative to the .env directory.
#     """
#     jdbc_path = os.getenv("JDBC_PATH")
#     if not jdbc_path:
#         raise ValueError("[ERROR] JDBC_PATH not found in .env")

#     jar_path = Path(jdbc_path)
#     if not jar_path.is_absolute():
#         jar_path = (env_path.parent / jar_path).resolve()

#     if not jar_path.exists():
#         raise FileNotFoundError(f"[ERROR] JDBC driver JAR not found at: {jar_path}")

#     jar_uri = jar_path.as_uri()  # file:///E:/.../mysql-connector-j-8.0.33.jar
#     print(f"[INFO] JDBC JAR -> {jar_uri}")
#     return jar_uri, str(jar_path)


# # ---------- public API ----------

# def spark_session_for_JDBC(app_name: str = "MKM_DB_Connections") -> SparkSession:
#     """
#     Create a Spark session with JDBC driver configured from .env.
#     - Loads nearest .env by walking up from this file.
#     - Requires a valid JAVA_HOME and ensures 'java' is invokable.
#     - Resolves JDBC_PATH relative to .env.
#     - Uses Windows-safe Spark configs.
#     """
#     # 1) Load .env
#     here = Path(__file__)
#     env_path = _find_env_path(here)
#     load_dotenv(env_path)
#     print(f"[SUCCESS] .env loaded from: {env_path}")
#     logger.info("Environment variables loaded", extra={"path": str(env_path)})

#     # 2) Verify JAVA_HOME and ensure java is reachable for this process
#     _require_java_home()
#     _ensure_java_access()

#     # --- Self-heal SPARK_HOME & PATH (protect against stale global env) ---
#     try:
#         import pyspark
#         from pyspark.find_spark_home import _find_spark_home
#         shp = os.environ.get("SPARK_HOME")
#         spark_submit_ok = bool(shp) and os.path.exists(os.path.join(shp, "bin", "spark-submit.cmd"))
#         if not spark_submit_ok:
#             # point SPARK_HOME to the pyspark bundled with the active venv
#             os.environ["SPARK_HOME"] = _find_spark_home()
#         spark_bin = os.path.join(os.environ["SPARK_HOME"], "bin")
#         if os.path.isdir(spark_bin) and spark_bin not in os.environ.get("PATH", ""):
#             os.environ["PATH"] = spark_bin + os.pathsep + os.environ.get("PATH", "")
#             logger.info("Prepended SPARK_HOME\\bin to PATH", extra={"spark_bin": spark_bin})
#     except Exception as e:
#         logger.warning("SPARK_HOME self-heal skipped", extra={"error": repr(e)})
#     # --- end self-heal ---


#     # 3) Resolve JDBC driver jar
#     jar_uri, jar_path = _resolve_jar_from_env(env_path)

#     # 4) Build Spark session
#     spark = (
#         SparkSession.builder
#         .appName(app_name)
#         .config("spark.jars", jar_uri)                    # accepts file:// URI
#         .config("spark.driver.extraClassPath", jar_path)  # filesystem path
#         .config("spark.executor.extraClassPath", jar_path)
#         # Windows-safe settings
#         .config("spark.hadoop.io.native.lib.available", "false")
#         .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
#         .config(
#             "spark.sql.sources.commitProtocolClass",
#             "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",
#         )
#         .getOrCreate()
#     )
#     print("[INFO] Spark session created with JDBC driver")
#     logger.info("Spark session created with JDBC driver", extra={"app_name": app_name})
#     return spark




















# # src/connections/db_connections.py

# import os
# from pathlib import Path
# from pyspark.sql import SparkSession
# from dotenv import load_dotenv
# from src.utils.log_utils import get_logger

# # initialize logger for this module
# logger = get_logger("connections.jdbc")

# # ---------- helpers ----------

# def _find_env_path(start_file: Path, max_up: int = 8) -> Path:
#     """
#     Walk upward from this file to find the nearest .env (works when .env is under Data_cleaning).
#     """
#     cur = start_file.resolve().parent
#     for _ in range(max_up):
#         env_candidate = cur / ".env"
#         if env_candidate.exists():
#             return env_candidate
#         cur = cur.parent
#     # Fallback: repo root guess (may not exist)
#     return start_file.resolve().parent / ".env"

# def _require_java_home() -> None:
#     """
#     Team-safe: require JAVA_HOME. Do not mutate env. Fail fast if invalid.
#     """
#     jh = os.environ.get("JAVA_HOME")
#     if not jh:
#         raise RuntimeError(
#             "JAVA_HOME is not set. Set JAVA_HOME to your JDK11 folder and add %JAVA_HOME%\\bin to PATH.\n"
#             "Example (PowerShell):\n"
#             "  $env:JAVA_HOME='C:\\Program Files\\Java\\jdk-11'\n"
#             "  $env:PATH=\"$env:JAVA_HOME\\bin;$env:PATH\""
#         )
#     java_exe = Path(jh) / "bin" / "java.exe"
#     if not java_exe.exists():
#         raise RuntimeError(
#             f"JAVA_HOME is set to '{jh}', but '{java_exe}' does not exist.\n"
#             "Point JAVA_HOME to the actual JDK folder (not JRE) that contains bin\\java.exe."
#         )

# def _resolve_jar_from_env(env_path: Path) -> tuple[str, str]:
#     """
#     Read JDBC_PATH from the loaded .env and resolve it to:
#       - jar_uri (file:///...) for spark.jars
#       - jar_path (absolute filesystem path) for extraClassPath
#     If JDBC_PATH is relative, resolve it relative to the .env directory.
#     """
#     jdbc_path = os.getenv("JDBC_PATH")
#     if not jdbc_path:
#         raise ValueError("[ERROR] JDBC_PATH not found in .env")

#     jar_path = Path(jdbc_path)
#     if not jar_path.is_absolute():
#         jar_path = (env_path.parent / jar_path).resolve()

#     if not jar_path.exists():
#         raise FileNotFoundError(f"[ERROR] JDBC driver JAR not found at: {jar_path}")

#     jar_uri = jar_path.as_uri()  # file:///E:/.../mysql-connector-j-8.0.33.jar
#     print(f"[INFO] JDBC JAR -> {jar_uri}")
#     return jar_uri, str(jar_path)

# # ---------- public API ----------

# def spark_session_for_JDBC(app_name: str = "MKM_DB_Connections") -> SparkSession:
#     """
#     Create a Spark session with JDBC driver configured from .env.
#     - Loads nearest .env by walking up from this file.
#     - Requires a valid JAVA_HOME (team-safe, no env mutation).
#     - Resolves JDBC_PATH relative to .env.
#     - Uses Windows-safe Spark configs.
#     """
#     # 1) Load .env
#     here = Path(__file__)
#     env_path = _find_env_path(here)
#     load_dotenv(env_path)
#     print(f"[SUCCESS] .env loaded from: {env_path}")
#     logger.info("Environment variables loaded", extra={"path": str(env_path)})

#     # 2) Require JAVA_HOME (deterministic)
#     _require_java_home()

#     # 3) Resolve JDBC driver jar
#     jar_uri, jar_path = _resolve_jar_from_env(env_path)

#     # 4) Build Spark session
#     spark = (
#         SparkSession.builder
#         .appName(app_name)
#         .config("spark.jars", jar_uri)                    # accepts file:// URI
#         .config("spark.driver.extraClassPath", jar_path)  # filesystem path
#         .config("spark.executor.extraClassPath", jar_path)
#         # Windows-safe settings
#         .config("spark.hadoop.io.native.lib.available", "false")
#         .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
#         .config(
#             "spark.sql.sources.commitProtocolClass",
#             "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",
#         )
#         .getOrCreate()
#     )
#     print("[INFO] Spark session created with JDBC driver")
#     logger.info("Spark session created with JDBC driver", extra={"app_name": app_name})
#     return spark

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