import os, sys, shutil, subprocess, glob
from pprint import pprint

def prepend_path(p: str):
    p = os.path.normpath(p)
    parts = os.environ.get("PATH", "").split(os.pathsep)
    if not parts or os.path.normpath(parts[0]) != p:
        os.environ["PATH"] = p + os.pathsep + os.environ.get("PATH", "")

def die(msg: str, code: int = 1):
    print("!!", msg)
    sys.exit(code)

print("\n======== PYTHON & JAVA DIAGNOSTICS ========")
print("PY exe        :", sys.executable)

# 1) Force Java inside THIS Python process
os.environ.setdefault("JAVA_HOME", r"C:\Program Files\Java\jdk-11")
prepend_path(os.path.join(os.environ["JAVA_HOME"], "bin"))

print("JAVA_HOME     :", os.environ.get("JAVA_HOME"))
print("which java    :", shutil.which("java"))

# Prove subprocess can run java
try:
    out = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT)
    print("java -version :\n" + out.decode("utf-8"))
except Exception as e:
    die(f"subprocess java -version failed: {e!r}")

print("\n======== PYSPARK INSTALL DIAGNOSTICS ========")
try:
    import pyspark
    print("PySpark file  :", pyspark.__file__)
    print("PySpark ver   :", pyspark.__version__)
except Exception as e:
    die(f"Failed to import pyspark: {e!r}")

# 2) Locate jars dir (pip install ships jars here)
pyspark_dir = os.path.dirname(pyspark.__file__)
jars_dir = os.path.join(pyspark_dir, "jars")
print("jars_dir      :", jars_dir, "| exists:", os.path.isdir(jars_dir))

# List a few jars just to ensure it’s not empty
some_jars = glob.glob(os.path.join(jars_dir, "*.jar"))[:5]
print("sample jars   :", [os.path.basename(j) for j in some_jars])
if not some_jars:
    die("No jars found in pyspark/jars. Broken PySpark install.")

# 3) Try launching the Spark launcher class directly (this mimics gateway)
java_exe = shutil.which("java")
cp_arg = os.path.join(jars_dir, "*")
manual_cmd = [java_exe, "-cp", cp_arg, "org.apache.spark.launcher.Main", "--help"]

print("\n======== MANUAL JAVA LAUNCH (spark launcher) ========")
print("Command:")
pprint(manual_cmd)
try:
    out = subprocess.check_output(manual_cmd, stderr=subprocess.STDOUT)
    print("Launcher OK. Output (truncated):\n", out.decode("utf-8")[:400])
except Exception as e:
    print("Launcher failed with:", repr(e))
    # Dump a bit more context to help
    print("Exists(java)?", os.path.exists(java_exe))
    print("Exists(jars_dir)?", os.path.exists(jars_dir))
    print("First 1 jar:", some_jars[0] if some_jars else "N/A")
    die("Manual Java launch failed — this is the root cause of [WinError 2].")

# 4) Finally, try to start Spark
print("\n======== SPARK BOOT TEST ========")
from pyspark.sql import SparkSession
try:
    spark = (
        SparkSession.builder
        .appName("SparkBootTestVerbose")
        .config("spark.hadoop.io.native.lib.available", "false")
        .getOrCreate()
    )
    print("[OK] Spark started. Version:", spark.version)
except Exception as e:
    die(f"Spark getOrCreate failed: {e!r}")
finally:
    try:
        spark.stop()
        print("[OK] Spark stopped.")
    except Exception:
        pass
