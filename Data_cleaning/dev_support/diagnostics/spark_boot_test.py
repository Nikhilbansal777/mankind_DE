import os, sys, shutil, subprocess, glob
from pprint import pprint

def prepend_path(p: str):
    p = os.path.normpath(p)
    parts = os.environ.get("PATH", "").split(os.pathsep)
    if not parts or os.path.normpath(parts[0]) != p:
        os.environ["PATH"] = p + os.pathsep + os.environ.get("PATH", "")

def section(title: str):
    print("\n" + "="*8, title, "="*8)

def run(cmd, check=False, capture=True):
    try:
        if capture:
            cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=check, text=True)
            return cp.returncode, cp.stdout
        else:
            cp = subprocess.run(cmd, check=check)
            return cp.returncode, ""
    except Exception as e:
        return None, f"<exception: {e!r}>"

def main():
    section("PYTHON & JAVA DIAGNOSTICS")
    print("PY exe        :", sys.executable)

    # Prefer existing JAVA_HOME; otherwise default to a common JDK path (Windows)
    os.environ.setdefault("JAVA_HOME", r"C:\Program Files\Java\jdk-11")
    prepend_path(os.path.join(os.environ["JAVA_HOME"], "bin"))

    print("JAVA_HOME     :", os.environ.get("JAVA_HOME"))
    print("which java    :", shutil.which("java"))

    rc, out = run(["java", "-version"])
    print("java -version :\n" + (out or "").strip())
    if rc is None or shutil.which("java") is None:
        print("!! Java is not invokable. Ensure JAVA_HOME is a valid JDK and %JAVA_HOME%\\bin is on PATH.")
        sys.exit(1)

    section("PYSPARK INSTALL DIAGNOSTICS")
    try:
        import pyspark
        print("PySpark file  :", pyspark.__file__)
        print("PySpark ver   :", pyspark.__version__)
    except Exception as e:
        print(f"!! Failed to import pyspark: {e!r}")
        sys.exit(1)

    pyspark_dir = os.path.dirname(pyspark.__file__)
    jars_dir = os.path.join(pyspark_dir, "jars")
    print("jars_dir      :", jars_dir, "| exists:", os.path.isdir(jars_dir))

    some_jars = glob.glob(os.path.join(jars_dir, "*.jar"))[:5]
    print("sample jars   :", [os.path.basename(j) for j in some_jars])
    if not some_jars:
        print("!! No jars found in pyspark/jars. Broken PySpark install.")
        sys.exit(1)

    # Ensure SPARK_HOME/bin is on PATH (use the venv's pyspark)
    section("SPARK_HOME & spark-submit check")
    try:
        from pyspark.find_spark_home import _find_spark_home
        sp_home = _find_spark_home()
    except Exception:
        sp_home = pyspark_dir
    os.environ["SPARK_HOME"] = sp_home
    prepend_path(os.path.join(sp_home, "bin"))
    print("SPARK_HOME    :", os.environ["SPARK_HOME"])
    print("spark-submit  :", shutil.which("spark-submit") or "<not found>")

    rc, out = run([os.path.join(sp_home, "bin", "spark-submit.cmd"), "--version"])
    if rc is None:
        print("!! spark-submit --version failed to execute.")
    else:
        print("spark-submit --version (truncated):\n", (out or "").strip()[:500])

    # Manual launcher probe (non-zero exit is OK; we only need to see output)
    section("MANUAL JAVA LAUNCH (spark launcher)")
    java_exe = shutil.which("java") or "java"
    cp_arg = os.path.join(jars_dir, "*")
    manual_cmd = [java_exe, "-cp", cp_arg, "org.apache.spark.launcher.Main", "--help"]
    print("Command:")
    pprint(manual_cmd)
    rc, out = run(manual_cmd)
    if rc is None:
        print("!! Manual launch could not start.")
    else:
        print(f"launcher rc={rc} (non-zero is OK). Output (truncated):\n{(out or '').strip()[:600]}")

    # Finally, try to start Spark
    section("SPARK BOOT TEST")
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
        print(f"!! Spark getOrCreate failed: {e!r}")
        sys.exit(1)
    finally:
        try:
            spark.stop()
            print("[OK] Spark stopped.")
        except Exception:
            pass

if __name__ == "__main__":
    main()


















# import os, sys, shutil, subprocess, glob
# from pprint import pprint

# def prepend_path(p: str):
#     p = os.path.normpath(p)
#     parts = os.environ.get("PATH", "").split(os.pathsep)
#     if not parts or os.path.normpath(parts[0]) != p:
#         os.environ["PATH"] = p + os.pathsep + os.environ.get("PATH", "")

# def die(msg: str, code: int = 1):
#     print("!!", msg)
#     sys.exit(code)

# print("\n======== PYTHON & JAVA DIAGNOSTICS ========")
# print("PY exe        :", sys.executable)

# # 1) Force Java inside THIS Python process
# os.environ.setdefault("JAVA_HOME", r"C:\Program Files\Java\jdk-11")
# prepend_path(os.path.join(os.environ["JAVA_HOME"], "bin"))

# print("JAVA_HOME     :", os.environ.get("JAVA_HOME"))
# print("which java    :", shutil.which("java"))

# # Prove subprocess can run java
# try:
#     out = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT)
#     print("java -version :\n" + out.decode("utf-8"))
# except Exception as e:
#     die(f"subprocess java -version failed: {e!r}")

# print("\n======== PYSPARK INSTALL DIAGNOSTICS ========")
# try:
#     import pyspark
#     print("PySpark file  :", pyspark.__file__)
#     print("PySpark ver   :", pyspark.__version__)
# except Exception as e:
#     die(f"Failed to import pyspark: {e!r}")

# # 2) Locate jars dir (pip install ships jars here)
# pyspark_dir = os.path.dirname(pyspark.__file__)
# jars_dir = os.path.join(pyspark_dir, "jars")
# print("jars_dir      :", jars_dir, "| exists:", os.path.isdir(jars_dir))

# # List a few jars just to ensure it’s not empty
# some_jars = glob.glob(os.path.join(jars_dir, "*.jar"))[:5]
# print("sample jars   :", [os.path.basename(j) for j in some_jars])
# if not some_jars:
#     die("No jars found in pyspark/jars. Broken PySpark install.")

# # 3) Try launching the Spark launcher class directly (this mimics gateway)
# java_exe = shutil.which("java")
# cp_arg = os.path.join(jars_dir, "*")
# manual_cmd = [java_exe, "-cp", cp_arg, "org.apache.spark.launcher.Main", "--help"]

# print("\n======== MANUAL JAVA LAUNCH (spark launcher) ========")
# print("Command:")
# pprint(manual_cmd)
# try:
#     out = subprocess.check_output(manual_cmd, stderr=subprocess.STDOUT)
#     print("Launcher OK. Output (truncated):\n", out.decode("utf-8")[:400])
# except Exception as e:
#     print("Launcher failed with:", repr(e))
#     # Dump a bit more context to help
#     print("Exists(java)?", os.path.exists(java_exe))
#     print("Exists(jars_dir)?", os.path.exists(jars_dir))
#     print("First 1 jar:", some_jars[0] if some_jars else "N/A")
#     die("Manual Java launch failed — this is the root cause of [WinError 2].")

# # 4) Finally, try to start Spark
# print("\n======== SPARK BOOT TEST ========")
# from pyspark.sql import SparkSession
# try:
#     spark = (
#         SparkSession.builder
#         .appName("SparkBootTestVerbose")
#         .config("spark.hadoop.io.native.lib.available", "false")
#         .getOrCreate()
#     )
#     print("[OK] Spark started. Version:", spark.version)
# except Exception as e:
#     die(f"Spark getOrCreate failed: {e!r}")
# finally:
#     try:
#         spark.stop()
#         print("[OK] Spark stopped.")
#     except Exception:
#         pass
