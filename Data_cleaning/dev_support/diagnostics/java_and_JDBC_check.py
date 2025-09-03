import os
import shutil

print("=== JAVA ENVIRONMENT CHECK ===")
print("[DEBUG] JAVA_HOME =", os.environ.get("JAVA_HOME"))
print("[DEBUG] java found at =", shutil.which("java"))
if not shutil.which("java"):
    print("[ERROR] Java is not on PATH. Set JAVA_HOME and update PATH.")
else:
    os.system("java -version")

print("\n=== JDBC DRIVER CHECK ===")
# Adjust this relative path if your .env has a different JDBC_PATH value
jdbc_rel_path = "src/resources/jdbc_drivers/mysql-connector-j-8.0.33.jar"
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
jdbc_abs_path = os.path.abspath(os.path.join(project_root, jdbc_rel_path))
print("[DEBUG] Resolved JDBC driver path =", jdbc_abs_path)
print("[DEBUG] Exists? =", os.path.exists(jdbc_abs_path))
