# Spark on Windows: Fix for `NativeIO$Windows.access0` / winutils

This readme captures exactly what we did to make **Spark 3.2.4** write files on **Windows**, and how to re-do it on a fresh machine. It also lists the symptoms and a few gotchas we hit.

---

## TL;DR (do this once)

1) Find your **Hadoop client version** bundled with PySpark:  
   Look under  
   `Data_cleaning\mankind_env\Lib\site-packages\pyspark\jars\`  
   and note the version in jar names like `hadoop-client-api-3.3.1.jar` → **3.3.1**.

2) Put **matching** Windows native helpers in a versioned folder:
```
C:\hadoop-3.3.1\bin\winutils.exe
C:\hadoop-3.3.1\bin\hadoop.dll        # recommended; some ops need it
```

3) Set environment variables (one-time; see “Make it persistent” below):
```powershell
$env:HADOOP_HOME = "C:\hadoop-3.3.1"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
```

4) Create tmp dir & (optionally) chmod it:
```powershell
New-Item -ItemType Directory -Force -Path C:\tmp | Out-Null
& "C:\hadoop-3.3.1\bin\winutils.exe" chmod 777 "C:\tmp"
# If "Access is denied", you can skip this or see the user-owned tmp option below.
```

5) Run your job. Writes (JSON/Parquet/CSV) should now succeed.

---

## Symptoms we saw

- Cleaning step failed on write with:
  ```
  java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(...)
  ```
- Pre-validation (read via JDBC) worked; **writes** failed (commit phase uses Hadoop FS → native helpers).

---

## Root cause (plain English)

On Windows, Hadoop’s filesystem layer expects small native shims:
- `winutils.exe` (file/permission helpers)
- `hadoop.dll` (JNI native library used by `NativeIO`)

If the **version doesn’t match** your Hadoop jars or the files aren’t on `PATH`, Spark can blow up during writes.

---

## Detailed steps (with alternatives)

### 1) Detect Hadoop client version
Check the jars shipped with your PySpark:
```
...\mankind_env\Lib\site-packages\pyspark\jars\
  hadoop-client-api-3.3.1.jar
  hadoop-client-runtime-3.3.1.jar
```
→ Version = **3.3.1** (use this number below).

### 2) Place matching native binaries
Create:
```
C:\hadoop-<VERSION>\bin\
```
Copy **both** files into `bin`:
```
C:\hadoop-3.3.1\bin\winutils.exe
C:\hadoop-3.3.1\bin\hadoop.dll
```
> In our testing, `winutils.exe` alone sometimes worked; **adding `hadoop.dll` made it reliable**.

### 3) Point your session to them

**Option A — Project-scoped (recommended): add to your venv’s `Activate.ps1`**  
`Data_cleaning\mankind_env\Scripts\Activate.ps1` → append:
```powershell
$env:HADOOP_HOME = "C:\hadoop-3.3.1"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
```
Then each time you activate the venv, it’s set.

**Option B — User/System env vars (global):**
- Add `HADOOP_HOME=C:\hadoop-3.3.1`
- Add `%HADOOP_HOME%\bin` to **Path**
- Restart terminal/VS Code.

### 4) tmp directory (two ways)

**Simplest (C drive):**
```powershell
New-Item -ItemType Directory -Force -Path C:\tmp | Out-Null
# If you have admin rights:
& "C:\hadoop-3.3.1\bin\winutils.exe" chmod 777 "C:\tmp"
# If "Access is denied", skip the chmod and try your job—it often still works.
```

**User-owned tmp (no admin needed):**
```powershell
$env:HADOOP_TMP_DIR = "$env:USERPROFILE\hadoop_tmp"
New-Item -ItemType Directory -Force -Path $env:HADOOP_TMP_DIR | Out-Null
& "C:\hadoop-3.3.1\bin\winutils.exe" chmod 777 "$env:HADOOP_TMP_DIR"
```

Optionally also set it in Spark builder (belt-and-suspenders):
```python
# inside spark_session_for_JDBC(...)
.config("spark.hadoop.hadoop.tmp.dir", os.environ.get("HADOOP_TMP_DIR", r"C:\tmp"))
```

### 5) Verify in PowerShell

> Note: In PowerShell, `where` is a filter alias; use `where.exe` or `Get-Command`.

```powershell
$env:HADOOP_HOME
$env:PATH -split ';' | ? { $_ -match 'hadoop' }
where.exe winutils
Get-Command winutils.exe
Test-Path "C:\hadoop-3.3.1\bin\hadoop.dll"
```
You should see `C:\hadoop-3.3.1\bin\winutils.exe` and the DLL present.

### 6) (Optional) also hint Spark via Java opts

In `spark_session_for_JDBC(...)`:
```python
.config("spark.driver.extraJavaOptions",  f"-Dhadoop.home.dir={os.environ.get('HADOOP_HOME', r'C:\hadoop-3.3.1')}")
.config("spark.executor.extraJavaOptions", f"-Dhadoop.home.dir={os.environ.get('HADOOP_HOME', r'C:\hadoop-3.3.1')}")
```

---

## Troubleshooting

- **`UnsatisfiedLinkError: NativeIO$Windows.access0`**  
  → Missing/wrong-version native bits.  
  Check: version matches jars, `HADOOP_HOME`, `%HADOOP_HOME%\bin` in PATH, files exist, `where.exe winutils`.

- **`winutils chmod ...` → “Access is denied”**  
  → Run PowerShell as Administrator, or use a **user-owned tmp** (set `HADOOP_TMP_DIR`) and chmod that path.

- **Illegal reflective access warnings**  
  → Harmless for Spark 3.2.4. Prefer Java 8 or 11 for fewer warnings.

- **My output path showed `Data_cleaning\Data_cleaning` twice**  
  → Our project’s `get_project_root()` returns the `Data_cleaning` folder (it finds `.env` there).  
    Patch `src/utils/path_utils.py` so writer functions don’t re-prepend `Data_cleaning`:
  ```python
  def cleaning_output_paths(table_name, file_format="csv"):
      root = get_project_root()  # resolves to ...\Data_cleaning
      folder = os.path.join(root, "MKM_Data_Validation_and_cleaning", "reports", "cleaned_outputs")
      os.makedirs(folder, exist_ok=True)
      return os.path.join(folder, f"{table_name}_cleaned.{file_format}")

  def get_validation_report_path(stage, filename):
      root = get_project_root()
      folder = os.path.join(root, "MKM_Data_Validation_and_cleaning", "reports", "validation_reports", stage)
      os.makedirs(folder, exist_ok=True)
      return os.path.join(folder, filename)
  ```

---

## What we actually did (our session notes)

1. Noticed write failing with `NativeIO$Windows.access0`.  
2. Checked PySpark jars → Hadoop **3.3.1**.  
3. Placed **3.3.1** `winutils.exe` **and** `hadoop.dll` in `C:\hadoop-3.3.1\bin\`.  
4. Set `HADOOP_HOME` + added `%HADOOP_HOME%\bin` to PATH.  
5. Created `C:\tmp`. `winutils chmod` gave “Access is denied”, so we **skipped chmod** and it still worked.  
   (Later verified: using a **user-owned tmp** avoids admin.)  
6. Re-ran cleaning → success.  
7. Fixed duplicate `Data_cleaning` in output paths by adjusting `path_utils.py`.

---

## Make it persistent (so you don’t redo after reboot)

- **Best (project-scoped):** add to venv’s `Activate.ps1`:
  ```powershell
  $env:HADOOP_HOME = "C:\hadoop-3.3.1"
  $env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
  ```
- **OR global:** set **User** (or System) env vars for `HADOOP_HOME` and `Path`.

Now, every time you `Activate.ps1` the venv, things are ready. No extra commands.

---

## FAQ

- **Do I need `hadoop.dll`?**  
  In our runs, yes—it made writes reliable. Keep both `winutils.exe` and `hadoop.dll` for your Hadoop version.

- **Can I place them on E: drive instead?**  
  Yes. Location doesn’t matter; version match + `HADOOP_HOME` + PATH do.

- **Do I always need `chmod`?**  
  Not always. If `C:\tmp` permissions are fine, you can skip it. If you hit permission errors, use a user-owned tmp and chmod that path.
