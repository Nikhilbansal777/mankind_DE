
# Git Contribution Guide for `feature/Data_Cleaning` Branch

This guide explains how to manage and collaborate on the `feature/Data_Cleaning` branch in the `mankind_matrix_data_engineering` repository.

---

## ✅ Purpose of This Branch

This branch includes:
- Data profiling scripts
- Data validation (pre/post)
- Data cleaning modules
- Common utilities (profilers, logging, file I/O)
- Folder restructuring and deprecated script cleanup

---

## 🔧 Daily Git Workflow

### 1. **Stage All Changes**
```bash
git add -A
```
This stages:
- Modified files
- New files
- Deleted files

### 2. **Commit Your Work**
```bash
git commit -m "Your meaningful commit message here"
```
Examples:
```bash
git commit -m "Added clean_users and validation scripts"
git commit -m "Removed deprecated validators and refactored paths"
```

### 3. **Push Your Branch**
```bash
git push origin feature/Data_Cleaning
```

This will update the existing Pull Request automatically.

---

## 🛠️ Handling Warnings

You might see this:
```
LF will be replaced by CRLF the next time Git touches it
```
**It’s safe to ignore on Windows.** Git is just converting line endings for compatibility.

---

## 🧑‍🤝‍🧑 Collaboration Tips

If teammates want to contribute to your work:
### Option 1: Work on Same Branch (Careful!)
- Ask them to pull latest changes:
```bash
git fetch origin
git checkout feature/Data_Cleaning
git pull
```
- Push changes:
```bash
git add -A
git commit -m "Added XYZ script"
git push origin feature/Data_Cleaning
```

⚠️ Everyone must **pull before pushing** to avoid conflicts.

### Option 2: Create a Branch from Yours (Recommended)
```bash
git checkout -b validation-refactor origin/feature/Data_Cleaning
```
Then raise a PR into `feature/Data_Cleaning`.

---

## 🧼 Final Tips

- Mark your PR as "Draft" until work is complete.
- Use meaningful folder and file names.
- Add a README in subfolders if they contain logic modules.

---

## 🔚 Commands Reference

| Action | Command |
|--------|---------|
| Stage All | `git add -A` |
| Commit | `git commit -m "..."` |
| Push | `git push origin feature/Data_Cleaning` |
| Pull | `git pull origin feature/Data_Cleaning` |
| Status Check | `git status` |

---

