
# Data Profiling - Project README

## Purpose
This folder contains scripts and configurations for profiling data tables in the project. Profiling is used to understand column distributions, null percentages, data types, distinct counts, and other metrics before cleaning or transformation.

## Structure
- `profilers_common/`: Reusable scripts for profiling (null counts, distinct counts, value frequencies, etc.)
- `{table_name}_profile.py`: Controller scripts for running profiling for specific tables (e.g., products_profile.py).
- `profiling_reports/`: Stores JSON outputs for profiling results.

## Usage
1. Ensure database connection is set up in `.env`.
2. Run the specific table profiling script:
```bash
python products_profile.py
```
3. Check results in `profiling_reports/profiling/`.

## Notes
- Profiling scripts are modular and reusable.
- Common profilers should be updated for logic changes across all tables.
