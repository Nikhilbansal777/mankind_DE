
# Data Transformations - Project README

## Purpose
This folder contains scripts for transforming and enriching cleaned datasets, including joins, aggregations, and dimensional modeling.

## Structure
- `join_enrich_transform.py`: Example transformation script for joining multiple tables.
- `transform_utils/`: Utility functions for transformations.
- `transformed_outputs/`: Stores transformed dataset outputs.

## Usage
1. Ensure all required cleaned datasets are available.
2. Run transformation script:
```bash
python join_enrich_transform.py
```
3. Output stored in `transformed_outputs/`.

## Notes
- Keep transformation logic modular for reuse.
- Consider partitioning and file formats for downstream efficiency.
