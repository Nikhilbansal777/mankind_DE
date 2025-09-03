import os
import yaml

yaml_path = os.path.join("src", "config", "master_schema_cleaning_rules.yaml")

with open(yaml_path, "r") as f:
    data = yaml.safe_load(f)

updated = False
for table, rules in data.get("tables", {}).items():
    type_conversions = rules.get("type_conversions", {})
    for col, dtype in list(type_conversions.items()):
        if dtype == "BooleanType":
            type_conversions[col] = "boolean"
            updated = True

if updated:
    with open(yaml_path, "w") as f:
        yaml.dump(data, f, sort_keys=False)
    print("✅ YAML types fixed successfully.")
else:
    print("✅ No BooleanType issues found. YAML is clean.")
