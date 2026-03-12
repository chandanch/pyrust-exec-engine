from pathlib import Path

import pyrust_exec_engine


base_dir = Path(__file__).resolve().parent
csv_path = base_dir / "data" / "dummy_sales.csv"
json_path = base_dir / "data" / "dummy_sales.json"

sum_result = pyrust_exec_engine.sum_as_string(12, 23)
print(f"sum_as_string result: {sum_result}")

conversion_result = pyrust_exec_engine.csv_to_json_file(
    str(csv_path),
    str(json_path),
)
print(conversion_result)
print(f"JSON output available at: {json_path}")
