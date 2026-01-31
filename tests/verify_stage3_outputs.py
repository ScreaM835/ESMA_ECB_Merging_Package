# =============================================================================
# Verification: Stage 3 output files exist and are readable
# (Derived from notebook CELL 8: Quick Verification of Output Files)
# =============================================================================
import json
import os

import pandas as pd

OUTPUT_DIR = os.environ.get("ECB_ESMA_STAGE3_OUTPUT_DIR", r"D:\ECB_ESMA_BY_COUNTRY_ALL")

log_path = os.path.join(OUTPUT_DIR, "merge_log.json")

print("=" * 90)
print("STAGE 3 VERIFICATION: Output files")
print("=" * 90)
print(f"Output directory: {OUTPUT_DIR}")
print()

results = None
if os.path.exists(log_path):
    with open(log_path, "r") as f:
        log_data = json.load(f)
    results = log_data.get("results", [])
    print(f"Loaded merge log: {log_path}")
    print(f"Countries in log: {len(results)}")
else:
    # Fallback: scan directory
    results = [{"country": os.path.splitext(fn)[0], "output_path": os.path.join(OUTPUT_DIR, fn)}
               for fn in os.listdir(OUTPUT_DIR) if fn.endswith(".csv")]
    print("merge_log.json not found; scanning directory instead.")
    print(f"CSV files found: {len(results)}")

print()

# Check each output file
for r in results:
    filepath = r.get("output_path") or os.path.join(OUTPUT_DIR, f"{r['country']}.csv")
    country = r["country"]

    if not os.path.exists(filepath):
        print(f"✗ {country}: File not found!")
        continue

    try:
        # Read first row to confirm file is valid
        df = pd.read_csv(filepath, nrows=1, low_memory=False)
        rows = sum(1 for _ in open(filepath, 'rb')) - 1  # cheap-ish line count; includes header
        cols = len(df.columns)
        size_gb = os.path.getsize(filepath) / (1024**3)
        print(f"✓ {country}: {rows:,} rows, {cols} cols, {size_gb:.2f} GB")
    except Exception as e:
        print(f"✗ {country}: Error reading file - {str(e)[:80]}")
