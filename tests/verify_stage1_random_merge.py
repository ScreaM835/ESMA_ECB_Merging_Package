# =============================================================================
# Verification: Stage 1 merged outputs (random sample)
# (Derived from notebook optional verification cell)
# =============================================================================
import os
import random

import pandas as pd

CSV_FOLDER = os.environ.get("ECB_ESMA_BASE_PATH", r"c:\Users\jonat\Downloads\_unique_csv_master")
OUTPUT_FOLDER = os.environ.get("ECB_ESMA_STAGE1_OUTPUT_DIR", os.path.join(CSV_FOLDER, "ESMA_UE_Collat_Merged"))

if not os.path.exists(OUTPUT_FOLDER):
    raise SystemExit(f"Output folder not found: {OUTPUT_FOLDER}")

merged_files = [f for f in os.listdir(OUTPUT_FOLDER) if f.endswith(".csv")]

if not merged_files:
    raise SystemExit(f"No merged CSVs found in: {OUTPUT_FOLDER}")

sample_file = random.choice(merged_files)

print(f"VERIFICATION SAMPLE: {sample_file}")
print("="*80)

merged_path = os.path.join(OUTPUT_FOLDER, sample_file)
merged_df = pd.read_csv(merged_path, nrows=5, low_memory=False)

print(f"Column count: {len(merged_df.columns)}")
print()
print("First 5 rows (selected key columns):")

key_cols = ['Sec_Id', 'Pool_Cutoff_Date']
key_cols.extend([c for c in merged_df.columns if c.endswith('L2') or c.endswith('C2')][:4])
key_cols = [c for c in key_cols if c in merged_df.columns]

print(merged_df[key_cols].to_string(index=False))
