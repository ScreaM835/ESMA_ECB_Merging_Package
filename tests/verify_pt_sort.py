# =============================================================================
# Verify PT Sort: RREL3 (Loan ID), RREC3 (Collateral ID), RREL6 (Date)
# (Derived from notebook verification cell)
# =============================================================================
import os
import duckdb

OUTPUT_DIR = os.environ.get("ECB_ESMA_STAGE4_OUTPUT_DIR", r"D:\ECB_ESMA_BY_COUNTRY_SORTED")
pt_path = os.path.join(OUTPUT_DIR, "PT.csv")

conn = duckdb.connect(':memory:')

print("=" * 70)
print("VERIFICATION: PT.csv sorted by RREL3 → RREC3 → RREL6")
print("=" * 70)
print(f"File: {pt_path}")

if not os.path.exists(pt_path):
    raise SystemExit("PT.csv not found in output directory")

# Check first 50 rows to see if loans are grouped
print("\n1. First 50 rows (should be sorted by Loan → Collateral → Date):")
query1 = f"""
SELECT 
    RREL3,
    RREC3,
    RREL6,
    RREL1
FROM read_csv('{pt_path}', all_varchar=true, header=true)
LIMIT 50
"""
df1 = conn.execute(query1).fetchdf()
print(df1.to_string())

# Check a specific loan to see its complete time series
print("\n\n2. Sample loan-collateral time series (dates should be ascending):")
query2 = f"""
WITH loan_collateral AS (
    SELECT RREL3, RREC3, COUNT(*) as obs
    FROM read_csv('{pt_path}', all_varchar=true, header=true)
    WHERE RREL3 IS NOT NULL AND RREL3 != '' AND RREC3 IS NOT NULL AND RREC3 != ''
    GROUP BY RREL3, RREC3
    HAVING COUNT(*) > 3
    LIMIT 1
)
SELECT t.RREL3, t.RREC3, t.RREL6, t.RREL1
FROM read_csv('{pt_path}', all_varchar=true, header=true) t
INNER JOIN loan_collateral lc ON t.RREL3 = lc.RREL3 AND t.RREC3 = lc.RREC3
ORDER BY t.RREL3, t.RREC3, t.RREL6
"""
df2 = conn.execute(query2).fetchdf()
print(df2.to_string())

# Check that the file is actually sorted
print("\n\n3. Verify sort order (RREL3 → RREC3 → RREL6):")
query3 = f"""
WITH sorted_check AS (
    SELECT 
        RREL3,
        RREC3,
        RREL6,
        LAG(RREL3) OVER () as prev_rrel3,
        LAG(RREC3) OVER () as prev_rrec3,
        LAG(RREL6) OVER () as prev_rrel6
    FROM read_csv('{pt_path}', all_varchar=true, header=true)
)
SELECT COUNT(*) as violations
FROM sorted_check
WHERE prev_rrel3 IS NOT NULL 
  AND (
      RREL3 < prev_rrel3 
      OR (RREL3 = prev_rrel3 AND RREC3 < prev_rrec3)
      OR (RREL3 = prev_rrel3 AND RREC3 = prev_rrec3 AND RREL6 < prev_rrel6)
  )
"""
df3 = conn.execute(query3).fetchdf()
violations = df3['violations'].iloc[0]
if violations == 0:
    print(f"✓ SORT VERIFIED: No violations found - file is correctly sorted!")
else:
    print(f"✗ ERROR: {violations} sort order violations found!")

conn.close()
