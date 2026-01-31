# =============================================================================
# Investigate: Why multiple rows per Loan ID per Date?
# (Derived from notebook investigative cell)
# =============================================================================
import os
import duckdb

OUTPUT_DIR = os.environ.get("ECB_ESMA_STAGE4_OUTPUT_DIR", r"D:\ECB_ESMA_BY_COUNTRY_SORTED")
pt_path = os.path.join(OUTPUT_DIR, "PT.csv")

if not os.path.exists(pt_path):
    raise SystemExit("PT.csv not found in output directory")

conn = duckdb.connect(':memory:')

# Look at PT sorted file - check rows for same loan+date
query = f"""
SELECT 
    RREL1,
    Pool_Cutoff_Date,
    RREL3,
    RREC6,
    source,
    COUNT(*) as cnt
FROM read_csv('{pt_path}', all_varchar=true, header=true)
WHERE Pool_Cutoff_Date = '2024-07-31'
GROUP BY RREL1, Pool_Cutoff_Date, RREL3, RREC6, source
ORDER BY RREL1, RREL3
LIMIT 50
"""
print("Checking what makes rows unique within same RREL1 + Date:")
print()
df = conn.execute(query).fetchdf()
print(df.to_string())

print("\n" + "="*60)
print("Let's check RREL3 (individual loan identifier) distribution:")
query2 = f"""
SELECT 
    RREL1,
    Pool_Cutoff_Date,
    COUNT(DISTINCT RREL3) as unique_loans,
    COUNT(*) as total_rows
FROM read_csv('{pt_path}', all_varchar=true, header=true)
WHERE Pool_Cutoff_Date = '2024-07-31'
GROUP BY RREL1, Pool_Cutoff_Date
ORDER BY total_rows DESC
LIMIT 10
"""
df2 = conn.execute(query2).fetchdf()
print(df2.to_string())

conn.close()
