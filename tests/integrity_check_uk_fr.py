# =============================================================================
# DATA INTEGRITY CHECK: UK and FR
# (Derived from notebook cell)
# =============================================================================
import os
import time
import duckdb

INPUT_DIR = os.environ.get("ECB_ESMA_STAGE3_OUTPUT_DIR", r"D:\ECB_ESMA_BY_COUNTRY_ALL")
OUTPUT_DIR = os.environ.get("ECB_ESMA_STAGE4_OUTPUT_DIR", r"D:\ECB_ESMA_BY_COUNTRY_SORTED")

countries_to_check = ['UK', 'FR']

print("=" * 90)
print("DATA INTEGRITY CHECK: UK and FR")
print("=" * 90)
print()

for country in countries_to_check:
    input_path = os.path.join(INPUT_DIR, f"{country}.csv")
    output_path = os.path.join(OUTPUT_DIR, f"{country}.csv")

    if not os.path.exists(input_path) or not os.path.exists(output_path):
        print(f"{country}: Files not found, skipping")
        continue

    size_gb = os.path.getsize(output_path) / (1024**3)
    print(f"Checking {country} ({size_gb:.2f} GB)...")

    start = time.time()
    issues = []

    try:
        conn = duckdb.connect(':memory:')
        conn.execute("SET threads = 4")

        # 1. ROW COUNT CHECK
        print("  - Row count...", end=" ", flush=True)
        input_rows = conn.execute(f"""
            SELECT COUNT(*) FROM read_csv('{input_path}', all_varchar=true, header=true, quote='"')
        """).fetchone()[0]

        output_rows = conn.execute(f"""
            SELECT COUNT(*) FROM read_csv('{output_path}', all_varchar=true, header=true, quote='"')
        """).fetchone()[0]

        if input_rows != output_rows:
            issues.append(f"ROW COUNT MISMATCH: input={input_rows:,}, output={output_rows:,}")
            print("✗")
        else:
            print(f"✓ ({input_rows:,})")

        # 2. COLUMN COUNT CHECK
        print("  - Column count...", end=" ", flush=True)
        input_cols = conn.execute(f"""
            SELECT * FROM read_csv('{input_path}', all_varchar=true, header=true, quote='"') LIMIT 0
        """).description
        input_col_count = len(input_cols)

        output_cols = conn.execute(f"""
            SELECT * FROM read_csv('{output_path}', all_varchar=true, header=true, quote='"') LIMIT 0
        """).description
        output_col_count = len(output_cols)

        if input_col_count != output_col_count:
            issues.append(f"COLUMN COUNT MISMATCH: input={input_col_count}, output={output_col_count}")
            print("✗")
        else:
            print(f"✓ ({input_col_count})")

        # 3. UNIQUE KEY COUNT CHECK
        print("  - Unique keys...", end=" ", flush=True)
        input_unique_keys = conn.execute(f"""
            SELECT COUNT(DISTINCT RREL3 || '|' || COALESCE(RREC3,'') || '|' || COALESCE(RREL6,''))
            FROM read_csv('{input_path}', all_varchar=true, header=true, quote='"')
        """).fetchone()[0]

        output_unique_keys = conn.execute(f"""
            SELECT COUNT(DISTINCT RREL3 || '|' || COALESCE(RREC3,'') || '|' || COALESCE(RREL6,''))
            FROM read_csv('{output_path}', all_varchar=true, header=true, quote='"')
        """).fetchone()[0]

        if input_unique_keys != output_unique_keys:
            issues.append(f"UNIQUE KEY COUNT MISMATCH: input={input_unique_keys:,}, output={output_unique_keys:,}")
            print("✗")
        else:
            print(f"✓ ({input_unique_keys:,})")

        # 4. DISTINCT VALUES CHECK for key columns
        print("  - Distinct values...", end=" ", flush=True)
        for col in ['RREL3', 'RREC3', 'RREL6', 'source']:
            input_distinct = conn.execute(f"""
                SELECT COUNT(DISTINCT {col}) FROM read_csv('{input_path}', all_varchar=true, header=true, quote='"')
            """).fetchone()[0]

            output_distinct = conn.execute(f"""
                SELECT COUNT(DISTINCT {col}) FROM read_csv('{output_path}', all_varchar=true, header=true, quote='"')
            """).fetchone()[0]

            if input_distinct != output_distinct:
                issues.append(f"DISTINCT {col} MISMATCH: input={input_distinct:,}, output={output_distinct:,}")

        if not any('DISTINCT' in i for i in issues):
            print("✓")
        else:
            print("✗")

        # 5. CHECKSUM
        print("  - Checksum...", end=" ", flush=True)
        input_hash = conn.execute(f"""
            SELECT MD5(STRING_AGG(COALESCE(RREL3,''), '' ORDER BY RREL3, RREC3, RREL6))
            FROM read_csv('{input_path}', all_varchar=true, header=true, quote='"')
        """).fetchone()[0]

        output_hash = conn.execute(f"""
            SELECT MD5(STRING_AGG(COALESCE(RREL3,''), '' ORDER BY RREL3, RREC3, RREL6))
            FROM read_csv('{output_path}', all_varchar=true, header=true, quote='"')
        """).fetchone()[0]

        if input_hash != output_hash:
            issues.append(f"CHECKSUM MISMATCH")
            print("✗")
        else:
            print("✓")

        conn.close()
        elapsed = time.time() - start

        if len(issues) == 0:
            print(f"\n  ✓ {country} PASSED - {input_rows:,} rows in {elapsed:.1f}s\n")
        else:
            print(f"\n  ✗ {country} FAILED:")
            for issue in issues:
                print(f"    - {issue}")
            print()

    except Exception as e:
        elapsed = time.time() - start
        print(f"\n  ✗ {country} ERROR: {str(e)[:80]}\n")

print("=" * 90)
print("DONE")
print("=" * 90)
