# =============================================================================
# COMPREHENSIVE DATA INTEGRITY CHECK: Input vs Output Comparison
# (Derived from notebook integrity-check cell)
# =============================================================================
import os
import time
import duckdb

INPUT_DIR = os.environ.get("ECB_ESMA_STAGE3_OUTPUT_DIR", r"D:\ECB_ESMA_BY_COUNTRY_ALL")
OUTPUT_DIR = os.environ.get("ECB_ESMA_STAGE4_OUTPUT_DIR", r"D:\ECB_ESMA_BY_COUNTRY_SORTED")

print("=" * 90)
print("COMPREHENSIVE DATA INTEGRITY CHECK: Comparing Input vs Output")
print("=" * 90)
print()

# Get matched input/output pairs
files_to_check = []
for fname in os.listdir(OUTPUT_DIR):
    if fname.endswith('.csv'):
        input_path = os.path.join(INPUT_DIR, fname)
        output_path = os.path.join(OUTPUT_DIR, fname)
        if os.path.exists(input_path):
            size_gb = os.path.getsize(output_path) / (1024**3)
            files_to_check.append({
                'country': fname.replace('.csv', ''),
                'input': input_path,
                'output': output_path,
                'size_gb': size_gb
            })

# Sort smallest first
files_to_check.sort(key=lambda x: x['size_gb'])

print(f"Checking {len(files_to_check)} countries for data integrity...")
print()

results = []
total_start = time.time()

for i, f in enumerate(files_to_check):
    print(f"[{i+1}/{len(files_to_check)}] {f['country']} ({f['size_gb']:.2f} GB)")

    start = time.time()
    issues = []

    try:
        conn = duckdb.connect(':memory:')
        conn.execute("SET threads = 4")

        # 1. ROW COUNT CHECK
        input_rows = conn.execute(f"""
            SELECT COUNT(*) FROM read_csv('{f['input']}', all_varchar=true, header=true, quote='"')
        """).fetchone()[0]

        output_rows = conn.execute(f"""
            SELECT COUNT(*) FROM read_csv('{f['output']}', all_varchar=true, header=true, quote='"')
        """).fetchone()[0]

        if input_rows != output_rows:
            issues.append(f"ROW COUNT MISMATCH: input={input_rows:,}, output={output_rows:,}, diff={input_rows - output_rows:,}")

        # 2. COLUMN COUNT CHECK
        input_cols = conn.execute(f"""
            SELECT * FROM read_csv('{f['input']}', all_varchar=true, header=true, quote='"') LIMIT 0
        """).description
        input_col_count = len(input_cols)
        input_col_names = [c[0] for c in input_cols]

        output_cols = conn.execute(f"""
            SELECT * FROM read_csv('{f['output']}', all_varchar=true, header=true, quote='"') LIMIT 0
        """).description
        output_col_count = len(output_cols)
        output_col_names = [c[0] for c in output_cols]

        if input_col_count != output_col_count:
            issues.append(f"COLUMN COUNT MISMATCH: input={input_col_count}, output={output_col_count}")

        if input_col_names != output_col_names:
            issues.append(f"COLUMN NAMES DIFFER")

        # 3. UNIQUE KEY COUNT CHECK (RREL3, RREC3, RREL6)
        input_unique_keys = conn.execute(f"""
            SELECT COUNT(DISTINCT RREL3 || '|' || COALESCE(RREC3,'') || '|' || COALESCE(RREL6,''))
            FROM read_csv('{f['input']}', all_varchar=true, header=true, quote='"')
        """).fetchone()[0]

        output_unique_keys = conn.execute(f"""
            SELECT COUNT(DISTINCT RREL3 || '|' || COALESCE(RREC3,'') || '|' || COALESCE(RREL6,''))
            FROM read_csv('{f['output']}', all_varchar=true, header=true, quote='"')
        """).fetchone()[0]

        if input_unique_keys != output_unique_keys:
            issues.append(f"UNIQUE KEY COUNT MISMATCH: input={input_unique_keys:,}, output={output_unique_keys:,}")

        # 4. DISTINCT VALUES CHECK for key columns
        for col in ['RREL3', 'RREC3', 'RREL6', 'RREL1', 'source']:
            if col in input_col_names:
                input_distinct = conn.execute(f"""
                    SELECT COUNT(DISTINCT {col}) FROM read_csv('{f['input']}', all_varchar=true, header=true, quote='"')
                """).fetchone()[0]

                output_distinct = conn.execute(f"""
                    SELECT COUNT(DISTINCT {col}) FROM read_csv('{f['output']}', all_varchar=true, header=true, quote='"')
                """).fetchone()[0]

                if input_distinct != output_distinct:
                    issues.append(f"DISTINCT {col} MISMATCH: input={input_distinct:,}, output={output_distinct:,}")

        # 5. CHECKSUM on text columns (hash of all RREL3 values)
        input_hash = conn.execute(f"""
            SELECT MD5(STRING_AGG(COALESCE(RREL3,''), '' ORDER BY RREL3, RREC3, RREL6))
            FROM read_csv('{f['input']}', all_varchar=true, header=true, quote='"')
        """).fetchone()[0]

        output_hash = conn.execute(f"""
            SELECT MD5(STRING_AGG(COALESCE(RREL3,''), '' ORDER BY RREL3, RREC3, RREL6))
            FROM read_csv('{f['output']}', all_varchar=true, header=true, quote='"')
        """).fetchone()[0]

        if input_hash != output_hash:
            issues.append(f"RREL3 CHECKSUM MISMATCH (data may be corrupted)")

        # 6. SPOT CHECK: Compare random sample of full rows
        sample_keys = conn.execute(f"""
            SELECT DISTINCT RREL3 FROM read_csv('{f['input']}', all_varchar=true, header=true, quote='"')
            WHERE RREL3 IS NOT NULL AND RREL3 != ''
            USING SAMPLE 5
        """).fetchall()

        for (key,) in sample_keys:
            key_escaped = key.replace("'", "''")

            input_sample = conn.execute(f"""
                SELECT * FROM read_csv('{f['input']}', all_varchar=true, header=true, quote='"')
                WHERE RREL3 = '{key_escaped}'
                ORDER BY RREC3, RREL6
            """).fetchall()

            output_sample = conn.execute(f"""
                SELECT * FROM read_csv('{f['output']}', all_varchar=true, header=true, quote='"')
                WHERE RREL3 = '{key_escaped}'
                ORDER BY RREC3, RREL6
            """).fetchall()

            if input_sample != output_sample:
                issues.append(f"SPOT CHECK FAILED: Data differs for RREL3='{key[:20]}...'")
                break

        conn.close()
        elapsed = time.time() - start

        if len(issues) == 0:
            print(f"    ✓ PASS - {input_rows:,} rows, {input_col_count} cols, {elapsed:.1f}s")
            results.append({
                'country': f['country'],
                'status': 'PASS',
                'rows': input_rows,
                'cols': input_col_count,
                'unique_keys': input_unique_keys,
                'time': elapsed
            })
        else:
            print(f"    ✗ FAIL - {len(issues)} issue(s):")
            for issue in issues:
                print(f"      - {issue}")
            results.append({
                'country': f['country'],
                'status': 'FAIL',
                'issues': issues,
                'time': elapsed
            })

    except Exception as e:
        elapsed = time.time() - start
        print(f"    ✗ ERROR: {str(e)[:60]}")
        results.append({
            'country': f['country'],
            'status': 'ERROR',
            'error': str(e),
            'time': elapsed
        })

total_elapsed = time.time() - total_start

print()
print("=" * 90)
print("DATA INTEGRITY SUMMARY")
print("=" * 90)

passed = sum(1 for r in results if r['status'] == 'PASS')
failed = sum(1 for r in results if r['status'] == 'FAIL')
errors = sum(1 for r in results if r['status'] == 'ERROR')
total_rows = sum(r.get('rows', 0) for r in results if r['status'] == 'PASS')

print(f"Total countries: {len(results)}")
print(f"  ✓ Passed: {passed}")
print(f"  ✗ Failed: {failed}")
print(f"  ⚠ Errors: {errors}")
print(f"Total rows verified: {total_rows:,}")
print(f"Total time: {total_elapsed/60:.1f} minutes")

if passed == len(results):
    print()
    print("🎉 ALL DATA INTEGRITY CHECKS PASSED!")
    print("   - Row counts match")
    print("   - Column counts match")
    print("   - Unique key counts match")
    print("   - Distinct value counts match")
    print("   - Checksums match")
    print("   - Spot checks passed")

if failed > 0:
    print()
    print("⚠️  FAILED COUNTRIES - DATA MAY BE CORRUPTED:")
    for r in results:
        if r['status'] == 'FAIL':
            print(f"\n  {r['country']}:")
            for issue in r['issues']:
                print(f"    - {issue}")

print()
print("=" * 90)
