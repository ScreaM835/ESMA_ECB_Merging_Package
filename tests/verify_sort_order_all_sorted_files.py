# =============================================================================
# VERIFICATION: Check sort order for ALL sorted country files
# (Derived from notebook verification cell)
# =============================================================================
import os
import time
import duckdb

OUTPUT_DIR = os.environ.get("ECB_ESMA_STAGE4_OUTPUT_DIR", r"D:\ECB_ESMA_BY_COUNTRY_SORTED")

print("=" * 80)
print("VERIFICATION: Checking sort order for ALL sorted country files")
print("Sort key: RREL3 (Loan) → RREC3 (Collateral) → RREL6 (Date)")
print("=" * 80)
print()

# Get all sorted files
sorted_files = []
for fname in os.listdir(OUTPUT_DIR):
    if fname.endswith('.csv'):
        filepath = os.path.join(OUTPUT_DIR, fname)
        size_gb = os.path.getsize(filepath) / (1024**3)
        sorted_files.append({
            'country': fname.replace('.csv', ''),
            'filepath': filepath,
            'size_gb': size_gb
        })

# Sort by size (smallest first for faster initial feedback)
sorted_files.sort(key=lambda x: x['size_gb'])

results = []
total_start = time.time()

for i, f in enumerate(sorted_files):
    print(f"[{i+1}/{len(sorted_files)}] {f['country']} ({f['size_gb']:.2f} GB)...", end=" ", flush=True)

    start = time.time()

    try:
        conn = duckdb.connect(':memory:')

        # Count rows
        count_query = f"""
            SELECT COUNT(*) FROM read_csv('{f['filepath']}', all_varchar=true, header=true, quote='"')
        """
        row_count = conn.execute(count_query).fetchone()[0]

        # Check sort order violations
        verify_query = f"""
            WITH sorted_check AS (
                SELECT 
                    RREL3, RREC3, RREL6,
                    LAG(RREL3) OVER () as prev_rrel3,
                    LAG(RREC3) OVER () as prev_rrec3,
                    LAG(RREL6) OVER () as prev_rrel6
                FROM read_csv('{f['filepath']}', all_varchar=true, header=true, quote='"')
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
        violations = conn.execute(verify_query).fetchone()[0]

        conn.close()

        elapsed = time.time() - start

        if violations == 0:
            print(f"✓ PASS ({row_count:,} rows, {elapsed:.1f}s)")
            results.append({
                'country': f['country'],
                'status': 'PASS',
                'rows': row_count,
                'violations': 0,
                'time': elapsed,
            })
        else:
            print(f"✗ FAIL - {violations:,} violations!")
            results.append({
                'country': f['country'],
                'status': 'FAIL',
                'rows': row_count,
                'violations': violations,
                'time': elapsed
            })

    except Exception as e:
        elapsed = time.time() - start
        print(f"✗ ERROR: {str(e)[:50]}")
        results.append({
            'country': f['country'],
            'status': 'ERROR',
            'error': str(e),
            'time': elapsed
        })

total_elapsed = time.time() - total_start

print()
print("=" * 80)
print("SUMMARY")
print("=" * 80)

passed = sum(1 for r in results if r['status'] == 'PASS')
failed = sum(1 for r in results if r['status'] == 'FAIL')
errors = sum(1 for r in results if r['status'] == 'ERROR')
total_rows = sum(r.get('rows', 0) for r in results)

print(f"Total countries: {len(results)}")
print(f"  ✓ Passed: {passed}")
print(f"  ✗ Failed: {failed}")
print(f"  ⚠ Errors: {errors}")
print(f"Total rows verified: {total_rows:,}")
print(f"Total verification time: {total_elapsed/60:.1f} minutes")

if failed > 0:
    print("\nFailed countries:")
    for r in results:
        if r['status'] == 'FAIL':
            print(f"  {r['country']}: {r['violations']:,} violations")

if errors > 0:
    print("\nError details:")
    for r in results:
        if r['status'] == 'ERROR':
            print(f"  {r['country']}: {r['error']}")

print()
print("=" * 80)
