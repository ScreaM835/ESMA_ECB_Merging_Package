# =============================================================================
# Test: Sort the smallest country file and verify ordering
# (Derived from notebook CELL 4 + CELL 5)
# =============================================================================
import os
import duckdb

from esma_ecb_merging_sorting.sort_country_files import SORT_COLUMNS, discover_country_files, sort_country_file

INPUT_DIR = os.environ.get("ECB_ESMA_STAGE3_OUTPUT_DIR", r"D:\ECB_ESMA_BY_COUNTRY_ALL")
OUTPUT_DIR = os.environ.get("ECB_ESMA_STAGE4_OUTPUT_DIR", r"D:\ECB_ESMA_BY_COUNTRY_SORTED")
os.makedirs(OUTPUT_DIR, exist_ok=True)

country_files = discover_country_files(INPUT_DIR)

# -----------------------------------------------------------------------------
# CELL 4: Test with smallest file
# -----------------------------------------------------------------------------
if country_files:
    test_file = country_files[0]  # Smallest file

    print(f"Testing sort on smallest file: {test_file['country']}")
    print(f"  Input: {test_file['filepath']}")
    print(f"  Size: {test_file['size_gb']:.2f} GB")
    print()

    output_path = os.path.join(OUTPUT_DIR, test_file['filename'])
    print(f"  Output: {output_path}")
    print()
    print("Sorting...")

    result = sort_country_file(
        test_file['filepath'],
        output_path,
        SORT_COLUMNS
    )

    if result['success']:
        print(f"\n✓ SUCCESS")
        print(f"  Rows: {result.get('row_count', result.get('rows_sorted', 0)):,}")
        print(f"  Sort columns: {result['sort_columns']}")
        print(f"  Time: {result['elapsed_seconds']:.1f} seconds")

        output_size = os.path.getsize(output_path) / (1024**3)
        print(f"  Output size: {output_size:.2f} GB")
    else:
        print(f"\n✗ FAILED: {result['error']}")
else:
    raise SystemExit("No country files found to test")

# -----------------------------------------------------------------------------
# CELL 5: Verify sorted output
# -----------------------------------------------------------------------------
if os.path.exists(output_path):
    print(f"\nVerifying sorted file: {output_path}")
    print()

    conn = duckdb.connect(':memory:')

    # Check first 30 rows - RREL3 is Loan ID, RREC3 is Collateral ID, RREL6 is Date
    sample_query = f"""
        SELECT RREL3 as Loan_ID, RREC3 as Collateral_ID, RREL6 as Date, RREL1 as Pool_ID
        FROM read_csv_auto('{output_path}')
        LIMIT 30
    """
    df_sample = conn.execute(sample_query).fetchdf()
    print("First 30 rows (sorted by RREL3 → RREC3 → RREL6):")
    print(df_sample.to_string())
    print()

    # Check sort order violations (RREL3 → RREC3 → RREL6)
    verify_query = f"""
        WITH sorted_check AS (
            SELECT 
                RREL3, RREC3, RREL6,
                LAG(RREL3) OVER () as prev_rrel3,
                LAG(RREC3) OVER () as prev_rrec3,
                LAG(RREL6) OVER () as prev_rrel6
            FROM read_csv_auto('{output_path}')
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
    if violations == 0:
        print(f"✓ SORT VERIFIED: No violations found!")
    else:
        print(f"✗ WARNING: {violations:,} sort order violations found!")

    conn.close()
else:
    raise SystemExit(f"Output file not found: {output_path}")
