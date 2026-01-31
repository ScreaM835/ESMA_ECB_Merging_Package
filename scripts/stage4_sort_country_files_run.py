# =============================================================================
# Stage 4 runner: Sort country files using DuckDB
# =============================================================================
import os
import time
from datetime import datetime

from esma_ecb_merging_sorting.sort_country_files import SORT_COLUMNS, discover_country_files, sort_country_file

# Input: country files from Stage 3


def main():
    INPUT_DIR = os.environ.get("ECB_ESMA_STAGE3_OUTPUT_DIR", r"D:\ECB_ESMA_BY_COUNTRY_ALL")

    # Output: sorted country files
    OUTPUT_DIR = os.environ.get("ECB_ESMA_STAGE4_OUTPUT_DIR", r"D:\ECB_ESMA_BY_COUNTRY_SORTED")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # =============================================================================
    # CELL 2: Discover Country Files to Sort
    # =============================================================================
    country_files = discover_country_files(INPUT_DIR)

    print(f"Found {len(country_files)} country files to sort:")
    print()
    total_size = 0
    for f in country_files:
        print(f"  {f['country']}: {f['size_gb']:.2f} GB")
        total_size += f['size_gb']
    print(f"\nTotal size: {total_size:.2f} GB")

    # =============================================================================
    # CELL 6: Sort All Country Files (Production Run)
    # =============================================================================
    # CAUTION: This may take many hours for large files!
    # Recommend running overnight.

    RUN_ALL = True  # Set to True to process all files

    if RUN_ALL and country_files:
        print("="*60)
        print("SORTING ALL COUNTRY FILES")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        print()

        results = []
        total_start = time.time()

        for i, f in enumerate(country_files):
            output_path = os.path.join(OUTPUT_DIR, f['filename'])

            # Skip if already sorted
            if os.path.exists(output_path):
                print(f"[{i+1}/{len(country_files)}] {f['country']}: Already exists, skipping")
                results.append({'country': f['country'], 'status': 'skipped'})
                continue

            print(f"[{i+1}/{len(country_files)}] {f['country']}: {f['size_gb']:.2f} GB")

            result = sort_country_file(
                f['filepath'],
                output_path,
                SORT_COLUMNS
            )

            if result.get('success'):
                print(f"  ✓ Success: {result.get('rows_sorted', 0):,} rows in {result.get('elapsed_seconds', 0):.1f}s")
                results.append({'country': f['country'], 'status': 'success', **result})
            else:
                print(f"  ✗ Failed: {result.get('error')}")
                results.append({'country': f['country'], 'status': 'failed', **result})

        total_elapsed = time.time() - total_start

        print()
        print("="*60)
        print("SORTING COMPLETE")
        print("="*60)
        print(f"Total time: {total_elapsed/60:.1f} minutes")

        successes = sum(1 for r in results if r['status'] == 'success')
        skipped = sum(1 for r in results if r['status'] == 'skipped')
        failed = sum(1 for r in results if r['status'] == 'failed')

        print(f"Success: {successes}, Skipped: {skipped}, Failed: {failed}")

    # =============================================================================
    # CELL 7: Summary Report
    # =============================================================================
    print("="*60)
    print("SORTED FILES SUMMARY")
    print("="*60)
    print()

    sorted_files = []
    if os.path.exists(OUTPUT_DIR):
        for fname in os.listdir(OUTPUT_DIR):
            if fname.endswith('.csv'):
                filepath = os.path.join(OUTPUT_DIR, fname)
                size_gb = os.path.getsize(filepath) / (1024**3)
                sorted_files.append({
                    'country': fname.replace('.csv', ''),
                    'size_gb': size_gb
                })

    sorted_files.sort(key=lambda x: x['size_gb'], reverse=True)

    print(f"Sorted files in {OUTPUT_DIR}:")
    print()
    total_sorted = 0
    for f in sorted_files:
        print(f"  {f['country']}: {f['size_gb']:.2f} GB")
        total_sorted += f['size_gb']

    print(f"\nTotal sorted: {len(sorted_files)} files, {total_sorted:.2f} GB")

    # Compare to input
    print(f"\nOriginal files: {len(country_files)}")
    remaining = len(country_files) - len(sorted_files)
    if remaining > 0:
        print(f"Remaining to sort: {remaining}")



if __name__ == '__main__':
    main()
