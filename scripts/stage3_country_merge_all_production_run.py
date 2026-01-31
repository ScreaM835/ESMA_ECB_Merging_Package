# =============================================================================
# Stage 3 runner: Country merge (pool-level -> country-level)
# =============================================================================
import gc
import json
import os
import time
import warnings
from datetime import datetime

import numpy as np
import pandas as pd

from esma_ecb_merging_sorting.country_merge_all_production import detect_country, merge_country_files

# Suppress fragmentation warning (cosmetic - doesn't affect correctness)


def main():
    warnings.filterwarnings('ignore', message='DataFrame is highly fragmented')

    # Input: merged pool files from Stage 2
    INPUT_DIR = os.environ.get("ECB_ESMA_STAGE2_OUTPUT_DIR", r"D:\ECB_ESMA_MERGED")

    # Output: NEW folder for all country files
    OUTPUT_DIR = os.environ.get("ECB_ESMA_STAGE3_OUTPUT_DIR", r"D:\ECB_ESMA_BY_COUNTRY_ALL")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Chunk size for memory-efficient processing
    CHUNK_SIZE = 100000

    print(f"Input directory: {INPUT_DIR}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Chunk size: {CHUNK_SIZE:,} rows")
    print()
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # =============================================================================
    # CELL 3: Scan All Files and Build Country Index
    # =============================================================================
    print("Scanning all merged files to build country index...")
    print()

    file_index = []  # List of {filepath, pool_id, folder, country, size_mb}

    for folder in ['matched', 'ecb_only', 'esma_only']:
        folder_path = os.path.join(INPUT_DIR, folder)
        if not os.path.exists(folder_path):
            print(f"Warning: {folder_path} does not exist, skipping...")
            continue

        files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
        print(f"Scanning {folder}: {len(files)} files...")

        for fname in files:
            filepath = os.path.join(folder_path, fname)
            file_size = os.path.getsize(filepath)

            # Read sample to detect country
            df_sample = pd.read_csv(filepath, nrows=100)
            pool_id = fname[:-4]  # Remove .csv extension
            country = detect_country(df_sample, pool_id)

            file_index.append({
                'filepath': filepath,
                'filename': fname,
                'pool_id': pool_id,
                'folder': folder,
                'country': country,
                'size_mb': file_size / (1024 * 1024)
            })

    print(f"\nTotal files indexed: {len(file_index)}")

    # Group by country and build statistics
    country_stats = {}
    for f in file_index:
        c = f['country']
        if c not in country_stats:
            country_stats[c] = {'count': 0, 'size_mb': 0, 'matched': 0, 'ecb_only': 0, 'esma_only': 0}
        country_stats[c]['count'] += 1
        country_stats[c]['size_mb'] += f['size_mb']
        country_stats[c][f['folder']] += 1

    print("\nCountry distribution:")
    print("-" * 90)
    for country in sorted(country_stats.keys()):
        s = country_stats[country]
        print(f"{country}: {s['count']} files ({s['size_mb']/1024:.1f} GB) | matched={s['matched']}, ecb={s['ecb_only']}, esma={s['esma_only']}")

    print()
    print(f"Total countries to process: {len(country_stats)}")

    # =============================================================================
    # CELL 5: Process ALL Countries (RESUME SAFE)
    # =============================================================================
    print("="*90)
    print("PROCESSING ALL COUNTRIES (RESUME SAFE)")
    print("="*90)
    print()

    # Get all countries to process
    all_countries = sorted(country_stats.keys())
    print(f"Countries to process: {all_countries}")
    print(f"Total: {len(all_countries)} countries")
    print()

    # Check for already completed countries
    already_done = []
    for cc in all_countries:
        check_path = os.path.join(OUTPUT_DIR, f"{cc}.csv")
        if os.path.exists(check_path):
            already_done.append(cc)

    if already_done:
        print(f"✓ Already completed (will skip): {already_done}")
        print(f"  Remaining to process: {len(all_countries) - len(already_done)} countries")
    print()

    # Track results
    merge_results = []
    skipped_countries = []
    failed_countries = []

    overall_start = time.time()

    for country_idx, country_code in enumerate(all_countries):
        print(f"\n[{country_idx+1}/{len(all_countries)}] Starting {country_code}...")

        try:
            # Get files for this country
            country_files = [f for f in file_index if f['country'] == country_code]

            # Merge files (returns None if skipped due to existing file)
            result = merge_country_files(
                country_code=country_code,
                country_files=country_files,
                output_dir=OUTPUT_DIR,
                chunk_size=CHUNK_SIZE
            )

            if result is None:
                skipped_countries.append(country_code)
            else:
                merge_results.append(result)

        except KeyboardInterrupt:
            print(f"\n\n⚠️ INTERRUPTED during {country_code}!")
            print(f"   Cleaning up incomplete temp file...")
            temp_path = os.path.join(OUTPUT_DIR, f"{country_code}.csv.tmp")
            if os.path.exists(temp_path):
                os.remove(temp_path)
                print(f"   ✓ Removed {temp_path}")
            print(f"\n   Resume safe: Re-run this script to continue from {country_code}")
            raise  # Re-raise to stop execution

        except Exception as e:
            print(f"\n  ✗ ERROR processing {country_code}: {str(e)}")
            failed_countries.append({'country': country_code, 'error': str(e)})
            # Clean up temp file on error too
            temp_path = os.path.join(OUTPUT_DIR, f"{country_code}.csv.tmp")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            continue

    overall_elapsed = time.time() - overall_start

    print("\n" + "="*90)
    print("ALL COUNTRIES PROCESSED")
    print("="*90)
    print(f"\n  New: {len(merge_results)}, Skipped: {len(skipped_countries)}, Failed: {len(failed_countries)}")

    # =============================================================================
    # CELL 6: Summary Report
    # =============================================================================
    print("="*90)
    print("FINAL SUMMARY REPORT")
    print("="*90)
    print()

    print(f"Total processing time: {overall_elapsed/60:.1f} minutes")
    print(f"Output directory: {OUTPUT_DIR}")
    print()

    # Success summary
    print(f"✓ Newly processed: {len(merge_results)} countries")
    print(f"⏭️  Skipped (already done): {len(skipped_countries)} countries")
    if failed_countries:
        print(f"✗ Failed: {len(failed_countries)} countries")
    print()

    # Detailed results table
    print("-"*90)
    print(f"{'Country':<10} {'Files':<8} {'Rows':<15} {'Columns':<10} {'Size (GB)':<12} {'Time (s)':<10}")
    print("-"*90)

    total_rows_all = 0
    total_size_all = 0

    for r in merge_results:
        print(f"{r['country']:<10} {r['files_merged']:<8} {r['total_rows']:>12,} {r['columns']:<10} {r['size_gb']:<12.2f} {r['time_seconds']:<10.1f}")
        total_rows_all += r['total_rows']
        total_size_all += r['size_gb']

    print("-"*90)
    print(f"{'TOTAL':<10} {len(file_index):<8} {total_rows_all:>12,} {'':<10} {total_size_all:<12.2f} {overall_elapsed:<10.1f}")
    print()

    # Failed countries details
    if failed_countries:
        print("\nFAILED COUNTRIES:")
        for f in failed_countries:
            print(f"  {f['country']}: {f['error']}")
        print()

    # List output files
    print("\nOUTPUT FILES:")
    for r in merge_results:
        print(f"  {r['output_path']}")

    print()
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # =============================================================================
    # CELL 7: Save Merge Log to JSON
    # =============================================================================
    log_data = {
        'run_timestamp': datetime.now().isoformat(),
        'input_dir': INPUT_DIR,
        'output_dir': OUTPUT_DIR,
        'total_files_processed': len(file_index),
        'countries_processed': len(merge_results),
        'countries_failed': len(failed_countries),
        'total_rows': total_rows_all,
        'total_size_gb': total_size_all,
        'total_time_seconds': overall_elapsed,
        'results': merge_results,
        'failures': failed_countries,
        'country_stats': {k: dict(v) for k, v in country_stats.items()}
    }

    log_path = os.path.join(OUTPUT_DIR, 'merge_log.json')
    with open(log_path, 'w') as f:
        json.dump(log_data, f, indent=2, default=str)

    print(f"Merge log saved to: {log_path}")



if __name__ == '__main__':
    main()
