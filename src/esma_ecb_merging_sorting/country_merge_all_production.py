"""
Stage 3 — Country merge (pool-level -> country-level)

This module contains the extracted (notebook-origin) logic from `country_merge_all_production.ipynb`.

Core methodology is preserved:
- Detect a country code for each pool CSV using a strict priority of fields
- Group pool files by country
- Merge all pools for a country using chunked streaming with a union-of-columns schema
- Resume-safe behaviour via final-file existence checks and atomic temp-file rename
"""

from __future__ import annotations

import gc
import os
import time
import warnings
from datetime import datetime
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

# Suppress fragmentation warning (cosmetic - doesn't affect correctness)
warnings.filterwarnings('ignore', message='DataFrame is highly fragmented')

# Default paths (preserved from notebook; override via scripts/arguments)
INPUT_DIR = r"D:\ECB_ESMA_MERGED"
OUTPUT_DIR = r"D:\ECB_ESMA_BY_COUNTRY_ALL"

# Chunk size for memory-efficient processing
CHUNK_SIZE = 100000


# =============================================================================
# CELL 2: Country Detection Function (VERIFIED - DO NOT MODIFY)
# =============================================================================
def detect_country(df, pool_id):
    """
    Detect country from file data or pool ID.
    Priority order:
    1. RREL81 (Lender Country - ESMA)
    2. RREL84 (Originator Country - ESMA)
    3. RREC6 (Geographic Region NUTS - from AR128)
    4. RREL11 (Geographic Region Obligor NUTS)
    5. AR129 (ECB geographic - sometimes has NUTS codes)
    6. NPEL20/NPEL23 (NPE template country fields)
    7. Pool ID fallback (RMBM/RMBS + 2-letter country)
    """
    # Method 1: RREL81 (clean 2-letter code)
    if 'RREL81' in df.columns:
        vals = df['RREL81'].dropna().astype(str).unique()
        valid = [v for v in vals if len(v) == 2 and v.isalpha()]
        if valid:
            return valid[0].upper()
    
    # Method 2: RREL84 (clean 2-letter code)
    if 'RREL84' in df.columns:
        vals = df['RREL84'].dropna().astype(str).unique()
        valid = [v for v in vals if len(v) == 2 and v.isalpha()]
        if valid:
            return valid[0].upper()
    
    # Method 3: RREC6 (NUTS code - first 2 chars)
    if 'RREC6' in df.columns:
        vals = df['RREC6'].dropna().astype(str).unique()
        for v in vals:
            if len(v) >= 2 and v[:2].isalpha():
                return v[:2].upper()
    
    # Method 4: RREL11 (NUTS code - first 2 chars)
    if 'RREL11' in df.columns:
        vals = df['RREL11'].dropna().astype(str).unique()
        for v in vals:
            if len(v) >= 2 and v[:2].isalpha() and not v.startswith('ND'):
                return v[:2].upper()
    
    # Method 5: AR129 (ECB geographic - check for NUTS pattern)
    if 'AR129' in df.columns:
        vals = df['AR129'].dropna().astype(str).unique()
        for v in vals:
            if len(v) >= 2 and v[:2].isalpha() and not v.startswith('ND'):
                return v[:2].upper()
    
    # Method 6: NPEL20/NPEL23 (NPE template)
    for col in ['NPEL20', 'NPEL23']:
        if col in df.columns:
            vals = df[col].dropna().astype(str).unique()
            valid = [v for v in vals if len(v) == 2 and v.isalpha()]
            if valid:
                return valid[0].upper()
    
    # Method 7: FALLBACK - Extract from pool ID
    if pool_id.startswith('RMBM') or pool_id.startswith('RMBS'):
        country_from_id = pool_id[4:6]
        if country_from_id.isalpha():
            return country_from_id.upper()
    
    return 'UNKNOWN'


# =============================================================================
# CELL 4: Define Country Merge Function (VERIFIED - RESUME SAFE)
# =============================================================================
def merge_country_files(country_code, country_files, output_dir, chunk_size=100000):
    """
    Merge all files for a single country into one CSV.
    Uses chunked streaming to handle large files efficiently.
    
    RESUME SAFE:
    - Skips if final output file already exists
    - Cleans up incomplete .tmp files from interrupted runs
    - Returns None if already completed (for skip tracking)
    
    Args:
        country_code: 2-letter country code (e.g., 'IT', 'DE')
        country_files: List of file info dicts for this country
        output_dir: Directory to write output file
        chunk_size: Number of rows per chunk (default 100000)
    
    Returns:
        dict with merge statistics, or None if skipped (already complete)
    """
    output_path = os.path.join(output_dir, f"{country_code}.csv")
    temp_path = output_path + ".tmp"
    
    # =========================================================================
    # RESUME SAFETY: Check if already completed
    # =========================================================================
    if os.path.exists(output_path):
        file_size_gb = os.path.getsize(output_path) / (1024**3)
        print(f"\n  ⏭️  SKIPPING {country_code}: Already exists ({file_size_gb:.2f} GB)")
        return None  # Signal that this was skipped
    
    # Clean up any incomplete temp files from previous interrupted runs
    if os.path.exists(temp_path):
        print(f"\n  🧹 Cleaning up incomplete temp file for {country_code}...")
        os.remove(temp_path)
    
    print(f"\n{'='*70}")
    print(f"PROCESSING: {country_code}")
    print(f"{'='*70}")
    print(f"Files to merge: {len(country_files)}")
    print(f"Output: {output_path}")
    
    # =========================================================================
    # PHASE 1: Scan all files to determine unified column schema
    # =========================================================================
    print("\nPhase 1: Building unified column schema...")
    all_columns = set()
    
    for f in country_files:
        df_header = pd.read_csv(f['filepath'], nrows=0)
        all_columns.update(df_header.columns.tolist())
    
    # Sort for consistent column ordering
    all_columns_sorted = sorted(list(all_columns))
    print(f"  Unified schema: {len(all_columns_sorted)} columns")
    
    # =========================================================================
    # PHASE 2: Stream-merge all files (chunked I/O)
    # =========================================================================
    print("\nPhase 2: Merging files (chunked streaming)...")
    start_time = time.time()
    total_rows = 0
    first_chunk = True
    
    for file_idx, f in enumerate(country_files):
        file_start = time.time()
        file_rows = 0
        
        print(f"  [{file_idx+1}/{len(country_files)}] [{f['folder']}] {f['filename'][:50]}... ({f['size_mb']:.0f} MB)")
        
        # Process file in chunks - NEVER load full file into memory
        for chunk in pd.read_csv(f['filepath'], chunksize=chunk_size, low_memory=False):
            # Add missing columns with NaN (fast operation)
            for col in all_columns_sorted:
                if col not in chunk.columns:
                    chunk[col] = np.nan
            
            # Reorder to unified schema
            chunk = chunk[all_columns_sorted]
            
            # Append to output (write header only on first chunk)
            chunk.to_csv(temp_path, mode='a', index=False, header=first_chunk)
            first_chunk = False
            
            file_rows += len(chunk)
            
            # Free chunk memory
            del chunk
        
        total_rows += file_rows
        file_elapsed = time.time() - file_start
        print(f"       -> {file_rows:,} rows in {file_elapsed:.1f}s")
        
        # Force garbage collection after each file
        gc.collect()
    
    # Rename temp to final
    os.replace(temp_path, output_path)
    
    total_elapsed = time.time() - start_time
    final_size_gb = os.path.getsize(output_path) / (1024**3)
    
    print(f"\n  ✓ COMPLETE: {country_code}")
    print(f"    Total rows: {total_rows:,}")
    print(f"    Columns: {len(all_columns_sorted)}")
    print(f"    File size: {final_size_gb:.2f} GB")
    print(f"    Time: {total_elapsed:.1f}s")
    
    return {
        'country': country_code,
        'files_merged': len(country_files),
        'total_rows': total_rows,
        'columns': len(all_columns_sorted),
        'size_gb': final_size_gb,
        'time_seconds': total_elapsed,
        'output_path': output_path
    }



def scan_all_files_build_country_index(input_dir: str) -> Tuple[List[Dict], Dict]:
    """
    Scan all merged pool files to build the country index (notebook CELL 3 logic).

    Returns:
        (file_index, country_stats)
    """
    file_index = []  # List of {filepath, pool_id, folder, country, size_mb}

    for folder in ['matched', 'ecb_only', 'esma_only']:
        folder_path = os.path.join(input_dir, folder)
        if not os.path.exists(folder_path):
            continue

        files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

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

    # Group by country and build statistics
    country_stats = {}
    for f in file_index:
        c = f['country']
        if c not in country_stats:
            country_stats[c] = {'count': 0, 'size_mb': 0, 'matched': 0, 'ecb_only': 0, 'esma_only': 0}
        country_stats[c]['count'] += 1
        country_stats[c]['size_mb'] += f['size_mb']
        country_stats[c][f['folder']] += 1

    return file_index, country_stats


def process_all_countries(
    input_dir: str = INPUT_DIR,
    output_dir: str = OUTPUT_DIR,
    chunk_size: int = CHUNK_SIZE,
):
    """
    Process ALL countries using the notebook's production logic (CELLS 3–6),
    expressed as a callable function.

    Returns:
        dict with keys:
          - merge_results
          - skipped_countries
          - failed_countries
          - overall_elapsed_seconds
          - file_index
          - country_stats
    """
    os.makedirs(output_dir, exist_ok=True)

    file_index, country_stats = scan_all_files_build_country_index(input_dir)

    # Get all countries to process
    all_countries = sorted(country_stats.keys())

    # Track results
    merge_results = []
    skipped_countries = []
    failed_countries = []

    overall_start = time.time()

    total_rows_all = 0

    for country_code in all_countries:
        try:
            # Get files for this country
            country_files = [f for f in file_index if f['country'] == country_code]

            # Merge files for this country
            result = merge_country_files(country_code, country_files, output_dir, chunk_size=chunk_size)

            if result is None:
                skipped_countries.append(country_code)
            else:
                merge_results.append(result)
                total_rows_all += result['total_rows']

        except Exception as e:
            failed_countries.append({'country': country_code, 'error': str(e)})

    overall_elapsed = time.time() - overall_start

    return {
        'merge_results': merge_results,
        'skipped_countries': skipped_countries,
        'failed_countries': failed_countries,
        'overall_elapsed_seconds': overall_elapsed,
        'file_index': file_index,
        'country_stats': country_stats,
        'total_rows_all': total_rows_all,
    }
