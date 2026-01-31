"""
Stage 1 — ESMA UE + Collateral merge

This module contains the extracted (notebook-origin) logic from `merge_ue_collateral.ipynb`.

Core methodology is preserved:
- Discover UE and Collateral CSVs in a folder
- Auto-detect merge keys (*L2 in UE ↔ *C2 in Collateral)
- Left-join UE to Collateral and write merged files

Note: Default paths are the original notebook defaults; override via function arguments
or your own orchestration code/scripts.
"""

from __future__ import annotations

import os
import re
import time
from collections import Counter
from typing import Dict, List, Optional, Tuple

import pandas as pd
from tqdm import tqdm


def parse_filename(filename):
    """
    Parse ESMA filename to extract components.

    Filename format: 1_<ASSET_TYPE>_<CATEGORY>_<IDENTIFIER>_<DATE>_<SEQUENCE>.csv
    Example: 1_RMB_UE_213800WQJJDCAN4BCO57N201901_2021-04-30_29907.csv

    Returns:
        dict with keys: asset_type, category, identifier, date, sequence, filename
        None if filename doesn't match expected pattern
    """
    pattern = r'^1_(\w+)_(UE|Collateral)_(.+)_(\d{4}-\d{2}-\d{2})_(\d+)\.csv$'
    match = re.match(pattern, filename)

    if match:
        return {
            'asset_type': match.group(1),
            'category': match.group(2),
            'identifier': match.group(3),
            'date': match.group(4),
            'sequence': match.group(5),
            'filename': filename
        }
    return None


def create_merged_filename(ue_filename):
    """
    Convert UE filename to merged filename.

    Example: 1_RMB_UE_xxx.csv -> 1_RMB_UE_Collateral_xxx.csv
    """
    return ue_filename.replace('_UE_', '_UE_Collateral_')


def detect_merge_keys(ue_columns, collateral_columns):
    """
    Auto-detect the correct merge key columns based on available columns.

    Looks for columns ending in 'L2' (UE) and 'C2' (Collateral) that share
    the same prefix pattern (e.g., RREL2/RREC2, NPEL2/NPEC2, CRPL2/CRPC2).

    Returns:
        tuple: (ue_key, collateral_key) or (None, None) if no match found
    """
    # Find all potential key columns (ending in L2 or C2, max 6 chars)
    ue_l2_cols = [c for c in ue_columns if c.endswith('L2') and len(c) <= 6]
    coll_c2_cols = [c for c in collateral_columns if c.endswith('C2') and len(c) <= 6]

    # Try to match by prefix pattern
    # RREL2 -> RRE -> RREC2
    # NPEL2 -> NPE -> NPEC2
    # CRPL2 -> CRP -> CRPC2
    for ue_col in ue_l2_cols:
        prefix = ue_col[:-2]  # Remove 'L2' -> e.g., 'RRE', 'NPE', 'CRP'
        expected_coll = prefix[:-1] + 'C2'  # Replace last char with 'C2'

        if expected_coll in coll_c2_cols:
            return ue_col, expected_coll

    return None, None


def get_columns_to_drop(collateral_columns, collateral_key):
    """
    Determine which columns to drop from collateral before merge.

    Drops:
    - Sec_Id and Pool_Cutoff_Date (already in UE)
    - Security identifier columns (*C1) to avoid duplication with *L1
    """
    cols_to_drop = ['Sec_Id', 'Pool_Cutoff_Date']

    # Find and drop *C1 columns (security identifiers)
    c1_cols = [c for c in collateral_columns if c.endswith('C1') and len(c) <= 6]
    cols_to_drop.extend(c1_cols)

    # Return only columns that actually exist
    return [c for c in cols_to_drop if c in collateral_columns]


def merge_ue_collateral(ue_path, collateral_path):
    """
    Merge a UE file with its corresponding Collateral file.

    Process:
    1. Load both files
    2. Auto-detect the correct merge key columns
    3. Convert merge keys to string (prevents type mismatch errors)
    4. Remove duplicate metadata columns from collateral
    5. Perform left join on loan identifier

    Args:
        ue_path: Full path to UE CSV file
        collateral_path: Full path to Collateral CSV file

    Returns:
        tuple: (merged_dataframe, statistics_dict)

    Raises:
        ValueError: If no valid merge keys can be detected
    """
    # Load files with low_memory=False to ensure consistent dtypes
    ue_df = pd.read_csv(ue_path, low_memory=False)
    collateral_df = pd.read_csv(collateral_path, low_memory=False)

    # Detect merge keys based on column names
    ue_key, collateral_key = detect_merge_keys(
        ue_df.columns.tolist(),
        collateral_df.columns.tolist()
    )

    if ue_key is None:
        ue_l2 = [c for c in ue_df.columns if 'L2' in c]
        coll_c2 = [c for c in collateral_df.columns if 'C2' in c]
        raise ValueError(
            f"Cannot detect merge keys. "
            f"UE *L2 columns: {ue_l2}, "
            f"Collateral *C2 columns: {coll_c2}"
        )

    # Convert merge keys to string to prevent type mismatch
    # (some files have numeric IDs, others have string IDs)
    ue_df[ue_key] = ue_df[ue_key].astype(str)
    collateral_df[collateral_key] = collateral_df[collateral_key].astype(str)

    # Prepare collateral for merge - remove duplicate columns
    cols_to_drop = get_columns_to_drop(collateral_df.columns.tolist(), collateral_key)
    collateral_for_merge = collateral_df.drop(columns=cols_to_drop)

    # Perform left join
    # Left join preserves all UE rows; collateral columns are NaN if no match
    merged_df = pd.merge(
        ue_df,
        collateral_for_merge,
        left_on=ue_key,
        right_on=collateral_key,
        how='left',
        suffixes=('', '_collateral')
    )

    # Calculate statistics
    stats = {
        'ue_rows': len(ue_df),
        'collateral_rows': len(collateral_df),
        'merged_rows': len(merged_df),
        'merged_cols': len(merged_df.columns),
        'matched_rows': merged_df[collateral_key].notna().sum(),
        'unmatched_rows': merged_df[collateral_key].isna().sum(),
        'merge_keys': f"{ue_key}={collateral_key}"
    }

    return merged_df, stats


def find_matching_pairs(csv_folder: str) -> List[Dict]:
    """
    Discover UE/Collateral file pairs in `csv_folder` using the original notebook logic.

    Returns:
        List of dicts: {"ue": <parsed UE dict>, "collateral": <parsed Collateral dict>}
    """
    # Get all CSV files starting with '1_'
    all_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv') and f.startswith('1_')]

    # Separate UE and Collateral files
    ue_files = [f for f in all_files if '_UE_' in f]
    collateral_files = [f for f in all_files if '_Collateral_' in f]

    # Parse all filenames
    ue_parsed = [parse_filename(f) for f in ue_files]
    ue_parsed = [p for p in ue_parsed if p is not None]  # Remove failed parses

    collateral_parsed = [parse_filename(f) for f in collateral_files]
    collateral_parsed = [p for p in collateral_parsed if p is not None]

    # Create lookup dictionary for collateral files
    # Key: (asset_type, identifier, date) -> ensures we match same asset type
    collateral_lookup = {
        (p['asset_type'], p['identifier'], p['date']): p
        for p in collateral_parsed
    }

    # Find matching pairs
    matching_pairs = []
    for ue in ue_parsed:
        key = (ue['asset_type'], ue['identifier'], ue['date'])
        if key in collateral_lookup:
            matching_pairs.append({
                'ue': ue,
                'collateral': collateral_lookup[key]
            })

    return matching_pairs


def batch_merge_ue_collateral(
    csv_folder: str,
    output_folder: Optional[str] = None,
    show_progress: bool = True,
):
    """
    Batch merge all discovered UE/Collateral pairs in `csv_folder` and write merged CSVs.

    This wraps the notebook execution cells without changing the core merge methodology.

    Returns:
        dict with keys:
          - successful (int)
          - failed (int)
          - failed_pairs (list)
          - all_stats (list)
          - elapsed_seconds (float)
          - output_folder (str)
    """
    if output_folder is None:
        output_folder = os.path.join(csv_folder, "ESMA_UE_Collat_Merged")

    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    matching_pairs = find_matching_pairs(csv_folder)

    # Track results
    successful = 0
    failed = 0
    failed_pairs = []
    all_stats = []

    start_time = time.time()

    iterator = matching_pairs
    if show_progress:
        iterator = tqdm(matching_pairs, desc="Merging files")

    for pair in iterator:
        ue_filename = pair['ue']['filename']
        collateral_filename = pair['collateral']['filename']

        ue_path = os.path.join(csv_folder, ue_filename)
        collateral_path = os.path.join(csv_folder, collateral_filename)

        try:
            # Merge the files
            merged_df, stats = merge_ue_collateral(ue_path, collateral_path)

            # Create output filename and save
            merged_filename = create_merged_filename(ue_filename)
            output_path = os.path.join(output_folder, merged_filename)
            merged_df.to_csv(output_path, index=False)

            # Record statistics
            stats['filename'] = merged_filename
            stats['asset_type'] = pair['ue']['asset_type']
            all_stats.append(stats)

            successful += 1

        except Exception as e:
            failed += 1
            failed_pairs.append({
                'ue': ue_filename,
                'collateral': collateral_filename,
                'error': str(e)
            })

    elapsed_time = time.time() - start_time

    return {
        'successful': successful,
        'failed': failed,
        'failed_pairs': failed_pairs,
        'all_stats': all_stats,
        'elapsed_seconds': elapsed_time,
        'output_folder': output_folder,
    }


def summarize_batch_merge(all_stats: List[Dict], failed_pairs: List[Dict]):
    """
    Produce the same high-level summary statistics as the notebook's reporting cells.

    This helper is optional and does not affect the merge outputs.
    """
    summary = {}

    if all_stats:
        success_by_type = Counter(s['asset_type'] for s in all_stats)
        keys_used = Counter(s['merge_keys'] for s in all_stats)

        total_ue_rows = sum(s['ue_rows'] for s in all_stats)
        total_merged_rows = sum(s['merged_rows'] for s in all_stats)
        total_matched = sum(s['matched_rows'] for s in all_stats)

        summary['success_by_type'] = dict(sorted(success_by_type.items()))
        summary['keys_used'] = dict(keys_used.most_common())
        summary['row_statistics'] = {
            'total_ue_rows_processed': total_ue_rows,
            'total_merged_rows': total_merged_rows,
            'rows_with_collateral_match': total_matched,
        }

    if failed_pairs:
        error_types = Counter(p['error'][:80] for p in failed_pairs)
        summary['failed'] = {
            'count': len(failed_pairs),
            'error_summary': dict(error_types.most_common()),
        }

    return summary
