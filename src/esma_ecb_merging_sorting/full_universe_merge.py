"""
Stage 2 — Full universe merge (ECB + ESMA)

This module contains the extracted (notebook-origin) logic from `full_universe_merge.ipynb`.

Core methodology is preserved:
- Load ECB gzip files and ESMA merged CSVs
- Harmonise ECB columns to ESMA codes via the ESMA template mapping
- Merge at pool level into three buckets: matched, ecb_only, esma_only
- Use size-aware chunked processing for large pools
- Use atomic writes for resume safety
- Deduplicate ONLY for pools in `POOLS_WITH_OVERLAP` (prefer ESMA rows)

Important:
- The functions here rely on module-level variables (as in the original notebook).
  Call `load_template_mapping()` and `build_file_index()` before running pool processing.
"""

from __future__ import annotations

import gc
import glob
import io
import json
import os
import time
import zlib
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# =============================================================================
# Configuration and Paths (defaults preserved from notebook)
# =============================================================================

# Paths
BASE_PATH = r"c:\Users\jonat\Downloads\_unique_csv_master"
ECB_PATH = os.path.join(BASE_PATH, "ECB_Data", "ECB Data")
ESMA_PATH = os.path.join(BASE_PATH, "ESMA_UE_Collat_Merged")
TEMPLATE_PATH = os.path.join(BASE_PATH, "ESMA Template (2).xlsx")
POOL_MAPPING_PATH = os.path.join(BASE_PATH, "pool_mapping.json")

# OUTPUT on D: drive
OUTPUT_DIR = r"D:\ECB_ESMA_MERGED"
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "_checkpoint.json")
LOG_FILE = os.path.join(OUTPUT_DIR, "_processing_log.txt")

# =============================================================================
# POOLS WITH TEMPORAL OVERLAP (verified via analysis)
# Only these 3 pools have ECB and ESMA data for the same months
# Deduplication is ONLY needed for these pools
# =============================================================================
POOLS_WITH_OVERLAP = {
    'RMBMBE000095100120084',  # 8 overlapping months: 2022-03 to 2023-12
    'RMBMFR000083100220149',  # 4 overlapping months: 2021-05 to 2021-08
    'RMBMNL000185100120109',  # 1 overlapping month: 2024-06
}


# =============================================================================
# Globals populated by template/index initialisation (as in notebook)
# =============================================================================
template_df = None
esma_to_ecb = {}
ecb_to_esma = {}

all_ecb_files = []
all_esma_files = []

ecb_pool_files = defaultdict(list)
esma_pool_files = defaultdict(list)

pool_mapping = {}
matched_pools = {}
matched_ecb_pools = set()
matched_esma_pools = set()
ecb_only_pools = set()
esma_only_pools = set()


def configure_paths(
    base_path: str | None = None,
    ecb_path: str | None = None,
    esma_path: str | None = None,
    template_path: str | None = None,
    pool_mapping_path: str | None = None,
    output_dir: str | None = None,
):
    """
    Configure module-level paths (optional helper).

    This does not change any merge methodology; it only updates the default path variables.
    """
    global BASE_PATH, ECB_PATH, ESMA_PATH, TEMPLATE_PATH, POOL_MAPPING_PATH, OUTPUT_DIR, CHECKPOINT_FILE, LOG_FILE

    if base_path is not None:
        BASE_PATH = base_path
        ECB_PATH = os.path.join(BASE_PATH, "ECB_Data", "ECB Data")
        ESMA_PATH = os.path.join(BASE_PATH, "ESMA_UE_Collat_Merged")
        TEMPLATE_PATH = os.path.join(BASE_PATH, "ESMA Template (2).xlsx")
        POOL_MAPPING_PATH = os.path.join(BASE_PATH, "pool_mapping.json")

    if ecb_path is not None:
        ECB_PATH = ecb_path
    if esma_path is not None:
        ESMA_PATH = esma_path
    if template_path is not None:
        TEMPLATE_PATH = template_path
    if pool_mapping_path is not None:
        POOL_MAPPING_PATH = pool_mapping_path
    if output_dir is not None:
        OUTPUT_DIR = output_dir
        CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "_checkpoint.json")
        LOG_FILE = os.path.join(OUTPUT_DIR, "_processing_log.txt")


def load_template_mapping(template_path: str | None = None):
    """
    Load Template Mapping (ECB <-> ESMA columns).

    This is the notebook's CELL 2, wrapped into a callable function.
    Populates module globals: template_df, esma_to_ecb, ecb_to_esma
    """
    global TEMPLATE_PATH, template_df, esma_to_ecb, ecb_to_esma

    if template_path is not None:
        TEMPLATE_PATH = template_path

    # =============================================================================
    # CELL 2: Load Template Mapping (ECB <-> ESMA columns)
    # =============================================================================
    template_df = pd.read_excel(TEMPLATE_PATH, sheet_name='Sheet1')
    template_df = template_df[['FIELD CODE', 'FIELD NAME', 'For info: existing ECB or EBA NPL template field code']].copy()
    template_df.columns = ['ESMA_Code', 'ESMA_Name', 'ECB_Code']
    template_df = template_df[template_df['ECB_Code'].notna()]

    # Create mappings
    esma_to_ecb = dict(zip(template_df['ESMA_Code'], template_df['ECB_Code']))

    ecb_to_esma = {}
    for _, row in template_df.iterrows():
        esma_code = row['ESMA_Code']
        ecb_code = row['ECB_Code']
        esma_name = row['ESMA_Name']
        if ecb_code not in ecb_to_esma:
            ecb_to_esma[ecb_code] = esma_code
        elif 'New' in str(esma_name):
            ecb_to_esma[ecb_code] = esma_code

    return ecb_to_esma, esma_to_ecb


def build_file_index(
    ecb_path: str | None = None,
    esma_path: str | None = None,
    pool_mapping_path: str | None = None,
):
    """
    Build File Index (All Pools).

    This is the notebook's CELL 3, wrapped into a callable function.
    Populates module globals:
      - all_ecb_files, ecb_pool_files
      - all_esma_files, esma_pool_files
      - pool_mapping, matched_pools, matched_ecb_pools, matched_esma_pools
      - ecb_only_pools, esma_only_pools
    """
    global ECB_PATH, ESMA_PATH, POOL_MAPPING_PATH
    global all_ecb_files, all_esma_files
    global ecb_pool_files, esma_pool_files
    global pool_mapping, matched_pools, matched_ecb_pools, matched_esma_pools, ecb_only_pools, esma_only_pools

    if ecb_path is not None:
        ECB_PATH = ecb_path
    if esma_path is not None:
        ESMA_PATH = esma_path
    if pool_mapping_path is not None:
        POOL_MAPPING_PATH = pool_mapping_path

    # =============================================================================
    # CELL 3: Build File Index (All Pools)
    # =============================================================================
    # Index ECB files by pool
    all_ecb_files = glob.glob(os.path.join(ECB_PATH, "*.gz"))
    ecb_pool_files = defaultdict(list)
    for f in all_ecb_files:
        fname = os.path.basename(f)
        pool_id = fname.split('_')[0]
        ecb_pool_files[pool_id].append(fname)

    # Index ESMA files by pool
    all_esma_files = glob.glob(os.path.join(ESMA_PATH, "*.csv"))
    esma_pool_files = defaultdict(list)
    for f in all_esma_files:
        fname = os.path.basename(f)
        parts = fname.split('_')
        pool_id = parts[-3]  # Pool ID is 3rd from last
        esma_pool_files[pool_id].append(fname)

    # Load pool mapping (ECB -> ESMA matches)
    with open(POOL_MAPPING_PATH) as f:
        pool_mapping = json.load(f)

    matched_pools = pool_mapping['pools']
    matched_ecb_pools = set(matched_pools.keys())
    matched_esma_pools = set(p['esma_pool'] for p in matched_pools.values())

    # Identify ECB-only and ESMA-only pools
    ecb_only_pools = set(ecb_pool_files.keys()) - matched_ecb_pools
    esma_only_pools = set(esma_pool_files.keys()) - matched_esma_pools

    return {
        'ecb_pools': ecb_pool_files,
        'esma_pools': esma_pool_files,
        'matched_pools': matched_pools,
        'ecb_only_pools': ecb_only_pools,
        'esma_only_pools': esma_only_pools,
    }


# =============================================================================
# CELL 4: Checkpoint Management
# =============================================================================
def scan_completed_pools():
    """
    Scan output folders to determine which pools are already processed.
    This is more reliable than checkpoint.json since it checks actual files.
    """
    completed = {
        'matched': set(),
        'ecb_only': set(),
        'esma_only': set()
    }
    
    for pool_type in ['matched', 'ecb_only', 'esma_only']:
        folder = os.path.join(OUTPUT_DIR, pool_type)
        if os.path.exists(folder):
            for fname in os.listdir(folder):
                if fname.endswith('.csv') and not fname.endswith('.tmp'):
                    # Pool ID is the filename without .csv
                    pool_id = fname[:-4]
                    completed[pool_type].add(pool_id)
    
    return completed

def log_message(msg):
    """Append message to log file and print"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {msg}"
    print(log_entry)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_entry + '\n')


# =============================================================================
# CELL 5: Data Loading Functions (with retry and chunked loading)
# =============================================================================
import gc
import io
import zlib

# Threshold for "large" pool (compressed size in bytes)
# 100 MB compressed - more conservative to avoid memory issues
LARGE_POOL_THRESHOLD = 100 * 1024 * 1024  # 100 MB compressed

def load_ecb_file(filepath, max_retries=3):
    """
    Load a single ECB .gz file with retry logic.
    
    Uses zlib fallback for files that fail with standard gzip reader
    (some ECB files have gzip format issues that zlib handles correctly).
    """
    fname = os.path.basename(filepath)
    
    for attempt in range(max_retries):
        try:
            # Method 1: Try standard pandas read (works for most files)
            df = pd.read_csv(filepath, compression='gzip', low_memory=False)
            return df
        except Exception as e:
            error_msg = str(e)
            
            # Check if this is a gzip-related error
            if 'gzip' in error_msg.lower() or 'Not a gzipped file' in error_msg:
                # Method 2: Use zlib directly (handles problematic gzip files)
                try:
                    with open(filepath, 'rb') as f:
                        raw = f.read()
                    
                    # Decompress using zlib (skip 10-byte gzip header)
                    decomp = zlib.decompressobj(-zlib.MAX_WBITS)
                    result = decomp.decompress(raw[10:])
                    result += decomp.flush()
                    
                    df = pd.read_csv(io.BytesIO(result), low_memory=False)
                    log_message(f"  Loaded {fname} using zlib fallback ({len(df):,} rows)")
                    return df
                except Exception as zlib_error:
                    if attempt < max_retries - 1:
                        log_message(f"  Retry {attempt+1}/{max_retries} for {fname}: zlib also failed: {zlib_error}")
                        time.sleep(0.5)
                        gc.collect()
                    else:
                        log_message(f"ERROR loading ECB file {filepath}: {e} (zlib fallback: {zlib_error})")
                        return pd.DataFrame()
            else:
                if attempt < max_retries - 1:
                    log_message(f"  Retry {attempt+1}/{max_retries} for {fname}: {e}")
                    time.sleep(0.5)
                    gc.collect()
                else:
                    log_message(f"ERROR loading ECB file {filepath}: {e}")
                    return pd.DataFrame()
    return pd.DataFrame()

def load_esma_file(filepath, max_retries=3):
    """Load a single ESMA .csv file with retry logic"""
    for attempt in range(max_retries):
        try:
            df = pd.read_csv(filepath, low_memory=False)
            return df
        except Exception as e:
            if attempt < max_retries - 1:
                log_message(f"  Retry {attempt+1}/{max_retries} for {os.path.basename(filepath)}: {e}")
                time.sleep(0.5)
                gc.collect()
            else:
                log_message(f"ERROR loading ESMA file {filepath}: {e}")
                return pd.DataFrame()
    return pd.DataFrame()

def get_pool_compressed_size(pool_id, source='ecb'):
    """Get total compressed size of files for a pool"""
    if source == 'ecb':
        files = ecb_pool_files.get(pool_id, [])
        base_path = ECB_PATH
    else:
        files = esma_pool_files.get(pool_id, [])
        base_path = ESMA_PATH
    
    total = 0
    for fname in files:
        try:
            total += os.path.getsize(os.path.join(base_path, fname))
        except:
            pass
    return total

def is_large_pool(pool_id, source='ecb'):
    """Check if pool exceeds size threshold"""
    return get_pool_compressed_size(pool_id, source) > LARGE_POOL_THRESHOLD

def load_all_ecb_for_pool(pool_id):
    """Load and concatenate all ECB files for a pool"""
    files = ecb_pool_files.get(pool_id, [])
    if not files:
        return pd.DataFrame()
    
    dfs = []
    for fname in files:
        filepath = os.path.join(ECB_PATH, fname)
        df = load_ecb_file(filepath)
        if len(df) > 0:
            dfs.append(df)
    
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()

def load_all_esma_for_pool(pool_id):
    """Load and concatenate all ESMA files for a pool"""
    files = esma_pool_files.get(pool_id, [])
    if not files:
        return pd.DataFrame()
    
    dfs = []
    for fname in files:
        filepath = os.path.join(ESMA_PATH, fname)
        df = load_esma_file(filepath)
        if len(df) > 0:
            dfs.append(df)
    
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


# =============================================================================
# CELL 6: Data Preparation Functions
# =============================================================================
def prepare_ecb_data(ecb_df):
    """
    Prepare ECB data:
    1. Rename columns to ESMA equivalents
    2. Add source marker
    3. Normalize date to YYYY-MM
    """
    if len(ecb_df) == 0:
        return pd.DataFrame()
    
    df = ecb_df.copy()
    
    # Rename ECB columns to ESMA equivalents
    rename_map = {ecb: esma for ecb, esma in ecb_to_esma.items() if ecb in df.columns}
    df = df.rename(columns=rename_map)
    
    # Add source marker
    df['source'] = 'ECB'
    
    # Normalize date to YYYY-MM for deduplication
    if 'RREL6' in df.columns:
        df['date_ym'] = df['RREL6'].astype(str).str[:7]
    elif 'AR1' in df.columns:
        df['date_ym'] = df['AR1'].astype(str).str[:7]
    else:
        df['date_ym'] = ''
    
    return df

def prepare_esma_data(esma_df):
    """
    Prepare ESMA data:
    1. Add source marker
    2. Normalize date to YYYY-MM
    """
    if len(esma_df) == 0:
        return pd.DataFrame()
    
    df = esma_df.copy()
    df['source'] = 'ESMA'
    
    # Normalize date
    if 'RREL6' in df.columns:
        df['date_ym'] = df['RREL6'].astype(str).str[:7]
    else:
        df['date_ym'] = ''
    
    return df

def remove_duplicates_prefer_esma(df):
    """
    Remove duplicate loan+date rows, preferring ESMA over ECB.
    """
    if len(df) == 0 or 'RREL3' not in df.columns:
        return df
    
    # Find loan+dates that have ESMA data
    esma_rows = df[df['source'] == 'ESMA']
    esma_loan_dates = set(zip(
        esma_rows['RREL3'].astype(str),
        esma_rows['date_ym'].astype(str)
    ))
    
    # Keep ESMA rows, or ECB rows without ESMA equivalent
    def should_keep(row):
        if row['source'] == 'ESMA':
            return True
        loan_date = (str(row['RREL3']), str(row['date_ym']))
        return loan_date not in esma_loan_dates
    
    mask = df.apply(should_keep, axis=1)
    result = df[mask].copy()
    
    # Drop helper column
    if 'date_ym' in result.columns:
        result = result.drop(columns=['date_ym'])
    
    return result

def get_non_empty_columns(df):
    """Return list of columns that have at least one non-null value"""
    non_empty = []
    for col in df.columns:
        if df[col].notna().any():
            non_empty.append(col)
    return non_empty


# =============================================================================
# CELL 7: Pool Processing Functions (with large pool handling)
# =============================================================================
def process_matched_pool(ecb_pool_id, esma_pool_id):
    """
    Process a matched ECB-ESMA pool pair.
    Returns merged DataFrame with source column.
    
    OPTIMIZATION: Only runs deduplication for pools with temporal overlap.
    For pools without overlap, simply concatenates ECB and ESMA data.
    """
    # Load data
    ecb_df = load_all_ecb_for_pool(ecb_pool_id)
    esma_df = load_all_esma_for_pool(esma_pool_id)
    
    log_message(f"  ECB rows: {len(ecb_df)}, ESMA rows: {len(esma_df)}")
    
    # Check if this pool needs deduplication
    needs_dedup = ecb_pool_id in POOLS_WITH_OVERLAP
    
    # Prepare data
    ecb_prepared = prepare_ecb_data(ecb_df)
    esma_prepared = prepare_esma_data(esma_df)
    
    # Combine
    if len(ecb_prepared) > 0 and len(esma_prepared) > 0:
        combined = pd.concat([ecb_prepared, esma_prepared], ignore_index=True)
    elif len(esma_prepared) > 0:
        combined = esma_prepared
    elif len(ecb_prepared) > 0:
        combined = ecb_prepared
    else:
        return pd.DataFrame()
    
    # Remove duplicates ONLY for pools with temporal overlap
    if needs_dedup:
        log_message(f"  Pool has temporal overlap - running deduplication...")
        merged = remove_duplicates_prefer_esma(combined)
        log_message(f"  After dedup: {len(merged)} rows (removed {len(combined) - len(merged)} duplicates)")
    else:
        # No overlap - skip dedup (significant speedup)
        merged = combined
        if 'date_ym' in merged.columns:
            merged = merged.drop(columns=['date_ym'])
        log_message(f"  No temporal overlap - skipping dedup: {len(merged)} rows")
    
    # Add pool identifiers
    merged['ecb_pool_id'] = ecb_pool_id
    merged['esma_pool_id'] = esma_pool_id
    
    return merged

def get_all_columns_for_ecb_pool(ecb_pool_id):
    """Scan all ECB files to determine ALL columns for consistent output."""
    all_columns = set()
    files = ecb_pool_files.get(ecb_pool_id, [])
    
    for fname in files:
        try:
            filepath = os.path.join(ECB_PATH, fname)
            df = load_ecb_file(filepath)
            if len(df) > 0:
                prepared = prepare_ecb_data(df)
                if 'date_ym' in prepared.columns:
                    prepared = prepared.drop(columns=['date_ym'])
                prepared['ecb_pool_id'] = ecb_pool_id
                prepared['esma_pool_id'] = None
                non_empty = get_non_empty_columns(prepared)
                all_columns.update(non_empty)
                del df, prepared
        except Exception as e:
            log_message(f"    Warning: Could not scan ECB file {fname}: {e}")
    
    gc.collect()
    return sorted(list(all_columns))

def get_all_columns_for_esma_pool(esma_pool_id):
    """Scan all ESMA files to determine ALL columns for consistent output."""
    all_columns = set()
    files = esma_pool_files.get(esma_pool_id, [])
    
    for fname in files:
        try:
            filepath = os.path.join(ESMA_PATH, fname)
            df = load_esma_file(filepath)
            if len(df) > 0:
                prepared = prepare_esma_data(df)
                if 'date_ym' in prepared.columns:
                    prepared = prepared.drop(columns=['date_ym'])
                prepared['ecb_pool_id'] = None
                prepared['esma_pool_id'] = esma_pool_id
                non_empty = get_non_empty_columns(prepared)
                all_columns.update(non_empty)
                del df, prepared
        except Exception as e:
            log_message(f"    Warning: Could not scan ESMA file {fname}: {e}")
    
    gc.collect()
    return sorted(list(all_columns))

def process_ecb_only_pool_chunked(ecb_pool_id, output_path):
    """
    Process a LARGE ECB-only pool file-by-file, appending to output.
    This avoids loading entire pool into memory.
    Returns total rows saved.
    """
    files = ecb_pool_files.get(ecb_pool_id, [])
    if not files:
        return 0
    
    # PHASE 1: Scan all files to determine consistent column schema
    log_message(f"  Scanning {len(files)} files for column schema...")
    all_columns = get_all_columns_for_ecb_pool(ecb_pool_id)
    log_message(f"  Found {len(all_columns)} columns")
    
    total_rows = 0
    first_file = True
    
    log_message(f"  Processing {len(files)} files in chunked mode...")
    
    for i, fname in enumerate(files):
        filepath = os.path.join(ECB_PATH, fname)
        df = load_ecb_file(filepath)
        
        if len(df) == 0:
            continue
        
        # Prepare (rename to ESMA schema)
        prepared = prepare_ecb_data(df)
        
        # Drop helper column
        if 'date_ym' in prepared.columns:
            prepared = prepared.drop(columns=['date_ym'])
        
        # Add pool identifiers
        prepared['ecb_pool_id'] = ecb_pool_id
        prepared['esma_pool_id'] = None
        
        # Use consistent columns - add missing as NaN
        for col in all_columns:
            if col not in prepared.columns:
                prepared[col] = np.nan
        
        df_to_save = prepared[all_columns]
        
        # Append to file (write header only for first chunk)
        df_to_save.to_csv(output_path, mode='a', index=False, header=first_file)
        first_file = False
        total_rows += len(df_to_save)
        
        # Free memory
        del df, prepared, df_to_save
        gc.collect()
        
        if (i + 1) % 20 == 0:
            log_message(f"    Processed {i+1}/{len(files)} files, {total_rows:,} rows so far")
    
    return total_rows

def process_ecb_only_pool(ecb_pool_id):
    """
    Process an ECB-only pool (no ESMA match).
    Rename columns to ESMA schema.
    """
    ecb_df = load_all_ecb_for_pool(ecb_pool_id)
    
    if len(ecb_df) == 0:
        return pd.DataFrame()
    
    log_message(f"  ECB rows: {len(ecb_df)}")
    
    # Prepare (rename to ESMA schema)
    prepared = prepare_ecb_data(ecb_df)
    
    # Drop helper column
    if 'date_ym' in prepared.columns:
        prepared = prepared.drop(columns=['date_ym'])
    
    # Add pool identifiers
    prepared['ecb_pool_id'] = ecb_pool_id
    prepared['esma_pool_id'] = None
    
    return prepared

def process_esma_only_pool_chunked(esma_pool_id, output_path):
    """
    Process a LARGE ESMA-only pool file-by-file, appending to output.
    Returns total rows saved.
    """
    files = esma_pool_files.get(esma_pool_id, [])
    if not files:
        return 0
    
    # PHASE 1: Scan all files to determine consistent column schema
    log_message(f"  Scanning {len(files)} files for column schema...")
    all_columns = get_all_columns_for_esma_pool(esma_pool_id)
    log_message(f"  Found {len(all_columns)} columns")
    
    total_rows = 0
    first_file = True
    
    log_message(f"  Processing {len(files)} files in chunked mode...")
    
    for i, fname in enumerate(files):
        filepath = os.path.join(ESMA_PATH, fname)
        df = load_esma_file(filepath)
        
        if len(df) == 0:
            continue
        
        # Prepare
        prepared = prepare_esma_data(df)
        
        # Drop helper column
        if 'date_ym' in prepared.columns:
            prepared = prepared.drop(columns=['date_ym'])
        
        # Add pool identifiers
        prepared['ecb_pool_id'] = None
        prepared['esma_pool_id'] = esma_pool_id
        
        # Use consistent columns - add missing as NaN
        for col in all_columns:
            if col not in prepared.columns:
                prepared[col] = np.nan
        
        df_to_save = prepared[all_columns]
        
        # Append to file
        df_to_save.to_csv(output_path, mode='a', index=False, header=first_file)
        first_file = False
        total_rows += len(df_to_save)
        
        # Free memory
        del df, prepared, df_to_save
        gc.collect()
        
        if (i + 1) % 20 == 0:
            log_message(f"    Processed {i+1}/{len(files)} files, {total_rows:,} rows so far")
    
    return total_rows

def process_esma_only_pool(esma_pool_id):
    """
    Process an ESMA-only pool (no ECB match).
    """
    esma_df = load_all_esma_for_pool(esma_pool_id)
    
    if len(esma_df) == 0:
        return pd.DataFrame()
    
    log_message(f"  ESMA rows: {len(esma_df)}")
    
    # Prepare
    prepared = prepare_esma_data(esma_df)
    
    # Drop helper column
    if 'date_ym' in prepared.columns:
        prepared = prepared.drop(columns=['date_ym'])
    
    # Add pool identifiers
    prepared['ecb_pool_id'] = None
    prepared['esma_pool_id'] = esma_pool_id
    
    return prepared

def get_all_columns_for_matched_pool(ecb_pool_id, esma_pool_id):
    """
    PHASE 1: Scan sample files from both ECB and ESMA to determine ALL columns.
    This ensures consistent column alignment when writing chunks.
    Returns sorted list of all non-empty columns across both sources.
    """
    all_columns = set()
    
    ecb_files = ecb_pool_files.get(ecb_pool_id, [])
    esma_files = esma_pool_files.get(esma_pool_id, [])
    
    # Scan ALL files to ensure we capture every possible column
    # This prevents column misalignment if later files have different columns
    
    # Scan ECB files
    for fname in ecb_files:
        try:
            filepath = os.path.join(ECB_PATH, fname)
            df = load_ecb_file(filepath)
            if len(df) > 0:
                prepared = prepare_ecb_data(df)
                if 'date_ym' in prepared.columns:
                    prepared = prepared.drop(columns=['date_ym'])
                prepared['ecb_pool_id'] = ecb_pool_id
                prepared['esma_pool_id'] = esma_pool_id
                non_empty = get_non_empty_columns(prepared)
                all_columns.update(non_empty)
                del df, prepared
        except Exception as e:
            log_message(f"    Warning: Could not scan ECB file {fname}: {e}")
    
    # Scan ESMA files
    for fname in esma_files:
        try:
            filepath = os.path.join(ESMA_PATH, fname)
            df = load_esma_file(filepath)
            if len(df) > 0:
                prepared = prepare_esma_data(df)
                if 'date_ym' in prepared.columns:
                    prepared = prepared.drop(columns=['date_ym'])
                prepared['ecb_pool_id'] = ecb_pool_id
                prepared['esma_pool_id'] = esma_pool_id
                non_empty = get_non_empty_columns(prepared)
                all_columns.update(non_empty)
                del df, prepared
        except Exception as e:
            log_message(f"    Warning: Could not scan ESMA file {fname}: {e}")
    
    gc.collect()
    
    # Return sorted list for consistent ordering
    return sorted(list(all_columns))

def process_matched_pool_chunked(ecb_pool_id, esma_pool_id, output_path):
    """
    Process a LARGE matched ECB-ESMA pool file-by-file.
    
    FIXED: Now determines ALL columns upfront to ensure consistent alignment.
    
    INTEGRITY LOGIC:
    - We prefer ESMA data over ECB when both have the same loan+date
    - Strategy depends on sizes:
      - If smaller side < 500MB: load smaller, chunk larger
      - If BOTH sides > 500MB: chunk both, skip dedup (rare overlap anyway)
    
    Returns total rows saved.
    """
    ecb_files = ecb_pool_files.get(ecb_pool_id, [])
    esma_files = esma_pool_files.get(esma_pool_id, [])
    
    if not ecb_files and not esma_files:
        return 0
    
    # Determine sizes
    ecb_size = get_pool_compressed_size(ecb_pool_id, 'ecb')
    esma_size = get_pool_compressed_size(esma_pool_id, 'esma')
    smaller_size = min(ecb_size, esma_size)
    
    # Threshold for "can fit in memory" - 500 MB compressed max
    MEMORY_SAFE_THRESHOLD = 500 * 1024 * 1024
    
    log_message(f"  ECB size: {ecb_size/1024/1024:.0f} MB, ESMA size: {esma_size/1024/1024:.0f} MB")
    
    # PHASE 1: Determine ALL columns upfront for consistent alignment
    log_message(f"  Phase 1: Scanning files to determine column schema...")
    all_columns = get_all_columns_for_matched_pool(ecb_pool_id, esma_pool_id)
    log_message(f"  Found {len(all_columns)} total columns across both sources")
    
    total_rows = 0
    first_file = True
    
    if smaller_size > MEMORY_SAFE_THRESHOLD:
        # CASE 0: BOTH sides are huge - chunk both, write ECB first, then ESMA (preferred at end)
        log_message(f"  Strategy: MEGA POOL - chunk both sides with consistent columns")
        
        # Process ALL ECB files first
        log_message(f"  Processing {len(ecb_files)} ECB files...")
        for i, fname in enumerate(ecb_files):
            filepath = os.path.join(ECB_PATH, fname)
            df = load_ecb_file(filepath)
            
            if len(df) == 0:
                continue
            
            prepared = prepare_ecb_data(df)
            if 'date_ym' in prepared.columns:
                prepared = prepared.drop(columns=['date_ym'])
            
            prepared['ecb_pool_id'] = ecb_pool_id
            prepared['esma_pool_id'] = esma_pool_id
            
            # Use consistent columns - add missing columns as NaN
            for col in all_columns:
                if col not in prepared.columns:
                    prepared[col] = np.nan
            
            df_to_save = prepared[all_columns]
            df_to_save.to_csv(output_path, mode='a', index=False, header=first_file)
            first_file = False
            total_rows += len(df_to_save)
            
            del df, prepared, df_to_save
            gc.collect()
            
            if (i + 1) % 20 == 0:
                log_message(f"    ECB: {i+1}/{len(ecb_files)} files, {total_rows:,} rows")
        
        ecb_rows = total_rows
        log_message(f"  ECB complete: {ecb_rows:,} rows")
        
        # Process ALL ESMA files (preferred source - at end)
        log_message(f"  Processing {len(esma_files)} ESMA files...")
        for i, fname in enumerate(esma_files):
            filepath = os.path.join(ESMA_PATH, fname)
            df = load_esma_file(filepath)
            
            if len(df) == 0:
                continue
            
            prepared = prepare_esma_data(df)
            if 'date_ym' in prepared.columns:
                prepared = prepared.drop(columns=['date_ym'])
            
            prepared['ecb_pool_id'] = ecb_pool_id
            prepared['esma_pool_id'] = esma_pool_id
            
            # Use consistent columns - add missing columns as NaN
            for col in all_columns:
                if col not in prepared.columns:
                    prepared[col] = np.nan
            
            df_to_save = prepared[all_columns]
            df_to_save.to_csv(output_path, mode='a', index=False, header=first_file)
            first_file = False
            total_rows += len(df_to_save)
            
            del df, prepared, df_to_save
            gc.collect()
            
            if (i + 1) % 5 == 0:
                log_message(f"    ESMA: {i+1}/{len(esma_files)} files, {total_rows - ecb_rows:,} ESMA rows")
        
        log_message(f"  ESMA complete: {total_rows - ecb_rows:,} rows")
        
        # POST-PROCESSING: Deduplicate the output file
        # Logic: Keep ALL ESMA rows. Remove ECB rows where same (loan, date) exists in ESMA.
        needs_dedup = ecb_pool_id in POOLS_WITH_OVERLAP
        
        if needs_dedup:
            log_message(f"  Post-processing: removing ECB duplicates where ESMA exists...")
            try:
                # PASS 1: Collect all ESMA (loan, date) keys
                esma_keys = set()
                for chunk in pd.read_csv(output_path, chunksize=100000, dtype=str, usecols=['RREL3', 'RREL6', 'source']):
                    esma_rows = chunk[chunk['source'] == 'ESMA']
                    if len(esma_rows) > 0 and 'RREL3' in esma_rows.columns:
                        keys = zip(esma_rows['RREL3'].fillna(''), esma_rows['RREL6'].str[:7].fillna(''))
                        esma_keys.update(keys)
                log_message(f"    Found {len(esma_keys):,} unique ESMA (loan, date) keys")
                
                # PASS 2: Write all ESMA rows + ECB rows not in esma_keys
                temp_path = output_path + ".dedup_temp"
                rows_before = total_rows
                total_rows = 0
                first_chunk = True
                
                for chunk in pd.read_csv(output_path, chunksize=100000, dtype=str):
                    if 'source' in chunk.columns and 'RREL3' in chunk.columns:
                        # Keep row if: ESMA, or ECB with no ESMA equivalent
                        is_esma = chunk['source'] == 'ESMA'
                        keys = list(zip(chunk['RREL3'].fillna(''), chunk['RREL6'].str[:7].fillna('')))
                        is_dup_ecb = [(not esma) and (k in esma_keys) for esma, k in zip(is_esma, keys)]
                        chunk_filtered = chunk.loc[~pd.Series(is_dup_ecb, index=chunk.index)]
                    else:
                        chunk_filtered = chunk
                    
                    chunk_filtered.to_csv(temp_path, mode='a', index=False, header=first_chunk)
                    first_chunk = False
                    total_rows += len(chunk_filtered)
                    
                    del chunk, chunk_filtered
                    gc.collect()
                
                os.replace(temp_path, output_path)
                log_message(f"  Deduplication complete: {rows_before - total_rows:,} ECB duplicates removed")
                
            except Exception as e:
                log_message(f"  WARNING: Deduplication failed ({e}), keeping original file")
                if os.path.exists(temp_path):
                    os.remove(temp_path)
        else:
            log_message(f"  Skipping deduplication - no temporal overlap")
        
    elif esma_size <= ecb_size:
        # CASE 1: ECB is larger - load ESMA first, chunk ECB
        log_message(f"  Strategy: Load ESMA (smaller), chunk ECB files")
        
        needs_dedup = ecb_pool_id in POOLS_WITH_OVERLAP
        if needs_dedup:
            log_message(f"  Pool has temporal overlap - will filter ECB duplicates")
        else:
            log_message(f"  No temporal overlap - skipping deduplication")
        
        # Load all ESMA data
        log_message(f"  Loading ESMA data ({len(esma_files)} files)...")
        esma_dfs = []
        for fname in esma_files:
            filepath = os.path.join(ESMA_PATH, fname)
            df = load_esma_file(filepath)
            if len(df) > 0:
                esma_dfs.append(df)
        
        if esma_dfs:
            esma_all = pd.concat(esma_dfs, ignore_index=True)
            esma_prepared = prepare_esma_data(esma_all)
            if needs_dedup:
                esma_loan_dates = set(zip(
                    esma_prepared['RREL3'].astype(str),
                    esma_prepared['date_ym'].astype(str)
                ))
                log_message(f"  ESMA: {len(esma_prepared)} rows, {len(esma_loan_dates)} unique loan-dates")
            else:
                esma_loan_dates = set()
                log_message(f"  ESMA: {len(esma_prepared)} rows")
            del esma_all
        else:
            esma_prepared = pd.DataFrame()
            esma_loan_dates = set()
        
        del esma_dfs
        gc.collect()
        
        # Process ECB files one by one
        log_message(f"  Processing {len(ecb_files)} ECB files...")
        for i, fname in enumerate(ecb_files):
            filepath = os.path.join(ECB_PATH, fname)
            df = load_ecb_file(filepath)
            
            if len(df) == 0:
                continue
            
            ecb_prepared = prepare_ecb_data(df)
            
            # Filter: keep ECB rows only if NOT in ESMA
            if needs_dedup and len(esma_loan_dates) > 0 and 'RREL3' in ecb_prepared.columns:
                ecb_prepared['_key'] = ecb_prepared['RREL3'].astype(str) + '|' + ecb_prepared['date_ym'].astype(str)
                mask = ~ecb_prepared['_key'].apply(lambda x: (x.split('|')[0], x.split('|')[1]) in esma_loan_dates)
                ecb_filtered = ecb_prepared[mask].drop(columns=['_key']).copy()
            else:
                ecb_filtered = ecb_prepared
            
            if 'date_ym' in ecb_filtered.columns:
                ecb_filtered = ecb_filtered.drop(columns=['date_ym'])
            
            ecb_filtered['ecb_pool_id'] = ecb_pool_id
            ecb_filtered['esma_pool_id'] = esma_pool_id
            
            if len(ecb_filtered) > 0:
                # Use consistent columns
                for col in all_columns:
                    if col not in ecb_filtered.columns:
                        ecb_filtered[col] = np.nan
                
                df_to_save = ecb_filtered[all_columns]
                df_to_save.to_csv(output_path, mode='a', index=False, header=first_file)
                first_file = False
                total_rows += len(df_to_save)
            
            del df, ecb_prepared, ecb_filtered
            gc.collect()
            
            if (i + 1) % 20 == 0:
                log_message(f"    Processed {i+1}/{len(ecb_files)} ECB files, {total_rows:,} ECB rows kept")
        
        # Append ALL ESMA rows (preferred source)
        if len(esma_prepared) > 0:
            if 'date_ym' in esma_prepared.columns:
                esma_prepared = esma_prepared.drop(columns=['date_ym'])
            esma_prepared['ecb_pool_id'] = ecb_pool_id
            esma_prepared['esma_pool_id'] = esma_pool_id
            
            # Use consistent columns
            for col in all_columns:
                if col not in esma_prepared.columns:
                    esma_prepared[col] = np.nan
            
            df_to_save = esma_prepared[all_columns]
            df_to_save.to_csv(output_path, mode='a', index=False, header=first_file)
            total_rows += len(df_to_save)
            log_message(f"  Added {len(df_to_save):,} ESMA rows (preferred)")
        
    else:
        # CASE 2: ESMA is larger - load ECB first, chunk ESMA
        log_message(f"  Strategy: Load ECB (smaller), chunk ESMA files")
        
        needs_dedup = ecb_pool_id in POOLS_WITH_OVERLAP
        if needs_dedup:
            log_message(f"  Pool has temporal overlap - will filter ECB duplicates")
        else:
            log_message(f"  No temporal overlap - skipping deduplication")
        
        # Load all ECB data
        log_message(f"  Loading ECB data ({len(ecb_files)} files)...")
        ecb_dfs = []
        for fname in ecb_files:
            filepath = os.path.join(ECB_PATH, fname)
            df = load_ecb_file(filepath)
            if len(df) > 0:
                ecb_dfs.append(df)
        
        if ecb_dfs:
            ecb_all = pd.concat(ecb_dfs, ignore_index=True)
            ecb_prepared = prepare_ecb_data(ecb_all)
            if needs_dedup:
                ecb_loan_dates = set(zip(
                    ecb_prepared['RREL3'].astype(str),
                    ecb_prepared['date_ym'].astype(str)
                ))
                log_message(f"  ECB: {len(ecb_prepared)} rows, {len(ecb_loan_dates)} unique loan-dates")
            else:
                ecb_loan_dates = set()
                log_message(f"  ECB: {len(ecb_prepared)} rows")
            del ecb_all
        else:
            ecb_prepared = pd.DataFrame()
            ecb_loan_dates = set()
        
        del ecb_dfs
        gc.collect()
        
        ecb_loan_dates_covered_by_esma = set()
        
        # Write ECB rows first (will be filtered at end if needed)
        if len(ecb_prepared) > 0:
            if 'date_ym' in ecb_prepared.columns:
                ecb_for_write = ecb_prepared.drop(columns=['date_ym']).copy()
            else:
                ecb_for_write = ecb_prepared.copy()
            
            ecb_for_write['ecb_pool_id'] = ecb_pool_id
            ecb_for_write['esma_pool_id'] = esma_pool_id
            
            # Use consistent columns
            for col in all_columns:
                if col not in ecb_for_write.columns:
                    ecb_for_write[col] = np.nan
            
            df_to_save = ecb_for_write[all_columns]
            df_to_save.to_csv(output_path, mode='a', index=False, header=first_file)
            first_file = False
            total_rows += len(df_to_save)
            log_message(f"  Wrote {len(df_to_save):,} ECB rows")
            del ecb_for_write
        
        # Process ESMA files one by one
        log_message(f"  Processing {len(esma_files)} ESMA files...")
        for i, fname in enumerate(esma_files):
            filepath = os.path.join(ESMA_PATH, fname)
            df = load_esma_file(filepath)
            
            if len(df) == 0:
                continue
            
            esma_chunk = prepare_esma_data(df)
            
            # Track covered loan-dates for dedup
            if needs_dedup and 'RREL3' in esma_chunk.columns:
                chunk_loan_dates = set(zip(
                    esma_chunk['RREL3'].astype(str),
                    esma_chunk['date_ym'].astype(str)
                ))
                ecb_loan_dates_covered_by_esma.update(chunk_loan_dates & ecb_loan_dates)
            
            if 'date_ym' in esma_chunk.columns:
                esma_chunk = esma_chunk.drop(columns=['date_ym'])
            
            esma_chunk['ecb_pool_id'] = ecb_pool_id
            esma_chunk['esma_pool_id'] = esma_pool_id
            
            # Use consistent columns
            for col in all_columns:
                if col not in esma_chunk.columns:
                    esma_chunk[col] = np.nan
            
            df_to_save = esma_chunk[all_columns]
            df_to_save.to_csv(output_path, mode='a', index=False, header=first_file)
            first_file = False
            total_rows += len(df_to_save)
            
            del df, esma_chunk
            gc.collect()
            
            if (i + 1) % 5 == 0:
                log_message(f"    Processed {i+1}/{len(esma_files)} ESMA files, {total_rows:,} total rows")
        
        # Post-processing dedup if needed
        if needs_dedup and len(ecb_loan_dates_covered_by_esma) > 0:
            log_message(f"  Post-processing: removing {len(ecb_loan_dates_covered_by_esma)} ECB duplicates...")
            try:
                temp_path = output_path + ".dedup_temp"
                rows_before = total_rows
                total_rows = 0
                first_chunk = True
                
                for chunk in pd.read_csv(output_path, chunksize=100000, dtype=str):
                    if 'source' in chunk.columns and 'RREL3' in chunk.columns and 'RREL6' in chunk.columns:
                        # Keep all ESMA rows, filter ECB rows
                        is_esma = chunk['source'] == 'ESMA'
                        keys = list(zip(chunk['RREL3'].fillna(''), chunk['RREL6'].str[:7].fillna('')))
                        is_dup_ecb = [(not esma) and (k in ecb_loan_dates_covered_by_esma) for esma, k in zip(is_esma, keys)]
                        chunk_filtered = chunk.loc[~pd.Series(is_dup_ecb, index=chunk.index)]
                    else:
                        chunk_filtered = chunk
                    
                    chunk_filtered.to_csv(temp_path, mode='a', index=False, header=first_chunk)
                    first_chunk = False
                    total_rows += len(chunk_filtered)
                    
                    del chunk, chunk_filtered
                    gc.collect()
                
                os.replace(temp_path, output_path)
                log_message(f"  Deduplication complete: {rows_before - total_rows:,} duplicates removed")
                
            except Exception as e:
                log_message(f"  WARNING: Deduplication failed ({e})")
                if os.path.exists(temp_path):
                    os.remove(temp_path)
    
    return total_rows

def save_pool_result(df, pool_type, pool_id):
    """
    Save pool result to disk using ATOMIC write (temp file + rename).
    This ensures partial files are never left behind if interrupted.
    Uses dynamic column selection - only non-empty columns.
    """
    if len(df) == 0:
        return 0
    
    # Dynamic column selection: only keep non-empty columns
    non_empty_cols = get_non_empty_columns(df)
    df_to_save = df[non_empty_cols]
    
    # Create subdirectory by pool type
    subdir = os.path.join(OUTPUT_DIR, pool_type)
    os.makedirs(subdir, exist_ok=True)
    
    # ATOMIC WRITE: Save to temp file first, then rename
    safe_pool_id = pool_id.replace('/', '_').replace('\\', '_')
    output_path = os.path.join(subdir, f"{safe_pool_id}.csv")
    temp_path = output_path + ".tmp"
    
    df_to_save.to_csv(temp_path, index=False)
    os.replace(temp_path, output_path)  # Atomic rename
    
    return len(df_to_save)

