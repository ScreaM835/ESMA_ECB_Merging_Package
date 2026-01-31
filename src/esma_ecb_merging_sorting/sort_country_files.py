"""
Stage 4 — Sort country files (deterministic ordering)

This module contains the extracted (notebook-origin) logic from `sort_country_files.ipynb`.

Core methodology is preserved:
- Discover per-country CSV files
- Sort each file by (RREL3, RREC3, RREL6) using DuckDB
- Write using temp output and atomic rename
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Dict, List

import duckdb

# Input: merged country files from country_merge_all_production
INPUT_DIR = r"D:\ECB_ESMA_BY_COUNTRY_ALL"

# Output: sorted country files
OUTPUT_DIR = r"D:\ECB_ESMA_BY_COUNTRY_SORTED"

# Sort columns - Preserves time series for each loan-collateral pair
# Sort order: Loan → Collateral → Date
# NOTE: Using RREL6 (unified date column) instead of Pool_Cutoff_Date
#       - RREL6 has data from BOTH ECB (AR1->RREL6) and ESMA sources
#       - Pool_Cutoff_Date only exists in ESMA source files (empty for ECB data)
# NOTE: RREC9 (Property Type) is NOT included - it's a time-varying attribute,
#       not part of the key. (RREL3, RREC3, RREL6) should be unique.
SORT_COLUMNS = [
    "RREL3",           # 1. Loan Identifier (individual loan)
    "RREC3",           # 2. Collateral Identifier (keeps each collateral's time series together)
    "RREL6",           # 3. Data Cut-Off Date (chronological within each loan-collateral pair)
]
# NOTE: RREL1 is Pool Identifier (securitization deal), NOT individual loan


def discover_country_files(input_dir: str = INPUT_DIR) -> List[Dict]:
    """
    Discover country files to sort (notebook CELL 2 logic).

    Returns:
        List of dicts with keys: country, filename, filepath, size_gb
    """
    country_files = []

    if os.path.exists(input_dir):
        for fname in os.listdir(input_dir):
            if fname.endswith('.csv') and not fname.endswith('.tmp'):
                filepath = os.path.join(input_dir, fname)
                size_gb = os.path.getsize(filepath) / (1024**3)
                country_code = fname.replace('.csv', '')
                country_files.append({
                    'country': country_code,
                    'filename': fname,
                    'filepath': filepath,
                    'size_gb': size_gb
                })

    # Sort by size (smallest first for testing)
    country_files.sort(key=lambda x: x['size_gb'])

    return country_files


# =============================================================================
# CELL 3: Sort Function using DuckDB (all VARCHAR to avoid type issues)
# =============================================================================
def sort_country_file(input_path, output_path, sort_columns):
    """
    Sort a country CSV file by multiple columns for deterministic ordering.
    Sort order: RREL3 (Loan ID), RREC3 (Collateral ID), RREL6 (Date)
    Uses DuckDB with PERSISTENT DATABASE FILE for true disk-based sorting.
    All columns read as VARCHAR to avoid type inference issues.
    Auto-detects system resources for optimal performance.
    """
    import psutil
    start_time = time.time()
    
    # Use a persistent database file on D: drive for large file sorting
    db_path = 'D:/duckdb_temp/sort_temp.duckdb'
    temp_dir = 'D:/duckdb_temp'
    os.makedirs(temp_dir, exist_ok=True)
    
    # Remove old database file if exists
    if os.path.exists(db_path):
        os.remove(db_path)
    if os.path.exists(db_path + '.wal'):
        os.remove(db_path + '.wal')
    
    # Auto-detect system resources
    available_ram_gb = psutil.virtual_memory().available / (1024**3)
    cpu_count = psutil.cpu_count(logical=False) or 4
    
    # Use 70% of available RAM, between 8GB and 64GB
    memory_limit_gb = int(available_ram_gb * 0.7)
    memory_limit_gb = max(8, min(memory_limit_gb, 64))
    
    # Use half of physical cores
    threads = max(2, cpu_count // 2)
    
    try:
        # Create PERSISTENT connection - this enables true disk-based operations
        conn = duckdb.connect(db_path)
        
        # Configure for large file handling - optimized settings
        conn.execute("SET preserve_insertion_order = false")
        conn.execute(f"SET threads = {threads}")
        conn.execute(f"SET memory_limit = '{memory_limit_gb}GB'")
        conn.execute(f"SET temp_directory = '{temp_dir}'")
        # Don't limit temp directory size - let it use available disk
        
        # Read with ALL columns as VARCHAR to avoid type inference errors
        # First get column names (use quote='"' to handle fields with commas inside quotes)
        cols_query = f"SELECT * FROM read_csv_auto('{input_path}', sample_size=100, quote='\"') LIMIT 0"
        cols_result = conn.execute(cols_query).description
        available_cols = [col[0] for col in cols_result]
        
        # Build ORDER BY clause from provided sort columns
        order_cols = []
        for col in sort_columns:
            if col in available_cols:
                order_cols.append(col)
            else:
                print(f"  Warning: {col} not found in file, skipping")
        
        if not order_cols:
            raise ValueError("No sort columns found in file")
        
        order_clause = ", ".join(order_cols)
        
        # Count rows (with all_varchar=true and quote='"')
        count_query = f"SELECT COUNT(*) FROM read_csv('{input_path}', all_varchar=true, header=true, quote='\"')"
        row_count = conn.execute(count_query).fetchone()[0]
        print(f"  Row count: {row_count:,}")
        
        # Sort and export (with all_varchar=true and quote='"' to handle commas in quoted fields)
        temp_path = output_path + '.tmp'
        
        export_query = f"""
            COPY (
                SELECT * FROM read_csv('{input_path}', all_varchar=true, header=true, quote='"')
                ORDER BY {order_clause}
            ) TO '{temp_path}' (HEADER, DELIMITER ',', QUOTE '"')
        """
        print(f"  Sorting by {order_cols}...")
        conn.execute(export_query)
        conn.close()
        
        # Clean up database file
        if os.path.exists(db_path):
            os.remove(db_path)
        if os.path.exists(db_path + '.wal'):
            os.remove(db_path + '.wal')
        
        # Rename temp to final
        if os.path.exists(output_path):
            os.remove(output_path)
        os.rename(temp_path, output_path)
        
        elapsed = time.time() - start_time
        
        return {
            'success': True,
            'row_count': row_count,
            'elapsed_seconds': elapsed,
            'sort_columns': order_cols
        }
        
    except Exception as e:
        elapsed = time.time() - start_time
        # Clean up on failure
        try:
            if os.path.exists(db_path):
                os.remove(db_path)
            if os.path.exists(db_path + '.wal'):
                os.remove(db_path + '.wal')
        except:
            pass
        return {
            'success': False,
            'error': str(e),
            'elapsed_seconds': elapsed
        }



def sort_all_country_files(
    input_dir: str = INPUT_DIR,
    output_dir: str = OUTPUT_DIR,
    sort_columns: List[str] = SORT_COLUMNS,
    run_all: bool = True,
):
    """
    Sort all country files (notebook CELL 6 production logic).

    Returns:
        List of per-country result dicts (success/failed/skipped).
    """
    os.makedirs(output_dir, exist_ok=True)

    country_files = discover_country_files(input_dir)

    results = []

    if run_all and country_files:
        for i, f in enumerate(country_files):
            output_path = os.path.join(output_dir, f['filename'])

            # Skip if already sorted
            if os.path.exists(output_path):
                results.append({'country': f['country'], 'status': 'skipped'})
                continue

            result = sort_country_file(
                f['filepath'],
                output_path,
                sort_columns
            )

            if result.get('success'):
                results.append({'country': f['country'], 'status': 'success', **result})
            else:
                results.append({'country': f['country'], 'status': 'failed', **result})

    return results
