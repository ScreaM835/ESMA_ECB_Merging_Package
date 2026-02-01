# ECB–ESMA Securitisation Data Processing Pipeline (Python Package)

This is a reusable **Python package** which processes (merges and sorts) ECB and ESMA RMBS datasets.

The pipeline processes securitisation loan‑level data from two regulatory sources:

- **ECB** (European Central Bank): ABS loan‑level data (gzipped CSV)
- **ESMA** (European Securities and Markets Authority): STS securitisation disclosures (CSV)

## What this repo contains

- **Package source**: `src/esma_ecb_merging_sorting/`
  - Stage 1: `merge_ue_collateral.py`
  - Stage 2: `full_universe_merge.py`
  - Stage 3: `country_merge_all_production.py`
  - Stage 4: `sort_country_files.py`
- **Run scripts** (thin orchestration wrappers): `scripts/`
- **Testing / verification scripts** (moved out of notebook “mixed” cells): `tests/`
- **Documentation (Read the Docs / Sphinx)**: `docs/` + `.readthedocs.yaml`

## Installation

```bash
python -m pip install -r requirements.txt
python -m pip install -e .
```

## Configuration (environment variables)

The original notebooks used hard‑coded Windows paths. The `scripts/` wrappers preserve those defaults,
but allow overrides via environment variables.

### Common

- `ECB_ESMA_BASE_PATH`
  - Default: `c:\Users\jonat\Downloads\_unique_csv_master`
  - Used to derive:
    - ECB input: `<BASE>\ECB_Data\ECB Data`
    - ESMA input (after Stage 1): `<BASE>\ESMA_UE_Collat_Merged`
    - Template mapping: `<BASE>\ESMA Template (2).xlsx`
    - Pool mapping JSON: `<BASE>\pool_mapping.json`

### Stage outputs

- `ECB_ESMA_STAGE2_OUTPUT_DIR`
  - Default: `D:\ECB_ESMA_MERGED`
- `ECB_ESMA_STAGE3_OUTPUT_DIR`
  - Default: `D:\ECB_ESMA_BY_COUNTRY_ALL`
- `ECB_ESMA_STAGE4_OUTPUT_DIR`
  - Default: `D:\ECB_ESMA_BY_COUNTRY_SORTED`

> If you do not set these variables, the scripts use the original notebook defaults.

## Running the pipeline

Run stages in order:

```bash
python scripts/stage1_merge_ue_collateral_run.py
python scripts/stage2_full_universe_merge_run.py
python scripts/stage3_country_merge_all_production_run.py
python scripts/stage4_sort_country_files_run.py
```

### Required external files (Stage 2)

Stage 2 expects these files under `ECB_ESMA_BASE_PATH` (or wherever you point the scripts):

- `ESMA Template (2).xlsx`
- `pool_mapping.json`
- ECB gz files under `ECB_Data/ECB Data/`
- Stage 1 outputs under `ESMA_UE_Collat_Merged/`

## Documentation (Read the Docs)

- Source: `docs/source/`
- Build locally:

```bash
python -m pip install -r docs/requirements.txt
sphinx-build -b html docs/source docs/_build/html
```

## Notes

- The pipeline is designed for **very large datasets** (hundreds of GB). Stage 2 and Stage 4 use
  disk‑based strategies (chunking and DuckDB sorting) to avoid RAM blow‑ups.
- Stage 2 performs deduplication **only** for the pools listed in `POOLS_WITH_OVERLAP`, matching the
  original analysis and performance optimisation.
- Stage 4 uses DuckDB with a temp directory and database file at `D:/duckdb_temp/` (as in the original notebook).
  Ensure the drive/path exists and has sufficient free space.

## ECB-ESMA Securitization Data Processing Pipeline (Alternative Full Documentation)

A comprehensive data processing pipeline for merging, harmonizing, and organizing European securitization loan-level data from two regulatory sources:
- **ECB (European Central Bank)**: ABS loan-level data
- **ESMA (European Securities and Markets Authority)**: STS securitization data

## Overview

This pipeline processes ~328GB of securitization data through four sequential stages:

| Stage | Notebook | Input | Output |
|-------|----------|-------|--------|
| 1 | `merge_ue_collateral.ipynb` | ESMA UE + Collateral CSVs | Merged ESMA files |
| 2 | `full_universe_merge.ipynb` | ECB .gz + Merged ESMA | Pool-level merged files |
| 3 | `country_merge_all_production.ipynb` | Pool-level files | Country-grouped files |
| 4 | `sort_country_files.ipynb` | Country files | Sorted country files |

---

## Stage 1: ESMA UE-Collateral Merge

**Notebook:** `merge_ue_collateral.ipynb`

### Purpose
Merge ESMA Underlying Exposures (UE) files with their corresponding Collateral files. ESMA provides loan data and collateral data as separate files that must be joined.

### Input
- **Location:** `c:\Users\jonat\Downloads\_unique_csv_master\`
- **File format:** `1_<ASSET_TYPE>_<CATEGORY>_<POOL_ID>_<DATE>_<SEQUENCE>.csv`
- **Example:** 
  - `1_RMB_UE_213800WQJJDCAN4BCO57N201901_2021-04-30_29907.csv` (UE file)
  - `1_RMB_Collateral_213800WQJJDCAN4BCO57N201901_2021-04-30_29907.csv` (Collateral file)

### Process

1. **Parse Filenames**: Extract asset type, category (UE/Collateral), pool identifier, date, and sequence number using regex pattern:

   ```text
   ^1_(\w+)_(UE|Collateral)_(.+)_(\d{4}-\d{2}-\d{2})_(\d+)\.csv$
   ```
   
   | Group | Captures |
   |-------|----------|
   | 1 | Asset type (RMB, CMR, NPE, etc.) |
   | 2 | Category (UE or Collateral) |
   | 3 | Pool identifier (LEI + vintage) |
   | 4 | Date (YYYY-MM-DD) |
   | 5 | Sequence number |

2. **Match File Pairs**: Create lookup by `(asset_type, identifier, date)` to find UE-Collateral pairs.

3. **Auto-Detect Merge Keys**: The pipeline automatically detects the correct join columns:
   - UE files: Columns ending in `L2` (e.g., `RREL2`, `NPEL2`, `CRPL2`)
   - Collateral files: Corresponding `C2` columns (e.g., `RREC2`, `NPEC2`, `CRPC2`)
   - Pattern matching: `RREL2` → `RREC2` (prefix `RRE` → `RRE` + `C2`)

4. **Merge Operations**:
   - Convert merge keys to string (prevents type mismatch errors)
   - Drop duplicate columns from Collateral: `Sec_Id`, `Pool_Cutoff_Date`, and all `*C1` columns
   - Perform LEFT JOIN on loan identifier (preserves all UE rows)

### Output
- **Location:** `c:\Users\jonat\Downloads\_unique_csv_master\ESMA_UE_Collat_Merged\`
- **File format:** `1_<ASSET_TYPE>_UE_Collateral_<POOL_ID>_<DATE>_<SEQUENCE>.csv`

### Key Functions

| Function | Description |
|----------|-------------|
| `parse_filename()` | Parse ESMA filename into components |
| `detect_merge_keys()` | Auto-detect `*L2`/`*C2` join columns |
| `merge_ue_collateral()` | Perform the actual merge with statistics |

---

## Stage 2: ECB-ESMA Pool Merge

**Notebook:** `full_universe_merge.ipynb`

### Purpose
Merge ECB and ESMA data at the pool level, harmonizing column names using the official ESMA template mapping. This creates a unified dataset from both regulatory sources.

### Input
- **ECB files:** `c:\Users\jonat\Downloads\_unique_csv_master\ECB_Data\ECB Data\*.gz`
- **ESMA files:** `c:\Users\jonat\Downloads\_unique_csv_master\ESMA_UE_Collat_Merged\*.csv`
- **Template:** `c:\Users\jonat\Downloads\_unique_csv_master\ESMA Template (2).xlsx`
- **Pool mapping:** `c:\Users\jonat\Downloads\_unique_csv_master\pool_mapping.json`

### Column Mapping

The ESMA template Excel file provides the mapping between ECB and ESMA column names:

| ECB Column | ESMA Column | Description |
|------------|-------------|-------------|
| `AR1` | `RREL6` | Data Cut-Off Date |
| `AR2` | `RREL3` | Loan Identifier |
| `AR128` | `RREC6` | Geographic Region (NUTS) |
| ... | ... | (72 ECB→ESMA mappings) |

The mapping is loaded from the Excel template with columns:
- `FIELD CODE` → ESMA column name
- `For info: existing ECB or EBA NPL template field code` → ECB column name

### Pool Categories

The `pool_mapping.json` file categorizes pools into three types:

1. **Matched Pools**: ECB pool has a corresponding ESMA pool
2. **ECB-Only Pools**: Only ECB data exists
3. **ESMA-Only Pools**: Only ESMA data exists

### ECB Data Preparation (`prepare_ecb_data()`)

When loading ECB data, the following transformations are applied:

1. **Column Renaming**: ECB columns are renamed to ESMA equivalents using the template mapping
2. **Source Marker**: A `source='ECB'` column is added to track data origin
3. **Date Normalization**: `RREL6` (or `AR1` if RREL6 missing) is truncated to `YYYY-MM` format into `date_ym` column for deduplication

### ESMA Data Preparation (`prepare_esma_data()`)

ESMA data requires minimal transformation:

1. **Source Marker**: A `source='ESMA'` column is added
2. **Date Normalization**: `RREL6` truncated to `YYYY-MM` format into `date_ym` column

### Deduplication Logic

**Critical**: Only 3 pools have temporal overlap (same loan on same date from both sources):

| Pool ID | Overlap Period | Months |
|---------|----------------|--------|
| `RMBMBE000095100120084` | 2022-03 to 2023-12 | 8 |
| `RMBMFR000083100220149` | 2021-05 to 2021-08 | 4 |
| `RMBMNL000185100120109` | 2024-06 | 1 |

For these pools only, deduplication is performed via `remove_duplicates_prefer_esma()`:

```python
# Algorithm:
# 1. Build set of (loan_id, date_ym) pairs from ESMA rows
# 2. For each row:
#    - If source='ESMA': keep
#    - If source='ECB' and (loan_id, date_ym) NOT in ESMA set: keep
#    - If source='ECB' and (loan_id, date_ym) IN ESMA set: drop
```

For all other matched pools, data is simply concatenated (no dedup needed) - this is a significant performance optimization.

### Process

1. **Load Template Mapping**: Read ECB→ESMA column mappings from Excel
2. **Index All Files**: Build dictionaries of ECB and ESMA files by pool ID
3. **Process Each Pool**:
   - **Matched pools**: Load both sources, rename ECB columns, add `source` marker, concat/dedup
   - **ECB-only pools**: Load, rename columns, add `source='ECB'`
   - **ESMA-only pools**: Load, add `source='ESMA'`
4. **Large Pool Handling**: Pools >100MB compressed are processed in chunks to avoid memory issues

### Output
- **Location:** `D:\ECB_ESMA_MERGED\`
- **Structure:**
  ```
  D:\ECB_ESMA_MERGED\
  ├── matched\         # ECB-ESMA matched pools
  │   └── {POOL_ID}.csv
  ├── ecb_only\        # ECB-only pools
  │   └── {POOL_ID}.csv
  └── esma_only\       # ESMA-only pools
      └── {POOL_ID}.csv
  ```

### Key Functions

| Function | Description |
|----------|-------------|
| `load_ecb_file()` | Load .gz file with zlib fallback for problematic gzip |
| `prepare_ecb_data()` | Rename ECB columns to ESMA equivalents, add source marker |
| `remove_duplicates_prefer_esma()` | Dedup by loan+date, keeping ESMA |
| `process_matched_pool()` | Full processing for matched pool pairs |

### Resume Safety
- Checkpoint file tracks completed pools
- Scans output folders to detect already-processed pools
- Cleans up `.tmp` files from interrupted runs

---

## Stage 3: Country Grouping

**Notebook:** `country_merge_all_production.ipynb`

### Purpose
Group all pool-level files by country, creating one large CSV per country. This enables country-specific analysis.

### Input
- **Location:** `D:\ECB_ESMA_MERGED\` (all subfolders)
- **Files:** `{POOL_ID}.csv` from matched, ecb_only, esma_only folders

### Country Detection Priority

The pipeline detects country using this priority order (first valid value wins):

| Priority | Column | Description | Example |
|----------|--------|-------------|---------|
| 1 | `RREL81` | Lender Country (ESMA) | `IT`, `DE` |
| 2 | `RREL84` | Originator Country (ESMA) | `FR`, `ES` |
| 3 | `RREC6` | Geographic Region NUTS (first 2 chars) | `DE21` → `DE` |
| 4 | `RREL11` | Geographic Region Obligor NUTS | `IT42` → `IT` |
| 5 | `AR129` | ECB Geographic | `NL31` → `NL` |
| 6 | `NPEL20`/`NPEL23` | NPE Template Country Fields | `PT` |
| 7 | Pool ID | Fallback: `RMBM{CC}...` or `RMBS{CC}...` | `RMBMIT...` → `IT` |
| 8 | — | Default | `UNKNOWN` |

### Process Overview

1. **Scan All Files**: Index all CSV files from matched, ecb_only, esma_only folders
2. **Detect Country**: Sample first 100 rows of each file to determine country
3. **Build Country Index**: Group files by detected country
4. **Phase 1 - Union of Columns**: Scan headers of ALL files for a country to determine full column set
5. **Phase 2 - Stream-Merge**: Process each file in chunks, append to output

### Unified Column Schema (Union of Columns)

**Critical Algorithm**: Different pools have different columns. To merge into one file:

```python
# Phase 1: Build unified column schema
all_columns = set()

for file in country_files:
    df_header = pd.read_csv(file, nrows=0)  # Read ONLY header row
    all_columns.update(df_header.columns.tolist())

# Sort for consistent ordering across runs
all_columns_sorted = sorted(list(all_columns))
```

This ensures:
- Every column from ANY file is included in the output
- Column order is deterministic (alphabetically sorted)
- Files with different schemas can be merged together

### Chunked I/O Strategy (Stream-Merge)

**Memory-Efficient Processing**: Files are processed in 100,000-row chunks:

```python
# Phase 2: Stream-merge all files
for file in country_files:
    for chunk in pd.read_csv(file, chunksize=100000, low_memory=False):
        # Add missing columns with NaN
        for col in all_columns_sorted:
            if col not in chunk.columns:
                chunk[col] = np.nan
        
        # Reorder to unified schema
        chunk = chunk[all_columns_sorted]
        
        # Append to temp file (header only on first chunk)
        chunk.to_csv(temp_path, mode='a', index=False, header=first_chunk)
        first_chunk = False
        
        del chunk  # Free memory immediately
```

**Why This Works**:
- Never loads entire file into memory
- Handles files of any size (tested on 100+ GB)
- Maintains consistent column order across all rows
- Uses `.tmp` file for atomic writes (rename on completion)

### Output
- **Location:** `D:\ECB_ESMA_BY_COUNTRY_ALL\`
- **Files:** `{COUNTRY_CODE}.csv` (e.g., `IT.csv`, `DE.csv`, `FR.csv`)
- **Log:** `merge_log.json` with statistics

### Countries Processed

| Country | Description |
|---------|-------------|
| `BE` | Belgium |
| `DE` | Germany |
| `ES` | Spain |
| `FR` | France |
| `IE` | Ireland |
| `IT` | Italy |
| `NL` | Netherlands |
| `PT` | Portugal |
| `UK` | United Kingdom |
| `UNKNOWN` | Could not detect country (later identified as Portugal legacy) |

### Resume Safety
- Skips countries with existing output files
- Cleans up `.tmp` files on error or interrupt
- Safe to re-run after interruption

---

## Stage 4: Sorting

**Notebook:** `sort_country_files.ipynb`

### Purpose
Sort each country file for deterministic ordering that groups loan-collateral time series together.

### Input
- **Location:** `D:\ECB_ESMA_BY_COUNTRY_ALL\`
- **Files:** `{COUNTRY_CODE}.csv`

### Sort Order

Files are sorted by three columns in this order:

| Order | Column | Description | Purpose |
|-------|--------|-------------|---------|
| 1 | `RREL3` | Loan Identifier | Group all observations for same loan |
| 2 | `RREC3` | Collateral Identifier | Group collateral within each loan |
| 3 | `RREL6` | Data Cut-Off Date | Chronological within loan-collateral pair |

**Important Notes:**
- `RREL1` is Pool Identifier (securitization deal), NOT individual loan
- `RREL6` has data from BOTH ECB (`AR1`→`RREL6`) and ESMA sources
- `Pool_Cutoff_Date` only exists in ESMA files (not used for sorting)
- `RREC9` (Property Type) is NOT included — it's a time-varying attribute

### Technology: DuckDB

The sorting uses **DuckDB** with a **persistent database file** for true disk-based external sorting:

```python
# Configuration
db_path = 'D:/duckdb_temp/sort_temp.duckdb'
temp_dir = 'D:/duckdb_temp'

# Auto-detect resources
available_ram_gb = psutil.virtual_memory().available / (1024**3)
memory_limit_gb = max(8, min(int(available_ram_gb * 0.7), 64))
threads = max(2, cpu_count // 2)
```

**Key Settings:**
- `preserve_insertion_order = false` (allows parallel sorting)
- `all_varchar = true` (avoids type inference issues)
- `quote = '"'` (handles fields with commas inside quotes)

### Process

1. **Create Persistent DB**: DuckDB database file on D: drive
2. **Read with VARCHAR**: All columns read as strings to avoid type errors
3. **ORDER BY**: Sort by detected columns
4. **COPY TO**: Export sorted result to `.tmp` file
5. **Atomic Rename**: Replace original with sorted file

### Output
- **Location:** `D:\ECB_ESMA_BY_COUNTRY_SORTED\`
- **Files:** `{COUNTRY_CODE}.csv` (sorted)

### Verification Query

The notebook includes a verification query to check for sort order violations:

```sql
WITH sorted_check AS (
    SELECT 
        RREL3, RREC3, RREL6,
        LAG(RREL3) OVER () as prev_rrel3,
        LAG(RREC3) OVER () as prev_rrec3,
        LAG(RREL6) OVER () as prev_rrel6
    FROM read_csv('{file}', all_varchar=true)
)
SELECT COUNT(*) as violations
FROM sorted_check
WHERE prev_rrel3 IS NOT NULL 
  AND (
      RREL3 < prev_rrel3 
      OR (RREL3 = prev_rrel3 AND RREC3 < prev_rrec3)
      OR (RREL3 = prev_rrel3 AND RREC3 = prev_rrec3 AND RREL6 < prev_rrel6)
  )
```

---

## Special Case: PT_LEGACY.csv

### Background
One file was originally labeled `UNKNOWN.csv` because country detection failed. Investigation revealed:

- **205,991 rows**, **1 pool**
- Pool ID: `213800WQJJDCAN4BCO57N202001`
- All data from ESMA source
- `RREC6` contains Portuguese NUTS codes (`PT118`, `PT171`, etc.)

### Why Detection Failed
This file uses a **different/older ESMA template**:
- Only **192 columns** (vs 328 in standard PT.csv)
- `RREC4` contains numeric values instead of ISO country codes
- Missing many columns present in newer template

### Resolution
Renamed to `PT_LEGACY.csv` to indicate:
1. It's Portuguese data
2. It uses a legacy schema incompatible with main PT.csv

---

## Directory Structure

```
c:\Users\jonat\Downloads\_unique_csv_master\
├── 1_*.csv                           # Raw ESMA files
├── ESMA_UE_Collat_Merged\            # Stage 1 output
├── ECB_Data\ECB Data\*.gz            # Raw ECB files
├── pool_mapping.json                 # ECB-ESMA pool matches
├── ESMA Template (2).xlsx            # Column mapping
├── merge_ue_collateral.ipynb         # Stage 1
├── full_universe_merge.ipynb         # Stage 2
├── country_merge_all_production.ipynb # Stage 3
└── sort_country_files.ipynb          # Stage 4

D:\ECB_ESMA_MERGED\                   # Stage 2 output
├── matched\
├── ecb_only\
└── esma_only\

D:\ECB_ESMA_BY_COUNTRY_ALL\           # Stage 3 output
└── {COUNTRY}.csv

D:\ECB_ESMA_BY_COUNTRY_SORTED\        # Stage 4 output (final)
├── BE.csv
├── DE.csv
├── ES.csv
├── FR.csv
├── IE.csv
├── IT.csv
├── NL.csv
├── PT.csv
├── PT_LEGACY.csv
└── UK.csv
```

---

## Key Column Reference

| Column | Description | Source |
|--------|-------------|--------|
| `RREL1` | Pool Identifier | Both |
| `RREL3` | Loan Identifier | Both |
| `RREL6` | Data Cut-Off Date | Both (ECB: mapped from AR1) |
| `RREC3` | Collateral Identifier | Both |
| `RREC6` | Geographic Region (NUTS) | Both (ECB: mapped from AR128) |
| `RREL81` | Lender Country | ESMA |
| `RREL84` | Originator Country | ESMA |
| `source` | Data source marker | Added by pipeline (`ECB` or `ESMA`) |
| `ecb_pool_id` | Original ECB pool ID | Added for matched pools |
| `esma_pool_id` | Original ESMA pool ID | Added for matched pools |

---

## Dependencies

```
pandas
numpy
duckdb >= 1.4.3
psutil
tqdm
openpyxl  # For reading Excel template
```

---

## Usage

Run notebooks in order:

```bash
# 1. Merge ESMA UE + Collateral files
jupyter notebook merge_ue_collateral.ipynb

# 2. Merge ECB + ESMA at pool level
jupyter notebook full_universe_merge.ipynb

# 3. Group by country
jupyter notebook country_merge_all_production.ipynb

# 4. Sort each country file
jupyter notebook sort_country_files.ipynb
```

Each notebook is **resume-safe** and can be re-run after interruption.

---

## License

This pipeline is for processing publicly available regulatory data from ECB and ESMA securitization disclosures.

