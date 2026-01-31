# ECB–ESMA Securitisation Data Processing Pipeline (Python Package)

This repository packages the original (working) notebook pipeline into a reusable **Python package**.
The core methodology and variable naming inside the stage implementations are preserved.

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

