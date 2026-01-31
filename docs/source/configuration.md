# Configuration

The original notebooks used hard‑coded Windows paths. The packaged repository keeps those defaults, but the
`scripts/` entry points allow you to override them using environment variables.

## Environment variables

### Common

- `ECB_ESMA_BASE_PATH`  
  Default: `c:\Users\jonat\Downloads\_unique_csv_master`

Stage 2 derives these paths from `ECB_ESMA_BASE_PATH`:

- ECB input directory: `<BASE>\ECB_Data\ECB Data`
- ESMA input directory: `<BASE>\ESMA_UE_Collat_Merged` (Stage 1 output)
- Template mapping: `<BASE>\ESMA Template (2).xlsx`
- Pool mapping: `<BASE>\pool_mapping.json`

### Stage outputs

- `ECB_ESMA_STAGE2_OUTPUT_DIR`  
  Default: `D:\ECB_ESMA_MERGED`

- `ECB_ESMA_STAGE3_OUTPUT_DIR`  
  Default: `D:\ECB_ESMA_BY_COUNTRY_ALL`

- `ECB_ESMA_STAGE4_OUTPUT_DIR`  
  Default: `D:\ECB_ESMA_BY_COUNTRY_SORTED`

## Data prerequisites

Stage 2 requires:
- `ESMA Template (2).xlsx`
- `pool_mapping.json`
- ECB `.gz` files
- Stage 1 merged ESMA CSVs
