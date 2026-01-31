# API overview

This package intentionally mirrors the original notebook structure.

## Stage 1 — `merge_ue_collateral`

Key functions:
- `merge_ue_collateral(ue_path, collateral_path)`
- `batch_merge_ue_collateral(csv_folder, output_folder=None, show_progress=True)`

## Stage 2 — `full_universe_merge`

Initialisation:
- `load_template_mapping(template_path=None)`
- `build_file_index(ecb_path=None, esma_path=None, pool_mapping_path=None)`

Pool processing (advanced):
- `process_matched_pool(...)`
- `process_matched_pool_chunked(...)`
- `process_ecb_only_pool(...)`
- `process_ecb_only_pool_chunked(...)`
- `process_esma_only_pool(...)`
- `process_esma_only_pool_chunked(...)`

## Stage 3 — `country_merge_all_production`

- `detect_country(df, pool_id)`
- `merge_country_files(country_code, country_files, output_dir, chunk_size=100000)`

## Stage 4 — `sort_country_files`

- `sort_country_file(input_path, output_path, sort_columns)`
- `sort_all_country_files(...)`
