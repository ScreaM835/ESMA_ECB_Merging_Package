# Usage

## Install

```bash
python -m pip install -r requirements.txt
python -m pip install -e .
```

## Run the pipeline

Run stages in order:

```bash
python scripts/stage1_merge_ue_collateral_run.py
python scripts/stage2_full_universe_merge_run.py
python scripts/stage3_country_merge_all_production_run.py
python scripts/stage4_sort_country_files_run.py
```

## Optional verification scripts

See the `tests/` folder for post‑run verification and integrity checks extracted from the original notebooks.
