"""
esma_ecb_merging_sorting

A production ETL pipeline for merging and organising ECB + ESMA loan-level securitisation data.

The package exposes the original notebook logic as importable Python modules.
"""

from __future__ import annotations

__all__ = [
    "__version__",
    # Stage 1
    "merge_ue_collateral",
    "batch_merge_ue_collateral",
    # Stage 2
    "load_template_mapping",
    "build_file_index",
    # Stage 3
    "detect_country",
    "merge_country_files",
    # Stage 4 (requires duckdb)
    "sort_country_file",
]

__version__ = "0.1.0"

from .merge_ue_collateral import batch_merge_ue_collateral, merge_ue_collateral
from .full_universe_merge import build_file_index, load_template_mapping
from .country_merge_all_production import detect_country, merge_country_files

try:
    from .sort_country_files import sort_country_file  # duckdb dependency
except ModuleNotFoundError:  # pragma: no cover
    sort_country_file = None
