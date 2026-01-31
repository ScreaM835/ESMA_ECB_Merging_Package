# Pipeline overview

This project processes securitisation loan‑level data from two sources:

- **ECB**: ABS loan‑level data (gzipped CSVs)
- **ESMA**: STS securitisation disclosures (CSV)

The pipeline is implemented as four sequential stages. Each stage’s logic is the original working notebook logic,
extracted into importable Python modules.

## Stages

| Stage | Implementation module | Runner script | Input | Output |
|---:|---|---|---|---|
| 1 | `esma_ecb_merging_sorting.merge_ue_collateral` | `scripts/stage1_merge_ue_collateral_run.py` | ESMA UE + Collateral CSVs | Merged ESMA UE+Collateral CSVs |
| 2 | `esma_ecb_merging_sorting.full_universe_merge` | `scripts/stage2_full_universe_merge_run.py` | ECB `.gz` + Stage 1 output | Pool‑level merged files (`matched/`, `ecb_only/`, `esma_only/`) |
| 3 | `esma_ecb_merging_sorting.country_merge_all_production` | `scripts/stage3_country_merge_all_production_run.py` | Pool‑level files | Country‑grouped files (`IT.csv`, `DE.csv`, …) |
| 4 | `esma_ecb_merging_sorting.sort_country_files` | `scripts/stage4_sort_country_files_run.py` | Country files | Sorted country files |

---

## Stage 1 — ESMA UE ↔ Collateral merge

**Goal:** Merge ESMA *Underlying Exposures* (UE) files with their corresponding *Collateral* files.

**Matching rule:** The project pairs files by `(asset_type, identifier, date)` parsed from the filename.

**Merge rule (core methodology):**
- Auto‑detect merge keys by scanning column names:
  - UE key: `*L2`
  - Collateral key: `*C2`
  - Match by prefix pattern (e.g., `RREL2` ↔ `RREC2`, `NPEL2` ↔ `NPEC2`, …)
- Convert merge keys to `str` to prevent dtype mismatch
- Drop duplicated metadata columns from collateral before merge (`Sec_Id`, `Pool_Cutoff_Date`, `*C1`)
- Left join (preserve all UE rows)

---

## Stage 2 — ECB ↔ ESMA full universe merge (pool level)

**Goal:** Combine ECB and ESMA datasets at pool level with harmonised column naming.

### Column mapping

Stage 2 uses the ESMA template mapping file:

- `ESMA Template (2).xlsx` (Sheet1)

The notebook logic builds:
- `esma_to_ecb`: ESMA code → ECB code
- `ecb_to_esma`: ECB code → ESMA code (prefers ESMA fields marked “New” when duplicates exist)

### Pool mapping

Stage 2 requires:

- `pool_mapping.json`

This provides ECB→ESMA pool matches and allows the pipeline to separate:
- `matched` pools
- `ecb_only` pools
- `esma_only` pools

### Performance & correctness safeguards

- **Large pool handling:** pools above a compressed-size threshold are processed in chunked mode to avoid memory pressure.
- **Atomic writes:** outputs are written to `*.tmp` first and then renamed (prevents partial/corrupted outputs after interruption).
- **Deduplication optimisation:** deduplication is only run for pools listed in `POOLS_WITH_OVERLAP` (the only pools with temporal overlap).

Deduplication logic: for duplicated loan+month pairs (`RREL3` + `date_ym`), keep ESMA rows and drop ECB rows where an ESMA equivalent exists.

---

## Stage 3 — Country merge

**Goal:** Consolidate pool‑level outputs into country‑level datasets.

### Country detection priority (VERIFIED — DO NOT MODIFY)

Country detection uses a strict priority order (as in the original code):

1. `RREL81` (Lender Country — ESMA)
2. `RREL84` (Originator Country — ESMA)
3. `RREC6` (NUTS code; first 2 chars)
4. `RREL11` (Obligor NUTS; first 2 chars, excluding `ND*`)
5. `AR129` (ECB geographic; NUTS-like)
6. `NPEL20` / `NPEL23` (NPE template country fields)
7. Fallback: extract from pool ID if it starts with `RMBM` or `RMBS`

### Merge strategy

- Build a unified schema by unioning the columns across all input files for a country
- Stream-merge input files with `chunksize` (never load entire large files into memory)
- Resume-safe:
  - Skip if final output exists
  - Remove incomplete `.tmp` files from prior interrupted runs
  - Rename temp to final upon success

---

## Stage 4 — Country file sorting (DuckDB)

**Goal:** Produce deterministic ordering for each country file.

**Sort key (preserved):**
1. `RREL3` — Loan Identifier
2. `RREC3` — Collateral Identifier
3. `RREL6` — Data Cut‑Off Date (chronological within each loan‑collateral pair)

**Implementation:** disk-backed sorting via DuckDB using a persistent database file and a temp directory.
All columns are read as `VARCHAR` to avoid type inference issues.
