import os

from esma_ecb_merging_sorting.merge_ue_collateral import batch_merge_ue_collateral, summarize_batch_merge

# Notebook default: CSV_FOLDER = r"c:\Users\jonat\Downloads\_unique_csv_master"
CSV_FOLDER = os.environ.get("ECB_ESMA_BASE_PATH", r"c:\Users\jonat\Downloads\_unique_csv_master")

# Notebook default: OUTPUT_FOLDER = os.path.join(CSV_FOLDER, "ESMA_UE_Collat_Merged")
OUTPUT_FOLDER = os.environ.get("ECB_ESMA_STAGE1_OUTPUT_DIR", os.path.join(CSV_FOLDER, "ESMA_UE_Collat_Merged"))

if __name__ == "__main__":
    print(f"Source folder: {CSV_FOLDER}")
    print(f"Output folder: {OUTPUT_FOLDER}")

    result = batch_merge_ue_collateral(
        csv_folder=CSV_FOLDER,
        output_folder=OUTPUT_FOLDER,
        show_progress=True,
    )

    print("\n" + "=" * 80)
    print("MERGE COMPLETE")
    print("=" * 80)
    print(f"Time elapsed: {result['elapsed_seconds']:.1f} seconds")
    print(f"Successful: {result['successful']}")
    print(f"Failed: {result['failed']}")

    summary = summarize_batch_merge(result["all_stats"], result["failed_pairs"])
    if summary:
        print("\nSummary:")
        for k, v in summary.items():
            print(f"- {k}: {v}")
