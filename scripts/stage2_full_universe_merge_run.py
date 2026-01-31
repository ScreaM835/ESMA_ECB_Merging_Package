import gc
import os
import time

import esma_ecb_merging_sorting.full_universe_merge as stage2

# -----------------------------------------------------------------------------
# Configuration (defaults preserved from original notebook, override via env vars)
# -----------------------------------------------------------------------------


def main():
    BASE_PATH = os.environ.get("ECB_ESMA_BASE_PATH", stage2.BASE_PATH)
    OUTPUT_DIR = os.environ.get("ECB_ESMA_STAGE2_OUTPUT_DIR", stage2.OUTPUT_DIR)

    # Apply configuration to the module (no methodology changes)
    stage2.configure_paths(base_path=BASE_PATH, output_dir=OUTPUT_DIR)

    # Ensure output directory exists (so logging + atomic writes work)
    os.makedirs(stage2.OUTPUT_DIR, exist_ok=True)

    # Initialise mappings and file index (required)
    stage2.load_template_mapping()
    stage2.build_file_index()

    # Alias functions used in the original notebook execution cells (safer than `from ... import *`)
    scan_completed_pools = stage2.scan_completed_pools
    log_message = stage2.log_message
    is_large_pool = stage2.is_large_pool
    get_pool_compressed_size = stage2.get_pool_compressed_size

    process_matched_pool = stage2.process_matched_pool
    process_matched_pool_chunked = stage2.process_matched_pool_chunked
    process_ecb_only_pool = stage2.process_ecb_only_pool
    process_ecb_only_pool_chunked = stage2.process_ecb_only_pool_chunked
    process_esma_only_pool = stage2.process_esma_only_pool
    process_esma_only_pool_chunked = stage2.process_esma_only_pool_chunked

    save_pool_result = stage2.save_pool_result

    # Alias module globals populated by build_file_index()
    matched_pools = stage2.matched_pools
    ecb_only_pools = stage2.ecb_only_pools
    esma_only_pools = stage2.esma_only_pools
    OUTPUT_DIR = stage2.OUTPUT_DIR

    # =============================================================================
    # CELL 8: Main Processing Loop - MATCHED POOLS (with large pool handling)
    # =============================================================================
    log_message("="*70)
    log_message("STARTING MATCHED POOLS PROCESSING")
    log_message("="*70)

    # Scan output folders to find already-completed pools (file-based tracking)
    completed_pools = scan_completed_pools()
    completed_matched = completed_pools['matched']

    # Track total rows for this session
    session_rows_processed = 0

    # Get pools to process (skip already completed)
    pools_to_process = [(ecb_id, info['esma_pool']) 
                        for ecb_id, info in matched_pools.items() 
                        if ecb_id not in completed_matched]

    log_message(f"Matched pools: {len(matched_pools)} total, {len(pools_to_process)} remaining")

    # Identify large pools - check BOTH ECB and ESMA sides (either can cause memory issues)
    def is_large_matched_pool(ecb_id, esma_id):
        return is_large_pool(ecb_id, 'ecb') or is_large_pool(esma_id, 'esma')

    large_matched = [(e, s) for e, s in pools_to_process if is_large_matched_pool(e, s)]
    normal_matched = [(e, s) for e, s in pools_to_process if not is_large_matched_pool(e, s)]
    log_message(f"  Normal pools: {len(normal_matched)}, Large pools (chunked): {len(large_matched)}")

    matched_start = time.time()

    # Process normal pools first
    for i, (ecb_pool_id, esma_pool_id) in enumerate(normal_matched):
        pool_start = time.time()
        log_message(f"\n[{i+1}/{len(normal_matched)}] Processing matched pool: {ecb_pool_id}")

        try:
            # Process pool
            result_df = process_matched_pool(ecb_pool_id, esma_pool_id)

            # Save result
            rows_saved = save_pool_result(result_df, "matched", ecb_pool_id)

            # Free memory
            del result_df
            gc.collect()

            # Track session progress (file-based tracking means completion is automatic)
            session_rows_processed += rows_saved

            pool_elapsed = time.time() - pool_start
            log_message(f"  Saved {rows_saved:,} rows in {pool_elapsed:.1f}s")

        except Exception as e:
            error_msg = f"ERROR processing {ecb_pool_id}: {str(e)}"
            log_message(error_msg)
            gc.collect()

    # Process large pools with chunked mode
    for i, (ecb_pool_id, esma_pool_id) in enumerate(large_matched):
        pool_start = time.time()
        ecb_size_mb = get_pool_compressed_size(ecb_pool_id, 'ecb') / 1024 / 1024
        esma_size_mb = get_pool_compressed_size(esma_pool_id, 'esma') / 1024 / 1024
        log_message(f"\n[LARGE {i+1}/{len(large_matched)}] Processing matched pool: {ecb_pool_id}")
        log_message(f"  ECB: {ecb_size_mb:.0f} MB, ESMA: {esma_size_mb:.0f} MB")

        # Aggressive memory cleanup before large pool
        gc.collect()
        gc.collect()  # Run twice to catch cyclic references
        time.sleep(1)  # Brief pause to let OS reclaim memory

        try:
            # Setup output path with ATOMIC write pattern
            subdir = os.path.join(OUTPUT_DIR, "matched")
            os.makedirs(subdir, exist_ok=True)
            safe_pool_id = ecb_pool_id.replace('/', '_').replace('\\', '_')
            output_path = os.path.join(subdir, f"{safe_pool_id}.csv")
            temp_path = output_path + ".tmp"  # Write to temp file first

            # Remove existing files if any (fresh start)
            if os.path.exists(output_path):
                os.remove(output_path)
            if os.path.exists(temp_path):
                os.remove(temp_path)

            # Process with chunked mode - writes to TEMP file
            rows_saved = process_matched_pool_chunked(ecb_pool_id, esma_pool_id, temp_path)

            # ATOMIC: Rename temp to final ONLY after complete success
            os.replace(temp_path, output_path)

            # Track session progress (file-based tracking means completion is automatic)
            session_rows_processed += rows_saved

            pool_elapsed = time.time() - pool_start
            log_message(f"  Saved {rows_saved:,} rows in {pool_elapsed:.1f}s (chunked)")

        except Exception as e:
            error_msg = f"ERROR processing {ecb_pool_id}: {str(e)}"
            log_message(error_msg)
            # Clean up temp file if it exists
            if os.path.exists(temp_path):
                os.remove(temp_path)
            gc.collect()

    matched_elapsed = time.time() - matched_start
    log_message(f"\nMatched pools complete: {matched_elapsed:.1f}s")
    log_message(f"Session rows processed: {session_rows_processed:,}")


    # =============================================================================
    # CELL 9: Main Processing Loop - ECB-ONLY POOLS (with large pool handling)
    # =============================================================================
    log_message("="*70)
    log_message("STARTING ECB-ONLY POOLS PROCESSING")
    log_message("="*70)

    # Scan output folders to find already-completed pools (file-based tracking)
    completed_pools = scan_completed_pools()
    completed_ecb_only = completed_pools['ecb_only']

    # Track total rows for this session
    session_rows_processed = 0

    # Get pools to process (skip already completed)
    ecb_only_to_process = [p for p in ecb_only_pools if p not in completed_ecb_only]

    log_message(f"ECB-only pools: {len(ecb_only_pools)} total, {len(ecb_only_to_process)} remaining")

    # Identify large pools
    large_pools = [p for p in ecb_only_to_process if is_large_pool(p, 'ecb')]
    normal_pools = [p for p in ecb_only_to_process if not is_large_pool(p, 'ecb')]
    log_message(f"  Normal pools: {len(normal_pools)}, Large pools (chunked): {len(large_pools)}")

    ecb_only_start = time.time()

    # Process normal pools first
    for i, ecb_pool_id in enumerate(normal_pools):
        pool_start = time.time()
        log_message(f"\n[{i+1}/{len(normal_pools)}] Processing ECB-only pool: {ecb_pool_id}")

        try:
            # Process pool
            result_df = process_ecb_only_pool(ecb_pool_id)

            # Save result
            rows_saved = save_pool_result(result_df, "ecb_only", ecb_pool_id)

            # Free memory
            del result_df
            gc.collect()

            # Track session progress (file-based tracking means completion is automatic)
            session_rows_processed += rows_saved

            pool_elapsed = time.time() - pool_start
            log_message(f"  Saved {rows_saved:,} rows in {pool_elapsed:.1f}s")

        except Exception as e:
            error_msg = f"ERROR processing {ecb_pool_id}: {str(e)}"
            log_message(error_msg)
            gc.collect()

    # Process large pools with chunked mode
    for i, ecb_pool_id in enumerate(large_pools):
        pool_start = time.time()
        pool_size_mb = get_pool_compressed_size(ecb_pool_id, 'ecb') / 1024 / 1024
        log_message(f"\n[LARGE {i+1}/{len(large_pools)}] Processing ECB-only pool: {ecb_pool_id} ({pool_size_mb:.0f} MB compressed)")

        try:
            # Setup output path with ATOMIC write pattern
            subdir = os.path.join(OUTPUT_DIR, "ecb_only")
            os.makedirs(subdir, exist_ok=True)
            safe_pool_id = ecb_pool_id.replace('/', '_').replace('\\', '_')
            output_path = os.path.join(subdir, f"{safe_pool_id}.csv")
            temp_path = output_path + ".tmp"

            # Remove existing files if any (fresh start)
            if os.path.exists(output_path):
                os.remove(output_path)
            if os.path.exists(temp_path):
                os.remove(temp_path)

            # Process with chunked mode - writes to TEMP file
            rows_saved = process_ecb_only_pool_chunked(ecb_pool_id, temp_path)

            # ATOMIC: Rename temp to final ONLY after complete success
            os.replace(temp_path, output_path)

            # Track session progress (file-based tracking means completion is automatic)
            session_rows_processed += rows_saved

            pool_elapsed = time.time() - pool_start
            log_message(f"  Saved {rows_saved:,} rows in {pool_elapsed:.1f}s (chunked)")

        except Exception as e:
            error_msg = f"ERROR processing {ecb_pool_id}: {str(e)}"
            log_message(error_msg)
            # Clean up temp file if it exists
            if os.path.exists(temp_path):
                os.remove(temp_path)
            gc.collect()

    ecb_only_elapsed = time.time() - ecb_only_start
    log_message(f"\nECB-only pools complete: {ecb_only_elapsed:.1f}s")
    log_message(f"Session rows processed: {session_rows_processed:,}")


    # =============================================================================
    # CELL 10: Main Processing Loop - ESMA-ONLY POOLS (with large pool handling)
    # =============================================================================
    log_message("="*70)
    log_message("STARTING ESMA-ONLY POOLS PROCESSING")
    log_message("="*70)

    # Scan output folders to find already-completed pools (file-based tracking)
    completed_pools = scan_completed_pools()
    completed_esma_only = completed_pools['esma_only']

    # Track total rows for this session
    session_rows_processed = 0

    # Get pools to process (skip already completed)
    esma_only_to_process = [p for p in esma_only_pools if p not in completed_esma_only]

    log_message(f"ESMA-only pools: {len(esma_only_pools)} total, {len(esma_only_to_process)} remaining")

    # Identify large pools
    large_pools = [p for p in esma_only_to_process if is_large_pool(p, 'esma')]
    normal_pools = [p for p in esma_only_to_process if not is_large_pool(p, 'esma')]
    log_message(f"  Normal pools: {len(normal_pools)}, Large pools (chunked): {len(large_pools)}")

    esma_only_start = time.time()

    # Process normal pools first
    for i, esma_pool_id in enumerate(normal_pools):
        pool_start = time.time()
        log_message(f"\n[{i+1}/{len(normal_pools)}] Processing ESMA-only pool: {esma_pool_id}")

        try:
            # Process pool
            result_df = process_esma_only_pool(esma_pool_id)

            # Save result
            rows_saved = save_pool_result(result_df, "esma_only", esma_pool_id)

            # Free memory
            del result_df
            gc.collect()

            # Track session progress (file-based tracking means completion is automatic)
            session_rows_processed += rows_saved

            pool_elapsed = time.time() - pool_start
            log_message(f"  Saved {rows_saved:,} rows in {pool_elapsed:.1f}s")

        except Exception as e:
            error_msg = f"ERROR processing {esma_pool_id}: {str(e)}"
            log_message(error_msg)
            gc.collect()

    # Process large pools with chunked mode
    for i, esma_pool_id in enumerate(large_pools):
        pool_start = time.time()
        pool_size_mb = get_pool_compressed_size(esma_pool_id, 'esma') / 1024 / 1024
        log_message(f"\n[LARGE {i+1}/{len(large_pools)}] Processing ESMA-only pool: {esma_pool_id} ({pool_size_mb:.0f} MB)")

        try:
            # Setup output path with ATOMIC write pattern
            subdir = os.path.join(OUTPUT_DIR, "esma_only")
            os.makedirs(subdir, exist_ok=True)
            safe_pool_id = esma_pool_id.replace('/', '_').replace('\\', '_')
            output_path = os.path.join(subdir, f"{safe_pool_id}.csv")
            temp_path = output_path + ".tmp"

            # Remove existing files if any (fresh start)
            if os.path.exists(output_path):
                os.remove(output_path)
            if os.path.exists(temp_path):
                os.remove(temp_path)

            # Process with chunked mode - writes to TEMP file
            rows_saved = process_esma_only_pool_chunked(esma_pool_id, temp_path)

            # ATOMIC: Rename temp to final ONLY after complete success
            os.replace(temp_path, output_path)

            # Track session progress (file-based tracking means completion is automatic)
            session_rows_processed += rows_saved

            pool_elapsed = time.time() - pool_start
            log_message(f"  Saved {rows_saved:,} rows in {pool_elapsed:.1f}s (chunked)")

        except Exception as e:
            error_msg = f"ERROR processing {esma_pool_id}: {str(e)}"
            log_message(error_msg)
            # Clean up temp file if it exists
            if os.path.exists(temp_path):
                os.remove(temp_path)
            gc.collect()

    esma_only_elapsed = time.time() - esma_only_start
    log_message(f"\nESMA-only pools complete: {esma_only_elapsed:.1f}s")
    log_message(f"Session rows processed: {session_rows_processed:,}")


    # =============================================================================
    # CELL 11: Final Summary
    # =============================================================================
    log_message("="*70)
    log_message("PROCESSING COMPLETE")
    log_message("="*70)

    # Scan output folders for final stats (file-based tracking)
    completed_pools = scan_completed_pools()

    log_message(f"\nCompleted pools:")
    log_message(f"  Matched: {len(completed_pools['matched'])}/{len(matched_pools)}")
    log_message(f"  ECB-only: {len(completed_pools['ecb_only'])}/{len(ecb_only_pools)}")
    log_message(f"  ESMA-only: {len(completed_pools['esma_only'])}/{len(esma_only_pools)}")

    # Check output size
    total_size = 0
    file_count = 0
    for subdir in ['matched', 'ecb_only', 'esma_only']:
        subdir_path = os.path.join(OUTPUT_DIR, subdir)
        if os.path.exists(subdir_path):
            for f in os.listdir(subdir_path):
                if f.endswith('.csv'):
                    total_size += os.path.getsize(os.path.join(subdir_path, f))
                    file_count += 1

    log_message(f"\nOutput files: {file_count}")
    log_message(f"Total output size: {total_size / (1024**3):.2f} GB")
    log_message(f"Output directory: {OUTPUT_DIR}")



if __name__ == '__main__':
    main()
