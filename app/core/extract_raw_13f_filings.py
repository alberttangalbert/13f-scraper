"""
Raw 13F Filings Downloader

Downloads raw 13F filing documents from SEC EDGAR using the ADSHs
extracted from master files in step 2. Includes a local cache system
to track downloaded filings and allow resuming from interruptions.

This module handles:
- Downloading individual 13F filing documents from SEC EDGAR
- Extracting 13F file numbers from filing content
- Organizing downloads by CIK (Company Identifier Key)
- Caching download progress to enable resume functionality
- Rate limiting to respect SEC server constraints
"""

import logging
import re
import time
from pathlib import Path
from typing import Dict, Any, Optional, Set
import pandas as pd

from app.utils.sec_edgar_download_utils import (
    download_file, save_file, respect_rate_limit, verify_download_completion,
    save_failed_downloads_json
)
from app.utils.download_progress_cache_utils import (
    load_download_cache, save_download_cache, get_filing_cache_key,
    load_progress_cache, update_cik_progress, get_resume_info,
    get_progress_summary, setup_file_logging, log_cik_start, log_cik_complete,
    log_progress_update, log_session_summary, get_failed_downloads_summary
)
from app.utils.missing_adsh_downloader_utils import ensure_all_adshs_downloaded
from app.utils.individual_filing_download_utils import download_single_filing





def download_raw_13f_filings(adsh_files_dir: str = "output/13f_filings/all_13f_adshs",
                            output_dir: str = "output/raw_13f_filings"):
    """
    Download raw 13F filings using the ADSHs extracted in step 2.
    
    This is the main function that orchestrates the download of all 13F filings.
    It processes each CIK file created in step 2, downloads the corresponding
    filings, and maintains a cache to enable resume functionality.
    
    The function includes:
    - Local cache management for tracking downloaded filings
    - Progress tracking and logging
    - Error handling and recovery
    - Rate limiting to respect SEC server constraints
    - Resume capability if the script is interrupted
    - Automatic checking and downloading of missing ADSHs before main extraction
    
    Args:
        adsh_files_dir: Directory containing CIK-specific ADSH files from step 2
        output_dir: Directory to save downloaded raw filings
        
    Returns:
        bool: True if the download process completed successfully, False otherwise
        
    Directory Structure Created:
        output_dir/
        ├── 0000001234/  # CIK directories
        │   ├── 0000001234-05-000009_13F-0001234567.txt
        │   └── 0000001234-06-000010.txt
        └── ...
        
        local_cache/
        └── download_cache.json  # Cache file for tracking downloads
        
    Example:
        >>> success = download_raw_13f_filings(
        ...     adsh_files_dir="output/13f_filings/all_13f_adshs",
        ...     output_dir="output/raw_13f_filings"
        ... )
        >>> if success:
        ...     print("All 13F filings downloaded successfully!")
    """
    logger = logging.getLogger(__name__)
    
    # Set up input and output directories
    adsh_path = Path(adsh_files_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Create local cache directory for tracking downloads
    # Use the repository root (two levels up from app/core/)
    cache_dir = Path(__file__).parent.parent.parent / "local_cache"
    cache_dir.mkdir(exist_ok=True)
    
    # Set up file logging
    file_logger = setup_file_logging(cache_dir)
    
    # Validate that ADSH files directory exists
    if not adsh_path.exists():
        logger.error(f"ADSH files directory not found: {adsh_path}")
        return False
    
    # Ensure all ADSHs are downloaded before starting main extraction
    logger.info("Checking for missing ADSHs before starting main extraction...")
    if not ensure_all_adshs_downloaded(adsh_files_dir, output_dir, cache_dir):
        logger.error("Failed to ensure all ADSHs are downloaded. Cannot proceed with main extraction.")
        return False
    
    # Verify if download is already complete
    is_complete, total_adshs, downloaded_count = verify_download_completion(adsh_files_dir, cache_dir)
    
    if is_complete:
        logger.info(f"Download already complete: {downloaded_count}/{total_adshs} ADSHs downloaded")
        return True
    
    # Get all CIK files to process
    cik_files = list(adsh_path.glob("*.csv"))
    logger.info(f"Found {len(cik_files)} CIK files to process")
    
    # Get progress summary and resume information
    progress_summary = get_progress_summary(cache_dir, len(cik_files))
    resume_cik, resume_index = get_resume_info(cache_dir, cik_files)
    
    logger.info(f"Progress: {progress_summary['completed_ciks']}/{len(cik_files)} CIKs completed "
                f"({progress_summary['cik_completion_rate']:.1f}%), "
                f"{progress_summary['total_downloaded_filings']} filings downloaded")
    
    if resume_cik:
        logger.info(f"Resuming from CIK {resume_cik} at index {resume_index}")
    else:
        logger.info("Starting fresh download process")
    
    # Initialize counters for progress tracking
    session_start_time = time.time()
    total_filings = 0
    successful_downloads = 0
    failed_downloads = 0
    skipped_downloads = 0
    last_request_time = 0
    newly_downloaded = set()
    ciks_processed_this_session = 0
    
    # Initialize failed downloads tracking
    failed_downloads_list = []
    
    # Process each CIK file with detailed progress tracking
    for i, cik_file in enumerate(cik_files, 1):
        try:
            # Extract CIK from filename
            cik = cik_file.stem
            cik_start_time = time.time()
            
            # Read the ADSH data for this CIK
            df = pd.read_csv(cik_file)
            total_adshs_for_cik = len(df)
            
            # Log CIK start
            log_cik_start(file_logger, cik, i, len(cik_files), total_adshs_for_cik, cik_start_time)
            logger.info(f"Processing CIK {cik} ({i}/{len(cik_files)})")
            
            # Check if we need to resume from this CIK
            resume_from_index = 0
            if cik == resume_cik:
                resume_from_index = resume_index
                logger.info(f"Resuming CIK {cik} from filing index {resume_from_index}")
            
            # Skip completed CIKs
            progress_data = load_progress_cache(cache_dir)
            if cik in progress_data.get('cik_adsh_counts', {}):
                logger.info(f"Skipping completed CIK {cik}")
                continue
            
            # Initialize CIK progress tracking
            completed_adshs_for_cik = 0
            cik_successful_downloads = 0
            cik_failed_downloads = 0
            cik_skipped_downloads = 0
            
            # Process each filing for this CIK
            for filing_index, (_, row) in enumerate(df.iterrows(), 1):
                # Skip filings before the resume point
                if filing_index <= resume_from_index:
                    continue
                
                total_filings += 1
                
                # Check if this filing has already been downloaded (cache lookup)
                cache_key = get_filing_cache_key(row['cik'], row['filename'].split('/')[-1].replace('.txt', ''))
                downloaded_cache, _, _ = load_download_cache(cache_dir)
                if cache_key in downloaded_cache:
                    skipped_downloads += 1
                    cik_skipped_downloads += 1
                    completed_adshs_for_cik += 1
                    continue  # Skip already downloaded filings
                
                # Download the filing
                result, last_request_time = download_single_filing(
                    cik=row['cik'],
                    accession_number=row['filename'].split('/')[-1].replace('.txt', ''),
                    form_type=row['form'],
                    output_dir=output_path,
                    last_request_time=last_request_time,
                    cache_dir=cache_dir
                )
                
                # Track download results
                if result:
                    successful_downloads += 1
                    cik_successful_downloads += 1
                    completed_adshs_for_cik += 1
                    newly_downloaded.add(cache_key)  # Add to cache
                else:
                    failed_downloads += 1
                    cik_failed_downloads += 1
                    
                    # Add to failed downloads list for CSV/JSON export
                    failed_downloads_list.append({
                        'cik': row['cik'],
                        'accession_number': row['filename'].split('/')[-1].replace('.txt', ''),
                        'form_type': row['form'],
                        'error_message': 'Download failed',
                        'timestamp': time.time()
                    })
                
                # Update progress cache after each ADSH
                update_cik_progress(
                    cache_dir, cik, completed_adshs_for_cik, 
                    total_adshs_for_cik, filing_index, 
                    is_completed=(completed_adshs_for_cik >= total_adshs_for_cik)
                )
                
                # Save download cache periodically to prevent data loss (every 50 downloads)
                if len(newly_downloaded) % 50 == 0 and newly_downloaded:
                    save_download_cache(cache_dir, newly_downloaded, cik, filing_index)
                    newly_downloaded.clear()
                
                # Log progress every 100 filings for monitoring
                if total_filings % 100 == 0:
                    log_progress_update(file_logger, total_filings, successful_downloads, 
                                      failed_downloads, skipped_downloads, cik)
                    logger.info(f"Progress: {total_filings} filings processed, "
                               f"{successful_downloads} successful, {failed_downloads} failed, {skipped_downloads} skipped")
            
            # Mark CIK as completed if all ADSHs processed
            if completed_adshs_for_cik >= total_adshs_for_cik:
                update_cik_progress(cache_dir, cik, completed_adshs_for_cik, 
                                   total_adshs_for_cik, total_adshs_for_cik, is_completed=True)
                
                # Log CIK completion with timing
                log_cik_complete(file_logger, cik, i, len(cik_files), completed_adshs_for_cik,
                               total_adshs_for_cik, cik_start_time, cik_successful_downloads,
                               cik_failed_downloads, cik_skipped_downloads)
                logger.info(f"Completed CIK {cik}: {completed_adshs_for_cik}/{total_adshs_for_cik} ADSHs")
                ciks_processed_this_session += 1
            
        except Exception as e:
            logger.error(f"Error processing CIK file {cik_file}: {e}")
            continue  # Continue with next CIK file even if one fails
    
    # Save final cache with any remaining downloads
    if newly_downloaded:
        save_download_cache(cache_dir, newly_downloaded, cik, total_filings)
    
    # Save failed downloads to JSON file
    if failed_downloads_list:
        logger.info(f"Saving {len(failed_downloads_list)} failed downloads to JSON file")
        save_failed_downloads_json(failed_downloads_list, cache_dir)
    else:
        logger.info("No failed downloads to save")
    
    # Get failed downloads summary
    failed_summary = get_failed_downloads_summary(cache_dir)
    if failed_summary['total_failed'] > 0:
        logger.info(f"Failed downloads summary: {failed_summary['total_failed']} total failures")
        logger.info(f"Failed by error type: {failed_summary['failed_by_error_type']}")
    
    # Get final progress summary
    final_summary = get_progress_summary(cache_dir, len(cik_files))
    
    # Log session summary to file
    log_session_summary(file_logger, session_start_time, ciks_processed_this_session,
                       total_filings, successful_downloads, failed_downloads, skipped_downloads)
    
    # Log final summary
    logger.info(f"Download completed: {total_filings} total filings, "
                f"{successful_downloads} successful, {failed_downloads} failed, {skipped_downloads} skipped")
    logger.info(f"Final progress: {final_summary['completed_ciks']}/{len(cik_files)} CIKs completed "
                f"({final_summary['cik_completion_rate']:.1f}%), "
                f"{final_summary['total_downloaded_filings']} total filings downloaded")
    
    return True 


 


 