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

from app.utils.download_utils import download_file, save_file, respect_rate_limit, verify_download_completion
from app.utils.filings_cache_utils import (
    load_download_cache, save_download_cache, get_filing_cache_key,
    load_progress_cache, update_cik_progress, get_resume_info,
    get_progress_summary, setup_file_logging, log_cik_start, log_cik_complete,
    log_progress_update, log_session_summary
)


def _save_raw_filing(output_dir: Path, cik: str, accession_number: str, 
                    content: str, form_13f_file_number: Optional[str] = None) -> str:
    """
    Save raw filing content to disk with proper organization.
    
    Creates a CIK-specific directory structure and saves the filing
    with a descriptive filename that includes the accession number
    and 13F file number (if available).
    
    Directory structure:
        output_dir/
        └── 0000001234/  # Padded CIK
            ├── 0000001234-05-000009_13F-0001234567.txt
            └── 0000001234-06-000010.txt
    
    Args:
        output_dir: Base directory for saving filings
        cik: Company Identifier Key
        accession_number: SEC filing accession number
        content: Raw filing content to save
        form_13f_file_number: 13F file number extracted from filing (optional)
        
    Returns:
        str: Path to the saved filing file
        
    Raises:
        Exception: If file saving fails
        
    Note:
        CIK is padded to 10 digits for consistent directory naming.
        If 13F file number is available, it's included in the filename.
    """
    # Create CIK-specific directory (padded to 10 digits)
    cik_dir = output_dir / str(cik).zfill(10)
    cik_dir.mkdir(parents=True, exist_ok=True)
    
    # Create filename with optional 13F file number
    if form_13f_file_number:
        filename = f"{accession_number}_{form_13f_file_number}.txt"
    else:
        filename = f"{accession_number}.txt"
    
    file_path = cik_dir / filename
    
    # Save the filing content
    if save_file(content, file_path):
        return str(file_path)
    else:
        raise Exception(f"Failed to save filing {filename}")


def download_single_filing(cik: str, accession_number: str, form_type: str, 
                          output_dir: Path, last_request_time: float) -> tuple[Optional[Dict[str, Any]], float]:
    """
    Download a single 13F filing from SEC EDGAR and save it locally.
    
    This function handles the complete process of downloading a filing:
    1. Constructs the proper SEC EDGAR URL
    2. Applies rate limiting to respect server constraints
    3. Downloads the filing content
    4. Extracts the 13F file number from the content
    5. Saves the filing to the appropriate directory
    
    Args:
        cik: Company Identifier Key (10-digit SEC identifier)
        accession_number: SEC filing accession number
        form_type: Type of SEC form (e.g., "13F-HR", "13F-HR/A")
        output_dir: Directory to save the downloaded filing
        last_request_time: Timestamp of the last request for rate limiting
        
    Returns:
        tuple: (filing_info_dict, new_request_time) where:
            - filing_info_dict: Dictionary with filing details if successful, None if failed
            - new_request_time: Updated timestamp for rate limiting
            
    Example:
        >>> result, new_time = download_single_filing(
        ...     cik="0000001234",
        ...     accession_number="0000001234-05-000009",
        ...     form_type="13F-HR",
        ...     output_dir=Path("output/raw_13f_filings"),
        ...     last_request_time=time.time()
        ... )
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Construct the SEC EDGAR URL
        # Remove hyphens from accession number for URL construction
        clean_accession = accession_number.replace('-', '')
        url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{clean_accession}/{accession_number}.txt"
        
        # Apply rate limiting to respect SEC server constraints
        new_request_time = respect_rate_limit(last_request_time)
        
        # Download the filing content
        response = download_file(url)
        
        if response is None:
            return None, new_request_time
        
        # Extract 13F file number from the filing content if it's a 13F filing
        form_13f_file_number = None
        if "13F" in form_type:
            # Look for form13FFileNumber tag in the XML content
            match = re.search(r"form13FFileNumber>([^<]+)</", response.text)
            if match:
                form_13f_file_number = match.group(1).strip()
            else:
                form_13f_file_number = "unknown_13F_file_number"
        
        # Save the filing to disk
        raw_path = _save_raw_filing(output_dir, cik, accession_number, response.text, form_13f_file_number)
        
        # Return filing information and updated request time
        return {
            'cik': cik,
            'accession_number': accession_number,
            'form_type': form_type,
            'raw_path': raw_path,
            'form_13f_file_number': form_13f_file_number,
            'content_length': len(response.text)
        }, new_request_time
        
    except Exception as e:
        logger.error(f"Error downloading filing {accession_number} for CIK {cik}: {e}")
        return None, last_request_time


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
            if cik in progress_data.get('completed_ciks', []):
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
                    last_request_time=last_request_time
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


 


 