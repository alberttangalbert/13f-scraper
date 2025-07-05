"""
Download Missing Utilities

Utilities for downloading missing ADSHs before starting the main extraction process.
Integrates logic from redownload_adsh.py to ensure all previous ADSHs have been downloaded.
"""

import logging
import time
from pathlib import Path
from typing import List, Dict
from app.utils.individual_filing_download_utils import download_single_filing
from app.utils.download_progress_cache_utils import save_download_cache
from app.utils.sec_edgar_download_utils import save_failed_downloads_json


def download_missing_adshs(adshs: List[Dict[str, str]], output_dir: str, cache_dir: Path) -> Dict[str, int]:
    """
    Download missing ADSHs using the existing download infrastructure.
    
    Args:
        adshs: List of ADSH dictionaries to download
        output_dir: Directory to save downloaded filings
        cache_dir: Directory containing the cache files
        
    Returns:
        Dictionary with download statistics
    """
    logger = logging.getLogger(__name__)
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Initialize counters
    total_adshs = len(adshs)
    successful_downloads = 0
    failed_downloads = 0
    last_request_time = 0
    newly_downloaded = set()
    failed_downloads_list = []
    
    if total_adshs == 0:
        logger.info("No missing ADSHs to download")
        return {
            'total_adshs': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'success_rate': 100.0
        }
    
    logger.info(f"Starting download of {total_adshs} missing ADSHs...")
    
    for i, adsh in enumerate(adshs, 1):
        try:
            # Log progress
            if i % 10 == 0 or i == total_adshs:
                logger.info(f"Progress: {i}/{total_adshs} ADSHs processed "
                           f"({successful_downloads} successful, {failed_downloads} failed)")
            
            # Download the filing
            result, last_request_time = download_single_filing(
                cik=adsh['cik'],
                accession_number=adsh['accession_number'],
                form_type=adsh['form_type'],
                output_dir=output_path,
                last_request_time=last_request_time,
                cache_dir=cache_dir
            )
            
            # Track results
            if result:
                successful_downloads += 1
                newly_downloaded.add(adsh['cache_key'])
                logger.debug(f"Successfully downloaded {adsh['accession_number']} for CIK {adsh['cik']}")
            else:
                failed_downloads += 1
                failed_downloads_list.append({
                    'cik': adsh['cik'],
                    'accession_number': adsh['accession_number'],
                    'form_type': adsh['form_type'],
                    'error_message': 'Download failed',
                    'timestamp': time.time()
                })
                logger.warning(f"Failed to download {adsh['accession_number']} for CIK {adsh['cik']}")
            
            # Save cache periodically
            if len(newly_downloaded) % 25 == 0 and newly_downloaded:
                save_download_cache(cache_dir, newly_downloaded, adsh['cik'], i)
                newly_downloaded.clear()
                
        except Exception as e:
            failed_downloads += 1
            failed_downloads_list.append({
                'cik': adsh['cik'],
                'accession_number': adsh['accession_number'],
                'form_type': adsh['form_type'],
                'error_message': f'Exception: {e}',
                'timestamp': time.time()
            })
            logger.error(f"Exception downloading {adsh['accession_number']} for CIK {adsh['cik']}: {e}")
    
    # Save final cache
    if newly_downloaded:
        save_download_cache(cache_dir, newly_downloaded, adsh['cik'], total_adshs)
    
    # Save failed downloads
    if failed_downloads_list:
        logger.info(f"Saving {len(failed_downloads_list)} failed downloads to JSON file")
        save_failed_downloads_json(failed_downloads_list, cache_dir)
    
    # Log final summary
    logger.info(f"Download completed: {total_adshs} total ADSHs, "
                f"{successful_downloads} successful, {failed_downloads} failed")
    
    return {
        'total_adshs': total_adshs,
        'successful_downloads': successful_downloads,
        'failed_downloads': failed_downloads,
        'success_rate': (successful_downloads / total_adshs * 100) if total_adshs > 0 else 0
    }


def ensure_all_adshs_downloaded(adsh_files_dir: str, output_dir: str, cache_dir: Path, 
                               force_check: bool = False) -> bool:
    """
    Ensure all ADSHs have been downloaded before starting main extraction.
    
    This function checks for missing ADSHs and downloads them if necessary.
    It integrates the logic from both test.py and redownload_adsh.py.
    
    Args:
        adsh_files_dir: Directory containing CIK-specific ADSH files
        output_dir: Directory to save downloaded filings
        cache_dir: Directory containing the cache files
        force_check: Whether to force a check even if cache suggests completion
        
    Returns:
        bool: True if all ADSHs are downloaded, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    logger.info("Ensuring all ADSHs are downloaded before starting main extraction...")
    
    # Import here to avoid circular imports
    from app.utils.download_status_analyzer_utils import check_download_progress
    
    # Check download progress
    missing_adshs, progress_summary = check_download_progress(adsh_files_dir, cache_dir)
    
    if not missing_adshs:
        logger.info("All ADSHs are already downloaded. Proceeding with main extraction.")
        return True
    
    # Log what needs to be downloaded
    logger.info(f"Found {len(missing_adshs)} missing ADSHs that need to be downloaded:")
    logger.info(f"  - Overall completion: {progress_summary['overall_completion_percentage']:.1f}%")
    logger.info(f"  - Partially downloaded CIKs: {progress_summary['partially_downloaded_ciks']}")
    logger.info(f"  - Not downloaded CIKs: {progress_summary['not_downloaded_ciks']}")
    
    # Confirm with user (optional - could be made configurable)
    print(f"\nFound {len(missing_adshs)} missing ADSHs that need to be downloaded.")
    print(f"Current completion: {progress_summary['overall_completion_percentage']:.1f}%")
    print(f"Output directory: {output_dir}")
    
    response = input("\nDownload missing ADSHs before proceeding? (Y/n): ").strip().lower()
    if response in ['n', 'no']:
        logger.warning("User chose not to download missing ADSHs. Proceeding with main extraction.")
        return True
    
    # Download missing ADSHs
    logger.info("Starting download of missing ADSHs...")
    start_time = time.time()
    
    stats = download_missing_adshs(missing_adshs, output_dir, cache_dir)
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Log download results
    logger.info("=" * 60)
    logger.info("MISSING ADSH DOWNLOAD SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total ADSHs processed: {stats['total_adshs']}")
    logger.info(f"Successful downloads: {stats['successful_downloads']}")
    logger.info(f"Failed downloads: {stats['failed_downloads']}")
    logger.info(f"Success rate: {stats['success_rate']:.1f}%")
    logger.info(f"Duration: {duration:.1f} seconds")
    
    if stats['failed_downloads'] > 0:
        logger.warning(f"{stats['failed_downloads']} ADSHs failed to download. Check the failed_downloads.json file.")
    
    # Check if we should proceed
    if stats['successful_downloads'] > 0:
        logger.info("Successfully downloaded missing ADSHs. Ready to proceed with main extraction.")
        return True
    else:
        logger.error("Failed to download any missing ADSHs. Cannot proceed with main extraction.")
        return False


def verify_download_completion_before_extraction(adsh_files_dir: str, cache_dir: Path) -> bool:
    """
    Verify that all ADSHs are downloaded before starting main extraction.
    
    This is a lightweight check that can be used to determine if the
    main extraction process should proceed or if missing ADSHs need
    to be downloaded first.
    
    Args:
        adsh_files_dir: Directory containing CIK-specific ADSH files
        cache_dir: Directory containing the cache files
        
    Returns:
        bool: True if all ADSHs are downloaded, False if any are missing
    """
    logger = logging.getLogger(__name__)
    
    # Import here to avoid circular imports
    from app.utils.download_status_analyzer_utils import check_download_progress
    
    # Quick check for missing ADSHs
    missing_adshs, progress_summary = check_download_progress(adsh_files_dir, cache_dir)
    
    if missing_adshs:
        logger.warning(f"Found {len(missing_adshs)} missing ADSHs. Consider running ensure_all_adshs_downloaded() first.")
        return False
    else:
        logger.info("All ADSHs are downloaded. Ready for main extraction.")
        return True 