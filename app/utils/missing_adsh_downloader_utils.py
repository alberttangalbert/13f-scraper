"""
Download Missing Utilities

Utilities for downloading missing ADSHs before starting the main extraction process.
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

    total_adshs = len(adshs)
    successful_downloads = 0
    failed_downloads = 0
    last_request_time = 0
    newly_downloaded = set()
    failed_downloads_list = []
    
    if total_adshs == 0:
        logger.info("No missing ADSHs to download")
        return {'total_adshs': 0, 'successful_downloads': 0, 'failed_downloads': 0}
    
    logger.info(f"Downloading {total_adshs} missing ADSHs...")
    
    for i, adsh in enumerate(adshs, 1):
        try:
            # Log progress every 10 downloads
            if i % 10 == 0 or i == total_adshs:
                logger.info(f"Progress: {i}/{total_adshs}")
            
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
            else:
                failed_downloads += 1
                failed_downloads_list.append({
                    'cik': adsh['cik'],
                    'accession_number': adsh['accession_number'],
                    'form_type': adsh['form_type'],
                    'error_message': 'Download failed',
                    'timestamp': time.time()
                })
            
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
        save_failed_downloads_json(failed_downloads_list, cache_dir)
    
    logger.info(f"Download: {successful_downloads} successful, {failed_downloads} failed")
    
    return {
        'total_adshs': total_adshs,
        'successful_downloads': successful_downloads,
        'failed_downloads': failed_downloads
    }


def ensure_all_adshs_downloaded(adsh_files_dir: str, output_dir: str, cache_dir: Path) -> bool:
    """
    Ensure all ADSHs have been downloaded before starting main extraction.
    
    Args:
        adsh_files_dir: Directory containing CIK-specific ADSH files
        output_dir: Directory to save downloaded filings
        cache_dir: Directory containing the cache files
        
    Returns:
        bool: True if ready to proceed with main extraction
    """
    logger = logging.getLogger(__name__)
    
    # Import here to avoid circular imports
    from app.utils.download_status_analyzer_utils import check_download_progress
    
    # Check download progress
    missing_adshs, progress_summary = check_download_progress(adsh_files_dir, cache_dir)
    
    if not missing_adshs:
        logger.info("All ADSHs are already downloaded. Proceeding with main extraction.")
        return True
    
    # Log what needs to be downloaded
    logger.info(f"Found {len(missing_adshs)} missing ADSHs ({progress_summary['overall_completion_percentage']:.1f}% complete)")
    
    # Download missing ADSHs
    stats = download_missing_adshs(missing_adshs, output_dir, cache_dir)
    
    # Log results
    if stats['failed_downloads'] > 0:
        logger.warning(f"{stats['failed_downloads']} ADSHs failed (see failed_downloads.json)")
    return True 