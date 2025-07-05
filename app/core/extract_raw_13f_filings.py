"""
Raw 13F Filings Downloader

Downloads raw 13F filing documents from SEC EDGAR using the ADSHs
extracted from master files in step 2. Includes automatic detection
and downloading of missing ADSHs before proceeding.

This module handles:
- Automatic detection of missing ADSHs across all CIKs
- Downloading missing 13F filing documents from SEC EDGAR
- Caching download progress to enable resume functionality
- Rate limiting to respect SEC server constraints
"""

import logging
from pathlib import Path

from app.utils.sec_edgar_download_utils import verify_download_completion
from app.utils.download_progress_cache_utils import setup_file_logging
from app.utils.missing_adsh_downloader_utils import ensure_all_adshs_downloaded


def download_raw_13f_filings(adsh_files_dir: str = "output/13f_filings/all_13f_adshs",
                            output_dir: str = "output/raw_13f_filings"):
    """
    Download raw 13F filings using the ADSHs extracted in step 2.
    
    This is the main function that orchestrates the download of all 13F filings.
    It automatically detects and downloads any missing ADSHs before proceeding.
    
    The function includes:
    - Automatic detection of missing ADSHs across all CIKs
    - Downloading missing 13F filing documents from SEC EDGAR
    - Caching download progress to enable resume functionality
    - Rate limiting to respect SEC server constraints
    
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
    
    # All ADSHs have been downloaded by ensure_all_adshs_downloaded()
    # No need for additional CIK-by-CIK processing
    logger.info("All ADSHs have been successfully downloaded. Extraction process complete.")
    return True 


 


 