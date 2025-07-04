"""
Download utilities for SEC EDGAR data processing.

Common functionality for downloading files from SEC EDGAR with proper
rate limiting, headers, and error handling.
"""

import logging
import time
import requests
import json
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, Set, Tuple, List

from app.config import SEC_HEADERS, DOWNLOAD_TIMEOUT, REQUEST_DELAY


def respect_rate_limit(last_request_time: float) -> float:
    """
    Ensure we don't exceed rate limits between requests.
    
    Args:
        last_request_time: Timestamp of the last request
        
    Returns:
        float: Current timestamp after respecting rate limit
    """
    current_time = time.time()
    time_since_last = current_time - last_request_time
    if time_since_last < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - time_since_last)
    return time.time()


def download_file(url: str, timeout: Optional[int] = None) -> Optional[requests.Response]:
    """
    Download a file from a URL with proper headers and error handling.
    
    Args:
        url: URL to download from
        timeout: Request timeout in seconds (uses config default if None)
        
    Returns:
        Optional[requests.Response]: Response object if successful, None if failed
    """
    logger = logging.getLogger(__name__)
    
    try:
        response = requests.get(
            url, 
            headers=SEC_HEADERS, 
            timeout=timeout or DOWNLOAD_TIMEOUT
        )
        
        if response.status_code != 200:
            logger.error(f"HTTP error {response.status_code} for {url}")
            return None
            
        return response
        
    except Exception as e:
        logger.error(f"Error downloading from {url}: {e}")
        return None


def save_file(content: str, file_path: Path, encoding: str = 'utf-8') -> bool:
    """
    Save content to a file with error handling.
    
    Args:
        content: Content to save
        file_path: Path where to save the file
        encoding: File encoding
        
    Returns:
        bool: True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Ensure parent directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'w', encoding=encoding) as f:
            f.write(content)
            
        return True
        
    except Exception as e:
        logger.error(f"Error saving file {file_path}: {e}")
        return False


def create_cik_directory(base_dir: Path, cik: str) -> Path:
    """
    Create a CIK-specific directory.
    
    Args:
        base_dir: Base directory
        cik: CIK number
        
    Returns:
        Path: Path to the created CIK directory
    """
    cik_dir = base_dir / str(cik).zfill(10)
    cik_dir.mkdir(parents=True, exist_ok=True)
    return cik_dir


def save_failed_downloads_json(failed_downloads: List[Dict[str, Any]], cache_dir: Path) -> bool:
    """
    Save detailed failed downloads information to a JSON file.
    
    Args:
        failed_downloads: List of dictionaries containing failed download information
        cache_dir: Directory to save the failed downloads JSON
        
    Returns:
        bool: True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    try:
        if not failed_downloads:
            logger.info("No failed downloads to save")
            return True
        
        # Create detailed JSON structure
        failed_data = {
            'summary': {
                'total_failed': len(failed_downloads),
                'timestamp': time.time(),
                'date': time.strftime('%Y-%m-%d %H:%M:%S')
            },
            'failed_downloads': failed_downloads
        }
        
        # Save to JSON
        failed_json_path = cache_dir / "failed_downloads.json"
        with open(failed_json_path, 'w') as f:
            json.dump(failed_data, f, indent=2)
        
        logger.info(f"Saved detailed failed downloads to {failed_json_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving failed downloads JSON: {e}")
        return False


def verify_download_completion(adsh_files_dir: str, cache_dir: Path) -> Tuple[bool, int, int]:
    """
    Verify if all ADSHs from the all_13f_adshs files have been downloaded.
    
    This function compares the total number of ADSHs in all CIK files against
    the number of downloaded filings in the cache to determine if the download
    process is complete.
    
    Args:
        adsh_files_dir: Directory containing CIK-specific ADSH files
        cache_dir: Directory containing the download cache
        
    Returns:
        tuple: (is_complete, total_adshs, downloaded_count) where:
            - is_complete: True if all ADSHs have been downloaded
            - total_adshs: Total number of ADSHs that should be downloaded
            - downloaded_count: Number of ADSHs actually downloaded
    """
    logger = logging.getLogger(__name__)
    
    # Load cache to get downloaded count
    cache_file = cache_dir / "download_cache.json"
    downloaded_count = 0
    
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
                downloaded_count = cache_data.get('total_count', 0)
        except Exception as e:
            logger.warning(f"Error reading cache file: {e}")
    
    # Count total ADSHs from all CIK files
    adsh_path = Path(adsh_files_dir)
    if not adsh_path.exists():
        logger.error(f"ADSH files directory not found: {adsh_path}")
        return False, 0, downloaded_count
    
    total_adshs = 0
    cik_files = list(adsh_path.glob("*.csv"))
    
    for cik_file in cik_files:
        try:
            df = pd.read_csv(cik_file)
            total_adshs += len(df)
        except Exception as e:
            logger.error(f"Error reading CIK file {cik_file}: {e}")
            continue
    
    # Determine if download is complete
    is_complete = downloaded_count >= total_adshs
    
    logger.info(f"Download verification: {downloaded_count}/{total_adshs} ADSHs downloaded")
    
    return is_complete, total_adshs, downloaded_count


 