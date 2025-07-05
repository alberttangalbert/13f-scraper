"""
Filings Cache Utilities

Utilities for managing cache files related to 13F filings download progress.
Includes functions for tracking download progress, CIK completion status,
and resume functionality.
"""

import logging
import json
from pathlib import Path
from typing import Set, Tuple


def setup_file_logging(cache_dir: Path) -> logging.Logger:
    """
    Set up file logging to track download progress in the local_cache directory.
    
    Args:
        cache_dir: Directory to store log files
        
    Returns:
        Logger: Configured logger for file logging
    """
    # Create a logger for file logging
    file_logger = logging.getLogger('filings_download')
    file_logger.setLevel(logging.INFO)
    
    # Prevent duplicate handlers
    if not file_logger.handlers:
        # Create file handler
        log_file = cache_dir / "download_progress.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        file_logger.addHandler(file_handler)
    
    return file_logger


def load_download_cache(cache_dir: Path) -> Tuple[Set[str], str, int]:
    """
    Load the download cache to see which filings have already been downloaded.
    
    The cache is stored as a JSON file containing a list of filing identifiers
    that have been successfully downloaded, the last CIK being processed,
    and the last filing index within that CIK.
    
    Args:
        cache_dir: Directory containing the cache file
        
    Returns:
        tuple: (downloaded_filings, last_cik, last_filing_index) where:
            - downloaded_filings: Set of filing cache keys that have been downloaded
            - last_cik: Last CIK being processed when interrupted
            - last_filing_index: Index of last filing processed within that CIK
        
    Example:
        >>> cache, last_cik, last_index = load_download_cache(Path("local_cache"))
        >>> print(f"Found {len(cache)} previously downloaded filings")
        >>> print(f"Last CIK: {last_cik}, Last index: {last_index}")
    """
    cache_file = cache_dir / "download_cache.json"
    
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
                downloaded_filings = set(cache_data.get('downloaded_filings', []))
                last_cik = cache_data.get('last_cik', '')
                last_filing_index = cache_data.get('last_filing_index', 0)
                return downloaded_filings, last_cik, last_filing_index
        except Exception as e:
            logging.getLogger(__name__).warning(f"Error loading cache: {e}")
    
    return set(), '', 0


def save_download_cache(cache_dir: Path, downloaded_filings: Set[str], 
                       current_cik: str = '', current_filing_index: int = 0):
    """
    Save the download cache with newly downloaded filings and progress tracking.
    
    This function merges newly downloaded filings with the existing cache
    to maintain a complete record of all downloads. It also tracks the current
    CIK and filing index being processed for resume functionality.
    
    Args:
        cache_dir: Directory to save the cache file
        downloaded_filings: Set of filing cache keys that were just downloaded
        current_cik: Current CIK being processed
        current_filing_index: Current filing index within the CIK
        
    Note:
        The cache file is saved as JSON with the structure:
        {
            "downloaded_filings": ["cik_accession_number", ...],
            "total_count": 1500,
            "last_cik": "0000001234",
            "last_filing_index": 45
        }
    """
    cache_file = cache_dir / "download_cache.json"
    
    try:
        # Load existing cache and merge with new downloads
        existing_cache, _, _ = load_download_cache(cache_dir)
        all_downloaded = existing_cache.union(downloaded_filings)
        
        # Create cache data structure with progress tracking
        cache_data = {
            'downloaded_filings': list(all_downloaded),
            'total_count': len(all_downloaded),
            'last_cik': current_cik,
            'last_filing_index': current_filing_index
        }
        
        # Save to JSON file
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
            
    except Exception as e:
        logging.getLogger(__name__).error(f"Error saving cache: {e}")