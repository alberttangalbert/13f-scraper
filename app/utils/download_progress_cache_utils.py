"""
Filings Cache Utilities

Utilities for managing cache files related to 13F filings download progress.
Includes functions for tracking download progress, CIK completion status,
and resume functionality.
"""

import logging
import json
import time
from pathlib import Path
from typing import Dict, Any, Set, Tuple, List


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


def log_cik_start(file_logger: logging.Logger, cik: str, cik_index: int, total_ciks: int, 
                  total_adshs: int, start_time: float):
    """
    Log the start of processing a CIK.
    
    Args:
        file_logger: Logger for file output
        cik: CIK being processed
        cik_index: Current CIK index (1-based)
        total_ciks: Total number of CIKs to process
        total_adshs: Total ADSHs for this CIK
        start_time: Start time timestamp
    """
    file_logger.info(f"START_CIK: {cik} ({cik_index}/{total_ciks}) - {total_adshs} ADSHs to process")


def log_cik_complete(file_logger: logging.Logger, cik: str, cik_index: int, total_ciks: int,
                     completed_adshs: int, total_adshs: int, start_time: float, 
                     successful_downloads: int, failed_downloads: int, skipped_downloads: int):
    """
    Log the completion of processing a CIK.
    
    Args:
        file_logger: Logger for file output
        cik: CIK that was processed
        cik_index: Current CIK index (1-based)
        total_ciks: Total number of CIKs to process
        completed_adshs: Number of ADSHs completed for this CIK
        total_adshs: Total ADSHs for this CIK
        start_time: Start time timestamp
        successful_downloads: Number of successful downloads
        failed_downloads: Number of failed downloads
        skipped_downloads: Number of skipped downloads
    """
    end_time = time.time()
    duration = end_time - start_time
    duration_str = f"{duration:.2f}s"
    
    if duration > 60:
        minutes = int(duration // 60)
        seconds = duration % 60
        duration_str = f"{minutes}m {seconds:.1f}s"
    
    file_logger.info(
        f"COMPLETE_CIK: {cik} ({cik_index}/{total_ciks}) - "
        f"{completed_adshs}/{total_adshs} ADSHs completed in {duration_str} - "
        f"Success: {successful_downloads}, Failed: {failed_downloads}, Skipped: {skipped_downloads}"
    )


def log_progress_update(file_logger: logging.Logger, total_filings: int, successful_downloads: int,
                       failed_downloads: int, skipped_downloads: int, current_cik: str):
    """
    Log progress updates during processing.
    
    Args:
        file_logger: Logger for file output
        total_filings: Total filings processed so far
        successful_downloads: Number of successful downloads
        failed_downloads: Number of failed downloads
        skipped_downloads: Number of skipped downloads
        current_cik: Current CIK being processed
    """
    file_logger.info(
        f"PROGRESS: {total_filings} filings processed - "
        f"Success: {successful_downloads}, Failed: {failed_downloads}, Skipped: {skipped_downloads} "
        f"(Current CIK: {current_cik})"
    )


def log_session_summary(file_logger: logging.Logger, session_start_time: float, 
                       total_ciks_processed: int, total_filings_processed: int,
                       total_successful: int, total_failed: int, total_skipped: int):
    """
    Log a summary of the download session.
    
    Args:
        file_logger: Logger for file output
        session_start_time: Session start time timestamp
        total_ciks_processed: Total CIKs processed in this session
        total_filings_processed: Total filings processed in this session
        total_successful: Total successful downloads in this session
        total_failed: Total failed downloads in this session
        total_skipped: Total skipped downloads in this session
    """
    session_end_time = time.time()
    session_duration = session_end_time - session_start_time
    
    if session_duration > 3600:
        hours = int(session_duration // 3600)
        minutes = int((session_duration % 3600) // 60)
        duration_str = f"{hours}h {minutes}m"
    elif session_duration > 60:
        minutes = int(session_duration // 60)
        seconds = session_duration % 60
        duration_str = f"{minutes}m {seconds:.1f}s"
    else:
        duration_str = f"{session_duration:.2f}s"
    
    file_logger.info(
        f"SESSION_SUMMARY: Processed {total_ciks_processed} CIKs, {total_filings_processed} filings "
        f"in {duration_str} - Success: {total_successful}, Failed: {total_failed}, Skipped: {total_skipped}"
    )


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


def get_filing_cache_key(cik: str, accession_number: str) -> str:
    """
    Create a unique cache key for a filing.
    
    The cache key combines the CIK and accession number to create
    a unique identifier for each filing. This allows efficient
    lookup to determine if a filing has already been downloaded.
    
    Args:
        cik: Company Identifier Key (10-digit SEC identifier)
        accession_number: SEC filing accession number (e.g., "0000001234-05-000009")
        
    Returns:
        str: Unique cache key in format "cik_accession_number"
        
    Example:
        >>> key = get_filing_cache_key("0000001234", "0000001234-05-000009")
        >>> print(key)  # "0000001234_0000001234-05-000009"
    """
    return f"{cik}_{accession_number}"


def load_progress_cache(cache_dir: Path) -> Dict[str, Dict[str, Any]]:
    """
    Load the progress cache to track CIK completion status and ADSH progress.
    
    The progress cache tracks:
    - Which CIKs are completed
    - Current CIK being processed
    - ADSHs completed for current CIK
    - Total ADSHs for current CIK
    
    Args:
        cache_dir: Directory containing the cache files
        
    Returns:
        Dict: Progress cache with CIK status information
    """
    progress_file = cache_dir / "progress_cache.json"
    
    if progress_file.exists():
        try:
            with open(progress_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.getLogger(__name__).warning(f"Error loading progress cache: {e}")
    
    return {
        'cik_adsh_counts': {},  # Track ADSH count per completed CIK
        'current_cik': '',
        'current_cik_progress': {
            'completed_adshs': 0,
            'total_adshs': 0,
            'last_processed_index': 0
        },
        'total_completed_adshs': 0
    }


def save_progress_cache(cache_dir: Path, progress_data: Dict[str, Any]):
    """
    Save the progress cache with updated CIK and ADSH progress information.
    
    Args:
        cache_dir: Directory to save the progress file
        progress_data: Progress data to save
    """
    progress_file = cache_dir / "progress_cache.json"
    
    try:
        with open(progress_file, 'w') as f:
            json.dump(progress_data, f, indent=2)
    except Exception as e:
        logging.getLogger(__name__).error(f"Error saving progress cache: {e}")


def update_cik_progress(cache_dir: Path, cik: str, completed_adshs: int, 
                       total_adshs: int, last_processed_index: int, is_completed: bool = False):
    """
    Update the progress cache for a specific CIK.
    
    Args:
        cache_dir: Directory containing the cache files
        cik: CIK being processed
        completed_adshs: Number of ADSHs completed for this CIK
        total_adshs: Total number of ADSHs for this CIK
        last_processed_index: Last processed filing index
        is_completed: Whether this CIK is fully completed
    """
    progress_data = load_progress_cache(cache_dir)
    
    if is_completed:
        # Mark CIK as completed by storing its ADSH count
        progress_data['cik_adsh_counts'][cik] = total_adshs
        progress_data['current_cik'] = ''
        progress_data['current_cik_progress'] = {
            'completed_adshs': 0,
            'total_adshs': 0,
            'last_processed_index': 0
        }
    else:
        # Update current CIK progress
        progress_data['current_cik'] = cik
        progress_data['current_cik_progress'] = {
            'completed_adshs': completed_adshs,
            'total_adshs': total_adshs,
            'last_processed_index': last_processed_index
        }
    
    # Update total completed ADSHs - sum all completed CIK ADSHs plus current progress
    total_completed = sum(progress_data['cik_adsh_counts'].values())
    if not is_completed:
        total_completed += completed_adshs
    progress_data['total_completed_adshs'] = total_completed
    
    save_progress_cache(cache_dir, progress_data)


def get_resume_info(cache_dir: Path, cik_files: list) -> Tuple[str, int]:
    """
    Get resume information from progress cache.
    
    Args:
        cache_dir: Directory containing the cache files
        cik_files: List of CIK files to process
        
    Returns:
        tuple: (resume_cik, resume_index) where to resume processing
    """
    progress_data = load_progress_cache(cache_dir)
    current_cik = progress_data.get('current_cik', '')
    current_progress = progress_data.get('current_cik_progress', {})
    
    if current_cik:
        # Resume from current CIK
        resume_index = current_progress.get('last_processed_index', 0)
        return current_cik, resume_index
    
    # Find first uncompleted CIK
    completed_ciks = set(progress_data.get('cik_adsh_counts', {}).keys())
    cik_names = [cik_file.stem for cik_file in cik_files]
    
    for cik in cik_names:
        if cik not in completed_ciks:
            return cik, 0
    
    # All CIKs completed
    return '', 0


def get_progress_summary(cache_dir: Path, total_cik_files: int = None) -> Dict[str, Any]:
    """
    Get a summary of the current download progress.
    
    Args:
        cache_dir: Directory containing the cache files
        total_cik_files: Total number of CIK files to process (optional)
        
    Returns:
        Dict: Progress summary with completion statistics
    """
    progress_data = load_progress_cache(cache_dir)
    download_cache, _, _ = load_download_cache(cache_dir)
    
    completed_ciks = list(progress_data.get('cik_adsh_counts', {}).keys())
    current_cik = progress_data.get('current_cik', '')
    current_progress = progress_data.get('current_cik_progress', {})
    total_completed_adshs = progress_data.get('total_completed_adshs', 0)
    total_downloaded_filings = len(download_cache)
    
    summary = {
        'completed_ciks': len(completed_ciks),
        'current_cik': current_cik,
        'current_cik_progress': current_progress,
        'total_completed_adshs': total_completed_adshs,
        'total_downloaded_filings': total_downloaded_filings,
        'cik_completion_rate': 0.0
    }
    
    if total_cik_files:
        summary['cik_completion_rate'] = len(completed_ciks) / total_cik_files * 100
    
    return summary


def get_failed_downloads_summary(cache_dir: Path) -> Dict[str, Any]:
    """
    Get a summary of failed downloads from the JSON file.
    
    Args:
        cache_dir: Directory containing the cache files
        
    Returns:
        Dict: Summary of failed downloads with statistics
    """
    logger = logging.getLogger(__name__)
    
    failed_file = cache_dir / "failed_downloads.json"
    
    if not failed_file.exists():
        return {
            'total_failed': 0,
            'failed_by_cik': {},
            'failed_by_error_type': {},
            'recent_failures': []
        }
    
    try:
        with open(failed_file, 'r') as f:
            json_data = json.load(f)
            failed_downloads = json_data.get('failed_downloads', [])
        
        # Analyze failed downloads
        failed_by_cik = {}
        failed_by_error_type = {}
        
        for failure in failed_downloads:
            # Count by CIK
            cik = failure.get('cik', 'unknown')
            failed_by_cik[cik] = failed_by_cik.get(cik, 0) + 1
            
            # Count by error type (simplified)
            error_msg = failure.get('error_message', 'unknown')
            error_type = 'HTTP Error' if 'HTTP error' in error_msg else 'Network Error' if 'Error downloading' in error_msg else 'Other'
            failed_by_error_type[error_type] = failed_by_error_type.get(error_type, 0) + 1
        
        # Get recent failures (last 10)
        recent_failures = sorted(failed_downloads, key=lambda x: x.get('timestamp', 0), reverse=True)[:10]
        
        return {
            'total_failed': len(failed_downloads),
            'failed_by_cik': failed_by_cik,
            'failed_by_error_type': failed_by_error_type,
            'recent_failures': recent_failures
        }
        
    except Exception as e:
        logger.error(f"Error getting failed downloads summary: {e}")
        return {
            'total_failed': 0,
            'failed_by_cik': {},
            'failed_by_error_type': {},
            'recent_failures': []
        }


def clear_failed_downloads_cache(cache_dir: Path) -> bool:
    """
    Clear the failed downloads JSON file.
    
    Args:
        cache_dir: Directory containing the cache files
        
    Returns:
        bool: True if successfully cleared, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    try:
        failed_file = cache_dir / "failed_downloads.json"
        if failed_file.exists():
            failed_file.unlink()
            logger.info("Cleared failed downloads JSON file")
        return True
        
    except Exception as e:
        logger.error(f"Error clearing failed downloads JSON file: {e}")
        return False 