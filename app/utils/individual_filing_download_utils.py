"""
Filing Download Utilities

Utilities for downloading individual 13F filings from SEC EDGAR.
This module contains the core download functionality to avoid circular imports.
"""

import logging
import re
import time
from pathlib import Path
from typing import Dict, Any, Optional
from app.utils.sec_edgar_download_utils import (
    download_file, save_file, respect_rate_limit
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
                          output_dir: Path, last_request_time: float, cache_dir: Path = None) -> tuple[Optional[Dict[str, Any]], float]:
    """
    Download a single 13F filing from SEC EDGAR and save it locally.
    
    This function handles the complete process of downloading a filing:
    1. Constructs the proper SEC EDGAR URL
    2. Applies rate limiting to respect server constraints
    3. Downloads the filing content
    4. Extracts the 13F file number from the content
    5. Saves the filing to the appropriate directory
    6. Tracks failed downloads if cache_dir is provided
    
    Args:
        cik: Company Identifier Key (10-digit SEC identifier)
        accession_number: SEC filing accession number
        form_type: Type of SEC form (e.g., "13F-HR", "13F-HR/A")
        output_dir: Directory to save the downloaded filing
        last_request_time: Timestamp of the last request for rate limiting
        cache_dir: Directory for caching failed downloads (optional)
        
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
        ...     last_request_time=time.time(),
        ...     cache_dir=Path("local_cache")
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
        error_msg = f"Error downloading filing {accession_number} for CIK {cik}: {e}"
        logger.error(error_msg)
        return None, last_request_time 