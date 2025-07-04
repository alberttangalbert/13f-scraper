"""
EDGAR Master Files Downloader

Downloads master files from SEC EDGAR containing lists of all filings
by SEC reporting institutions with details like CIK, name, form type, and address.

This module is responsible for the first step in the SEC EDGAR processing pipeline.
It downloads quarterly master index files from SEC EDGAR that contain metadata
for all filings submitted to the SEC during each quarter.

The master files are essential for:
- Identifying all 13F filings across all reporting institutions
- Obtaining filing metadata (CIK, company name, form type, reporting date, filename)
- Enabling systematic processing of SEC filings
- Providing the foundation for subsequent analysis steps

Each master file contains pipe-delimited data with filing information for
all SEC filings in a given quarter, making it possible to filter and process
specific filing types (like 13F-HR) in downstream steps.
"""

import logging
from datetime import datetime
from pathlib import Path

from app.utils.sec_edgar_download_utils import download_file, save_file, respect_rate_limit


def download_master_files(start_year=1993, output_dir="output/master_files"):
    """
    Download SEC EDGAR master files from start_year to current year/quarter.
    
    This function downloads quarterly master index files from SEC EDGAR that contain
    metadata for all filings submitted during each quarter. The master files are
    essential for identifying and processing specific filing types like 13F-HR.
    
    The download process includes:
    1. Determining the current year and quarter
    2. Iterating through all quarters from start_year to current
    3. Constructing proper SEC EDGAR URLs for each master file
    4. Downloading files with rate limiting to respect SEC servers
    5. Saving files with proper naming convention
    
    Args:
        start_year: Year to start downloading from (default: 1993, when 13F filings began)
        output_dir: Directory to save downloaded master files
        
    Returns:
        None: Function performs downloads but doesn't return a value
        
    Directory Structure Created:
        output_dir/
        ├── 1993QTR1master.txt
        ├── 1993QTR2master.txt
        ├── ...
        ├── 2023QTR4master.txt
        └── 2024QTR1master.txt
        
    File Format:
        Each master file contains pipe-delimited data with columns:
        cik|company_name|form|rdate|filename
        
    URL Pattern:
        https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx
        
    Example:
        >>> download_master_files(start_year=2020, output_dir="output/master_files")
        >>> # Downloads all master files from 2020 to current quarter
        
    Note:
        - Rate limiting is applied to respect SEC server constraints
        - Files are downloaded sequentially to avoid overwhelming the server
        - Existing files are overwritten if the function is run multiple times
        - The function stops at the current quarter to avoid downloading future files
    """
    logger = logging.getLogger(__name__)
    
    # Set up output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Calculate current year and quarter for download range
    # SEC quarters: Q1 (Jan-Mar), Q2 (Apr-Jun), Q3 (Jul-Sep), Q4 (Oct-Dec)
    end_year = datetime.now().year
    end_quarter = (datetime.now().month - 1) // 3 + 1
    
    logger.info(f"Downloading master files from {start_year} to {end_year}")
    
    # Initialize rate limiting tracker
    last_request_time = 0
    
    # Iterate through all years from start_year to current year
    for year in range(start_year, end_year + 1):
        # Iterate through all quarters (1-4) for each year
        for quarter in range(1, 5):
            # Stop if we've reached the current year and quarter
            # This prevents downloading future files that don't exist yet
            if year == end_year and quarter > end_quarter:
                break
                
            # Create quarter string for file naming and URL construction
            quarter_str = f"QTR{quarter}"
            filename = f"{year}{quarter_str}master.txt"
            file_path = output_path / filename
            
            # Construct the SEC EDGAR URL for this master file
            # SEC EDGAR organizes files by year and quarter
            url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/{quarter_str}/master.idx"
            
            try:
                logger.info(f"Downloading {year} {quarter_str}...")
                
                # Apply rate limiting to respect SEC server constraints
                # This prevents overwhelming the server with too many requests
                last_request_time = respect_rate_limit(last_request_time)
                
                # Download the master file using utility function
                # This handles proper headers, timeouts, and error checking
                response = download_file(url)
                if response is None:
                    logger.error(f"Failed to download {year} {quarter_str}")
                    continue  # Continue with next quarter even if one fails
                
                # Save the downloaded content to local file
                # This preserves the master file for subsequent processing
                if save_file(response.text, file_path):
                    logger.info(f"Successfully downloaded {filename}")
                else:
                    logger.error(f"Failed to save {filename}")
                    
            except Exception as e:
                logger.error(f"Failed to download {year} {quarter_str}: {e}")
                # Continue with next quarter even if one fails
                # This ensures the process doesn't stop due to individual file issues 