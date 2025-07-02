"""
SEC EDGAR Master Files Downloader - Main Entry Point

Downloads master files from SEC EDGAR containing lists of all filings
by SEC reporting institutions with details like CIK, name, form type, and address.
"""

import logging
import sys
from pathlib import Path

from app.core.retrieve_master_files import download_master_files
from app.core.extract_filing_adshs import extract_13f_filing_adshs
from app.core.extract_raw_13f_filings import download_raw_13f_filings


def step1_download_master_files():
    """Step 1: Download SEC EDGAR master files."""
    logger = logging.getLogger(__name__)
    output_dir = Path("output/master_files")
    
    # Check if step 1 output already exists
    if output_dir.exists() and any(output_dir.glob("*master.txt")):
        logger.info("Step 1: Master files already exist, skipping download")
        return True
    
    logger.info("Step 1: Starting SEC EDGAR master files download")
    
    try:
        download_master_files(start_year=1993, output_dir="output/master_files")
        logger.info("Step 1: Download process completed successfully")
        return True
    except Exception as e:
        logger.error(f"Step 1: Download process failed: {e}")
        return False


def step2_extract_13f_filing_adshs():
    """Step 2: Extract 13F filing ADSHs from master files."""
    logger = logging.getLogger(__name__)
    output_dir = Path("output/13f_filings")
    
    # Check if step 2 output already exists
    if output_dir.exists() and (output_dir / "all_13f_adshs").exists():
        logger.info("Step 2: 13F filing ADSH files already exist, skipping extraction")
        return True
    
    logger.info("Step 2: Starting 13F filing ADSH extraction")
    
    try:
        extract_13f_filing_adshs()
        logger.info("Step 2: 13F filing ADSH extraction completed successfully")
        return True
    except Exception as e:
        logger.error(f"Step 2: 13F filing ADSH extraction failed: {e}")
        return False


def step3_download_raw_13f_filings():
    """Step 3: Download raw 13F filings using ADSHs from step 2."""
    logger = logging.getLogger(__name__)
    output_dir = Path("output/raw_13f_filings")
    
    # Check if step 3 output already exists
    if output_dir.exists() and any(output_dir.iterdir()):
        logger.info("Step 3: Raw 13F filings directory exists, checking progress...")
    else:
        logger.info("Step 3: Starting raw 13F filings download")
    
    try:
        download_raw_13f_filings()
        logger.info("Step 3: Raw 13F filings download completed successfully")
        return True
    except Exception as e:
        logger.error(f"Step 3: Raw 13F filings download failed: {e}")
        return False


def main():
    """Execute the SEC EDGAR processing pipeline."""
    # Setup basic logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Starting SEC EDGAR processing pipeline")
    
    # Execute each step
    steps = [
        step1_download_master_files,
        step2_extract_13f_filing_adshs,
        step3_download_raw_13f_filings
    ]
    
    for i, step in enumerate(steps, 1):
        logger.info(f"Executing step {i}...")
        if not step():
            logger.error(f"Step {i} failed, stopping pipeline")
            sys.exit(1)
        logger.info(f"Step {i} completed successfully")
    
    logger.info("All steps completed successfully!")


if __name__ == "__main__":
    main()
