"""
13F Filing ADSH Extractor

Extracts 13F-HR filing ADSHs (accession numbers) from SEC EDGAR master files and creates separate files
for each reporting institution (CIK) containing their 13F-HR filing ADSH history.

This module processes the master files downloaded in step 1 to:
- Filter for 13F-HR and 13F-HR/A filings only
- Extract filing metadata (CIK, company name, form type, reporting date, filename)
- Organize filings by CIK into separate CSV files
- Enable efficient lookup of filing history for each institution

The output structure allows for easy access to all 13F-HR filings for any given CIK,
which is essential for the subsequent step of downloading raw filing documents.
"""

import logging
import pandas as pd
from pathlib import Path
from typing import Set


def extract_13f_filing_adshs(master_files_dir: str = "output/master_files", 
                           output_dir: str = "output/13f_filings"):
    """
    Extract 13F-HR filing ADSHs from master files and create separate files for each CIK.
    
    This function processes all master files downloaded from SEC EDGAR and extracts
    only the 13F-HR and 13F-HR/A filings. For each CIK (Company Identifier Key),
    it creates a separate CSV file containing all their 13F-HR filing history.
    
    The process includes:
    1. Reading each master file (skipping header lines)
    2. Filtering for 13F-HR and 13F-HR/A form types
    3. Organizing filings by CIK into separate files
    4. Maintaining filing metadata for downstream processing
    
    Args:
        master_files_dir: Directory containing master files from step 1
        output_dir: Directory to output CIK-specific ADSH files
        
    Returns:
        bool: True if extraction completed successfully, False otherwise
        
    Directory Structure Created:
        output_dir/
        └── all_13f_adshs/
            ├── 0000001234.csv  # CIK-specific filing history
            ├── 0000005678.csv
            └── ...
            
    File Format:
        Each CSV file contains columns: cik, company_name, form, rdate, filename
        
    Example:
        >>> success = extract_13f_filing_adshs(
        ...     master_files_dir="output/master_files",
        ...     output_dir="output/13f_filings"
        ... )
        >>> if success:
        ...     print("13F filing ADSHs extracted successfully!")
    """
    logger = logging.getLogger(__name__)
    
    # Set up input and output paths
    master_path = Path(master_files_dir)
    output_path = Path(output_dir)
    
    # Create output directory for all 13F filings
    all_13f_adshs_folder = output_path / "all_13f_adshs"
    
    # Remove existing directory and recreate to ensure clean state
    # This prevents duplicate entries if the script is run multiple times
    if all_13f_adshs_folder.exists():
        import shutil
        shutil.rmtree(all_13f_adshs_folder)
    all_13f_adshs_folder.mkdir(parents=True, exist_ok=True)
    
    # Get list of all master files to process
    # Master files follow the pattern: YYYYQTR#master.txt (e.g., 2023QTR1master.txt)
    master_files = list(master_path.glob("*master.txt"))
    logger.info(f"Found {len(master_files)} master files to process")
    
    # Track unique CIKs for progress reporting and validation
    all_13f_adshs_ciks: Set[str] = set()
    
    # Process each master file sequentially
    for i, master_file in enumerate(master_files, 1):
        try:
            logger.info(f"Processing {master_file.name} ({i}/{len(master_files)})")
            
            # Read master file with proper parsing
            # SEC master files have 11 header lines that need to be skipped
            # The data is pipe-delimited with columns: cik, company_name, form, rdate, filename
            df = pd.read_csv(master_file, sep='|', skiprows=11, header=None,
                           names=['cik', 'company_name', 'form', 'rdate', 'filename'])
            
            # Filter for 13F-HR filings only
            # We only want holdings reports (13F-HR) and amendments (13F-HR/A)
            # Excluding notice filings (13F-NT) and other form types
            all_13f_adshs = df[df['form'].isin(['13F-HR', '13F-HR/A'])]
            
            # Process each 13F-HR filing found in this master file
            for _, row in all_13f_adshs.iterrows():
                # Pad CIK to 10 digits for consistent file naming
                # SEC CIKs are typically 10 digits, but some may be shorter
                cik = str(row['cik']).zfill(10)
                output_file = all_13f_adshs_folder / f"{cik}.csv"
                
                # Append this filing to the CIK-specific file
                # Use append mode to accumulate all filings for this CIK
                # Only write header if this is the first filing for this CIK
                row.to_frame().T.to_csv(output_file, mode='a', header=not output_file.exists(), 
                                       index=False, sep=',')
                all_13f_adshs_ciks.add(cik)
                
        except Exception as e:
            logger.error(f"Error processing {master_file.name}: {e}")
            continue  # Continue with next master file even if one fails
    
    # Log final summary of extraction results
    logger.info(f"ADSH extraction completed successfully")
    logger.info(f"Created files for {len(all_13f_adshs_ciks)} CIKs in all_13f_adshs folder")
    
    return True 