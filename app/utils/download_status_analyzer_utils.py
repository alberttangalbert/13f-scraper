"""
Load Progress Utilities

Utilities for checking download progress and identifying missing ADSHs
before starting the main extraction process. Integrates logic from test.py
to ensure all previous ADSHs have been downloaded.
"""

import logging
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Set, Tuple
from app.utils.download_progress_cache_utils import load_progress_cache, load_download_cache


def get_actual_adsh_data(adsh_files_dir: str) -> Dict[str, List[Dict[str, str]]]:
    """
    Get actual ADSH data from all CIK files.
    
    Args:
        adsh_files_dir: Directory containing CIK-specific ADSH files
        
    Returns:
        Dict mapping CIK to list of ADSH records
    """
    logger = logging.getLogger(__name__)
    actual_data = {}
    
    adsh_path = Path(adsh_files_dir)
    if not adsh_path.exists():
        logger.error(f"ADSH files directory not found: {adsh_path}")
        return actual_data
    
    cik_files = list(adsh_path.glob("*.csv"))
    logger.info(f"Found {len(cik_files)} CIK files to check")
    
    for cik_file in cik_files:
        try:
            cik = cik_file.stem
            df = pd.read_csv(cik_file)
            
            # Convert DataFrame to list of dictionaries
            adsh_records = []
            for _, row in df.iterrows():
                adsh_records.append({
                    'cik': str(row['cik']),
                    'accession_number': row['filename'].split('/')[-1].replace('.txt', ''),
                    'form_type': row['form'],
                    'filename': row['filename']
                })
            
            actual_data[cik] = adsh_records
            logger.debug(f"CIK {cik}: {len(adsh_records)} ADSHs")
        except Exception as e:
            logger.error(f"Error reading CIK file {cik_file}: {e}")
            continue
    
    return actual_data


def get_downloaded_cache_data(cache_dir: Path) -> Set[str]:
    """
    Get downloaded filings from the download cache.
    
    Args:
        cache_dir: Directory containing the cache files
        
    Returns:
        Set of downloaded filing cache keys
    """
    logger = logging.getLogger(__name__)
    
    try:
        downloaded_filings, _, _ = load_download_cache(cache_dir)
        logger.info(f"Found {len(downloaded_filings)} downloaded filings in cache")
        return downloaded_filings
    except Exception as e:
        logger.error(f"Error loading download cache: {e}")
        return set()


def analyze_download_status(actual_data: Dict[str, List[Dict[str, str]]], 
                           downloaded_filings: Set[str]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Analyze download status for all CIKs.
    
    Args:
        actual_data: Dict mapping CIK to list of ADSH records
        downloaded_filings: Set of downloaded filing cache keys
        
    Returns:
        Tuple of (missing_downloads, not_downloaded_ciks, complete_ciks)
    """
    logger = logging.getLogger(__name__)
    
    missing_downloads = []
    not_downloaded_ciks = []
    complete_ciks = []
    
    for cik, adsh_records in actual_data.items():
        downloaded_count = 0
        missing_records = []
        
        for adsh_record in adsh_records:
            # Create cache key in same format as download cache
            cache_key = f"{adsh_record['cik']}_{adsh_record['accession_number']}"
            
            if cache_key in downloaded_filings:
                downloaded_count += 1
            else:
                missing_records.append(adsh_record)
        
        total_adshs = len(adsh_records)
        completion_percentage = (downloaded_count / total_adshs * 100) if total_adshs > 0 else 0
        
        if downloaded_count == 0:
            # CIK not downloaded at all
            not_downloaded_ciks.append({
                'cik': cik,
                'total_adshs': total_adshs,
                'downloaded_adshs': 0,
                'missing_adshs': total_adshs,
                'completion_percentage': 0.0,
                'status': 'not_downloaded',
                'adsh_records': adsh_records
            })
            logger.warning(f"CIK {cik}: Not downloaded at all ({total_adshs} ADSHs)")
            
        elif downloaded_count < total_adshs:
            # CIK partially downloaded
            missing_downloads.append({
                'cik': cik,
                'total_adshs': total_adshs,
                'downloaded_adshs': downloaded_count,
                'missing_adshs': total_adshs - downloaded_count,
                'completion_percentage': completion_percentage,
                'status': 'partially_downloaded',
                'missing_records': missing_records
            })
            logger.warning(f"CIK {cik}: {total_adshs - downloaded_count}/{total_adshs} ADSHs missing "
                          f"({completion_percentage:.1f}% complete)")
        else:
            # CIK completely downloaded
            complete_ciks.append({
                'cik': cik,
                'total_adshs': total_adshs,
                'downloaded_adshs': downloaded_count,
                'missing_adshs': 0,
                'completion_percentage': 100.0,
                'status': 'complete'
            })
            logger.info(f"CIK {cik}: Complete ({total_adshs} ADSHs)")
    
    return missing_downloads, not_downloaded_ciks, complete_ciks


def get_missing_adshs_for_download(missing_downloads: List[Dict], 
                                  not_downloaded_ciks: List[Dict]) -> List[Dict[str, str]]:
    """
    Extract missing ADSHs from analysis results for downloading.
    
    Args:
        missing_downloads: List of missing download records
        not_downloaded_ciks: List of not downloaded CIK records
        
    Returns:
        List of ADSH dictionaries ready for downloading
    """
    logger = logging.getLogger(__name__)
    
    all_adshs = []
    
    # Add missing ADSHs from partially downloaded CIKs
    for record in missing_downloads:
        for adsh_record in record['missing_records']:
            all_adshs.append({
                'cik': adsh_record['cik'],
                'accession_number': adsh_record['accession_number'],
                'form_type': adsh_record['form_type'],
                'filename': adsh_record['filename'],
                'cache_key': f"{adsh_record['cik']}_{adsh_record['accession_number']}"
            })
    
    # Add all ADSHs from not downloaded CIKs
    for record in not_downloaded_ciks:
        for adsh_record in record['adsh_records']:
            all_adshs.append({
                'cik': adsh_record['cik'],
                'accession_number': adsh_record['accession_number'],
                'form_type': adsh_record['form_type'],
                'filename': adsh_record['filename'],
                'cache_key': f"{adsh_record['cik']}_{adsh_record['accession_number']}"
            })
    
    logger.info(f"Total missing ADSHs identified: {len(all_adshs)}")
    return all_adshs


def filter_already_downloaded(adshs: List[Dict[str, str]], cache_dir: Path) -> List[Dict[str, str]]:
    """
    Filter out ADSHs that have already been downloaded.
    
    Args:
        adshs: List of ADSH dictionaries
        cache_dir: Directory containing the cache files
        
    Returns:
        List of ADSHs that still need to be downloaded
    """
    logger = logging.getLogger(__name__)
    
    try:
        downloaded_cache, _, _ = load_download_cache(cache_dir)
        
        filtered_adshs = []
        already_downloaded = 0
        
        for adsh in adshs:
            if adsh['cache_key'] in downloaded_cache:
                already_downloaded += 1
            else:
                filtered_adshs.append(adsh)
        
        logger.info(f"Filtered out {already_downloaded} already downloaded ADSHs")
        logger.info(f"Remaining ADSHs to download: {len(filtered_adshs)}")
        
        return filtered_adshs
        
    except Exception as e:
        logger.error(f"Error filtering already downloaded ADSHs: {e}")
        return adshs


def check_download_progress(adsh_files_dir: str, cache_dir: Path) -> Tuple[List[Dict[str, str]], Dict[str, any]]:
    """
    Check download progress and identify missing ADSHs.
    
    Args:
        adsh_files_dir: Directory containing CIK-specific ADSH files
        cache_dir: Directory containing the cache files
        
    Returns:
        Tuple of (missing_adshs, progress_summary) where:
            - missing_adshs: List of ADSHs that need to be downloaded
            - progress_summary: Summary statistics of download progress
    """
    logger = logging.getLogger(__name__)
    
    logger.info("Checking download progress...")
    
    # Get actual ADSH data from files
    actual_data = get_actual_adsh_data(adsh_files_dir)
    
    if not actual_data:
        logger.error("No actual ADSH data found.")
        return [], {}
    
    # Get downloaded cache data
    downloaded_filings = get_downloaded_cache_data(cache_dir)
    
    # Analyze download status
    missing_downloads, not_downloaded_ciks, complete_ciks = analyze_download_status(
        actual_data, downloaded_filings
    )
    
    # Get missing ADSHs for downloading
    all_missing_adshs = get_missing_adshs_for_download(missing_downloads, not_downloaded_ciks)
    
    # Filter out already downloaded ADSHs
    missing_adshs = filter_already_downloaded(all_missing_adshs, cache_dir)
    
    # Create progress summary
    total_ciks = len(missing_downloads) + len(not_downloaded_ciks) + len(complete_ciks)
    total_adshs = sum(r['total_adshs'] for r in missing_downloads + not_downloaded_ciks + complete_ciks)
    total_downloaded = sum(r['downloaded_adshs'] for r in missing_downloads + not_downloaded_ciks + complete_ciks)
    total_missing = sum(r['missing_adshs'] for r in missing_downloads + not_downloaded_ciks + complete_ciks)
    
    progress_summary = {
        'total_ciks': total_ciks,
        'complete_ciks': len(complete_ciks),
        'partially_downloaded_ciks': len(missing_downloads),
        'not_downloaded_ciks': len(not_downloaded_ciks),
        'total_adshs': total_adshs,
        'total_downloaded_adshs': total_downloaded,
        'total_missing_adshs': total_missing,
        'overall_completion_percentage': (total_downloaded / total_adshs * 100) if total_adshs > 0 else 0,
        'missing_adshs_to_download': len(missing_adshs)
    }
    
    logger.info(f"Progress summary:")
    logger.info(f"  Complete CIKs: {len(complete_ciks)}")
    logger.info(f"  Partially downloaded CIKs: {len(missing_downloads)}")
    logger.info(f"  Not downloaded CIKs: {len(not_downloaded_ciks)}")
    logger.info(f"  Total ADSHs: {total_adshs}")
    logger.info(f"  Downloaded ADSHs: {total_downloaded}")
    logger.info(f"  Overall completion: {progress_summary['overall_completion_percentage']:.1f}%")
    logger.info(f"  Missing ADSHs to download: {len(missing_adshs)}")
    
    return missing_adshs, progress_summary 