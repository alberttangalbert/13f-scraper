"""
Core modules for SEC EDGAR data processing.
"""

from .retrieve_master_files import download_master_files

__all__ = ['download_master_files'] 