"""
Configuration settings for SEC EDGAR data processing.
"""

# SEC EDGAR request headers - SEC requires proper identification
SEC_HEADERS = {
    'User-Agent': 'Your Name (your.email@domain.com)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# Download settings
DOWNLOAD_TIMEOUT = 30
REQUEST_DELAY = 0.05  # Delay between requests to be respectful to SEC servers
