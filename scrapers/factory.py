# File: src/scrapers/factory.py
"""Scraper factory for creating appropriate scraper instances"""
from typing import Dict, Any, List

from scrapers.api_scraper import CryptoCompareAPIScraper
from scrapers.base import RSSAsyncScraper
from scrapers.reddit_scraper import RedditScraper
from scrapers.web_scraper import WebArchiveScraper
from utils.http_client import AsyncHTTPClient
from utils.logger import get_logger

logger = get_logger(__name__)

class ScraperFactory:
    """Factory for creating scraper instances"""

    def __init__(self, http_client: AsyncHTTPClient, global_config: Dict[str, Any] = None):
        self.http_client = http_client
        self.global_config = global_config or {}

    async def create_scraper(self, source_config: Dict[str, Any]):
        """Create appropriate scraper based on source configuration"""
        source_name = source_config.get('name', 'Unknown')
        source_type = source_config.get('source_type', 'rss')

        try:
            if source_type == 'rss' or source_config.get('rss_url'):
                return RSSAsyncScraper(source_config, self.http_client, self.global_config)
            elif source_type == 'api':
                # Determine which API scraper to use
                if 'cryptocompare' in source_name.lower():
                    return CryptoCompareAPIScraper(source_config, self.http_client, self.global_config)
                else:
                    logger.warning(f"Unknown API source: {source_name}")
                    return None
            elif source_type == 'web' or source_config.get('enable_web_archive'):
                return WebArchiveScraper(source_config, self.http_client, self.global_config)
            elif source_type == 'reddit':
                return RedditScraper(source_config, self.http_client, self.global_config)
            else:
                logger.warning(f"Unknown source type: {source_type} for {source_name}")
                return None

        except Exception as e:
            logger.error(f"Failed to create scraper for {source_name}: {e}")
            return None

    @classmethod
    def get_available_scrapers(cls) -> List[str]:
        """Get list of available scraper types"""
        return ['rss', 'api_cryptocompare']