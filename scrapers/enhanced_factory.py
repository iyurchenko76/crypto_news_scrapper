# File: src/scrapers/enhanced_factory.py  
"""Enhanced factory that includes web scraping capabilities"""
from typing import Dict, Any

from scrapers.factory import ScraperFactory
from scrapers.web_scraper import WebArchiveScraper
from utils.logger import get_logger

logger = get_logger(__name__)
class EnhancedScraperFactory(ScraperFactory):
    """Enhanced factory with web scraping support"""

    async def create_scraper(self, source_config: Dict[str, Any]):
        """Create scraper with web archive support"""
        source_name = source_config.get('name', 'Unknown')
        source_type = source_config.get('source_type', 'rss')
        enable_web_archive = source_config.get('enable_web_archive', False)

        try:
            # Create base scraper (RSS or API)
            base_scraper = await super().create_scraper(source_config)

            # If web archive is enabled, return web scraper instead
            if enable_web_archive and source_type in ['rss', 'web']:
                return WebArchiveScraper(source_config, self.http_client, self.global_config)

            return base_scraper

        except Exception as e:
            logger.error(f"Failed to create enhanced scraper for {source_name}: {e}")
            return None