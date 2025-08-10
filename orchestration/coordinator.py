# File: src/orchestration/coordinator.py
"""Orchestration layer for coordinated scraping"""
import asyncio
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Any

from processing.content_filter import ContentFilter
from scrapers.factory import ScraperFactory
from storage.database import AsyncNewsDatabase
from utils.http_client import AsyncHTTPClient
from utils.logger import get_logger

logger = get_logger(__name__)

class ScrapingCoordinator:
    """Coordinates scraping across multiple sources with prioritization"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.db = AsyncNewsDatabase(config['database_path'])
        self.content_filter = ContentFilter(config)

        # HTTP client configuration
        http_config = {
            'connection_pool_size': config.get('max_connections', 100),
            'connections_per_host': config.get('connections_per_host', 10),
            'total_timeout': config.get('request_timeout_seconds', 30),
            'max_retries': config.get('max_retries', 3),
            'user_agent': config.get('user_agent', 'CryptoScraper/2.0')
        }

        self.http_client = AsyncHTTPClient(http_config)
        # FIXED: Pass global config to factory
        self.scraper_factory = ScraperFactory(self.http_client, config)

        # Coordination settings
        self.max_concurrent_sources = config.get('max_concurrent_sources', 5)
        self.priority_delay = config.get('priority_delay_seconds', 1.0)

    async def initialize(self):
        """Initialize coordinator components"""
        await self.db.initialize()
        logger.info("Scraping coordinator initialized")

    async def run_coordinated_scraping(self, hours_back: int = 24) -> Dict[str, Any]:
        """Run coordinated scraping across all enabled sources"""
        start_time = time.time()

        logger.info("=== Starting Coordinated Scraping ===")
        logger.info(f"Looking back {hours_back} hours")

        async with self.http_client:
            # Get sources grouped by priority
            sources_by_priority = self._group_sources_by_priority()

            all_results = {}
            total_new_articles = 0

            # Process each priority tier
            for priority in sorted(sources_by_priority.keys()):
                sources = sources_by_priority[priority]
                logger.info(f"Processing priority {priority} sources ({len(sources)} sources)...")

                priority_results = await self._process_priority_tier(sources, hours_back)
                all_results.update(priority_results)

                # Calculate articles for this priority
                priority_articles = sum(r.get('new_articles', 0) for r in priority_results.values())
                total_new_articles += priority_articles

                logger.info(f"Priority {priority} complete: {priority_articles} new articles")

                # Brief pause between priority tiers
                if priority < max(sources_by_priority.keys()):
                    await asyncio.sleep(self.priority_delay)

        duration = time.time() - start_time

        logger.info("=== Coordinated Scraping Complete ===")
        logger.info(f"Total new articles: {total_new_articles}")
        logger.info(f"Duration: {duration:.1f} seconds")

        return {
            'total_new_articles': total_new_articles,
            'duration': duration,
            'source_results': all_results,
            'timestamp': datetime.now().isoformat(),
            'hours_back': hours_back
        }

    def _group_sources_by_priority(self) -> Dict[int, List[Dict[str, Any]]]:
        """Group enabled sources by priority level"""
        sources_by_priority = defaultdict(list)

        for source in self.config.get('sources', []):
            if source.get('enabled', True):
                priority = source.get('priority', 5)
                sources_by_priority[priority].append(source)

        return dict(sources_by_priority)

    async def _process_priority_tier(self, sources: List[Dict[str, Any]], hours_back: int) -> Dict[str, Dict[str, Any]]:
        """Process all sources in a priority tier concurrently"""
        semaphore = asyncio.Semaphore(self.max_concurrent_sources)

        async def process_single_source(source_config):
            async with semaphore:
                return await self._scrape_and_process_source(source_config, hours_back)

        # Create tasks for all sources in this tier
        tasks = []
        for source_config in sources:
            task = process_single_source(source_config)
            tasks.append((task, source_config['name']))

        # Execute all tasks concurrently
        results = await asyncio.gather(*[task for task, _ in tasks], return_exceptions=True)

        # Process results
        source_results = {}
        for i, result in enumerate(results):
            source_name = tasks[i][1]

            if isinstance(result, Exception):
                logger.error(f"Source {source_name} failed: {result}")
                source_results[source_name] = {
                    'success': False,
                    'error': str(result),
                    'new_articles': 0,
                    'processing_time': 0
                }
            else:
                source_results[source_name] = result

        return source_results

    async def _scrape_and_process_source(self, source_config: Dict[str, Any], hours_back: int) -> Dict[str, Any]:
        """Scrape and process a single source"""
        source_name = source_config['name']
        start_time = time.time()

        try:
            # Create scraper
            scraper = await self.scraper_factory.create_scraper(source_config)
            if not scraper:
                return {
                    'success': False,
                    'error': 'Failed to create scraper',
                    'new_articles': 0,
                    'processing_time': time.time() - start_time
                }

            # Validate source accessibility
            if not await scraper.validate_source():
                return {
                    'success': False,
                    'error': 'Source validation failed',
                    'new_articles': 0,
                    'processing_time': time.time() - start_time
                }

            # Scrape articles
            max_articles = source_config.get('max_articles_per_run', 100)
            raw_articles = await scraper.scrape_articles(max_articles)

            if not raw_articles:
                return {
                    'success': True,
                    'articles_scraped': 0,
                    'articles_valid': 0,
                    'new_articles': 0,
                    'duplicates': 0,
                    'processing_time': time.time() - start_time
                }

            # Filter articles by time range
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            recent_articles = [
                article for article in raw_articles
                if article.timestamp >= cutoff_time
            ]

            # Apply content filtering
            valid_articles = []
            for article in recent_articles:
                if self.content_filter.is_valid_article(article):
                    # Enrich article with additional processing
                    enriched_article = await self.content_filter.enrich_article(article)
                    valid_articles.append(enriched_article)

            # Save to database
            if valid_articles:
                save_results = await self.db.save_article_batch(valid_articles)
            else:
                save_results = {'new': 0, 'duplicates': 0, 'errors': 0}

            processing_time = time.time() - start_time

            result = {
                'success': True,
                'articles_scraped': len(raw_articles),
                'articles_recent': len(recent_articles),
                'articles_valid': len(valid_articles),
                'new_articles': save_results['new'],
                'duplicates': save_results['duplicates'],
                'errors': save_results['errors'],
                'processing_time': processing_time
            }

            logger.info(f"{source_name}: {save_results['new']} new / {len(valid_articles)} valid / {len(raw_articles)} scraped")

            return result

        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Error processing {source_name}: {e}")

            return {
                'success': False,
                'error': str(e),
                'new_articles': 0,
                'processing_time': processing_time
            }