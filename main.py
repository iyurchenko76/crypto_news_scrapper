# File: src/main.py
"""Main entry point for the refactored crypto scraper"""
import asyncio
import csv
import json
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List

from config.settings import ConfigManager
from core.exceptions import ConfigurationError, ScraperError
from orchestration.coordinator import ScrapingCoordinator
from storage.database import AsyncNewsDatabase
from utils.logger import setup_logging, get_logger


class CryptoScraperApp:
    """Main application class"""

    def __init__(self, config_path: str = "crypto_scraper_config.yaml"):
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.load_config()

        # Setup logging
        setup_logging(self.config.get('logging', {}))
        self.logger = get_logger('main')

        # Initialize components
        self.coordinator = ScrapingCoordinator(self.config)
        self.db = AsyncNewsDatabase(self.config['database_path'])

    async def initialize(self):
        """Initialize application components"""
        await self.coordinator.initialize()
        self.logger.info("Application initialized successfully")

    async def run_single_collection(self, hours_back: int = 24) -> Dict[str, Any]:
        """Run a single collection cycle"""
        self.logger.info("=== Starting Single Collection ===")

        # Show current stats
        stats = await self.db.get_database_stats(24)
        self.logger.info(f"Current database: {stats['total_articles_period']} articles in last 24h")
        self.logger.info(f"Total articles: {stats['total_articles_all']}")

        # Run coordinated scraping
        result = await self.coordinator.run_coordinated_scraping(hours_back)

        # Show results
        self.logger.info(f"Collection complete: {result['total_new_articles']} new articles")
        self.logger.info(f"Duration: {result['duration']:.1f} seconds")

        # Show per-source breakdown
        self.logger.info("Per-source results:")
        for source, data in result['source_results'].items():
            if data.get('success'):
                self.logger.info(f"  {source}: {data.get('new_articles', 0)} new articles")
            else:
                self.logger.warning(f"  {source}: FAILED - {data.get('error', 'Unknown error')}")

        return result

    async def run_scheduled_collection(self):
        """Run scheduled collection loop"""
        interval_seconds = self.config.get('update_interval_seconds', 300)
        self.logger.info(f"Starting scheduled collection every {interval_seconds} seconds")

        async def collection_job():
            try:
                result = await self.coordinator.run_coordinated_scraping()
                self.logger.info(f"Scheduled collection: {result['total_new_articles']} new articles")
            except Exception as e:
                self.logger.error(f"Scheduled collection failed: {e}")

        # Run immediately
        await collection_job()

        # Schedule regular runs
        try:
            while True:
                await asyncio.sleep(interval_seconds)
                await collection_job()

        except KeyboardInterrupt:
            self.logger.info("Scheduled collection stopped by user")

    async def show_stats(self, hours: int = 24):
        """Show database and performance statistics"""
        # Database stats
        db_stats = await self.db.get_database_stats(hours)
        self.logger.info(f"=== Database Statistics (last {hours}h) ===")
        self.logger.info(f"Articles in period: {db_stats['total_articles_period']}")
        self.logger.info(f"Total articles: {db_stats['total_articles_all']}")
        self.logger.info(f"Unique sources: {db_stats['unique_sources']}")
        self.logger.info(f"Average relevance: {db_stats['avg_relevance_score']}")

        if db_stats['source_counts']:
            self.logger.info("Top sources:")
            for source, count in list(db_stats['source_counts'].items())[:10]:
                self.logger.info(f"  {source}: {count} articles")

    async def export_data(self, hours: int = 24, format: str = 'csv'):
        """Export recent articles"""
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)

        articles = await self.db.get_articles_by_timerange(start_time, end_time)

        if format == 'csv':
            filename = f'crypto_news_export_{hours}h.csv'
            await self._export_csv(articles, filename)
        elif format == 'json':
            filename = f'crypto_news_export_{hours}h.json'
            await self._export_json(articles, filename)
        else:
            self.logger.error(f"Unsupported export format: {format}")
            return None

        self.logger.info(f"Exported {len(articles)} articles to {filename}")
        return filename

    async def _export_csv(self, articles: List, filename: str):
        """Export articles to CSV"""
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'id', 'title', 'content', 'url', 'source', 'timestamp',
                'author', 'category', 'sentiment', 'relevance_score',
                'source_type', 'tags', 'content_length'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for article in articles:
                writer.writerow({
                    'id': article.id,
                    'title': article.title,
                    'content': article.content[:1000] if article.content else '',  # Truncate
                    'url': article.url,
                    'source': article.source,
                    'timestamp': article.timestamp.isoformat(),
                    'author': article.author,
                    'category': article.category,
                    'sentiment': article.sentiment,
                    'relevance_score': article.relevance_score,
                    'source_type': article.source_type.value,
                    'tags': ','.join(article.tags) if article.tags else '',
                    'content_length': len(article.content) if article.content else 0
                })

    async def _export_json(self, articles: List, filename: str):
        """Export articles to JSON"""
        data = [article.to_dict() for article in articles]

        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, indent=2, ensure_ascii=False, default=str)

async def main():
    """Main CLI interface"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m main run [hours_back]    # Single collection run")
        print("  python -m main schedule             # Start scheduled collection")
        print("  python -m main stats [hours]        # Show statistics")
        print("  python -m main export [hours] [format] # Export data")
        return

    command = sys.argv[1]

    try:
        # Initialize app
        app = CryptoScraperApp()
        await app.initialize()

        if command == "run":
            hours_back = int(sys.argv[2]) if len(sys.argv) > 2 else 24
            await app.run_single_collection(hours_back)

        elif command == "schedule":
            await app.run_scheduled_collection()

        elif command == "stats":
            hours = int(sys.argv[2]) if len(sys.argv) > 2 else 24
            await app.show_stats(hours)

        elif command == "export":
            hours = int(sys.argv[2]) if len(sys.argv) > 2 else 24
            format = sys.argv[3] if len(sys.argv) > 3 else 'csv'
            await app.export_data(hours, format)

        else:
            print(f"Unknown command: {command}")

    except ConfigurationError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    except ScraperError as e:
        print(f"Scraper error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("Application stopped by user")
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())# File: src/__init__.py