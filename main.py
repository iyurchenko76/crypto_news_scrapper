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

    async def run_single_collection(self, days_back: int = 1) -> Dict[str, Any]:
        """Run a single collection cycle with days_back parameter"""
        # Convert days to hours for internal use (your existing system expects hours)
        hours_back = days_back * 24

        self.logger.info("=== Starting Single Collection ===")
        self.logger.info(f"📅 Collection time range: {days_back} days ({hours_back} hours)")

        # Show current stats
        stats = await self.db.get_database_stats(24)
        self.logger.info(f"Current database: {stats['total_articles_period']} articles in last 24h")
        self.logger.info(f"Total articles: {stats['total_articles_all']}")

        # Run coordinated scraping with hours parameter (unchanged)
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

    async def show_stats(self, days: int = 1):
        """Show database and performance statistics"""
        hours = days * 24  # Convert to hours for existing system

        # Database stats
        db_stats = await self.db.get_database_stats(hours)
        self.logger.info(f"=== Database Statistics (last {days} days) ===")
        self.logger.info(f"Articles in period: {db_stats['total_articles_period']}")
        self.logger.info(f"Total articles: {db_stats['total_articles_all']}")
        self.logger.info(f"Unique sources: {db_stats['unique_sources']}")
        self.logger.info(f"Average relevance: {db_stats['avg_relevance_score']}")

        if db_stats['source_counts']:
            self.logger.info("Top sources:")
            for source, count in list(db_stats['source_counts'].items())[:10]:
                self.logger.info(f"  {source}: {count} articles")

    async def export_data(self, days: int = 1, format: str = 'csv'):
        """Export recent articles"""
        hours = days * 24  # Convert to hours
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)

        articles = await self.db.get_articles_by_timerange(start_time, end_time)

        if format == 'csv':
            filename = f'crypto_news_export_{days}d.csv'
            await self._export_csv(articles, filename)
        elif format == 'json':
            filename = f'crypto_news_export_{days}d.json'
            await self._export_json(articles, filename)
        else:
            self.logger.error(f"Unsupported export format: {format}")
            return None

        self.logger.info(f"Exported {len(articles)} articles ({days} days) to {filename}")
        return filename

    # Keep existing _export_csv and _export_json methods unchanged

async def main():
    """Main CLI interface - simple days_back parameter"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m main run [days_back]      # Single collection run")
        print("  python -m main schedule             # Start scheduled collection")
        print("  python -m main stats [days]         # Show statistics")
        print("  python -m main export [days] [format] # Export data")
        print()
        print("Examples:")
        print("  python -m main run 3               # Collect last 3 days")
        print("  python -m main stats 7             # Stats for last 7 days")
        print("  python -m main export 2 json       # Export 2 days as JSON")
        return

    command = sys.argv[1]

    try:
        # Initialize app
        app = CryptoScraperApp()
        await app.initialize()

        if command == "run":
            days_back = int(sys.argv[2]) if len(sys.argv) > 2 else 1
            await app.run_single_collection(days_back)

        elif command == "schedule":
            await app.run_scheduled_collection()

        elif command == "stats":
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 1
            await app.show_stats(days)

        elif command == "export":
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 1
            format = sys.argv[3] if len(sys.argv) > 3 else 'csv'
            await app.export_data(days, format)

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
    asyncio.run(main())