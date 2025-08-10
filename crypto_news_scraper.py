import asyncio
import aiohttp
import feedparser
import sqlite3
import json
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple
from collections import deque
from urllib.parse import urljoin, urlparse
import hashlib
import time
import requests
from bs4 import BeautifulSoup
import re

@dataclass
class NewsArticle:
    """Standardized news article structure"""
    id: str
    title: str
    content: str
    url: str
    source: str
    timestamp: datetime
    author: Optional[str] = None
    category: Optional[str] = None
    sentiment: Optional[float] = None
    relevance_score: Optional[float] = None

    def __post_init__(self):
        if not self.id:
            # Generate unique ID from URL and timestamp
            content_hash = hashlib.md5(f"{self.url}{self.timestamp}".encode()).hexdigest()
            self.id = content_hash[:16]

class NewsDatabase:
    """SQLite database for storing and retrieving news articles"""

    def __init__(self, db_path: str = "crypto_news.db"):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Initialize database schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS articles (
                                                               id TEXT PRIMARY KEY,
                                                               title TEXT NOT NULL,
                                                               content TEXT,
                                                               url TEXT UNIQUE NOT NULL,
                                                               source TEXT NOT NULL,
                                                               timestamp DATETIME NOT NULL,
                                                               author TEXT,
                                                               category TEXT,
                                                               sentiment REAL,
                                                               relevance_score REAL,
                                                               created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                       )
                       ''')

        # Create indexes for efficient querying
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON articles(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_source ON articles(source)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sentiment ON articles(sentiment)')

        conn.commit()
        conn.close()

    def save_article(self, article: NewsArticle) -> bool:
        """Save article to database, return True if new article"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute('''
                           INSERT INTO articles (id, title, content, url, source, timestamp,
                                                 author, category, sentiment, relevance_score)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                           ''', (
                               article.id, article.title, article.content, article.url,
                               article.source, article.timestamp, article.author,
                               article.category, article.sentiment, article.relevance_score
                           ))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Article already exists
            return False
        finally:
            conn.close()

    def get_articles_by_timerange(self, start_time: datetime, end_time: datetime) -> List[NewsArticle]:
        """Retrieve articles within time range"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
                       SELECT id, title, content, url, source, timestamp, author, category, sentiment, relevance_score
                       FROM articles
                       WHERE timestamp BETWEEN ? AND ?
                       ORDER BY timestamp DESC
                       ''', (start_time, end_time))

        rows = cursor.fetchall()
        conn.close()

        articles = []
        for row in rows:
            articles.append(NewsArticle(
                id=row[0], title=row[1], content=row[2], url=row[3],
                source=row[4], timestamp=datetime.fromisoformat(row[5]),
                author=row[6], category=row[7], sentiment=row[8], relevance_score=row[9]
            ))

        return articles

    def get_latest_timestamp(self, source: str = None) -> Optional[datetime]:
        """Get timestamp of most recent article (optionally by source)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if source:
            cursor.execute('SELECT MAX(timestamp) FROM articles WHERE source = ?', (source,))
        else:
            cursor.execute('SELECT MAX(timestamp) FROM articles')

        result = cursor.fetchone()[0]
        conn.close()

        return datetime.fromisoformat(result) if result else None

class NewsSource():
    """Base class for news sources"""

    def __init__(self, name: str, base_url: str, rss_url: str = None):
        self.name = name
        self.base_url = base_url
        self.rss_url = rss_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def parse_rss_feed(self) -> List[NewsArticle]:
        """Parse RSS feed and return articles"""
        if not self.rss_url:
            return []

        try:
            feed = feedparser.parse(self.rss_url)
            articles = []

            for entry in feed.entries:
                # Extract content
                content = ""
                if hasattr(entry, 'content'):
                    content = entry.content[0].value if entry.content else ""
                elif hasattr(entry, 'summary'):
                    content = entry.summary

                # Parse timestamp
                timestamp = datetime.now()
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    timestamp = datetime(*entry.published_parsed[:6])

                article = NewsArticle(
                    id="",  # Will be generated in __post_init__
                    title=entry.title,
                    content=self.clean_content(content),
                    url=entry.link,
                    source=self.name,
                    timestamp=timestamp,
                    author=getattr(entry, 'author', None)
                )

                articles.append(article)

            return articles

        except Exception as e:
            logging.error(f"Error parsing RSS feed for {self.name}: {e}")
            return []

    def clean_content(self, content: str) -> str:
        """Clean HTML tags and normalize content"""
        if not content:
            return ""

        # Remove HTML tags
        soup = BeautifulSoup(content, 'html.parser')
        text = soup.get_text()

        # Clean whitespace
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    def scrape_archive(self, start_date: datetime, end_date: datetime) -> List[NewsArticle]:
        """Scrape historical articles (to be implemented by subclasses)"""
        raise NotImplementedError("Archive scraping must be implemented by subclasses")

class CoinDeskSource(NewsSource):
    """CoinDesk-specific scraper"""

    def __init__(self):
        super().__init__(
            name="CoinDesk",
            base_url="https://www.coindesk.com",
            rss_url="https://www.coindesk.com/arc/outboundfeeds/rss/"
        )

    def scrape_archive(self, start_date: datetime, end_date: datetime) -> List[NewsArticle]:
        """Scrape CoinDesk archives"""
        articles = []
        current_date = start_date

        while current_date <= end_date:
            try:
                # CoinDesk archive URL format
                archive_url = f"https://www.coindesk.com/{current_date.year}/{current_date.month:02d}/{current_date.day:02d}/"

                response = self.session.get(archive_url, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')

                    # Extract article links (adjust selector based on site structure)
                    article_links = soup.find_all('a', class_='card-title')

                    for link in article_links:
                        article_url = urljoin(self.base_url, link.get('href'))
                        article = self.scrape_single_article(article_url, current_date)
                        if article:
                            articles.append(article)
                else:
                    logging.warn(f"Status code {response.status_code} returned for archive_url: {archive_url}")

                current_date += timedelta(days=1)
                time.sleep(1)  # Rate limiting

            except Exception as e:
                logging.error(f"Error scraping CoinDesk archive for {current_date}: {e}")
                current_date += timedelta(days=1)

        return articles

    def scrape_single_article(self, url: str, fallback_date: datetime) -> Optional[NewsArticle]:
        """Scrape individual article"""
        try:
            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract title
            title_elem = soup.find('h1') or soup.find('title')
            title = title_elem.get_text().strip() if title_elem else ""

            # Extract content
            content_elem = soup.find('div', class_='entry-content') or soup.find('article')
            content = content_elem.get_text().strip() if content_elem else ""

            # Extract timestamp (try multiple formats)
            timestamp = fallback_date
            time_elem = soup.find('time')
            if time_elem and time_elem.get('datetime'):
                try:
                    timestamp = datetime.fromisoformat(time_elem['datetime'].replace('Z', '+00:00'))
                except:
                    pass

            return NewsArticle(
                id="",
                title=title,
                content=self.clean_content(content),
                url=url,
                source=self.name,
                timestamp=timestamp
            )

        except Exception as e:
            logging.error(f"Error scraping article {url}: {e}")
            return None

class CoinTelegraphSource(NewsSource):
    """CoinTelegraph-specific scraper"""

    def __init__(self):
        super().__init__(
            name="CoinTelegraph",
            base_url="https://cointelegraph.com",
            rss_url="https://cointelegraph.com/rss"
        )

    def scrape_archive(self, start_date: datetime, end_date: datetime) -> List[NewsArticle]:
        """Implement CoinTelegraph archive scraping"""
        # Similar to CoinDesk but with different URL patterns and selectors
        # Implementation would be customized for CoinTelegraph's structure
        articles = []
        # ... implementation details
        return articles

class CryptoNewsScraper:
    """Main scraper orchestrator supporting both real-time and archive modes"""

    def __init__(self, db_path: str = "crypto_news.db"):
        self.db = NewsDatabase(db_path)
        self.sources = {
            'coindesk': CoinDeskSource(),
            'cointelegraph': CoinTelegraphSource(),
            # Add more sources as needed
        }

        # Sliding window buffer for real-time mode
        self.article_buffer = deque(maxlen=10000)
        self.is_realtime_mode = False

        # # Configure logging
        # logging.basicConfig(level=logging.INFO,
        #                     format='%(asctime)s - %(levelname)s - %(message)s')

    def initialize_realtime_mode(self, lookback_days: int = 7):
        """Initialize real-time mode with historical data buffer"""
        logging.info(f"Initializing real-time mode with {lookback_days} days of lookback data")

        end_time = datetime.now()
        start_time = end_time - timedelta(days=lookback_days)

        # Load recent articles from database
        articles = self.db.get_articles_by_timerange(start_time, end_time)

        # If database is empty, scrape recent data
        if not articles:
            logging.info("No historical data found, scraping recent articles...")
            self.run_archive_mode(start_time, end_time)
            articles = self.db.get_articles_by_timerange(start_time, end_time)

        # Populate buffer
        for article in articles:
            self.article_buffer.append(article)

        self.is_realtime_mode = True
        logging.info(f"Real-time mode initialized with {len(self.article_buffer)} articles")

    def run_archive_mode(self, start_date: datetime, end_date: datetime) -> int:
        """Run scraper in archive mode to collect historical data"""
        logging.info(f"Running archive mode from {start_date} to {end_date}")

        total_articles = 0

        for source_name, source in self.sources.items():
            logging.info(f"Scraping {source_name} archives...")

            try:
                # First try RSS for recent articles
                rss_articles = source.parse_rss_feed()

                # Filter RSS articles by date range
                filtered_rss = [
                    article for article in rss_articles
                    if start_date <= article.timestamp <= end_date
                ]

                # Save RSS articles
                for article in filtered_rss:
                    if self.db.save_article(article):
                        total_articles += 1

                # Then scrape archives for older articles
                archive_articles = source.scrape_archive(start_date, end_date)

                # Save archive articles
                for article in archive_articles:
                    if self.db.save_article(article):
                        total_articles += 1

                logging.info(f"Completed {source_name}: {len(filtered_rss)} RSS + {len(archive_articles)} archive articles")

            except Exception as e:
                logging.error(f"Error scraping {source_name}: {e}")

        logging.info(f"Archive mode completed: {total_articles} new articles saved")
        return total_articles

    def run_realtime_mode(self, update_interval: int = 300):  # 5 minutes
        """Run scraper in real-time mode"""
        if not self.is_realtime_mode:
            raise ValueError("Must call initialize_realtime_mode() first")

        logging.info(f"Starting real-time mode with {update_interval}s intervals")

        while True:
            try:
                new_articles = self.fetch_latest_articles()

                for article in new_articles:
                    # Save to database
                    if self.db.save_article(article):
                        # Add to buffer
                        self.article_buffer.append(article)
                        logging.info(f"New article: {article.title[:100]}...")

                # Clean old articles from buffer (keep only last 7 days)
                cutoff_time = datetime.now() - timedelta(days=7)
                while (self.article_buffer and
                       self.article_buffer[0].timestamp < cutoff_time):
                    self.article_buffer.popleft()

                logging.info(f"Buffer contains {len(self.article_buffer)} articles")

                time.sleep(update_interval)

            except KeyboardInterrupt:
                logging.info("Real-time mode stopped by user")
                break
            except Exception as e:
                logging.error(f"Error in real-time mode: {e}")
                time.sleep(60)  # Wait before retrying

    def fetch_latest_articles(self) -> List[NewsArticle]:
        """Fetch latest articles from all sources"""
        articles = []

        for source_name, source in self.sources.items():
            try:
                # Get latest timestamp for this source
                latest_timestamp = self.db.get_latest_timestamp(source_name)

                # Parse RSS feed
                rss_articles = source.parse_rss_feed()

                # Filter only new articles
                new_articles = []
                for article in rss_articles:
                    if not latest_timestamp or article.timestamp > latest_timestamp:
                        new_articles.append(article)

                articles.extend(new_articles)

            except Exception as e:
                logging.error(f"Error fetching from {source_name}: {e}")

        return articles

    def get_sliding_window_data(self, window_hours: int = 24) -> List[NewsArticle]:
        """Get articles from sliding window buffer"""
        if not self.is_realtime_mode:
            raise ValueError("Sliding window only available in real-time mode")

        cutoff_time = datetime.now() - timedelta(hours=window_hours)

        return [
            article for article in self.article_buffer
            if article.timestamp >= cutoff_time
        ]

    def get_articles_by_timerange(self, start_time: datetime, end_time: datetime) -> List[NewsArticle]:
        """Get articles by time range (from database)"""
        return self.db.get_articles_by_timerange(start_time, end_time)

# Example usage
if __name__ == "__main__":
    scraper = CryptoNewsScraper()

    # Example 1: Archive mode - collect 30 days of historical data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)

    print("Running in archive mode...")
    total_articles = scraper.run_archive_mode(start_date, end_date)
    print(f"Collected {total_articles} articles")

    # Example 2: Initialize and run real-time mode
    print("Initializing real-time mode...")
    scraper.initialize_realtime_mode(lookback_days=7)

    # Get current sliding window data
    recent_articles = scraper.get_sliding_window_data(window_hours=24)
    print(f"Current 24h window contains {len(recent_articles)} articles")

    # Start real-time monitoring (uncomment to run)
    # scraper.run_realtime_mode(update_interval=300)