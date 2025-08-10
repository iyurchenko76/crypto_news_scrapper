# File: src/storage/database.py
"""Enhanced async database operations"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

import aiosqlite

from core.models import NewsArticle, SourceType
from utils.logger import get_logger

logger = get_logger(__name__)

class AsyncNewsDatabase:
    """Async database operations with connection pooling"""

    def __init__(self, db_path: str, max_connections: int = 10):
        self.db_path = db_path
        self.max_connections = max_connections
        self.connection_semaphore = asyncio.Semaphore(max_connections)

    async def initialize(self):
        """Initialize database schema with optimizations"""
        async with self.get_connection() as db:
            # Main articles table
            await db.execute('''
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
                                                                     source_type TEXT DEFAULT 'rss',
                                                                     content_hash TEXT,
                                                                     created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                                                                     updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                             )
                             ''')

            # Article metadata table for flexible storage
            await db.execute('''
                             CREATE TABLE IF NOT EXISTS article_metadata (
                                                                             article_id TEXT,
                                                                             key TEXT,
                                                                             value TEXT,
                                                                             FOREIGN KEY (article_id) REFERENCES articles (id),
                                 PRIMARY KEY (article_id, key)
                                 )
                             ''')

            # Article tags table
            await db.execute('''
                             CREATE TABLE IF NOT EXISTS article_tags (
                                                                         article_id TEXT,
                                                                         tag TEXT,
                                                                         FOREIGN KEY (article_id) REFERENCES articles (id),
                                 PRIMARY KEY (article_id, tag)
                                 )
                             ''')

            # Performance monitoring table
            await db.execute('''
                             CREATE TABLE IF NOT EXISTS scraping_sessions (
                                                                              id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                              timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                                                                              total_articles INTEGER,
                                                                              duration REAL,
                                                                              sources_attempted INTEGER,
                                                                              sources_successful INTEGER,
                                                                              metadata TEXT
                             )
                             ''')

            # Create indexes for performance
            indexes = [
                'CREATE INDEX IF NOT EXISTS idx_articles_timestamp ON articles(timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source)',
                'CREATE INDEX IF NOT EXISTS idx_articles_content_hash ON articles(content_hash)',
                'CREATE INDEX IF NOT EXISTS idx_articles_url_hash ON articles(url)',
                'CREATE INDEX IF NOT EXISTS idx_articles_relevance ON articles(relevance_score)',
                'CREATE INDEX IF NOT EXISTS idx_articles_source_timestamp ON articles(source, timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_metadata_key ON article_metadata(key)',
                'CREATE INDEX IF NOT EXISTS idx_tags_tag ON article_tags(tag)',
                'CREATE INDEX IF NOT EXISTS idx_sessions_timestamp ON scraping_sessions(timestamp)'
            ]

            for index_sql in indexes:
                await db.execute(index_sql)

            await db.commit()
            logger.info("Database initialized successfully")

    @asynccontextmanager
    async def get_connection(self):
        """Get database connection with proper configuration"""
        async with self.connection_semaphore:
            async with aiosqlite.connect(self.db_path) as db:
                # Enable WAL mode for better concurrency
                await db.execute('PRAGMA journal_mode=WAL')
                await db.execute('PRAGMA synchronous=NORMAL')
                await db.execute('PRAGMA cache_size=10000')
                await db.execute('PRAGMA temp_store=MEMORY')
                await db.execute('PRAGMA mmap_size=268435456')  # 256MB
                yield db

    async def save_article_batch(self, articles: List[NewsArticle]) -> Dict[str, int]:
        """Save multiple articles in a single transaction"""
        new_count = 0
        duplicate_count = 0
        error_count = 0

        if not articles:
            return {'new': 0, 'duplicates': 0, 'errors': 0}

        async with self.get_connection() as db:
            await db.execute('BEGIN TRANSACTION')

            try:
                for article in articles:
                    try:
                        # Insert main article record
                        result = await db.execute('''
                                                  INSERT OR IGNORE INTO articles 
                            (id, title, content, url, source, timestamp, author, 
                             category, sentiment, relevance_score, source_type, content_hash)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                  ''', (
                                                      article.id, article.title, article.content, article.url,
                                                      article.source, article.timestamp, article.author,
                                                      article.category, article.sentiment, article.relevance_score,
                                                      article.source_type.value, article.get_content_hash()
                                                  ))

                        if result.rowcount > 0:
                            new_count += 1

                            # Insert metadata
                            if article.metadata:
                                for key, value in article.metadata.items():
                                    await db.execute('''
                                        INSERT OR REPLACE INTO article_metadata (article_id, key, value)
                                        VALUES (?, ?, ?)
                                    ''', (article.id, key, str(value)))

                            # Insert tags
                            if article.tags:
                                for tag in article.tags:
                                    await db.execute('''
                                                     INSERT OR IGNORE INTO article_tags (article_id, tag)
                                        VALUES (?, ?)
                                                     ''', (article.id, tag))
                        else:
                            duplicate_count += 1

                    except Exception as e:
                        error_count += 1
                        logger.error(f"Error saving article {article.id}: {e}")

                await db.execute('COMMIT')

            except Exception as e:
                await db.execute('ROLLBACK')
                logger.error(f"Transaction failed, rolling back: {e}")
                raise e

        logger.debug(f"Batch save: {new_count} new, {duplicate_count} duplicates, {error_count} errors")
        return {'new': new_count, 'duplicates': duplicate_count, 'errors': error_count}

    async def get_articles_by_timerange(self, start_time: datetime, end_time: datetime) -> List[NewsArticle]:
        """Retrieve articles within time range with metadata"""
        articles = []

        async with self.get_connection() as db:
            # Get main article data
            cursor = await db.execute('''
                                      SELECT id, title, content, url, source, timestamp, author, category,
                                          sentiment, relevance_score, source_type, content_hash
                                      FROM articles
                                      WHERE timestamp BETWEEN ? AND ?
                                      ORDER BY timestamp DESC
                                      ''', (start_time, end_time))

            rows = await cursor.fetchall()

            # Build articles with metadata
            for row in rows:
                article_id = row[0]

                # Get metadata
                metadata_cursor = await db.execute('''
                                                   SELECT key, value FROM article_metadata WHERE article_id = ?
                                                   ''', (article_id,))
                metadata_rows = await metadata_cursor.fetchall()
                metadata = {key: value for key, value in metadata_rows}

                # Get tags
                tags_cursor = await db.execute('''
                                               SELECT tag FROM article_tags WHERE article_id = ?
                                               ''', (article_id,))
                tags_rows = await tags_cursor.fetchall()
                tags = [tag[0] for tag in tags_rows]

                # Create article object
                article = NewsArticle(
                    id=row[0],
                    title=row[1],
                    content=row[2],
                    url=row[3],
                    source=row[4],
                    timestamp=datetime.fromisoformat(row[5]) if isinstance(row[5], str) else row[5],
                    author=row[6],
                    category=row[7],
                    sentiment=row[8],
                    relevance_score=row[9],
                    source_type=SourceType(row[10]) if row[10] else SourceType.RSS,
                    tags=tags,
                    metadata=metadata
                )

                articles.append(article)

        return articles

    async def get_latest_timestamp(self, source: str = None) -> Optional[datetime]:
        """Get timestamp of most recent article"""
        async with self.get_connection() as db:
            if source:
                cursor = await db.execute('''
                                          SELECT MAX(timestamp) FROM articles WHERE source = ?
                                          ''', (source,))
            else:
                cursor = await db.execute('SELECT MAX(timestamp) FROM articles')

            result = await cursor.fetchone()

            if result and result[0]:
                timestamp_str = result[0]
                return datetime.fromisoformat(timestamp_str) if isinstance(timestamp_str, str) else timestamp_str

            return None

    async def get_database_stats(self, hours: int = 24) -> Dict[str, Any]:
        """Get comprehensive database statistics"""
        async with self.get_connection() as db:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)

            # Total articles in time range
            cursor = await db.execute('''
                                      SELECT COUNT(*) FROM articles WHERE timestamp BETWEEN ? AND ?
                                      ''', (start_time, end_time))
            total_articles = (await cursor.fetchone())[0]

            # Articles by source
            cursor = await db.execute('''
                                      SELECT source, COUNT(*) FROM articles
                                      WHERE timestamp BETWEEN ? AND ?
                                      GROUP BY source ORDER BY COUNT(*) DESC
                                      ''', (start_time, end_time))
            source_counts = dict(await cursor.fetchall())

            # Average relevance score
            cursor = await db.execute('''
                                      SELECT AVG(relevance_score) FROM articles
                                      WHERE timestamp BETWEEN ? AND ? AND relevance_score IS NOT NULL
                                      ''', (start_time, end_time))
            avg_relevance = (await cursor.fetchone())[0] or 0

            # Database size info
            cursor = await db.execute("SELECT COUNT(*) FROM articles")
            total_all_articles = (await cursor.fetchone())[0]

            return {
                'total_articles_period': total_articles,
                'total_articles_all': total_all_articles,
                'time_range_hours': hours,
                'source_counts': source_counts,
                'avg_relevance_score': round(avg_relevance, 2),
                'unique_sources': len(source_counts)
            }