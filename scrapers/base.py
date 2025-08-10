# File: src/scrapers/base.py
"""Base scraper classes with common functionality"""
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Dict, Any, Set

import feedparser
from bs4 import BeautifulSoup

from core.models import NewsArticle, SourceType
from utils.http_client import AsyncHTTPClient
from utils.logger import get_logger
from utils.rate_limiter import AdaptiveRateLimiter

logger = get_logger(__name__)

class BaseAsyncScraper(ABC):
    """Base class for all scrapers with common functionality"""

    def __init__(self, config: Dict[str, Any], http_client: AsyncHTTPClient, global_config: Dict[str, Any] = None):
        self.config = config
        self.global_config = global_config or {}  # Add global config
        self.http_client = http_client
        self.name = config.get('name', 'Unknown')
        self.source_type = SourceType(config.get('source_type', 'rss'))

        # Initialize rate limiter
        rate_limit = config.get('rate_limit_seconds', 1.0)
        self.rate_limiter = AdaptiveRateLimiter(
            initial_rate=1.0 / rate_limit if rate_limit > 0 else 1.0,
            min_rate=0.1,
            max_rate=5.0
        )

        # Tracking
        self.last_scrape_time: Optional[datetime] = None
        self.consecutive_failures = 0
        self.max_failures = config.get('max_failures', 5)

        # Content filtering - FIXED to use global config
        self.crypto_keywords = self._load_crypto_keywords()
        self.min_content_length = self.global_config.get('min_content_length', 50)
        self.max_content_length = self.global_config.get('max_content_length', 50000)

        # Debug log to verify keywords are loaded
        logger.info(f"🔑 {self.name}: Loaded {len(self.crypto_keywords)} crypto keywords: {list(self.crypto_keywords)[:10]}...")

    def _load_crypto_keywords(self) -> Set[str]:
        """Load crypto keywords for relevance filtering - FIXED"""
        # Try to get keywords from global config first, then source config
        keywords = self.global_config.get('crypto_keywords', []) or self.config.get('crypto_keywords', [])
        keyword_set = {keyword.lower() for keyword in keywords}

        logger.info(f"🔍 Loading keywords for {self.name}: found {len(keyword_set)} keywords")
        if not keyword_set:
            logger.warning(f"⚠️  No crypto keywords found for {self.name}! Check config file.")

        return keyword_set

    @abstractmethod
    async def scrape_articles(self, max_articles: int = 100) -> List[NewsArticle]:
        """Scrape articles from this source"""
        pass

    async def validate_source(self) -> bool:
        """Check if source is accessible and responding"""
        try:
            test_url = self.config.get('base_url') or self.config.get('rss_url')
            if not test_url:
                return False

            content = await self.http_client.get_with_retry(test_url)
            return content is not None
        except Exception as e:
            logger.error(f"Source validation failed for {self.name}: {e}")
            return False

    def is_crypto_relevant(self, title: str, content: str) -> bool:
        """Check if article is crypto-relevant with debug logging"""
        text = f"{title} {content}".lower()

        # Count keyword matches
        matches = 0
        matched_keywords = []
        for keyword in self.crypto_keywords:
            if keyword in text:
                matches += 1
                matched_keywords.append(keyword)

        min_matches = self.config.get('min_keyword_matches', 1)
        is_relevant = matches >= min_matches

        # Debug logging - always log the first few articles to see what we're getting
        if not is_relevant:
            logger.warning(f"❌ FILTERED OUT - Title: '{title[:80]}' - "
                           f"Matches: {matches}/{min_matches} - "
                           f"Keywords found: {matched_keywords} - "
                           f"Content preview: '{content[:100] if content else 'No content'}'")
        else:
            logger.info(f"✅ ACCEPTED - Title: '{title[:80]}' - "
                        f"Matches: {matches} - Keywords: {matched_keywords}")

        return is_relevant


    def is_valid_content(self, article: NewsArticle) -> bool:
        """Validate article content with debug logging"""

        # Check content length
        content_length = len(article.content) if article.content else 0
        min_length = self.min_content_length
        max_length = self.max_content_length

        if content_length < min_length:
            logger.warning(f"❌ CONTENT TOO SHORT - Title: '{article.title[:50]}...' - "
                           f"Length: {content_length} < {min_length} - "
                           f"Content: '{article.content[:100] if article.content else 'EMPTY'}'")
            return False

        if content_length > max_length:
            logger.warning(f"❌ CONTENT TOO LONG - Title: '{article.title[:50]}...' - "
                           f"Length: {content_length} > {max_length}")
            return False

        # Check title
        title_length = len(article.title.strip()) if article.title else 0
        if title_length < 10:
            logger.warning(f"❌ TITLE TOO SHORT - Title: '{article.title}' - Length: {title_length}")
            return False

        # Check URL
        if not article.url or not article.url.startswith(('http://', 'https://')):
            logger.warning(f"❌ INVALID URL - Title: '{article.title[:50]}...' - URL: '{article.url}'")
            return False

        logger.info(f"✅ CONTENT VALID - Title: '{article.title[:50]}...' - Length: {content_length}")
        return True

    def clean_content(self, content: str) -> str:
        """Clean and normalize content"""
        if not content:
            return ""

        # Remove HTML tags
        soup = BeautifulSoup(content, 'html.parser')
        text = soup.get_text()

        # Normalize whitespace
        import re
        text = re.sub(r'\s+', ' ', text).strip()

        # Remove common unwanted patterns
        unwanted_patterns = [
            r'Subscribe to.*?newsletter',
            r'Follow us on.*?social media',
            r'Click here to.*?',
            r'Read more.*?',
            r'© \d{4}.*?'
        ]

        for pattern in unwanted_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)

        return text.strip()

class RSSAsyncScraper(BaseAsyncScraper):
    """RSS-specific scraper implementation"""

    def __init__(self, config: Dict[str, Any], http_client: AsyncHTTPClient, global_config: Dict[str, Any] = None):
        super().__init__(config, http_client, global_config)  # Pass global_config to parent
        self.rss_url = config.get('rss_url')
        if not self.rss_url:
            raise ValueError(f"RSS URL required for {self.name}")

    async def scrape_articles(self, max_articles: int = 100) -> List[NewsArticle]:
        """Scrape articles from RSS feed"""
        if not self.rss_url:
            return []

        try:
            await self.rate_limiter.acquire()
            rss_content = await self.http_client.get_with_retry(self.rss_url)

            if not rss_content:
                await self.rate_limiter.record_failure()
                return []

            await self.rate_limiter.record_success()
            return await self._parse_rss_content(rss_content, max_articles)

        except Exception as e:
            await self.rate_limiter.record_failure()
            logger.error(f"RSS scraping failed for {self.name}: {e}")
            return []

    async def _parse_rss_content(self, rss_content: str, max_articles: int) -> List[NewsArticle]:
        """Parse RSS content and extract articles"""
        articles = []

        try:
            feed = feedparser.parse(rss_content)

            logger.info(f"📥 RSS {self.name}: Found {len(feed.entries)} total entries in feed")

            for i, entry in enumerate(feed.entries[:max_articles]):
                try:
                    article = await self._create_article_from_rss_entry(entry)
                    if article:
                        logger.info(f"📄 Entry {i+1}: Created article '{article.title[:50]}...'")

                        # Check crypto relevance with debug output
                        if self.is_crypto_relevant(article.title, article.content):
                            if self.is_valid_content(article):
                                articles.append(article)
                                logger.info(f"✅ Entry {i+1}: ADDED to results")
                            else:
                                logger.warning(f"❌ Entry {i+1}: REJECTED - Failed content validation")
                        # Note: crypto relevance logging happens inside is_crypto_relevant
                    else:
                        logger.warning(f"❌ Entry {i+1}: Failed to create article from RSS entry")

                except Exception as e:
                    logger.warning(f"❌ Entry {i+1}: Error processing RSS entry: {e}")
                    continue

            logger.info(f"📊 RSS {self.name}: Final result - {len(articles)} relevant articles from {len(feed.entries)} total entries")

        except Exception as e:
            logger.error(f"❌ RSS parsing failed for {self.name}: {e}")

        return articles

    async def _create_article_from_rss_entry(self, entry) -> Optional[NewsArticle]:
        """Create NewsArticle from RSS entry"""
        try:
            # Extract basic data
            title = getattr(entry, 'title', '').strip()
            link = getattr(entry, 'link', '').strip()

            if not title or not link:
                return None

            # Extract content
            content = ""
            if hasattr(entry, 'content') and entry.content:
                content = entry.content[0].value
            elif hasattr(entry, 'summary'):
                content = entry.summary
            elif hasattr(entry, 'description'):
                content = entry.description

            # Clean content
            content = self.clean_content(content)

            # Parse timestamp
            timestamp = datetime.now()
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                try:
                    timestamp = datetime(*entry.published_parsed[:6])
                except:
                    pass

            # Create article
            article = NewsArticle(
                id="",  # Will be generated
                title=title,
                content=content,
                url=link,
                source=self.name,
                timestamp=timestamp,
                author=getattr(entry, 'author', None),
                source_type=SourceType.RSS,
                metadata={
                    'rss_id': getattr(entry, 'id', ''),
                    'rss_tags': getattr(entry, 'tags', [])
                }
            )

            return article

        except Exception as e:
            logger.error(f"Failed to create article from RSS entry: {e}")
            return None