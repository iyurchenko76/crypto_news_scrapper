# File: src/scrapers/telegram_api_scraper.py
"""Telegram API scraper for crypto news channels"""

import asyncio
import re
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup

from core.models import NewsArticle, SourceType
from scrapers.base import BaseAsyncScraper
from utils.http_client import AsyncHTTPClient
from utils.logger import get_logger

logger = get_logger(__name__)

# Import Telethon (optional dependency)
try:
    from telethon import TelegramClient
    from telethon.errors import SessionPasswordNeededError #, PhoneCodeRequiredError
    from telethon.tl.types import MessageMediaWebPage
except ImportError:
    logger.error("❌ Telethon library not installed. Install with: pip install telethon")
    raise

_SESSION_LOCKS: Dict[str, asyncio.Lock] = {}
_SESSION_LOCKS_GUARD = asyncio.Lock()

class TelegramAPIScraper(BaseAsyncScraper):
    """Telegram API scraper using Telethon library"""

    def __init__(self, config: Dict[str, Any], http_client: AsyncHTTPClient, global_config: Dict[str, Any] = None):
        super().__init__(config, http_client, global_config)
        self.source_type = SourceType.TELEGRAM_API
        self.channel_username = config.get('channel_username', '').replace('@', '').replace('t.me/', '')
        self.max_messages = config.get('max_messages', 50)
        self.min_message_length = config.get('min_message_length', 20)

        # API settings
        self.fetch_article_content = config.get('fetch_article_content', True)
        self.max_fetch_attempts = config.get('max_fetch_attempts', 10)
        self.fetch_timeout = config.get('fetch_timeout', 15)

        # Telegram API credentials (from config or environment)
        self.api_id = config.get('telegram_api_id') or global_config.get('telegram_api_id')
        self.api_hash = config.get('telegram_api_hash') or global_config.get('telegram_api_hash')
        self.phone_number = config.get('telegram_phone') or global_config.get('telegram_phone')

        # Session file for persistent login
        self.session_name = config.get('session_name', 'crypto_scraper_session')

        if not self.channel_username:
            raise ValueError(f"Telegram channel username required for {self.name}")

        if not (self.api_id and self.api_hash):
            logger.warning(f"⚠️  Telegram API credentials not provided for {self.name}. Will attempt web scraping fallback.")

    async def scrape_articles(self, max_articles: int = 100, hours_back: int = None) -> List[NewsArticle]:
        """Scrape messages from Telegram channel using API"""
        all_articles = []

        # Convert hours_back to appropriate time filter
        days_back = self._convert_hours_to_days(hours_back)

        logger.info(f"📱 {self.name}: Starting Telegram API scraping")
        logger.info(f"📅 Time range: {days_back} days back ({hours_back or 'default'} hours)")
        logger.info(f"🔗 Channel: @{self.channel_username}")

        try:
            # Try Telegram API first (most reliable)
            if self.api_id and self.api_hash:
                articles = await self._scrape_with_telegram_api(days_back, max_articles)
                if articles:
                    all_articles.extend(articles)
                    logger.info(f"✅ Telegram API: {len(articles)} articles")
                else:
                    logger.warning(f"⚠️  Telegram API returned no results, trying fallback methods")

            # Fallback to web scraping if API fails or not configured
            if not all_articles:
                logger.info(f"🌐 Falling back to web scraping methods...")
                articles = await self._scrape_telegram_web_fallback(days_back, max_articles)
                all_articles.extend(articles)

        except Exception as e:
            logger.error(f"❌ Telegram scraping error for @{self.channel_username}: {e}")

        # Filter by time and crypto relevance
        filtered_articles = []
        cutoff_time = datetime.now() - timedelta(days=days_back)

        for article in all_articles:
            if (article.timestamp >= cutoff_time and
                    self.is_crypto_relevant(article.title, article.content) and
                    self.is_valid_content(article)):
                filtered_articles.append(article)

                if len(filtered_articles) >= max_articles:
                    break

        logger.info(f"🎯 {self.name}: Final result - {len(filtered_articles)} relevant messages")
        return filtered_articles

    @classmethod
    async def _get_session_lock(cls, session_name: str) -> asyncio.Lock:
        async with _SESSION_LOCKS_GUARD:
            lock = _SESSION_LOCKS.get(session_name)
            if lock is None:
                lock = asyncio.Lock()
                _SESSION_LOCKS[session_name] = lock
            return lock

    async def _scrape_with_telegram_api(self, days_back: int, max_articles: int) -> List[NewsArticle]:
        """Scrape using Telegram API (Telethon)"""
        articles = []

        try:
            session_lock = await self._get_session_lock(self.session_name)
            client = None
            async with session_lock:
                logger.info(f"🔑 Connecting to Telegram API...")


                # Create client
                client = TelegramClient(self.session_name, self.api_id, self.api_hash)

                # Connect
                await client.connect()

                # Check if we need to authenticate
                if not await client.is_user_authorized():
                    logger.info(f"📱 Telegram authentication required...")
                    if self.phone_number:
                        await client.send_code_request(self.phone_number)
                        logger.warning(f"⚠️  Authentication code sent to {self.phone_number}. Please run authentication setup separately.")
                        await client.disconnect()
                        return articles
                    else:
                        logger.error(f"❌ Phone number required for Telegram authentication")
                        await client.disconnect()
                        return articles

                logger.info(f"✅ Connected to Telegram API")

                # Get the channel entity
                try:
                    entity = await client.get_entity(self.channel_username)
                    logger.info(f"📋 Found channel: {entity.title}")
                except Exception as e:
                    logger.error(f"❌ Could not find channel @{self.channel_username}: {e}")
                    await client.disconnect()
                    return articles

                # Calculate time limit
                time_limit = datetime.now() - timedelta(days=days_back)

                # Fetch messages
                logger.info(f"📨 Fetching messages from @{self.channel_username}...")

                message_count = 0
                async for message in client.iter_messages(entity, limit=self.max_messages):
                    message_count += 1

                    # Check if message is within time range
                    if message.date.replace(tzinfo=None) < time_limit:
                        logger.debug(f"📅 Message {message_count} too old ({message.date}), stopping")
                        break

                    try:
                        article = await self._create_article_from_api_message(message, message_count)
                        if article:
                            articles.append(article)
                            logger.debug(f"✅ Message {message_count}: Created article - {article.title[:50]}...")
                        else:
                            logger.debug(f"❌ Message {message_count}: Skipped (not suitable)")

                    except Exception as e:
                        logger.warning(f"❌ Message {message_count}: Error processing: {e}")
                        continue

                    # Small delay to be respectful
                    await asyncio.sleep(0.1)

                await client.disconnect()
                logger.info(f"📱 Telegram API: {len(articles)} articles extracted from {message_count} messages")

        except Exception as e:
            logger.error(f"❌ Telegram API error: {e}")
            try:
                await client.disconnect()
            except:
                pass

        return articles

    async def _create_article_from_api_message(self, message, message_num: int) -> Optional[NewsArticle]:
        """Create article from Telegram API message object"""
        try:
            # Extract message text
            message_text = message.text or ""

            if not message_text:
                logger.debug(f"📱 Message {message_num}: No text content")
                return None

            # Extract URLs from message
            urls_in_message = re.findall(r'https?://[^\s\)]+', message_text)

            # Also check for webpage media (link previews)
            webpage_url = None
            if hasattr(message, 'media') and message.media:
                try:
                    from telethon.tl.types import MessageMediaWebPage
                    if isinstance(message.media, MessageMediaWebPage):
                        webpage_url = message.media.webpage.url
                        if webpage_url and webpage_url not in urls_in_message:
                            urls_in_message.append(webpage_url)
                except:
                    pass

            # Filter news URLs
            news_urls = [url for url in urls_in_message
                         if not any(skip in url.lower() for skip in [
                    't.me', 'twitter.com', 'x.com', 'instagram.com',
                    'facebook.com', 'linkedin.com', 'youtube.com'
                ])]

            # Determine if this is a link-only message
            is_link_only_message = (
                    len(message_text.strip()) < self.min_message_length and
                    news_urls and
                    len(news_urls) > 0
            )

            # Handle link-only messages by fetching article content
            if is_link_only_message and news_urls and self.fetch_article_content:
                logger.info(f"📱 Message {message_num}: Link-only message detected, fetching article content...")

                article = await self._create_article_from_link(
                    news_urls[0], message_text, message.date.replace(tzinfo=None),
                    message_num, urls_in_message
                )

                if article:
                    return article
                else:
                    logger.debug(f"📱 Message {message_num}: Failed to fetch article content, using original message")

            # Handle messages with substantial content or fallback
            if len(message_text) < self.min_message_length and not news_urls:
                logger.debug(f"📱 Message {message_num}: Text too short ({len(message_text)} chars) and no fetchable links")
                return None

            # Create title from first part of message
            title = message_text[:100].strip()
            if len(message_text) > 100:
                title += "..."

            # Use the main news URL if available, otherwise use Telegram message link
            main_url = news_urls[0] if news_urls else f"https://t.me/{self.channel_username}/{message.id}"

            # Get author info
            author = f"@{self.channel_username}"
            if hasattr(message, 'forward') and message.forward:
                if hasattr(message.forward, 'from_name') and message.forward.from_name:
                    author = f"{author} (via {message.forward.from_name})"

            article = NewsArticle(
                id="",  # Will be generated
                title=title,
                content=self.clean_content(message_text),
                url=main_url,
                source=self.name,
                timestamp=message.date.replace(tzinfo=None),
                author=author,
                source_type=SourceType.TELEGRAM_API,
                metadata={
                    'telegram_channel': self.channel_username,
                    'message_id': message.id,
                    'message_number': message_num,
                    'original_text_length': len(message_text),
                    'urls_in_message': urls_in_message,
                    'news_urls_found': news_urls,
                    'content_source': 'telegram_api',
                    'has_media': bool(message.media),
                    'is_forwarded': bool(hasattr(message, 'forward') and message.forward),
                    'views': getattr(message, 'views', 0)
                }
            )

            return article

        except Exception as e:
            logger.error(f"❌ Failed to create article from API message: {e}")
            return None

    async def _create_article_from_link(self, url: str, original_message: str, timestamp: datetime,
                                        message_num: int, all_urls: List[str]) -> Optional[NewsArticle]:
        """Fetch and create article from a news URL (same as web scraper)"""
        try:
            logger.debug(f"🔗 Fetching article content from: {url[:60]}...")

            await asyncio.sleep(0.5)  # Be respectful

            response_text = await self.http_client.get_with_retry(url, timeout=self.fetch_timeout)

            if not response_text:
                return None

            soup = BeautifulSoup(response_text, 'html.parser')

            # Extract article details
            title = self._extract_article_title(soup, url)
            content = self._extract_article_content(soup, url)
            author = self._extract_article_author(soup) or f"@{self.channel_username}"

            if not title or len(title.strip()) < 10:
                return None

            if not content or len(content.strip()) < self.min_message_length:
                return None

            content = self.clean_content(content)

            article = NewsArticle(
                id="",
                title=title.strip(),
                content=content,
                url=url,
                source=self.name,
                timestamp=timestamp,
                author=author,
                source_type=SourceType.TELEGRAM_API,
                metadata={
                    'telegram_channel': self.channel_username,
                    'message_number': message_num,
                    'original_message': original_message,
                    'content_source': 'fetched_article_api',
                    'original_article_url': url,
                    'all_urls_in_message': all_urls,
                    'content_length': len(content),
                    'extraction_success': True
                }
            )

            logger.info(f"✅ Successfully fetched article: {title[:50]}... ({len(content)} chars)")
            return article

        except Exception as e:
            logger.warning(f"❌ Failed to fetch article from {url}: {e}")
            return None

    async def _scrape_telegram_web_fallback(self, days_back: int, max_articles: int) -> List[NewsArticle]:
        """Fallback to web scraping methods when API is not available"""
        articles = []

        # Try alternative web methods
        try:
            # Method 1: Try RSS bridges
            bridge_articles = await self._try_telegram_rss_bridge()
            articles.extend(bridge_articles)

            # Method 2: Try web preview with different user agents
            if not articles:
                preview_articles = await self._try_web_preview_with_rotation()
                articles.extend(preview_articles)

        except Exception as e:
            logger.warning(f"❌ Web fallback methods failed: {e}")

        return articles[:max_articles]

    async def _try_telegram_rss_bridge(self) -> List[NewsArticle]:
        """Try RSS bridge services"""
        articles = []

        bridge_services = [
            f"https://rsshub.app/telegram/channel/{self.channel_username}",
            f"https://rss-bridge.org/bridge01/?action=display&bridge=Telegram&username={self.channel_username}&format=Atom",
        ]

        for bridge_url in bridge_services:
            try:
                logger.debug(f"🌉 Trying RSS bridge: {bridge_url}")

                response_text = await self.http_client.get_with_retry(bridge_url, timeout=10)

                if response_text:
                    import feedparser
                    feed = feedparser.parse(response_text)

                    for entry in feed.entries[:10]:
                        try:
                            title = getattr(entry, 'title', '')
                            content = getattr(entry, 'summary', '') or getattr(entry, 'description', '')
                            link = getattr(entry, 'link', f"https://t.me/{self.channel_username}")

                            if len(content) >= self.min_message_length:
                                timestamp = datetime.now()
                                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                                    timestamp = datetime(*entry.published_parsed[:6])

                                article = NewsArticle(
                                    id="",
                                    title=title,
                                    content=self.clean_content(content),
                                    url=link,
                                    source=self.name,
                                    timestamp=timestamp,
                                    author=f"@{self.channel_username}",
                                    source_type=SourceType.TELEGRAM_API,
                                    metadata={
                                        'telegram_channel': self.channel_username,
                                        'extraction_method': 'rss_bridge',
                                        'bridge_service': bridge_url
                                    }
                                )

                                articles.append(article)
                        except Exception:
                            continue

                    if articles:
                        logger.info(f"🌉 RSS bridge success: {len(articles)} articles")
                        break

            except Exception as e:
                logger.debug(f"RSS bridge failed: {e}")
                continue

        return articles

    async def _try_web_preview_with_rotation(self) -> List[NewsArticle]:
        """Try web preview with different user agents"""
        articles = []

        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]

        urls_to_try = [
            f"https://t.me/s/{self.channel_username}",
            f"https://telegram.me/s/{self.channel_username}",
        ]

        for user_agent in user_agents:
            for url in urls_to_try:
                try:
                    # Temporarily change user agent
                    original_headers = self.http_client.session.headers
                    self.http_client.session.headers.update({'User-Agent': user_agent})

                    response_text = await self.http_client.get_with_retry(url, timeout=10)

                    # Restore original headers
                    self.http_client.session.headers = original_headers

                    if response_text and "tgme_widget_message" in response_text:
                        # Parse messages using the same logic as web scraper
                        soup = BeautifulSoup(response_text, 'html.parser')
                        messages = soup.select('.tgme_widget_message')

                        for i, message in enumerate(messages[:10]):
                            # Use simplified extraction for fallback
                            text = message.get_text(strip=True)
                            if len(text) >= self.min_message_length:
                                article = NewsArticle(
                                    id="",
                                    title=text[:100] + ("..." if len(text) > 100 else ""),
                                    content=self.clean_content(text),
                                    url=f"https://t.me/{self.channel_username}",
                                    source=self.name,
                                    timestamp=datetime.now(),
                                    author=f"@{self.channel_username}",
                                    source_type=SourceType.TELEGRAM_API,
                                    metadata={
                                        'telegram_channel': self.channel_username,
                                        'extraction_method': 'web_preview_rotation',
                                        'user_agent': user_agent
                                    }
                                )
                                articles.append(article)

                        if articles:
                            logger.info(f"🔄 Web preview success: {len(articles)} articles")
                            return articles

                except Exception as e:
                    logger.debug(f"Web preview attempt failed: {e}")
                    continue

        return articles

    def _convert_hours_to_days(self, hours_back: int = None) -> int:
        """Convert hours_back to days_back"""
        if hours_back is None:
            return 3

        if hours_back <= 24:
            return 1
        elif hours_back <= 72:
            return 3
        elif hours_back <= 168:
            return max(3, hours_back // 24)
        else:
            return min(14, hours_back // 24)

    # Include the same content extraction methods as the web scraper
    def _extract_article_title(self, soup: BeautifulSoup, url: str) -> str:
        """Extract article title from webpage"""
        title_selectors = [
            'h1', '.headline', '.entry-title', '.post-title',
            '[data-testid="headline"]', 'title', '.article-title'
        ]

        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                title = title_elem.get_text(strip=True)
                if 10 < len(title) < 200:
                    return title

        meta_title = soup.select_one('meta[property="og:title"]')
        if meta_title:
            return meta_title.get('content', '').strip()

        return ""

    def _extract_article_content(self, soup: BeautifulSoup, url: str) -> str:
        """Extract article content from webpage"""
        content_selectors = [
            'article', '.article-content', '.entry-content', '.post-content',
            '.story-body', '.article-body', '[data-testid="article-body"]',
            '.content', 'main'
        ]

        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                for unwanted in content_elem.select('script, style, nav, header, footer, .ad'):
                    unwanted.decompose()

                content = content_elem.get_text(strip=True)
                if len(content) > 100:
                    return content

        return ""

    def _extract_article_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract article author from webpage"""
        author_selectors = [
            '.author', '.byline', '.writer', '[rel="author"]',
            '.article-author', '.post-author'
        ]

        for selector in author_selectors:
            author_elem = soup.select_one(selector)
            if author_elem:
                author = author_elem.get_text(strip=True)
                if 2 < len(author) < 50:
                    return author

        return None

    def is_valid_content(self, article: NewsArticle) -> bool:
        """Custom validation for Telegram messages"""
        title_length = len(article.title.strip()) if article.title else 0
        if title_length < 10:
            return False

        if not article.url:
            return False

        content_length = len(article.content) if article.content else 0
        if content_length < self.min_message_length:
            return False

        return True