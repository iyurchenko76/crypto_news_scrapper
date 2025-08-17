# File: src/scrapers/telegram_scraper.py
"""Telegram channel scraper for crypto news"""

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

class TelegramWebScraper(BaseAsyncScraper):
    """Telegram channel scraper for crypto news"""

    def __init__(self, config: Dict[str, Any], http_client: AsyncHTTPClient, global_config: Dict[str, Any] = None):
        super().__init__(config, http_client, global_config)
        self.source_type = SourceType.TELEGRAM_WEB
        self.channel_username = config.get('channel_username', '').replace('@', '').replace('t.me/', '')
        self.max_messages = config.get('max_messages', 50)
        self.min_message_length = config.get('min_message_length', 20)

        # New options for handling link-only messages
        self.fetch_article_content = config.get('fetch_article_content', True)  # Fetch content from links
        self.max_fetch_attempts = config.get('max_fetch_attempts', 10)  # Limit fetching to avoid overload
        self.fetch_timeout = config.get('fetch_timeout', 15)  # Timeout for article fetching

        if not self.channel_username:
            raise ValueError(f"Telegram channel username required for {self.name}")

    async def scrape_articles(self, max_articles: int = 100, hours_back: int = None) -> List[NewsArticle]:
        """Scrape messages from Telegram channel"""
        all_articles = []

        # Convert hours_back to appropriate time filter
        days_back = self._convert_hours_to_days(hours_back)

        logger.info(f"📱 {self.name}: Starting Telegram channel scraping")
        logger.info(f"📅 Time range: {days_back} days back ({hours_back or 'default'} hours)")
        logger.info(f"🔗 Channel: @{self.channel_username}")

        try:
            await self.rate_limiter.acquire()

            # Method 1: Try Telegram web preview (most reliable)
            articles = await self._scrape_telegram_web_preview(days_back, max_articles)

            if not articles:
                # Method 2: Try alternative approaches if web preview fails
                logger.info(f"📱 Trying alternative Telegram scraping methods...")
                articles = await self._scrape_telegram_alternative_methods(days_back, max_articles)

            await self.rate_limiter.record_success()
            all_articles.extend(articles)

        except Exception as e:
            await self.rate_limiter.record_failure()
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

    def _convert_hours_to_days(self, hours_back: int = None) -> int:
        """Convert hours_back to days_back"""
        if hours_back is None:
            return 3  # Default to 3 days for Telegram

        if hours_back <= 24:
            return 1
        elif hours_back <= 72:
            return 3
        elif hours_back <= 168:  # 1 week
            return max(3, hours_back // 24)
        else:
            return min(14, hours_back // 24)  # Cap at 2 weeks

    async def _scrape_telegram_web_preview(self, days_back: int, max_articles: int) -> List[NewsArticle]:
        """Scrape using Telegram's web preview (t.me/channel)"""
        articles = []

        try:
            # Telegram web preview URL
            url = f"https://t.me/s/{self.channel_username}"

            logger.info(f"🌐 Fetching Telegram web preview: {url}")

            response_text = await self.http_client.get_with_retry(url)

            if not response_text:
                logger.warning(f"❌ No response from Telegram web preview")
                return articles

            # Check if channel exists and is accessible
            if "channel doesn't exist" in response_text.lower() or "private" in response_text.lower():
                logger.error(f"❌ Channel @{self.channel_username} doesn't exist or is private")
                return articles

            soup = BeautifulSoup(response_text, 'html.parser')

            # Find message containers
            message_selectors = [
                '.tgme_widget_message',     # Main message container
                '.tgme_widget_message_wrap', # Message wrapper
                '[data-post]'               # Message with post data
            ]

            messages = []
            for selector in message_selectors:
                found_messages = soup.select(selector)
                if found_messages:
                    messages = found_messages
                    logger.info(f"📄 Found {len(messages)} messages using selector: {selector}")
                    break

            if not messages:
                logger.warning(f"❌ No messages found in Telegram preview")
                return articles

            # Process each message
            for i, message in enumerate(messages[:max_articles]):
                try:
                    article = await self._extract_article_from_message(message, i+1)
                    if article:
                        articles.append(article)
                        logger.debug(f"✅ Message {i+1}: Created article - {article.title[:50]}...")
                    else:
                        logger.debug(f"❌ Message {i+1}: Skipped (not suitable)")

                except Exception as e:
                    logger.warning(f"❌ Message {i+1}: Error processing: {e}")
                    continue

            logger.info(f"📱 Telegram web preview: {len(articles)} articles extracted")

        except Exception as e:
            logger.error(f"❌ Telegram web preview error: {e}")

        return articles

    async def _extract_article_from_message(self, message_elem, message_num: int) -> Optional[NewsArticle]:
        """Extract article data from Telegram message element"""
        try:
            # Extract message text
            text_selectors = [
                '.tgme_widget_message_text',
                '.tgme_widget_message_bubble_body',
                '.message_text',
                '.js-message_text'
            ]

            message_text = ""
            for selector in text_selectors:
                text_elem = message_elem.select_one(selector)
                if text_elem:
                    message_text = text_elem.get_text(strip=True)
                    break

            if not message_text:
                logger.debug(f"📱 Message {message_num}: No text found")
                return None

            # Extract timestamp
            timestamp = datetime.now()
            time_selectors = [
                '.tgme_widget_message_date time',
                '.tgme_widget_message_date',
                'time[datetime]',
                '.message_date'
            ]

            for selector in time_selectors:
                time_elem = message_elem.select_one(selector)
                if time_elem:
                    datetime_attr = time_elem.get('datetime') or time_elem.get('title')
                    if datetime_attr:
                        try:
                            timestamp = datetime.fromisoformat(datetime_attr.replace('Z', '+00:00')).replace(tzinfo=None)
                            break
                        except:
                            pass

            # Extract URLs from message
            urls_in_message = re.findall(r'https?://[^\s\)]+', message_text)

            # Also check for links in HTML elements
            link_elements = message_elem.select('a[href]')
            for link_elem in link_elements:
                href = link_elem.get('href')
                if href and href.startswith('http') and href not in urls_in_message:
                    urls_in_message.append(href)

            # Filter out non-news URLs
            news_urls = [url for url in urls_in_message
                         if not any(skip in url.lower() for skip in [
                    't.me', 'twitter.com', 'x.com', 'instagram.com',
                    'facebook.com', 'linkedin.com', 'youtube.com',
                    'tiktok.com', 'discord.gg'
                ])]

            # Determine if this is a link-only message or has substantial content
            is_link_only_message = (
                    len(message_text.strip()) < self.min_message_length and
                    news_urls and
                    len(news_urls) > 0
            )

            # Handle link-only messages by fetching article content
            if is_link_only_message and news_urls and self.fetch_article_content:
                logger.info(f"📱 Message {message_num}: Link-only message detected, fetching article content...")

                article = await self._create_article_from_link(
                    news_urls[0], message_text, timestamp, message_num, urls_in_message
                )

                if article:
                    return article
                else:
                    logger.debug(f"📱 Message {message_num}: Failed to fetch article content, using original message")
                    # Fall through to use original message text

            # Handle messages with substantial content
            if len(message_text) < self.min_message_length:
                logger.debug(f"📱 Message {message_num}: Text too short ({len(message_text)} chars) and no fetchable links")
                return None

            # Create title from first part of message
            title = message_text[:100].strip()
            if len(message_text) > 100:
                title += "..."

            # Use the main news URL if available, otherwise use Telegram link
            main_url = news_urls[0] if news_urls else f"https://t.me/{self.channel_username}/{message_num}"

            # Extract author/channel info
            author = f"@{self.channel_username}"

            # Look for forwarded from info
            forward_elem = message_elem.select_one('.tgme_widget_message_forward_from')
            if forward_elem:
                forward_text = forward_elem.get_text(strip=True)
                if forward_text:
                    author = f"{author} (via {forward_text})"

            article = NewsArticle(
                id="",  # Will be generated
                title=title,
                content=self.clean_content(message_text),
                url=main_url,
                source=self.name,
                timestamp=timestamp,
                author=author,
                source_type=SourceType.TELEGRAM_WEB,
                metadata={
                    'telegram_channel': self.channel_username,
                    'message_number': message_num,
                    'original_text_length': len(message_text),
                    'urls_in_message': urls_in_message,
                    'news_urls_found': news_urls,
                    'content_source': 'telegram_message',
                    'has_media': bool(message_elem.select_one('.tgme_widget_message_photo, .tgme_widget_message_video')),
                    'is_forwarded': bool(message_elem.select_one('.tgme_widget_message_forward_from'))
                }
            )

            return article

        except Exception as e:
            logger.error(f"❌ Failed to extract article from Telegram message: {e}")
            return None

    async def _create_article_from_link(self, url: str, original_message: str, timestamp: datetime,
                                        message_num: int, all_urls: List[str]) -> Optional[NewsArticle]:
        """Fetch and create article from a news URL found in Telegram message"""
        try:
            logger.debug(f"🔗 Fetching article content from: {url[:60]}...")

            # Add a small delay to be respectful
            await asyncio.sleep(0.5)

            # Fetch the article page
            response_text = await self.http_client.get_with_retry(url, timeout=self.fetch_timeout)

            if not response_text:
                logger.debug(f"❌ No response from URL: {url}")
                return None

            soup = BeautifulSoup(response_text, 'html.parser')

            # Extract article title
            title = self._extract_article_title(soup, url)

            # Extract article content
            content = self._extract_article_content(soup, url)

            # Extract author if available
            author = self._extract_article_author(soup) or f"@{self.channel_username}"

            # Validate extracted content
            if not title or len(title.strip()) < 10:
                logger.debug(f"❌ Invalid title extracted from: {url}")
                return None

            if not content or len(content.strip()) < self.min_message_length:
                logger.debug(f"❌ Insufficient content extracted from: {url} ({len(content)} chars)")
                return None

            # Clean up content
            content = self.clean_content(content)

            # Create enhanced article with fetched content
            article = NewsArticle(
                id="",  # Will be generated
                title=title.strip(),
                content=content,
                url=url,
                source=self.name,
                timestamp=timestamp,
                author=author,
                source_type=SourceType.TELEGRAM_WEB,
                metadata={
                    'telegram_channel': self.channel_username,
                    'message_number': message_num,
                    'original_message': original_message,
                    'content_source': 'fetched_article',
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

    def _extract_article_title(self, soup: BeautifulSoup, url: str) -> str:
        """Extract article title from webpage"""
        title_selectors = [
            'h1',                          # Main heading
            '.headline',                   # News sites
            '.entry-title',               # WordPress
            '.post-title',                # Blog posts
            '[data-testid="headline"]',   # Modern sites
            'title',                      # Fallback to page title
            '.article-title',             # News articles
            '.story-headline',            # News stories
            'h1.title',                   # Specific title class
        ]

        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                title = title_elem.get_text(strip=True)
                if len(title) > 10 and len(title) < 200:  # Reasonable title length
                    return title

        # Fallback: try to extract from URL or meta tags
        meta_title = soup.select_one('meta[property="og:title"]')
        if meta_title:
            return meta_title.get('content', '').strip()

        return ""

    def _extract_article_content(self, soup: BeautifulSoup, url: str) -> str:
        """Extract article content from webpage"""
        content_selectors = [
            'article',                     # Semantic article
            '.article-content',           # Common article class
            '.entry-content',             # WordPress content
            '.post-content',              # Blog content
            '.story-body',                # News story body
            '.article-body',              # Article body
            '[data-testid="article-body"]', # Modern sites
            '.content',                   # Generic content
            'main',                       # Main content area
            '.article-text',              # Article text
            '.story-content',             # Story content
        ]

        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                # Remove unwanted elements
                for unwanted in content_elem.select('script, style, nav, header, footer, .advertisement, .ad, .social-share'):
                    unwanted.decompose()

                content = content_elem.get_text(strip=True)
                if len(content) > 100:  # Minimum meaningful content
                    return content

        # Fallback: try paragraphs
        paragraphs = soup.select('p')
        if paragraphs:
            content_parts = []
            for p in paragraphs:
                text = p.get_text(strip=True)
                if len(text) > 20:  # Skip very short paragraphs
                    content_parts.append(text)
                if len(content_parts) >= 5:  # Don't take too many paragraphs
                    break

            if content_parts:
                return ' '.join(content_parts)

        return ""

    def _extract_article_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract article author from webpage"""
        author_selectors = [
            '.author',
            '.byline',
            '.writer',
            '[rel="author"]',
            '.article-author',
            '.post-author',
            '[data-testid="author"]',
            '.story-author'
        ]

        for selector in author_selectors:
            author_elem = soup.select_one(selector)
            if author_elem:
                author = author_elem.get_text(strip=True)
                if len(author) > 2 and len(author) < 50:  # Reasonable author name length
                    return author

        # Try meta tags
        meta_author = soup.select_one('meta[name="author"]')
        if meta_author:
            return meta_author.get('content', '').strip()

        return None

    async def _scrape_telegram_alternative_methods(self, days_back: int, max_articles: int) -> List[NewsArticle]:
        """Alternative methods if web preview fails"""
        articles = []

        # Method 1: Try RSS bridge services (if available)
        try:
            articles.extend(await self._try_telegram_rss_bridge())
        except Exception as e:
            logger.debug(f"RSS bridge method failed: {e}")

        # Method 2: Try other Telegram web interfaces
        try:
            articles.extend(await self._try_alternative_telegram_web())
        except Exception as e:
            logger.debug(f"Alternative web method failed: {e}")

        return articles[:max_articles]

    async def _try_telegram_rss_bridge(self) -> List[NewsArticle]:
        """Try using RSS bridge services for Telegram"""
        articles = []

        # Some public RSS bridge services that support Telegram
        bridge_services = [
            f"https://rss-bridge.org/bridge01/?action=display&bridge=Telegram&username={self.channel_username}&format=Atom",
            f"https://rsshub.app/telegram/channel/{self.channel_username}",
        ]

        for bridge_url in bridge_services:
            try:
                logger.debug(f"🌉 Trying RSS bridge: {bridge_url}")

                response_text = await self.http_client.get_with_retry(bridge_url, timeout=10)

                if response_text:
                    # Parse as RSS/Atom feed
                    import feedparser
                    feed = feedparser.parse(response_text)

                    for entry in feed.entries[:10]:  # Limit per bridge
                        try:
                            title = entry.title if hasattr(entry, 'title') else ""
                            content = entry.summary if hasattr(entry, 'summary') else ""
                            link = entry.link if hasattr(entry, 'link') else f"https://t.me/{self.channel_username}"

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
                                    source_type=SourceType.TELEGRAM_WEB,
                                    metadata={
                                        'telegram_channel': self.channel_username,
                                        'extraction_method': 'rss_bridge',
                                        'bridge_service': bridge_url
                                    }
                                )

                                articles.append(article)
                        except Exception as e:
                            continue

                    if articles:
                        logger.info(f"🌉 RSS bridge success: {len(articles)} articles from {bridge_url}")
                        break

            except Exception as e:
                logger.debug(f"RSS bridge {bridge_url} failed: {e}")
                continue

        return articles

    async def _try_alternative_telegram_web(self) -> List[NewsArticle]:
        """Try alternative Telegram web interfaces"""
        articles = []

        # Alternative URLs to try
        alternative_urls = [
            f"https://telegram.me/s/{self.channel_username}",
            f"https://web.telegram.org/k/#{self.channel_username}",
        ]

        for alt_url in alternative_urls:
            try:
                logger.debug(f"🔄 Trying alternative URL: {alt_url}")

                response_text = await self.http_client.get_with_retry(alt_url, timeout=10)

                if response_text and len(response_text) > 1000:
                    # Try to extract content using different selectors
                    soup = BeautifulSoup(response_text, 'html.parser')

                    # Look for any text content that might be messages
                    text_elements = soup.find_all(['div', 'p', 'span'], string=True)

                    for elem in text_elements[:20]:  # Limit attempts
                        text = elem.get_text(strip=True)
                        if (len(text) >= self.min_message_length and
                                any(keyword in text.lower() for keyword in ['crypto', 'bitcoin', 'blockchain', 'ethereum'])):

                            article = NewsArticle(
                                id="",
                                title=text[:100] + ("..." if len(text) > 100 else ""),
                                content=self.clean_content(text),
                                url=f"https://t.me/{self.channel_username}",
                                source=self.name,
                                timestamp=datetime.now(),
                                author=f"@{self.channel_username}",
                                source_type=SourceType.TELEGRAM_WEB,
                                metadata={
                                    'telegram_channel': self.channel_username,
                                    'extraction_method': 'alternative_web',
                                    'source_url': alt_url
                                }
                            )

                            articles.append(article)

                    if articles:
                        logger.info(f"🔄 Alternative method success: {len(articles)} articles")
                        break

            except Exception as e:
                logger.debug(f"Alternative URL {alt_url} failed: {e}")
                continue

        return articles

    def is_valid_content(self, article: NewsArticle) -> bool:
        """Custom validation for Telegram messages"""

        # Check title
        title_length = len(article.title.strip()) if article.title else 0
        if title_length < 10:
            logger.debug(f"❌ TITLE TOO SHORT - Title: '{article.title}' - Length: {title_length}")
            return False

        # Check URL
        if not article.url:
            logger.debug(f"❌ NO URL - Title: '{article.title[:50]}...'")
            return False

        # For Telegram, be more lenient with content length
        content_length = len(article.content) if article.content else 0
        if content_length < self.min_message_length:
            logger.debug(f"❌ CONTENT TOO SHORT - Title: '{article.title[:50]}...' - "
                         f"Length: {content_length} < {self.min_message_length}")
            return False

        # Check for spam-like content
        if self._is_spam_content(article.content):
            logger.debug(f"❌ SPAM CONTENT - Title: '{article.title[:50]}...'")
            return False

        logger.debug(f"✅ CONTENT VALID - Title: '{article.title[:50]}...' - Length: {content_length}")
        return True

    def _is_spam_content(self, content: str) -> bool:
        """Check if content appears to be spam"""
        if not content:
            return True

        content_lower = content.lower()

        # Common spam indicators
        spam_indicators = [
            'join our telegram',
            'click here to earn',
            'free money',
            'guaranteed profit',
            'investment opportunity',
            '100% return',
            'risk free',
            'make money fast'
        ]

        # Check for excessive repetition
        words = content.split()
        if len(words) > 10:
            unique_words = set(words)
            if len(unique_words) / len(words) < 0.3:  # Less than 30% unique words
                return True

        # Check for spam phrases
        return any(indicator in content_lower for indicator in spam_indicators)