# File: src/scrapers/google_news_scraper.py
"""Google News scraper for crypto news"""

import asyncio
import json
import feedparser
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any
from urllib.parse import urlencode, urljoin, urlparse, quote_plus
from bs4 import BeautifulSoup

from core.models import NewsArticle, SourceType
from scrapers.base import BaseAsyncScraper
from utils.http_client import AsyncHTTPClient
from utils.logger import get_logger

logger = get_logger(__name__)

class GoogleNewsRSSScaper(BaseAsyncScraper):
    """Google News RSS scraper for crypto news"""

    def __init__(self, config: Dict[str, Any], http_client: AsyncHTTPClient, global_config: Dict[str, Any] = None):
        super().__init__(config, http_client, global_config)
        self.source_type = SourceType.RSS
        self.language = config.get('language', 'en')
        self.country = config.get('country', 'US')
        self.search_queries = config.get('search_queries', [
            'cryptocurrency',
            'bitcoin',
            'ethereum',
            'crypto news',
            'blockchain'
        ])
        self.max_articles_per_query = config.get('max_articles_per_query', 20)

        # Google News specific settings
        self.fetch_full_content = config.get('fetch_full_content', False)  # Disabled by default for performance
        self.allow_title_only = config.get('allow_title_only', True)  # Allow using title as content
        self.google_news_min_content = config.get('google_news_min_content', 10)  # Lower threshold for Google News
        self.use_date_enhancement = config.get('use_date_enhancement', True)  # Add date ranges to queries
        self.default_days_back = config.get('default_days_back', 7)  # Default time range when no hours_back provided

    async def scrape_articles(self, max_articles: int = 100, hours_back: int = None) -> List[NewsArticle]:
        """Scrape articles from Google News RSS feeds with dynamic date range"""
        all_articles = []
        seen_urls = set()

        # Convert hours_back to days_back for better usability
        days_back = self._convert_hours_to_days(hours_back)

        logger.info(f"🔍 {self.name}: Starting Google News RSS scraping")
        logger.info(f"📅 Time range: {days_back} days back ({hours_back or 'default'} hours)")
        logger.info(f"🔎 Search queries: {len(self.search_queries)}")

        for i, query in enumerate(self.search_queries, 1):
            if len(all_articles) >= max_articles:
                break

            try:
                await self.rate_limiter.acquire()

                # Enhanced query with dynamic date range
                enhanced_query = self._enhance_query_with_dynamic_date_range(query, days_back)

                # Construct Google News RSS search URL
                search_params = {
                    'q': enhanced_query,
                    'hl': self.language,
                    'gl': self.country,
                    'ceid': f'{self.country}:{self.language}'
                }

                rss_url = f"https://news.google.com/rss/search?{urlencode(search_params)}"

                logger.info(f"📡 Query {i}/{len(self.search_queries)}: '{enhanced_query[:60]}...'")

                response_text = await self.http_client.get_with_retry(rss_url)

                if not response_text:
                    await self.rate_limiter.record_failure()
                    logger.warning(f"❌ Query {i}: No response for '{query}'")
                    continue

                await self.rate_limiter.record_success()

                # Parse RSS feed using enhanced method
                query_articles = await self._parse_rss_feed_enhanced(response_text, query)

                # Filter articles by time range and duplicates
                new_articles = []
                cutoff_time = datetime.now() - timedelta(days=days_back)

                for article in query_articles:
                    # Check if article is within time range and not duplicate
                    if (article.timestamp >= cutoff_time and
                            article.url not in seen_urls):
                        seen_urls.add(article.url)
                        new_articles.append(article)

                        if len(all_articles) + len(new_articles) >= max_articles:
                            break

                all_articles.extend(new_articles[:self.max_articles_per_query])

                logger.info(f"✅ Query {i}: '{query}' -> {len(new_articles)} new articles")

                # Small delay between queries to be respectful
                await asyncio.sleep(1)

            except Exception as e:
                await self.rate_limiter.record_failure()
                logger.error(f"❌ Query {i}: Error for '{query}': {e}")
                continue

        # Sort by timestamp (newest first)
        all_articles.sort(key=lambda x: x.timestamp, reverse=True)

        logger.info(f"🎯 {self.name}: Final result - {len(all_articles)} total articles")
        return all_articles[:max_articles]

    def _convert_hours_to_days(self, hours_back: int = None) -> int:
        """Convert hours_back to days_back with smart defaults"""
        if hours_back is None:
            return self.default_days_back

        # Convert hours to days with reasonable minimums
        if hours_back <= 24:
            return 1  # At least 1 day for very short periods
        elif hours_back <= 48:
            return 2  # 2 days for up to 48 hours
        elif hours_back <= 168:  # 1 week
            return max(3, hours_back // 24)  # At least 3 days, or hours/24
        else:
            return min(30, hours_back // 24)  # Cap at 30 days for very long periods

    def _enhance_query_with_dynamic_date_range(self, base_query: str, days_back: int) -> str:
        """Enhance query with dynamic date range based on hours_back parameter"""
        if not self.use_date_enhancement:
            return base_query

        try:
            # Calculate date range based on days_back
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)

            after_date = start_date.strftime('%Y-%m-%d')
            before_date = end_date.strftime('%Y-%m-%d')

            # Format: "bitcoin after:2024-01-01 before:2024-01-31"
            enhanced_query = f'{base_query} after:{after_date} before:{before_date}'

            logger.debug(f"🗓️  Enhanced query: '{enhanced_query}'")
            return enhanced_query
        except Exception as e:
            logger.warning(f"Date enhancement failed: {e}, using original query")
            # Fallback to original query if date enhancement fails
            return base_query

    async def _parse_rss_feed_enhanced(self, rss_content: str, query: str) -> List[NewsArticle]:
        """Enhanced RSS parsing using both feedparser and BeautifulSoup approaches"""
        articles = []

        try:
            # Method 1: Use feedparser (primary)
            feed = feedparser.parse(rss_content)

            if feed.entries:
                logger.info(f"📥 RSS feedparser for '{query}': Found {len(feed.entries)} entries")

                for i, entry in enumerate(feed.entries):
                    try:
                        article = await self._create_article_from_rss_entry(entry, query)

                        if article:
                            if self.is_crypto_relevant(article.title, article.content):
                                if self.is_valid_content(article):
                                    articles.append(article)
                    except Exception as e:
                        logger.debug(f"❌ Entry {i+1}: Error processing RSS entry: {e}")
                        continue

            # Method 2: Fallback to BeautifulSoup XML parsing (from old script)
            if not articles:
                logger.info(f"📄 Trying BeautifulSoup XML parsing for '{query}'...")
                soup = BeautifulSoup(rss_content, 'xml')
                items = soup.find_all('item')

                logger.info(f"📥 RSS BeautifulSoup for '{query}': Found {len(items)} items")

                for i, item in enumerate(items):
                    try:
                        article = await self._create_article_from_xml_item(item, query)

                        if article:
                            if self.is_crypto_relevant(article.title, article.content):
                                if self.is_valid_content(article):
                                    articles.append(article)
                    except Exception as e:
                        logger.debug(f"❌ Item {i+1}: Error processing XML item: {e}")
                        continue

            logger.info(f"📊 Query '{query}': {len(articles)} relevant articles")

        except Exception as e:
            logger.error(f"❌ RSS parsing error for '{query}': {e}")

        return articles

    def is_valid_content(self, article: NewsArticle) -> bool:
        """Custom validation for Google News articles (more lenient than base class)"""

        # Check title
        title_length = len(article.title.strip()) if article.title else 0
        if title_length < 10:
            logger.warning(f"❌ TITLE TOO SHORT - Title: '{article.title}' - Length: {title_length}")
            return False

        # Check URL
        if not article.url or not article.url.startswith(('http://', 'https://')):
            logger.warning(f"❌ INVALID URL - Title: '{article.title[:50]}...' - URL: '{article.url}'")
            return False

        # For Google News, be more lenient with content length
        content_length = len(article.content) if article.content else 0
        min_length = self.google_news_min_content  # Use Google News specific minimum

        # If content is too short but we allow title-only and title is good, that's OK
        if content_length < min_length:
            if self.allow_title_only and title_length >= 20:
                logger.info(f"✅ CONTENT ACCEPTED (title-only) - Title: '{article.title[:50]}...' - Title length: {title_length}")
                return True
            else:
                logger.warning(f"❌ CONTENT TOO SHORT - Title: '{article.title[:50]}...' - "
                               f"Length: {content_length} < {min_length} - "
                               f"Content: '{article.content[:100] if article.content else 'EMPTY'}'")
                return False

        # Check max length (use parent class limit)
        if content_length > self.max_content_length:
            logger.warning(f"❌ CONTENT TOO LONG - Title: '{article.title[:50]}...' - "
                           f"Length: {content_length} > {self.max_content_length}")
            return False

        logger.info(f"✅ CONTENT VALID - Title: '{article.title[:50]}...' - Length: {content_length}")
        return True

    async def _create_article_from_rss_entry(self, entry, query: str) -> Optional[NewsArticle]:
        """Create NewsArticle from RSS entry"""
        try:
            # Extract basic data
            title = getattr(entry, 'title', '').strip()
            link = getattr(entry, 'link', '').strip()

            if not title or not link:
                return None

            # Parse publication date
            timestamp = datetime.now()
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                try:
                    timestamp = datetime(*entry.published_parsed[:6])
                except:
                    pass
            elif hasattr(entry, 'published'):
                try:
                    # Try common date formats
                    timestamp = datetime.strptime(entry.published, '%a, %d %b %Y %H:%M:%S %Z')
                except:
                    try:
                        timestamp = datetime.strptime(entry.published, '%a, %d %b %Y %H:%M:%S %z').replace(tzinfo=None)
                    except:
                        pass

            # Skip articles older than 30 days
            if timestamp < datetime.now() - timedelta(days=30):
                return None

            # Extract source from Google News
            source = self.name  # Use configured source name
            original_source = "Google News"
            if hasattr(entry, 'source') and hasattr(entry.source, 'title'):
                original_source = entry.source.title
            elif 'source' in entry:
                original_source = entry.source.get('title', 'Google News')

            # Get content/description
            content = getattr(entry, 'summary', '') or getattr(entry, 'description', '')
            content = self.clean_content(content)  # Use parent class method

            # IMPORTANT: For Google News RSS, content is often empty or very short
            # Handle this by either fetching full content or using title as fallback
            original_content_length = len(content)

            if original_content_length < self.google_news_min_content:
                logger.debug(f"📄 Content too short ({original_content_length} chars) for: {title[:50]}...")

                if self.fetch_full_content:
                    # Try to fetch the full article content
                    try:
                        full_content = await self._fetch_article_content(link)
                        if full_content and len(full_content) >= self.google_news_min_content:
                            content = full_content
                            logger.debug(f"✅ Fetched full content ({len(content)} chars)")
                        else:
                            # Fallback to title if enabled
                            if self.allow_title_only:
                                content = f"{title}. {content}".strip()
                                logger.debug(f"💡 Using title + summary as content ({len(content)} chars)")
                            else:
                                logger.debug(f"❌ Could not get sufficient content")
                                return None
                    except Exception as e:
                        logger.debug(f"❌ Failed to fetch full content: {e}")
                        if self.allow_title_only:
                            content = f"{title}. {content}".strip()
                            logger.debug(f"💡 Fallback: using title + summary ({len(content)} chars)")
                        else:
                            return None
                else:
                    # Just use title + whatever summary we have
                    if self.allow_title_only:
                        content = f"{title}. {content}".strip()
                        logger.debug(f"💡 Using title + summary as content ({len(content)} chars)")
                    else:
                        logger.debug(f"❌ Content too short and fetch_full_content disabled")
                        return None

            article = NewsArticle(
                id="",  # Will be generated by the system
                title=title,
                content=content,
                url=link,
                source=source,
                timestamp=timestamp,
                source_type=SourceType.RSS,
                metadata={
                    'google_news_query': query,
                    'google_news_source': True,
                    'original_source': original_source,
                    'feed_entry_id': getattr(entry, 'id', ''),
                    'language': self.language,
                    'country': self.country,
                    'content_source': 'rss_summary' if original_content_length > 10 else 'title_fallback'
                }
            )

            return article

        except Exception as e:
            logger.error(f"Failed to create article from RSS entry: {e}")
            return None

    async def _fetch_article_content(self, url: str) -> Optional[str]:
        """Attempt to fetch full article content from the original URL"""
        try:
            # Rate limit for content fetching
            await asyncio.sleep(0.5)  # Be respectful

            response_text = await self.http_client.get_with_retry(url, timeout=10)
            if not response_text:
                return None

            soup = BeautifulSoup(response_text, 'html.parser')

            # Try common content selectors
            content_selectors = [
                'article',
                '.article-content',
                '.entry-content',
                '.post-content',
                '.content',
                '[data-testid="article-body"]',
                '.story-body',
                'main'
            ]

            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    content = content_elem.get_text().strip()
                    if len(content) > 100:  # Reasonable content length
                        return self.clean_content(content)

            return None

        except Exception as e:
            logger.debug(f"Content fetching failed for {url}: {e}")
            return None

    async def _create_article_from_xml_item(self, item, query: str) -> Optional[NewsArticle]:
        """Create article from BeautifulSoup XML item (from old script approach)"""
        try:
            title = item.find('title').text if item.find('title') else ""
            link = item.find('link').text if item.find('link') else ""
            pub_date = item.find('pubDate').text if item.find('pubDate') else ""
            description = item.find('description').text if item.find('description') else ""

            if not title or not link:
                return None

            # Parse publication date (from old script)
            timestamp = datetime.now()
            if pub_date:
                try:
                    from email.utils import parsedate_to_datetime
                    timestamp = parsedate_to_datetime(pub_date).replace(tzinfo=None)
                except Exception:
                    try:
                        # Alternative parsing
                        timestamp = datetime.strptime(pub_date, '%a, %d %b %Y %H:%M:%S %Z')
                    except:
                        pass

            # Skip very old articles
            if timestamp < datetime.now() - timedelta(days=30):
                return None

            # Clean and prepare content
            content = self.clean_content(description)

            # Handle short content similar to RSS entry method
            original_content_length = len(content)

            if original_content_length < self.google_news_min_content:
                if self.allow_title_only:
                    content = f"{title}. {content}".strip()
                    logger.debug(f"💡 XML item: Using title + description ({len(content)} chars)")
                else:
                    return None

            # Extract source from link or description
            source = self.name
            original_source = "Google News"

            # Try to extract original source from description or link
            if description:
                # Look for source mentions in description
                soup = BeautifulSoup(description, 'html.parser')
                source_text = soup.get_text()
                words = source_text.split()
                # Often the source appears at the end
                if len(words) > 2:
                    potential_source = words[-1]
                    if len(potential_source) > 3 and not potential_source.isdigit():
                        original_source = potential_source

            article = NewsArticle(
                id="",  # Will be generated by the system
                title=title,
                content=content,
                url=link,
                source=source,
                timestamp=timestamp,
                source_type=SourceType.RSS,
                metadata={
                    'google_news_query': query,
                    'google_news_source': True,
                    'original_source': original_source,
                    'parsing_method': 'xml_beautifulsoup',
                    'language': self.language,
                    'country': self.country,
                    'content_source': 'xml_description' if original_content_length > 10 else 'title_fallback'
                }
            )

            return article

        except Exception as e:
            logger.error(f"Failed to create article from XML item: {e}")
            return None


class GoogleNewsWebScraper(BaseAsyncScraper):
    """Google News web scraper as backup to RSS"""

    def __init__(self, config: Dict[str, Any], http_client: AsyncHTTPClient, global_config: Dict[str, Any] = None):
        super().__init__(config, http_client, global_config)
        self.source_type = SourceType.WEB
        self.language = config.get('language', 'en')
        self.country = config.get('country', 'US')
        self.search_queries = config.get('search_queries', [
            'cryptocurrency news today',
            'bitcoin news',
            'ethereum price news',
            'crypto market news'
        ])

        # Google News specific settings
        self.allow_title_only = config.get('allow_title_only', True)
        self.google_news_min_content = config.get('google_news_min_content', 10)
        self.default_days_back = config.get('default_days_back', 3)  # Shorter default for web scraping

    async def scrape_articles(self, max_articles: int = 50, hours_back: int = None) -> List[NewsArticle]:
        """Scrape articles from Google News web interface with dynamic time range"""
        all_articles = []
        seen_urls = set()

        # Convert hours_back to days_back
        days_back = self._convert_hours_to_days(hours_back)

        logger.info(f"🌐 {self.name}: Starting Google News web scraping")
        logger.info(f"📅 Time range: {days_back} days back ({hours_back or 'default'} hours)")

        for i, query in enumerate(self.search_queries, 1):
            if len(all_articles) >= max_articles:
                break

            try:
                await self.rate_limiter.acquire()

                # Construct Google News search URL - try direct Google News instead of Google Search
                # Method 1: Try Google News direct search
                search_params = {
                    'q': query,
                    'hl': self.language,
                    'gl': self.country,
                    'ceid': f'{self.country}:{self.language}'
                }

                # Use Google News website instead of Google Search
                search_url = f"https://news.google.com/search?{urlencode(search_params)}"

                logger.info(f"🔍 Query {i}/{len(self.search_queries)}: Web search for '{query}'")

                response_text = await self.http_client.get_with_retry(search_url)

                if not response_text:
                    await self.rate_limiter.record_failure()
                    logger.warning(f"❌ Query {i}: No response for '{query}' - URL: {search_url}")
                    continue

                # Check if we got blocked or redirected
                if len(response_text) < 1000:
                    logger.warning(f"⚠️  Query {i}: Suspiciously short response ({len(response_text)} chars) for '{query}'")
                    logger.debug(f"📄 Response preview: {response_text[:200]}...")

                if "our systems have detected unusual traffic" in response_text.lower():
                    logger.error(f"🚫 Query {i}: Google detected unusual traffic - rate limiting needed")
                    await self.rate_limiter.record_failure()
                    # Increase delay for next request
                    await asyncio.sleep(5)
                    continue

                await self.rate_limiter.record_success()

                # Parse web results
                query_articles = await self._parse_web_results(response_text, query)

                # Filter duplicates and by time range
                new_articles = []
                cutoff_time = datetime.now() - timedelta(days=days_back)

                for article in query_articles:
                    if (article.timestamp >= cutoff_time and
                            article.url not in seen_urls):
                        seen_urls.add(article.url)
                        new_articles.append(article)

                        if len(all_articles) + len(new_articles) >= max_articles:
                            break

                all_articles.extend(new_articles)

                logger.info(f"✅ Query {i}: '{query}' -> {len(new_articles)} articles")

                # Delay between queries
                await asyncio.sleep(2)

            except Exception as e:
                await self.rate_limiter.record_failure()
                logger.error(f"❌ Query {i}: Error for '{query}': {e}")
                continue

        logger.info(f"🎯 {self.name}: Final result - {len(all_articles)} total articles")
        return all_articles[:max_articles]

    def _convert_hours_to_days(self, hours_back: int = None) -> int:
        """Convert hours_back to days_back (same logic as RSS scraper)"""
        if hours_back is None:
            return self.default_days_back

        if hours_back <= 24:
            return 1
        elif hours_back <= 48:
            return 2
        elif hours_back <= 168:  # 1 week
            return max(2, hours_back // 24)  # At least 2 days for web scraping
        else:
            return min(14, hours_back // 24)  # Cap at 14 days for web scraping

    def is_valid_content(self, article: NewsArticle) -> bool:
        """Custom validation for Google News articles (more lenient than base class)"""

        # Check title
        title_length = len(article.title.strip()) if article.title else 0
        if title_length < 10:
            logger.warning(f"❌ TITLE TOO SHORT - Title: '{article.title}' - Length: {title_length}")
            return False

        # Check URL
        if not article.url or not article.url.startswith(('http://', 'https://')):
            logger.warning(f"❌ INVALID URL - Title: '{article.title[:50]}...' - URL: '{article.url}'")
            return False

        # For Google News, be more lenient with content length
        content_length = len(article.content) if article.content else 0
        min_length = self.google_news_min_content  # Use Google News specific minimum

        # If content is too short but we allow title-only and title is good, that's OK
        if content_length < min_length:
            if self.allow_title_only and title_length >= 20:
                logger.info(f"✅ CONTENT ACCEPTED (title-only) - Title: '{article.title[:50]}...' - Title length: {title_length}")
                return True
            else:
                logger.warning(f"❌ CONTENT TOO SHORT - Title: '{article.title[:50]}...' - "
                               f"Length: {content_length} < {min_length} - "
                               f"Content: '{article.content[:100] if article.content else 'EMPTY'}'")
                return False

        # Check max length (use parent class limit)
        if content_length > self.max_content_length:
            logger.warning(f"❌ CONTENT TOO LONG - Title: '{article.title[:50]}...' - "
                           f"Length: {content_length} > {self.max_content_length}")
            return False

        logger.info(f"✅ CONTENT VALID - Title: '{article.title[:50]}...' - Length: {content_length}")
        return True

    async def _parse_web_results(self, html_content: str, query: str) -> List[NewsArticle]:
        """Parse Google News web search results"""
        articles = []

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Google News website has different structure than Google Search
            # Look for Google News specific selectors
            result_selectors = [
                'article[jslog]',           # Google News articles
                '[data-n-au]',              # Google News author containers
                'h3 a',                     # Headline links
                'article h3',               # Article headlines
                '.xrnccd',                  # Google News card
                '.WwrzSb',                  # Google News item
                'div[role="article"]',      # ARIA article role
                '.g',                       # Fallback to general results
                'article'                   # Generic article tags
            ]

            results = []
            for selector in result_selectors:
                found_results = soup.select(selector)
                if found_results and len(found_results) > 2:  # Need reasonable number of results
                    results = found_results
                    logger.info(f"📍 Found {len(results)} results using selector: {selector}")
                    break
                elif found_results:
                    logger.debug(f"🔍 Selector '{selector}' found {len(found_results)} results (too few)")

            if not results:
                logger.warning(f"❌ No results found with any selector for query: '{query}'")
                # Debug: log part of the HTML to see what we're getting
                logger.debug(f"📄 HTML preview: {html_content[:500]}...")
                return articles

            for i, result in enumerate(results[:20]):  # Limit per query
                try:
                    article = await self._extract_article_from_result(result, query)
                    if article:
                        # Use parent class validation methods
                        if self.is_crypto_relevant(article.title, article.content):
                            if self.is_valid_content(article):
                                articles.append(article)
                                logger.debug(f"✅ Result {i+1}: ADDED - {article.title[:50]}...")
                            else:
                                logger.debug(f"❌ Result {i+1}: REJECTED - Content validation failed")
                        # Crypto relevance logging happens in parent class
                    else:
                        logger.debug(f"❌ Result {i+1}: Failed to extract article")

                except Exception as e:
                    logger.warning(f"❌ Result {i+1}: Error extracting article: {e}")
                    continue

            logger.info(f"📊 Query '{query}': {len(articles)} relevant articles from {len(results)} web results")

        except Exception as e:
            logger.error(f"❌ Web results parsing error for '{query}': {e}")

        return articles

    async def _extract_article_from_result(self, result, query: str) -> Optional[NewsArticle]:
        """Extract article data from search result element"""
        try:
            # Extract title - try multiple approaches
            title = ""

            # Method 1: Direct text if this is a headline element
            if result.name in ['h3', 'h4', 'h2']:
                title = result.get_text(strip=True)

            # Method 2: Look for headline selectors
            if not title:
                title_selectors = [
                    'h3 a',                    # Headline link
                    'h3',                      # Just headline
                    'h4 a',                    # Secondary headline link
                    'h4',                      # Secondary headline
                    'a[data-n-au]',           # Google News author link
                    '[role="heading"]',        # ARIA heading
                    '.headline',               # Class-based headline
                    'a'                        # Fallback to any link
                ]

                for selector in title_selectors:
                    title_elem = result.select_one(selector)
                    if title_elem and title_elem.get_text(strip=True):
                        title = title_elem.get_text(strip=True)
                        break

            # Method 3: If result itself has good text
            if not title and len(result.get_text(strip=True)) > 10:
                title = result.get_text(strip=True)

            if not title or len(title) < 10:
                logger.debug(f"❌ No valid title found in result")
                return None

            # Extract URL - try multiple approaches
            url = ""

            # Method 1: Direct href if this is a link
            if result.name == 'a' and result.get('href'):
                url = result.get('href')

            # Method 2: Look for link selectors
            if not url:
                link_selectors = [
                    'a[href]',
                    'h3 a[href]',
                    'h4 a[href]',
                    '[data-n-au] a[href]'
                ]

                for selector in link_selectors:
                    link_elem = result.select_one(selector)
                    if link_elem and link_elem.get('href'):
                        url = link_elem.get('href')
                        break

            if not url:
                logger.debug(f"❌ No URL found for title: {title[:30]}...")
                return None

            # Clean Google News URLs
            if url.startswith('./'):
                url = f"https://news.google.com{url[1:]}"
            elif url.startswith('/'):
                url = f"https://news.google.com{url}"
            elif not url.startswith('http'):
                url = f"https://news.google.com/{url}"

            # Extract description/snippet
            description = ""

            # Look in nearby elements or parent containers
            parent = result.parent if result.parent else result
            desc_selectors = [
                '.st',                     # Google snippet class
                '.s',                      # Alternative snippet class
                'span',                    # Generic span
                'div',                     # Generic div
                'p'                        # Paragraph
            ]

            for selector in desc_selectors:
                desc_elem = parent.select_one(selector)
                if desc_elem:
                    desc_text = desc_elem.get_text(strip=True)
                    if len(desc_text) > len(title) and len(desc_text) > 20:
                        description = desc_text
                        break

            # If no description found, try siblings
            if not description and result.next_sibling:
                sibling_text = result.next_sibling.get_text(strip=True) if hasattr(result.next_sibling, 'get_text') else str(result.next_sibling).strip()
                if len(sibling_text) > 20:
                    description = sibling_text

            # Extract source
            source = self.name  # Use configured source name
            original_source = "Google News"

            # Look for source indicators
            source_selectors = ['cite', '.source', '.domain', '[data-n-au]']
            for selector in source_selectors:
                source_elem = result.select_one(selector) or parent.select_one(selector)
                if source_elem:
                    source_text = source_elem.get_text(strip=True)
                    if source_text:
                        original_source = source_text.split(' › ')[0].split(' - ')[0]
                        break

            # Create content from title + description
            content = title
            if description and description != title:
                content = f"{title}. {description}"

            content = self.clean_content(content)

            # Extract date if available
            timestamp = datetime.now()
            time_selectors = ['time', '.date', '[data-testid="publish-date"]', '.timestamp']
            for selector in time_selectors:
                time_elem = result.select_one(selector) or parent.select_one(selector)
                if time_elem:
                    date_text = time_elem.get_text(strip=True)
                    # Try to parse relative dates like "3 hours ago"
                    if 'hour' in date_text.lower():
                        try:
                            hours = int(date_text.split()[0])
                            timestamp = datetime.now() - timedelta(hours=hours)
                            break
                        except:
                            pass
                    elif 'day' in date_text.lower():
                        try:
                            days = int(date_text.split()[0])
                            timestamp = datetime.now() - timedelta(days=days)
                            break
                        except:
                            pass

            article = NewsArticle(
                id="",  # Will be generated by the system
                title=title,
                content=content,
                url=url,
                source=source,
                timestamp=timestamp,
                source_type=SourceType.WEB,
                metadata={
                    'google_news_query': query,
                    'google_news_web': True,
                    'original_source': original_source,
                    'extracted_from': 'google_news_web',
                    'language': self.language,
                    'country': self.country
                }
            )

            return article

        except Exception as e:
            logger.error(f"Failed to extract article from web result: {e}")
            return None


class GoogleNewsCombinedScraper(BaseAsyncScraper):
    """Combined Google News scraper using both RSS and web methods"""

    def __init__(self, config: Dict[str, Any], http_client: AsyncHTTPClient, global_config: Dict[str, Any] = None):
        super().__init__(config, http_client, global_config)
        self.source_type = SourceType.RSS  # Primary method

        # Initialize both scrapers with the same config
        self.rss_scraper = GoogleNewsRSSScaper(config, http_client, global_config)
        self.web_scraper = GoogleNewsWebScraper(config, http_client, global_config)

        self.use_web_backup = config.get('use_web_backup', True)
        self.rss_ratio = config.get('rss_ratio', 0.8)  # 80% from RSS, 20% from web

    async def scrape_articles(self, max_articles: int = 100, hours_back: int = None) -> List[NewsArticle]:
        """Scrape using both RSS and web methods with dynamic time range"""
        all_articles = []
        seen_urls = set()

        # Convert hours_back to days for logging
        days_back = self.rss_scraper._convert_hours_to_days(hours_back) if hasattr(self.rss_scraper, '_convert_hours_to_days') else 7

        logger.info(f"🔗 {self.name}: Starting combined Google News scraping (RSS + Web)")
        logger.info(f"📅 Time range: {days_back} days back ({hours_back or 'default'} hours)")

        # Method 1: RSS scraping (primary)
        rss_max = int(max_articles * self.rss_ratio)
        try:
            logger.info(f"📡 Phase 1: RSS scraping (target: {rss_max} articles)")
            rss_articles = await self.rss_scraper.scrape_articles(rss_max, hours_back)

            for article in rss_articles:
                if article.url not in seen_urls:
                    seen_urls.add(article.url)
                    all_articles.append(article)

            logger.info(f"✅ RSS phase: {len(all_articles)} articles collected")

        except Exception as e:
            logger.error(f"❌ RSS scraping failed: {e}")

        # Method 2: Web scraping (backup/supplement)
        if self.use_web_backup and len(all_articles) < max_articles:
            web_max = max_articles - len(all_articles)

            try:
                logger.info(f"🌐 Phase 2: Web scraping (target: {web_max} additional articles)")
                web_articles = await self.web_scraper.scrape_articles(web_max, hours_back)

                new_web_articles = []
                for article in web_articles:
                    if article.url not in seen_urls:
                        seen_urls.add(article.url)
                        new_web_articles.append(article)

                all_articles.extend(new_web_articles)
                logger.info(f"✅ Web phase: {len(new_web_articles)} additional articles")

            except Exception as e:
                logger.error(f"❌ Web scraping failed: {e}")

        # Sort by timestamp (newest first)
        all_articles.sort(key=lambda x: x.timestamp, reverse=True)

        logger.info(f"🎯 {self.name}: Final result - {len(all_articles)} total articles from combined methods")
        return all_articles[:max_articles]