# File: src/scrapers/web_scraper.py
"""Web scraper for deeper archive collection beyond RSS feeds"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from core.models import NewsArticle, SourceType
from scrapers.base import BaseAsyncScraper
from utils.http_client import AsyncHTTPClient
from utils.logger import get_logger

logger = get_logger(__name__)

class WebArchiveScraper(BaseAsyncScraper):
    """Web scraper for deep archive collection"""

    def __init__(self, config: Dict[str, Any], http_client: AsyncHTTPClient, global_config: Dict[str, Any] = None):
        super().__init__(config, http_client, global_config)
        self.source_type = SourceType.WEB
        self.archive_patterns = config.get('archive_patterns', {})
        self.selectors = config.get('selectors', {})
        self.max_pages = config.get('max_archive_pages', 10)

    async def scrape_articles(self, max_articles: int = 100) -> List[NewsArticle]:
        """Scrape articles from web archives"""
        articles = []

        # Method 1: Try archive page patterns
        archive_articles = await self._scrape_archive_pages(max_articles)
        articles.extend(archive_articles)

        # Method 2: Try sitemap if available
        if len(articles) < max_articles:
            sitemap_articles = await self._scrape_from_sitemap(max_articles - len(articles))
            articles.extend(sitemap_articles)

        # Method 3: Try category pages
        if len(articles) < max_articles:
            category_articles = await self._scrape_category_pages(max_articles - len(articles))
            articles.extend(category_articles)

        logger.info(f"Web archive {self.name}: collected {len(articles)} articles total")
        return articles[:max_articles]

    async def _scrape_archive_pages(self, max_articles: int) -> List[NewsArticle]:
        """Scrape from archive/category pages"""
        articles = []
        article_urls = set()

        # Get archive URLs to scrape
        archive_urls = self._get_archive_urls()

        for archive_url in archive_urls:
            if len(articles) >= max_articles:
                break

            logger.info(f"🔍 Scraping archive page: {archive_url}")

            try:
                await self.rate_limiter.acquire()
                page_content = await self.http_client.get_with_retry(archive_url)

                if not page_content:
                    await self.rate_limiter.record_failure()
                    continue

                await self.rate_limiter.record_success()

                # Extract article URLs from this page
                page_urls = self._extract_article_urls_from_page(page_content, archive_url)

                # Add new URLs (avoid duplicates)
                new_urls = [url for url in page_urls if url not in article_urls]
                article_urls.update(new_urls)

                logger.info(f"📄 Found {len(new_urls)} new article URLs on {archive_url}")

                # Scrape individual articles (in batches to avoid overwhelming)
                batch_size = 5
                for i in range(0, len(new_urls), batch_size):
                    batch_urls = new_urls[i:i + batch_size]
                    batch_articles = await self._scrape_article_batch(batch_urls)

                    # Filter for crypto relevance and validity
                    for article in batch_articles:
                        if (article and
                                self.is_crypto_relevant(article.title, article.content) and
                                self.is_valid_content(article)):
                            articles.append(article)

                            if len(articles) >= max_articles:
                                break

                    if len(articles) >= max_articles:
                        break

            except Exception as e:
                logger.error(f"Error scraping archive page {archive_url}: {e}")
                await self.rate_limiter.record_failure()

        return articles

    def _get_archive_urls(self) -> List[str]:
        """Generate archive URLs to scrape based on site patterns"""
        base_url = self.config.get('base_url', '')
        archive_urls = []

        # CoinDesk specific patterns
        if 'coindesk.com' in base_url:
            # Try recent date-based archives
            today = datetime.now()
            for days_back in range(0, 14):  # Last 2 weeks
                date = today - timedelta(days=days_back)
                archive_urls.extend([
                    f"https://www.coindesk.com/{date.year}/{date.month:02d}/{date.day:02d}/",
                    f"https://www.coindesk.com/tag/bitcoin/",
                    f"https://www.coindesk.com/tag/ethereum/",
                    f"https://www.coindesk.com/tag/crypto/",
                    f"https://www.coindesk.com/markets/",
                    f"https://www.coindesk.com/policy/",
                    f"https://www.coindesk.com/business/"
                ])

        # CoinTelegraph specific patterns
        elif 'cointelegraph.com' in base_url:
            archive_urls.extend([
                "https://cointelegraph.com/tags/bitcoin",
                "https://cointelegraph.com/tags/ethereum",
                "https://cointelegraph.com/tags/altcoin",
                "https://cointelegraph.com/category/market-analysis",
                "https://cointelegraph.com/category/blockchain",
                "https://cointelegraph.com/news"
            ])

        # Generic patterns
        else:
            # Try common archive patterns
            potential_paths = [
                "/news/", "/articles/", "/blog/", "/category/crypto/",
                "/tag/bitcoin/", "/tag/cryptocurrency/", "/archives/"
            ]
            for path in potential_paths:
                archive_urls.append(urljoin(base_url, path))

        # Remove duplicates and limit
        archive_urls = list(set(archive_urls))[:self.max_pages]

        logger.info(f"📋 Generated {len(archive_urls)} archive URLs to scrape")
        return archive_urls

    def _extract_article_urls_from_page(self, html_content: str, base_url: str) -> List[str]:
        """Extract article URLs from an archive/category page"""
        soup = BeautifulSoup(html_content, 'html.parser')
        article_urls = []
        domain = urlparse(self.config.get('base_url', '')).netloc

        # Common selectors for article links
        link_selectors = [
            'a[href*="/news/"]',
            'a[href*="/article"]',
            'a[href*="/story"]',
            'a[href*="/post"]',
            'article a',
            '.article-title a',
            '.headline a',
            '.entry-title a',
            'h2 a',
            'h3 a',
            '.post-title a'
        ]

        # Site-specific selectors
        if 'coindesk.com' in domain:
            link_selectors.extend([
                '.card-title',
                '.headline-link',
                'a.card-link',
                '.story-link'
            ])
        elif 'cointelegraph.com' in domain:
            link_selectors.extend([
                '.post-card-inline__title-link',
                '.post__title a',
                '.posts-listing__item a'
            ])

        # Extract URLs
        for selector in link_selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href:
                    # Convert relative to absolute URL
                    full_url = urljoin(base_url, href)

                    # Filter for valid article URLs
                    if self._is_valid_article_url(full_url, domain):
                        article_urls.append(full_url)

        # Remove duplicates while preserving order
        seen = set()
        unique_urls = []
        for url in article_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)

        return unique_urls[:50]  # Limit per page

    def _is_valid_article_url(self, url: str, expected_domain: str) -> bool:
        """Check if URL looks like a valid article"""
        try:
            parsed = urlparse(url)

            # Must be from the same domain
            if expected_domain not in parsed.netloc:
                return False

            # Skip non-article URLs
            skip_patterns = [
                '/tag/', '/category/', '/author/', '/page/',
                '/search/', '/archive/', '?', '#',
                'mailto:', 'tel:', 'javascript:',
                '.pdf', '.jpg', '.png', '.gif',
                '/rss', '/feed'
            ]

            for pattern in skip_patterns:
                if pattern in url.lower():
                    return False

            # Should contain article indicators
            article_indicators = [
                '/news/', '/article', '/story', '/post',
                '/business/', '/markets/', '/policy/',
                '/crypto', '/bitcoin', '/ethereum',
                # Date patterns
                '/2024/', '/2025/'
            ]

            return any(indicator in url.lower() for indicator in article_indicators)

        except Exception:
            return False

    async def _scrape_article_batch(self, urls: List[str]) -> List[Optional[NewsArticle]]:
        """Scrape multiple article URLs concurrently"""
        semaphore = asyncio.Semaphore(3)  # Limit concurrent requests

        async def scrape_single_article(url: str) -> Optional[NewsArticle]:
            async with semaphore:
                try:
                    await self.rate_limiter.acquire()
                    return await self._scrape_individual_article(url)
                except Exception as e:
                    logger.warning(f"Failed to scrape article {url}: {e}")
                    return None

        tasks = [scrape_single_article(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        articles = []
        for result in results:
            if isinstance(result, NewsArticle):
                articles.append(result)
            elif isinstance(result, Exception):
                logger.warning(f"Article scraping task failed: {result}")

        return articles

    async def _scrape_individual_article(self, url: str) -> Optional[NewsArticle]:
        """Scrape a single article page"""
        try:
            content = await self.http_client.get_with_retry(url)
            if not content:
                return None

            soup = BeautifulSoup(content, 'html.parser')

            # Extract article data
            article_data = self._extract_article_data(soup, url)

            if not article_data['title'] or not article_data['content']:
                return None

            # Create article object
            article = NewsArticle(
                id="",  # Will be generated
                title=article_data['title'],
                content=self.clean_content(article_data['content']),
                url=url,
                source=self.name,
                timestamp=article_data['timestamp'],
                author=article_data.get('author'),
                source_type=SourceType.WEB,
                metadata={
                    'scraped_from': 'web_archive',
                    'word_count': len(article_data['content'].split()) if article_data['content'] else 0
                }
            )

            return article

        except Exception as e:
            logger.warning(f"Error scraping article {url}: {e}")
            return None

    def _extract_article_data(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Extract article data from soup using selectors"""
        data = {
            'title': '',
            'content': '',
            'timestamp': datetime.now(),
            'author': None
        }

        # Extract title
        title_selectors = [
            'h1', '.headline', '.entry-title', '.post-title',
            '.article-title', '[data-module="ArticleHeader"] h1'
        ]

        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                data['title'] = title_elem.get_text().strip()
                break

        # Extract content
        content_selectors = [
            '.entry-content', '.post-content', '.article-content',
            '.article-body', '[data-module="ArticleBody"]',
            'article .content', '.post-body'
        ]

        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                data['content'] = content_elem.get_text().strip()
                break

        # Extract timestamp
        time_selectors = [
            'time[datetime]', '.timestamp', '.date', '.published',
            '[data-module="Timestamp"]'
        ]

        for selector in time_selectors:
            time_elem = soup.select_one(selector)
            if time_elem:
                datetime_attr = time_elem.get('datetime')
                if datetime_attr:
                    try:
                        # Parse ISO datetime
                        data['timestamp'] = datetime.fromisoformat(datetime_attr.replace('Z', '+00:00')).replace(tzinfo=None)
                        break
                    except:
                        pass

        # Extract author
        author_selectors = [
            '.author', '.byline', '.writer', '[rel="author"]'
        ]

        for selector in author_selectors:
            author_elem = soup.select_one(selector)
            if author_elem:
                data['author'] = author_elem.get_text().strip()
                break

        return data

    async def _scrape_from_sitemap(self, max_articles: int) -> List[NewsArticle]:
        """Try to scrape from XML sitemap"""
        articles = []

        sitemap_urls = [
            urljoin(self.config.get('base_url', ''), '/sitemap.xml'),
            urljoin(self.config.get('base_url', ''), '/sitemap_index.xml'),
            urljoin(self.config.get('base_url', ''), '/news-sitemap.xml'),
        ]

        for sitemap_url in sitemap_urls:
            try:
                await self.rate_limiter.acquire()
                sitemap_content = await self.http_client.get_with_retry(sitemap_url)

                if sitemap_content:
                    # Parse sitemap and extract recent article URLs
                    recent_urls = self._parse_sitemap_for_recent_urls(sitemap_content, max_articles)

                    if recent_urls:
                        logger.info(f"📄 Found {len(recent_urls)} URLs in sitemap {sitemap_url}")
                        sitemap_articles = await self._scrape_article_batch(recent_urls)

                        for article in sitemap_articles:
                            if (article and
                                    self.is_crypto_relevant(article.title, article.content) and
                                    self.is_valid_content(article)):
                                articles.append(article)

                                if len(articles) >= max_articles:
                                    break

                        if len(articles) >= max_articles:
                            break

            except Exception as e:
                logger.warning(f"Failed to process sitemap {sitemap_url}: {e}")

        return articles

    def _parse_sitemap_for_recent_urls(self, sitemap_content: str, max_urls: int) -> List[str]:
        """Parse sitemap XML for recent article URLs"""
        try:
            soup = BeautifulSoup(sitemap_content, 'xml')
            urls = []

            # Look for URL entries
            url_elements = soup.find_all('url')

            for url_elem in url_elements:
                loc_elem = url_elem.find('loc')
                lastmod_elem = url_elem.find('lastmod')

                if loc_elem:
                    url = loc_elem.get_text().strip()

                    # Check if it's a recent article URL
                    if self._is_valid_article_url(url, urlparse(self.config.get('base_url', '')).netloc):
                        # Check if recent (if lastmod available)
                        if lastmod_elem:
                            try:
                                lastmod = datetime.fromisoformat(lastmod_elem.get_text().replace('Z', '+00:00'))
                                if lastmod >= datetime.now() - timedelta(days=30):  # Last 30 days
                                    urls.append(url)
                            except:
                                urls.append(url)  # Include if can't parse date
                        else:
                            urls.append(url)

                if len(urls) >= max_urls:
                    break

            return urls

        except Exception as e:
            logger.error(f"Error parsing sitemap: {e}")
            return []

    async def _scrape_category_pages(self, max_articles: int) -> List[NewsArticle]:
        """Scrape from category/tag pages"""
        # This is similar to _scrape_archive_pages but focuses on specific crypto categories
        # Implementation would be similar but targeting crypto-specific category pages
        return []