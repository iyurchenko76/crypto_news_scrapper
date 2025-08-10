# File: src/scrapers/api_scraper.py
"""API-based scrapers for specific services"""
import json
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any
from urllib.parse import urlencode

from core.models import NewsArticle, SourceType
from scrapers.base import BaseAsyncScraper
from utils.http_client import AsyncHTTPClient
from utils.logger import get_logger

logger = get_logger(__name__)

class CryptoCompareAPIScraper(BaseAsyncScraper):
    """CryptoCompare API scraper"""

    def __init__(self, config: Dict[str, Any], http_client: AsyncHTTPClient, global_config: Dict[str, Any] = None):
        super().__init__(config, http_client, global_config)  # Pass global_config to parent
        self.api_url = "https://min-api.cryptocompare.com/data/v2/news/"
        self.source_type = SourceType.API

    async def scrape_articles(self, max_articles: int = 100) -> List[NewsArticle]:
        """Scrape articles from CryptoCompare API"""
        try:
            await self.rate_limiter.acquire()

            # Calculate time range
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=24)

            params = {
                'lang': 'EN',
                'sortOrder': 'latest',
                'lTs': int(start_time.timestamp()),
                'hTs': int(end_time.timestamp()),
                'limit': min(max_articles, 2000)
            }

            url = f"{self.api_url}?{urlencode(params)}"
            response_text = await self.http_client.get_with_retry(url)

            if not response_text:
                await self.rate_limiter.record_failure()
                return []

            await self.rate_limiter.record_success()
            return await self._parse_cryptocompare_response(response_text)

        except Exception as e:
            await self.rate_limiter.record_failure()
            logger.error(f"CryptoCompare API error: {e}")
            return []

    async def _parse_cryptocompare_response(self, response_text: str) -> List[NewsArticle]:
        """Parse CryptoCompare API response"""
        articles = []

        try:
            data = json.loads(response_text)

            if data.get('Message') == 'News list successfully returned' and data.get('Data'):
                for item in data.get('Data', []):
                    try:
                        article = await self._create_article_from_cryptocompare_item(item)
                        if article and self.is_crypto_relevant(article.title, article.content):
                            if self.is_valid_content(article):
                                articles.append(article)
                    except Exception as e:
                        logger.warning(f"Failed to process CryptoCompare item: {e}")
                        continue

                logger.info(f"CryptoCompare: extracted {len(articles)} relevant articles")
            else:
                logger.warning(f"CryptoCompare API response issue: {data.get('Message', 'Unknown')}")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse CryptoCompare JSON response: {e}")
        except Exception as e:
            logger.error(f"CryptoCompare parsing error: {e}")

        return articles

    async def _create_article_from_cryptocompare_item(self, item: Dict[str, Any]) -> Optional[NewsArticle]:
        """Create NewsArticle from CryptoCompare API item"""
        try:
            # Parse timestamp
            published_on = item.get('published_on', 0)
            timestamp = datetime.fromtimestamp(published_on, tz=timezone.utc).replace(tzinfo=None)

            # Calculate relevance score
            upvotes = float(item.get('upvotes', 0))
            downvotes = float(item.get('downvotes', 0))
            relevance_score = upvotes - downvotes

            article = NewsArticle(
                id=f"cc_{item.get('id', hash(item.get('title', '')))}",
                title=item.get('title', ''),
                content=item.get('body', ''),
                url=item.get('url', ''),
                source=item.get('source_info', {}).get('name', 'CryptoCompare'),
                timestamp=timestamp,
                category=item.get('categories', ''),
                relevance_score=relevance_score,
                source_type=SourceType.API,
                metadata={
                    'upvotes': upvotes,
                    'downvotes': downvotes,
                    'cryptocompare_id': item.get('id'),
                    'lang': item.get('lang', 'EN')
                }
            )

            return article

        except Exception as e:
            logger.error(f"Failed to create article from CryptoCompare item: {e}")
            return None