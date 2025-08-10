# File: src/scrapers/reddit_scraper.py
"""Reddit scraper for crypto subreddits"""

import asyncio
import json
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from urllib.parse import urlencode

from scrapers.base import BaseAsyncScraper
from core.models import NewsArticle, SourceType
from utils.http_client import AsyncHTTPClient
from utils.logger import get_logger

logger = get_logger(__name__)

class RedditScraper(BaseAsyncScraper):
    """Reddit scraper for crypto subreddits"""

    def __init__(self, config: Dict[str, Any], http_client: AsyncHTTPClient, global_config: Dict[str, Any] = None):
        super().__init__(config, http_client, global_config)
        self.subreddit = config.get('subreddit', 'CryptoCurrency')
        self.max_posts = config.get('max_posts', 50)
        self.source_type = SourceType.SOCIAL

    async def scrape_articles(self, max_articles: int = 100) -> List[NewsArticle]:
        """Scrape posts from Reddit subreddit"""
        articles = []

        try:
            await self.rate_limiter.acquire()

            # Use Reddit's JSON API (no authentication required for public posts)
            url = f"https://www.reddit.com/r/{self.subreddit}/hot.json"
            params = {
                'limit': min(max_articles, self.max_posts)
            }

            full_url = f"{url}?{urlencode(params)}"
            response_text = await self.http_client.get_with_retry(full_url)

            if not response_text:
                await self.rate_limiter.record_failure()
                return []

            await self.rate_limiter.record_success()
            articles = await self._parse_reddit_response(response_text)

            logger.info(f"Reddit r/{self.subreddit}: extracted {len(articles)} relevant posts")

        except Exception as e:
            await self.rate_limiter.record_failure()
            logger.error(f"Reddit scraping error for r/{self.subreddit}: {e}")

        return articles

    async def _parse_reddit_response(self, response_text: str) -> List[NewsArticle]:
        """Parse Reddit JSON response"""
        articles = []

        try:
            data = json.loads(response_text)

            if 'data' in data and 'children' in data['data']:
                for post in data['data']['children']:
                    try:
                        post_data = post['data']
                        article = await self._create_article_from_reddit_post(post_data)

                        if article and self.is_crypto_relevant(article.title, article.content):
                            if self.is_valid_content(article):
                                articles.append(article)

                    except Exception as e:
                        logger.warning(f"Failed to process Reddit post: {e}")
                        continue

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Reddit JSON response: {e}")
        except Exception as e:
            logger.error(f"Reddit parsing error: {e}")

        return articles

    async def _create_article_from_reddit_post(self, post_data: Dict[str, Any]) -> Optional[NewsArticle]:
        """Create NewsArticle from Reddit post data"""
        try:
            # Parse timestamp
            created_utc = post_data.get('created_utc', 0)
            timestamp = datetime.fromtimestamp(created_utc)

            # Get content
            title = post_data.get('title', '')
            content = post_data.get('selftext', '') or post_data.get('url', '')

            # Skip if too old (last 7 days)
            if timestamp < datetime.now() - timedelta(days=7):
                return None

            # Skip if no crypto relevance in title (quick filter)
            if not any(keyword in title.lower() for keyword in ['crypto', 'bitcoin', 'eth', 'btc', 'coin', 'defi', 'nft']):
                return None

            article = NewsArticle(
                id=f"reddit_{self.subreddit}_{post_data.get('id')}",
                title=title,
                content=self.clean_content(content),
                url=post_data.get('url', f"https://reddit.com{post_data.get('permalink', '')}"),
                source=f"Reddit-{self.subreddit}",
                timestamp=timestamp,
                author=post_data.get('author', ''),
                relevance_score=float(post_data.get('score', 0)),
                source_type=SourceType.SOCIAL,
                metadata={
                    'reddit_id': post_data.get('id'),
                    'subreddit': self.subreddit,
                    'num_comments': post_data.get('num_comments', 0),
                    'upvote_ratio': post_data.get('upvote_ratio', 0.0),
                    'is_self': post_data.get('is_self', False)
                }
            )

            return article

        except Exception as e:
            logger.error(f"Failed to create article from Reddit post: {e}")
            return None