from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from core.models import NewsArticle
from scrapers.base import RSSAsyncScraper
from utils.logger import get_logger

logger = get_logger(__name__)


class TwitterWebScraper:
    """
    Scrapes tweets via Nitter RSS feeds and delegates parsing to RSSAsyncScraper.

    Expected source_config keys:
      - name: str
      - source_type: 'twitter_web'
      - handles: List[str]                 # e.g., ["binance", "cz_binance"]
      - nitter_base: str (optional)        # default "https://nitter.net"
      - per_handle_limit: int (optional)   # items per handle to keep (post-merge), default 50
      - tweet_min_length: int (optional)   # override min content length for tweets (default 5)

    Note: Nitter instances may be rate-limited/unreliable; consider the API-based scraper for robustness.
    """

    def __init__(self, source_config: Dict[str, Any], http_client, global_config: Optional[Dict[str, Any]] = None):
        self.source_config = source_config
        self.http_client = http_client
        self.global_config = global_config or {}

    async def scrape(self) -> List[NewsArticle]:
        # Delegate to scrape_articles to ensure correct limiting behavior
        return await self.scrape_articles()

    async def scrape_articles(self, max_articles: int = 100, hours_back: int = None) -> List[NewsArticle]:
        """
        Fetch via RSS with proper per-handle limiting and diagnostics.
        Uses a relaxed min content length suitable for tweets.
        """
        name = self.source_config.get("name", "TwitterWeb")
        handles = self.source_config.get("handles") or []
        if not handles:
            logger.warning(f"[{name}] No 'handles' provided; returning empty result.")
            return []

        nitter_base = (self.source_config.get("nitter_base") or "https://nitter.net").rstrip("/")
        configured_cap = int(self.source_config.get("per_handle_limit", 50))
        per_handle_from_max = max(1, math.ceil((max_articles or 100) / max(1, len(handles))))
        per_handle_limit = min(per_handle_from_max, configured_cap) if configured_cap > 0 else per_handle_from_max

        # Override content length threshold for tweets by cloning global_config
        tweet_min_length = int(self.source_config.get("tweet_min_length", 5))
        gc = dict(self.global_config or {})
        gc["min_content_length"] = tweet_min_length  # tweets are short; allow shorter content

        logger.info(f"[{name}] Starting Nitter RSS fetch: base={nitter_base}, handles={handles}, "
                    f"per_handle_limit={per_handle_limit}, total_max={max_articles}, tweet_min_length={tweet_min_length}")

        aggregated: List[NewsArticle] = []
        for handle in handles:
            rss_url = f"{nitter_base}/{handle}/rss"
            child_config = {
                "name": f"{name} @{handle}",
                "source_type": "rss",
                "rss_url": rss_url,
            }
            rss_scraper = RSSAsyncScraper(child_config, self.http_client, gc)
            try:
                logger.info(f"[{name}] Fetching RSS for @{handle}: {rss_url} (limit={per_handle_limit})")
                items = await rss_scraper.scrape_articles(max_articles=per_handle_limit, hours_back=hours_back)
                count = len(items or [])
                logger.info(f"[{name}] @{handle}: fetched {count} item(s)")
                if items:
                    aggregated.extend(items)
            except Exception as e:
                logger.warning(f"[{name}] Failed to fetch RSS for @{handle} via {rss_url}: {e}")

        if max_articles and len(aggregated) > max_articles:
            aggregated = aggregated[:max_articles]

        logger.info(f"[{name}] Final aggregated items: {len(aggregated)}")
        return aggregated

    async def validate_source(self) -> bool:
        """
        Validate configuration and that at least one Nitter RSS URL is reachable with diagnostics.
        """
        name = self.source_config.get("name", "TwitterWeb")
        try:
            handles = self.source_config.get("handles") or []
            if not handles:
                logger.warning(f"[{name}] validate_source: no 'handles' provided")
                return False

            nitter_base = (self.source_config.get("nitter_base") or "https://nitter.net").rstrip("/")
            test_handle = handles[0]
            rss_url = f"{nitter_base}/{test_handle}/rss"
            logger.info(f"[{name}] validate_source: GET {rss_url}")

            # Reuse RSS scraper validation if available, with relaxed min length
            gc = dict(self.global_config or {})
            gc["min_content_length"] = int(self.source_config.get("tweet_min_length", 5))
            child_config = {
                "name": f"{name} @{test_handle}",
                "source_type": "rss",
                "rss_url": rss_url,
            }
            rss_scraper = RSSAsyncScraper(child_config, self.http_client, gc)
            if hasattr(rss_scraper, "validate_source"):
                ok = await rss_scraper.validate_source()
                logger.info(f"[{name}] validate_source: {'OK' if ok else 'FAILED'} for {rss_url}")
                return ok

            # Fallback: simple GET
            resp = await self.http_client.get_with_retry(rss_url)
            ok = resp is not None
            logger.info(f"[{name}] validate_source (fallback): {'OK' if ok else 'FAILED'} for {rss_url}")
            return ok
        except Exception as e:
            logger.error(f"[{name}] validate_source error for RSS @{test_handle}: {e}")
            return False