from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from core.models import NewsArticle, SourceType
from utils.logger import get_logger

logger = get_logger(__name__)


class TwitterAPIScraper:
    """
    Scrapes recent tweets via the Twitter API v2.

    Expected source_config keys:
      - name: str
      - source_type: 'twitter_api'
      - query: str                      # Twitter search query (e.g., "bitcoin OR btc -is:retweet lang:en")
      - max_results: int (10..100)      # default 25
      - since_id: str (optional)        # fetch tweets newer than this tweet id
      - expansions, tweet_fields, user_fields (optional) for advanced usage
      - bearer_token: str (optional)    # overrides global_config['twitter_bearer_token']

    Global config:
      - twitter_bearer_token: str
    """

    SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"

    def __init__(self, source_config: Dict[str, Any], http_client, global_config: Optional[Dict[str, Any]] = None):
        self.source_config = source_config
        self.http_client = http_client
        self.global_config = global_config or {}

    async def scrape(self) -> List[NewsArticle]:
        # Delegate to scrape_articles to ensure correct limiting behavior
        return await self.scrape_articles()

    async def scrape_articles(self, max_articles: int = 100, hours_back: int = None) -> List[NewsArticle]:
        """
        Fetch tweets with server-side limiting; avoids over-fetching then trimming.
        Adds detailed diagnostic logging and returns NewsArticle objects.
        """
        name = self.source_config.get("name", "TwitterAPI")
        bearer = self.source_config.get("bearer_token") or self.global_config.get("twitter_bearer_token")
        if not bearer:
            logger.warning(f"[{name}] validate/scrape: Missing bearer token. "
                           f"Set env TWITTER_BEARER_TOKEN or source_config.bearer_token")
            return []

        query = self.source_config.get("query")
        if not query:
            logger.warning(f"[{name}] validate/scrape: Missing 'query' in source_config")
            return []

        configured_max = int(self.source_config.get("max_results", 25))
        # Twitter API cap is 100; also respect caller's max_articles; enforce minimum 10 (API constraint)
        api_max = max(10, min(100, min(configured_max, int(max_articles or 100))))

        params = {
            "query": query,
            "max_results": str(api_max),
            "tweet.fields": self.source_config.get(
                "tweet_fields",
                "id,text,author_id,created_at,lang,public_metrics,entities"
            ),
            "expansions": self.source_config.get("expansions", "author_id"),
            "user.fields": self.source_config.get(
                "user_fields",
                "id,name,username,profile_image_url,verified"
            ),
        }
        since_id = self.source_config.get("since_id")
        if since_id:
            params["since_id"] = since_id

        headers = {
            "Authorization": f"Bearer {bearer}",
            "Accept": "application/json",
        }

        logger.info(f"[{name}] Requesting Twitter recent search: "
                    f"query='{query}', max_results={api_max}, since_id={since_id or 'None'}")

        try:
            resp_text = await self.http_client.get_with_retry(self.SEARCH_URL, headers=headers, params=params)
            if not resp_text:
                logger.warning(f"[{name}] Empty response from Twitter API. "
                               f"Verify token permissions and network access.")
                return []

            data = json.loads(resp_text) if isinstance(resp_text, str) else resp_text
            meta = data.get("meta", {})
            logger.info(f"[{name}] API meta: {meta}")

            tweets = data.get("data", []) or []
            includes = data.get("includes", {})
            users = {u["id"]: u for u in includes.get("users", [])}

            logger.info(f"[{name}] Parsed {len(tweets)} tweets; {len(users)} users in includes")

            articles: List[NewsArticle] = []
            for t in tweets:
                tweet_id = t.get("id")
                author = users.get(t.get("author_id"))
                text = (t.get("text") or "").strip()
                url = f"https://x.com/i/web/status/{tweet_id}" if tweet_id else ""

                article = NewsArticle(
                    id=f"twitter_api_{tweet_id}" if tweet_id else "",
                    title=text[:100] + ("..." if len(text) > 100 else ""),
                    content=text,
                    url=url,
                    source=name,
                    timestamp=t.get("created_at"),
                    author=(author or {}).get("username"),
                    source_type=SourceType.API,
                    metadata={
                        "public_metrics": t.get("public_metrics"),
                        "lang": t.get("lang"),
                        "entities": t.get("entities"),
                    },
                )
                articles.append(article)

            logger.info(f"[{name}] Returning {len(articles)} NewsArticle item(s)")
            return articles

        except Exception as e:
            logger.error(f"[{name}] TwitterAPIScraper request/parse failed: {e}")
            return []

    async def validate_source(self) -> bool:
        """
        Validate configuration and basic API accessibility.
        Performs a lightweight request to the recent search endpoint with detailed logging.
        """
        name = self.source_config.get("name", "TwitterAPI")
        try:
            bearer = self.source_config.get("bearer_token") or self.global_config.get("twitter_bearer_token")
            if not bearer:
                logger.warning(f"[{name}] validate_source: Missing bearer token")
                return False

            query = self.source_config.get("query")
            if not query:
                logger.warning(f"[{name}] validate_source: Missing 'query'")
                return False

            params = {"query": query, "max_results": "10", "tweet.fields": "id"}
            headers = {"Authorization": f"Bearer {bearer}", "Accept": "application/json"}

            logger.info(f"[{name}] validate_source: GET {self.SEARCH_URL} with query='{query}'")
            resp = await self.http_client.get_with_retry(self.SEARCH_URL, headers=headers, params=params)
            ok = bool(resp)
            if not ok:
                logger.warning(f"[{name}] validate_source: No response or empty body from API")
            else:
                try:
                    meta = (json.loads(resp) if isinstance(resp, str) else resp).get("meta", {})
                    logger.info(f"[{name}] validate_source: OK, meta={meta}")
                except Exception:
                    logger.info(f"[{name}] validate_source: OK (non-JSON or meta missing)")

            return ok
        except Exception as e:
            logger.error(f"[{name}] validate_source error: {e}")
            return False