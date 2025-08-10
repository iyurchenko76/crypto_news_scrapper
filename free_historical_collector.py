import os

import requests
import time
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
import json
import logging
from typing import List, Dict, Optional
import re
import hashlib
from dotenv import find_dotenv,load_dotenv

# Import checking with better error handling
try:
    import tweepy
    TWITTER_AVAILABLE = True
except ImportError:
    TWITTER_AVAILABLE = False

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

try:
    import feedparser
    FEEDPARSER_AVAILABLE = True
except ImportError:
    FEEDPARSER_AVAILABLE = False

from ta_news import NewsDatabase, NewsArticle

class FixedEnhancedCollector:
    """Fixed version with proper timezone handling and better Twitter integration"""

    def __init__(self, config, db_path: str = "crypto_news.db"):
        self.config = config
        self.db = NewsDatabase(db_path)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        self.driver = None
        self.logger = logging.getLogger(__name__)

    def get_cryptocompare_news_fixed(self, start_timestamp: int, end_timestamp: int) -> List[NewsArticle]:
        """Fixed CryptoCompare API with proper error handling"""
        articles = []
        base_url = "https://min-api.cryptocompare.com/data/v2/news/"

        # Try the API without time filters first (they might have changed the API)
        params = {
            'lang': 'EN',
            'sortOrder': 'latest',
            'limit': 2000  # Get more articles in one call
        }

        try:
            print(f"Fetching CryptoCompare news (latest 2000 articles)...")
            response = self.session.get(base_url, params=params, timeout=30)

            if response.status_code == 200:
                data = response.json()
                print(f"CryptoCompare API Response: {data.get('Response', 'Unknown')}")

                if data.get('Message') == 'News list successfully returned' and data.get('Data'):
                    collected_articles = []

                    for article_data in data.get('Data', []):
                        # Parse timestamp
                        published_on = article_data.get('published_on', 0)
                        article_timestamp = datetime.fromtimestamp(published_on, tz=timezone.utc)

                        # Filter by date range
                        start_dt = datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
                        end_dt = datetime.fromtimestamp(end_timestamp, tz=timezone.utc)

                        if start_dt <= article_timestamp <= end_dt:
                            news_article = NewsArticle(
                                id=f"cc_{article_data.get('id', hash(article_data.get('title', '')))}",
                                title=article_data.get('title', ''),
                                content=article_data.get('body', ''),
                                url=article_data.get('url', ''),
                                source=article_data.get('source_info', {}).get('name', 'CryptoCompare'),
                                timestamp=article_timestamp.replace(tzinfo=None),  # Remove timezone for database
                                category=article_data.get('categories', ''),
                                relevance_score=(float(article_data.get('upvotes', 0)) - float(article_data.get('downvotes', 0)))
                            )

                            # Save to database immediately
                            if self.db.save_article(news_article):
                                collected_articles.append(news_article)

                    articles.extend(collected_articles)
                    print(f"   Saved {len(collected_articles)} new CryptoCompare articles")
                else:
                    print(f"   CryptoCompare API issue: {data}")
                    # Try alternative approach
                    alt_articles = self._try_cryptocompare_alternative()
                    articles.extend(alt_articles)
            else:
                print(f"   CryptoCompare HTTP error: {response.status_code}")

        except Exception as e:
            self.logger.error(f"CryptoCompare API error: {e}")
            print(f"   CryptoCompare failed: {e}")

        return articles

    def _try_cryptocompare_alternative(self) -> List[NewsArticle]:
        """Try alternative CryptoCompare endpoints"""
        articles = []

        # Try the news sources endpoint
        try:
            sources_url = "https://min-api.cryptocompare.com/data/news/sources"
            response = self.session.get(sources_url, timeout=15)

            if response.status_code == 200:
                print("   Trying CryptoCompare sources endpoint...")
                # This gives us available news sources
                # We can then use individual source APIs
        except Exception as e:
            pass

        return articles

    def scrape_github_crypto_repositories_fixed(self, days_back: int = 365) -> List[NewsArticle]:
        """Fixed GitHub scraping with proper timezone handling"""
        articles = []

        crypto_repos = [
            'bitcoin/bitcoin',
            'ethereum/go-ethereum',
            'binance-chain/bsc',
            'solana-labs/solana',
            'cardano-foundation/cardano-node',
            'polkadot-js/apps',
            'chainlink/chainlink',
            'uniswap/v3-core'
        ]

        for repo in crypto_repos:
            try:
                releases_url = f"https://api.github.com/repos/{repo}/releases"
                response = self.session.get(releases_url, timeout=15)

                if response.status_code == 200:
                    releases = response.json()
                    repo_articles = 0

                    for release in releases:
                        # Parse release date with proper timezone handling
                        published_at_str = release.get('published_at', '')
                        print(f"GitHub {repo}: {repo_articles + 1} releases, published_at: {published_at_str}")
                        if not published_at_str:
                            continue

                        try:
                            # Parse as UTC timezone-aware datetime
                            published_at = datetime.fromisoformat(published_at_str.replace('Z', '+00:00'))

                            # Create timezone-aware comparison datetime
                            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)

                            if published_at >= cutoff_date:
                                article = NewsArticle(
                                    id=f"github_{repo.replace('/', '_')}_{release['id']}",
                                    title=f"{repo}: {release.get('name') or release.get('tag_name', 'Release')}",
                                    content=release.get('body', ''),
                                    url=release['html_url'],
                                    source=f"GitHub-{repo.split('/')[0]}",
                                    timestamp=published_at.replace(tzinfo=None),  # Remove timezone for database
                                    author=release.get('author', {}).get('login', 'unknown') if release.get('author') else 'unknown',
                                    category='release'
                                )

                                if self.db.save_article(article):
                                    articles.append(article)
                                    repo_articles += 1
                            time.sleep(0.1)
                        except Exception as date_error:
                            self.logger.error(f"Date parsing error for {repo}: {date_error}")
                            continue

                    if repo_articles > 0:
                        print(f"   GitHub {repo}: {repo_articles} releases")

                time.sleep(1)  # GitHub API rate limiting

            except Exception as e:
                self.logger.error(f"GitHub scraping error for {repo}: {e}")

        return articles

    def scrape_twitter_with_selenium(self, days_back: int = 365) -> List[NewsArticle]:
        """Use Selenium to scrape Twitter directly"""
        articles = []

        if not SELENIUM_AVAILABLE:
            print("   Selenium not available, skipping Twitter scraping")
            return articles

        try:
            # Setup Chrome in headless mode
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')

            self.driver = webdriver.Chrome(options=chrome_options)

            # Crypto Twitter accounts to scrape
            crypto_accounts = [
                'coindesk', 'cointelegraph', 'whale_alert',
                'santimentfeed', 'lookonchain', 'bitcoin'
            ]

            for account in crypto_accounts:
                try:
                    print(f"   Scraping Twitter @{account}...")

                    # Go to Twitter profile
                    twitter_url = f"https://twitter.com/{account}"
                    self.driver.get(twitter_url)

                    # Wait for page to load
                    time.sleep(5)

                    # Scroll to load more tweets
                    for _ in range(3):
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(2)

                    # Extract tweets
                    tweet_elements = self.driver.find_elements(By.CSS_SELECTOR, 'article')

                    account_tweets = 0
                    for tweet_elem in tweet_elements[:20]:  # Limit per account
                        try:
                            # Extract tweet text
                            text_elements = tweet_elem.find_elements(By.CSS_SELECTOR, '[data-testid="tweetText"]')
                            if not text_elements:
                                continue

                            tweet_text = text_elements[0].text

                            # Check if crypto-relevant and not too short
                            if len(tweet_text) > 30 and self._is_crypto_content(tweet_text, ''):
                                # Extract tweet URL if possible
                                link_elements = tweet_elem.find_elements(By.CSS_SELECTOR, 'a[href*="/status/"]')
                                tweet_url = f"https://twitter.com/{account}"
                                if link_elements:
                                    tweet_url = "https://twitter.com" + link_elements[0].get_attribute('href').replace('https://twitter.com', '')

                                article = NewsArticle(
                                    id=f"twitter_selenium_{account}_{hash(tweet_text)}",
                                    title=tweet_text[:100] + "..." if len(tweet_text) > 100 else tweet_text,
                                    content=tweet_text,
                                    url=tweet_url,
                                    source=f"Twitter-{account}",
                                    timestamp=datetime.now(),  # Approximate timestamp
                                    author=account
                                )

                                if self.db.save_article(article):
                                    articles.append(article)
                                    account_tweets += 1

                        except Exception as tweet_error:
                            continue  # Skip problematic tweets

                    if account_tweets > 0:
                        print(f"     @{account}: {account_tweets} tweets")

                    time.sleep(3)  # Rate limiting between accounts

                except Exception as account_error:
                    self.logger.error(f"Twitter scraping error for @{account}: {account_error}")

        except Exception as e:
            self.logger.error(f"Twitter Selenium setup error: {e}")
        finally:
            if self.driver:
                self.driver.quit()
                self.driver = None

        return articles

    def scrape_google_news_archive(self, query: str, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Search Google News for historical articles (free but limited)"""
        articles = []

        # Google News search with date range
        base_url = "https://news.google.com/rss/search"

        # Format dates for Google News
        after_date = start_date.strftime('%Y-%m-%d')
        before_date = end_date.strftime('%Y-%m-%d')

        # Enhanced query with date range
        full_query = f'{query} after:{after_date} before:{before_date}'

        params = {
            'q': full_query,
            'hl': 'en',
            'gl': 'US',
            'ceid': 'US:en'
        }

        try:
            print(f"Searching Google News for: {full_query}")
            response = self.session.get(base_url, params=params, timeout=15)

            if response.status_code == 200:
                # Parse RSS response
                soup = BeautifulSoup(response.content, 'xml')
                items = soup.find_all('item')

                for item in items:
                    title = item.find('title').text if item.find('title') else ""
                    link = item.find('link').text if item.find('link') else ""
                    pub_date = item.find('pubDate').text if item.find('pubDate') else ""
                    description = item.find('description').text if item.find('description') else ""

                    # Parse publication date
                    try:
                        from email.utils import parsedate_to_datetime
                        timestamp = parsedate_to_datetime(pub_date)
                    except:
                        timestamp = datetime.now()

                    articles.append(NewsArticle(
                        id=f"gnews_{hash(link)}",
                        title=title,
                        content=description,
                        url=link,
                        source="Google-News",
                        timestamp=timestamp
                    ))

                    # articles.append({
                    #     'id': f"gnews_{hash(link)}",
                    #     'title': title,
                    #     'content': description,
                    #     'url': link,
                    #     'source': 'Google-News',
                    #     'timestamp': timestamp
                    # })

            time.sleep(2)  # Rate limiting

        except Exception as e:
            logging.error(f"Google News search error: {e}")

        return articles

    def scrape_wayback_machine_snapshots(self, base_url: str, start_date: datetime, end_date: datetime) -> List[NewsArticle]:
        """Use Wayback Machine to get historical snapshots (free)"""
        articles = []
        cdx_url = "http://web.archive.org/cdx/search/cdx"
        params = {
            'url': f"{base_url}*",
            'from': start_date.strftime('%Y%m%d'),
            'to': end_date.strftime('%Y%m%d'),
            'output': 'json',
            'fl': 'timestamp,original',
            'filter': 'statuscode:200',
            'limit': 1000
        }

        saved_count = 0
        try:
            print(f"Checking Wayback Machine for {base_url}")
            response = self.session.get(cdx_url, params=params, timeout=60)
            if response.status_code == 200:
                data = response.json()
                total_snapshots = len(data[1:]) if data else 0
                print(f"Found {total_snapshots} snapshots to process")

                # Skip header row
                for i, row in enumerate(data[1:], 1):
                    timestamp, original_url = row[0], row[1]
                    # Convert timestamp to datetime
                    snapshot_date = datetime.strptime(timestamp, '%Y%m%d%H%M%S')
                    # Build wayback URL
                    wayback_url = f"http://web.archive.org/web/{timestamp}/{original_url}"

                    # Try to extract and save content
                    article = self.extract_from_wayback_snapshot(wayback_url, snapshot_date)
                    if article:
                        articles.append(article)
                        saved_count += 1

                    if i % 10 == 0:  # Progress update every 10 snapshots
                        print(f"Processed {i}/{total_snapshots} snapshots. Saved {saved_count} articles")

                    time.sleep(1)  # Be respectful to Archive.org

        except Exception as e:
            logging.error(f"Wayback Machine error for {base_url}: {e}")

        print(f"Completed processing {base_url}: Saved {saved_count} articles")
        return articles

    def extract_from_wayback_snapshot(self, wayback_url: str, snapshot_date: datetime) -> Optional[NewsArticle]:
        """Extract article content from Wayback Machine snapshot"""
        try:
            response = self.session.get(wayback_url, timeout=60)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                # Remove Wayback Machine toolbar
                wayback_toolbar = soup.find('div', {'id': 'wm-ipp-base'})
                if wayback_toolbar:
                    wayback_toolbar.decompose()

                # Extract title
                title_elem = soup.find('h1') or soup.find('title')
                title = title_elem.get_text().strip() if title_elem else ""

                # Extract content
                content_selectors = [
                    'article', '.entry-content', '.post-content',
                    '.article-body', '.news-content', '.content'
                ]
                content = ""
                for selector in content_selectors:
                    content_elem = soup.select_one(selector)
                    if content_elem:
                        content = content_elem.get_text().strip()
                        break

                # Basic crypto relevance check
                text_to_check = f"{title} {content}".lower()
                crypto_terms = ['bitcoin', 'crypto', 'ethereum', 'blockchain', 'digital currency']
                if any(term in text_to_check for term in crypto_terms) and len(content) > 100:
                    article = NewsArticle(
                        id=f"wayback_{int(snapshot_date.timestamp())}",
                        title=title,
                        content=content,
                        url=wayback_url,
                        source='Wayback-Archive',
                        timestamp=snapshot_date,
                        relevance_score=1.0 if len(content) > 500 else 0.5  # Basic relevance scoring
                    )
                    # Save article immediately to database
                    if self.db.save_article(article):
                        return article
        except Exception as e:
            logging.error(f"Error extracting from {wayback_url}: {e}")
        return None

    def scrape_twitter_api_v2(self, bearer_token: str, days_back: int = 365) -> List[NewsArticle]:
        """Use Twitter API v2 if bearer token is provided"""
        articles = []

        if not TWITTER_AVAILABLE or not bearer_token:
            return articles

        try:
            client = tweepy.Client(bearer_token=bearer_token)

            # Search for recent crypto tweets
            crypto_queries = [
                'bitcoin OR btc', 'ethereum OR eth', 'crypto news',
                'cryptocurrency announcement', 'blockchain news'
            ]

            for query in crypto_queries:
                try:
                    tweets = tweepy.Paginator(
                        client.search_recent_tweets,
                        query=query,
                        tweet_fields=['created_at', 'public_metrics', 'author_id'],
                        max_results=100
                    ).flatten(limit=500)

                    query_tweets = 0
                    for tweet in tweets:
                        # Check if tweet is recent enough
                        tweet_age = datetime.now(timezone.utc) - tweet.created_at
                        if tweet_age.days <= days_back:
                            article = NewsArticle(
                                id=f"twitter_api_{tweet.id}",
                                title=tweet.text[:100] + "..." if len(tweet.text) > 100 else tweet.text,
                                content=tweet.text,
                                url=f"https://twitter.com/user/status/{tweet.id}",
                                source="Twitter-API",
                                timestamp=tweet.created_at.replace(tzinfo=None),
                                relevance_score=float(tweet.public_metrics.get('like_count', 0) +
                                                      tweet.public_metrics.get('retweet_count', 0))
                            )

                            if self.db.save_article(article):
                                articles.append(article)
                                query_tweets += 1

                    if query_tweets > 0:
                        print(f"   Twitter API '{query}': {query_tweets} tweets")

                    time.sleep(1)  # Rate limiting

                except Exception as e:
                    self.logger.error(f"Twitter API error for query '{query}': {e}")

        except Exception as e:
            self.logger.error(f"Twitter API setup error: {e}")

        return articles

    def scrape_alternative_crypto_sources(self, days_back: int = 365) -> List[NewsArticle]:
        """Scrape additional crypto news sources"""
        articles = []

        # CryptoPanic API (free tier)
        try:
            print("   Trying CryptoPanic API...")
            crypto_panic_url = "https://cryptopanic.com/api/free/v1/posts/"
            params = {
                'auth_token': 'free',
                'public': 'true',
                'filter': 'hot'
            }

            response = self.session.get(crypto_panic_url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()

                cryptopanic_articles = 0
                for post in data.get('results', []):
                    try:
                        # Parse timestamp
                        created_at = post.get('created_at', '')
                        timestamp = datetime.now()
                        if created_at:
                            timestamp = datetime.fromisoformat(created_at.replace('Z', '+00:00')).replace(tzinfo=None)

                        # Check date range
                        if timestamp >= datetime.now() - timedelta(days=days_back):
                            article = NewsArticle(
                                id=f"cryptopanic_{post.get('id', hash(post.get('title', '')))}",
                                title=post.get('title', ''),
                                content=post.get('title', ''),
                                url=post.get('url', ''),
                                source=f"CryptoPanic-{post.get('source', {}).get('title', 'Unknown')}",
                                timestamp=timestamp,
                                relevance_score=float(post.get('votes', {}).get('positive', 0))
                            )

                            if self.db.save_article(article):
                                articles.append(article)
                                cryptopanic_articles += 1
                    except Exception as post_error:
                        continue

                print(f"     CryptoPanic: {cryptopanic_articles} posts")

        except Exception as e:
            self.logger.error(f"CryptoPanic error: {e}")

        # CoinMarketCap News (if available)
        try:
            print("   Trying CoinMarketCap news...")
            cmc_url = "https://api.coinmarketcap.com/content/v3/news"

            response = self.session.get(cmc_url, timeout=15)
            if response.status_code == 200:
                data = response.json()

                cmc_articles = 0
                for article_data in data.get('data', []):
                    try:
                        # Parse timestamp
                        released_at = article_data.get('releasedAt', '')
                        timestamp = datetime.now()
                        if released_at:
                            timestamp = datetime.fromisoformat(released_at.replace('Z', '+00:00')).replace(tzinfo=None)

                        if timestamp >= datetime.now() - timedelta(days=days_back):
                            article = NewsArticle(
                                id=f"cmc_{article_data.get('id', hash(article_data.get('title', '')))}",
                                title=article_data.get('title', ''),
                                content=article_data.get('subtitle', ''),
                                url=f"https://coinmarketcap.com/news/{article_data.get('slug', '')}",
                                source="CoinMarketCap",
                                timestamp=timestamp
                            )

                            if self.db.save_article(article):
                                articles.append(article)
                                cmc_articles += 1
                    except Exception as article_error:
                        continue

                print(f"     CoinMarketCap: {cmc_articles} articles")

        except Exception as e:
            self.logger.error(f"CoinMarketCap error: {e}")

        return articles

    def _is_crypto_content(self, title: str, content: str) -> bool:
        """Enhanced crypto content detection"""
        text = f"{title} {content}".lower()

        # Expanded crypto terms
        crypto_terms = [
            # Major cryptocurrencies
            'bitcoin', 'btc', 'ethereum', 'eth', 'crypto', 'cryptocurrency',
            'blockchain', 'altcoin', 'dogecoin', 'doge', 'litecoin', 'ltc',
            'cardano', 'ada', 'solana', 'sol', 'polkadot', 'dot', 'avalanche',
            'avax', 'chainlink', 'link', 'polygon', 'matic', 'uniswap', 'uni',

            # DeFi and Web3
            'defi', 'decentralized finance', 'web3', 'dapp', 'smart contract',
            'yield farming', 'liquidity mining', 'staking', 'nft', 'metaverse',

            # Trading and markets
            'trading', 'exchange', 'binance', 'coinbase', 'kraken', 'gemini',
            'wallet', 'mining', 'hodl', 'bull market', 'bear market',

            # Technical terms
            'hash rate', 'difficulty', 'block', 'transaction', 'address',
            'private key', 'public key', 'satoshi', 'wei', 'gwei',

            # Regulatory and news
            'sec', 'cftc', 'regulation', 'etf', 'institutional adoption'
        ]

        return any(term in text for term in crypto_terms)

    def run_fixed_enhanced_collection(self, days_back: int = 365, twitter_bearer_token: str = None) -> List[NewsArticle]:
        """Run collection with all fixes applied"""
        all_articles = []
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        print("=== FIXED ENHANCED COLLECTION ===")
        print(f"Date range: {start_date.date()} to {end_date.date()}")

        # Method 1: Fixed CryptoCompare
        print("\n1. CryptoCompare News API (fixed)...")
        try:
            cc_articles = self.get_cryptocompare_news_fixed(
                int(start_date.timestamp()),
                int(end_date.timestamp())
            )
            all_articles.extend(cc_articles)
            print(f"   CryptoCompare: {len(cc_articles)} articles")
        except Exception as e:
            print(f"   CryptoCompare failed: {e}")

        # Method 2: Fixed GitHub
        print("\n2. GitHub repositories (timezone fixed)...")
        try:
            github_articles = self.scrape_github_crypto_repositories_fixed(days_back)
            all_articles.extend(github_articles)
            print(f"   GitHub: {len(github_articles)} releases")
        except Exception as e:
            print(f"   GitHub failed: {e}")

        # Method 3: Twitter with multiple approaches
        print("\n3. Twitter integration...")
        twitter_total = 0

        # Try Twitter API if token provided
        if twitter_bearer_token and TWITTER_AVAILABLE:
            try:
                api_articles = self.scrape_twitter_api_v2(twitter_bearer_token, days_back)
                all_articles.extend(api_articles)
                twitter_total += len(api_articles)
                print(f"   Twitter API: {len(api_articles)} tweets")
            except Exception as e:
                print(f"   Twitter API failed: {e}")

        # Try Selenium scraping
        if SELENIUM_AVAILABLE:
            try:
                selenium_articles = self.scrape_twitter_with_selenium(days_back)
                all_articles.extend(selenium_articles)
                twitter_total += len(selenium_articles)
                print(f"   Twitter Selenium: {len(selenium_articles)} tweets")
            except Exception as e:
                print(f"   Twitter Selenium failed: {e}")

        print(f"   Twitter total: {twitter_total} tweets")

        # Method 4: Alternative crypto sources
        print("\n4. Alternative crypto news sources...")
        try:
            alt_articles = self.scrape_alternative_crypto_sources(days_back)
            all_articles.extend(alt_articles)
            print(f"   Alternative sources: {len(alt_articles)} articles")
        except Exception as e:
            print(f"   Alternative sources failed: {e}")

        # Method 5: Enhanced Reddit (from your working version)
        print("\n5. Reddit communities...")
        reddit_total = 0
        subreddits = ['CryptoCurrency', 'Bitcoin', 'ethereum', 'CryptoMarkets', 'btc', 'ethtrader']

        for subreddit in subreddits:
            try:
                reddit_articles = self.scrape_reddit_crypto_historical(subreddit, days_back)
                saved_count = 0

                for article in reddit_articles:
                    if self.db.save_article(article):
                        saved_count += 1
                        all_articles.append(article)

                reddit_total += saved_count
                print(f"   r/{subreddit}: {saved_count} new posts")
            except Exception as e:
                print(f"   r/{subreddit} failed: {e}")

        print("\n6. Wayback Machine historical snapshots...")
        wb_saved_count = 0
        major_crypto_sites = [
            'https://www.coindesk.com',
            'https://cointelegraph.com/news',
            'https://decrypt.co'
        ]

        for site in major_crypto_sites:
            # Limit wayback to avoid overwhelming Archive.org
            wb_start = end_date - timedelta(days=min(90, days_back))
            wb_articles = self.scrape_wayback_machine_snapshots(site, wb_start, end_date)
            site_saved = len(wb_articles)  # Articles are already saved in extract_from_wayback_snapshot
            wb_saved_count += site_saved
            all_articles.extend(wb_articles)

            print(f"   Site {site}: Saved {site_saved} new articles")

        print("\n7. Google News archive search...")
        gn_saved_count = 0
        crypto_queries = [
            'bitcoin news', 'ethereum news', 'cryptocurrency news',
            'crypto market', 'blockchain news', 'digital currency'
        ]

        for query in crypto_queries:
            gn_articles = self.scrape_google_news_archive(query, start_date, end_date)
            query_saved = 0

            for article in gn_articles:
                if self.db.save_article(article):
                    gn_saved_count += 1
                    query_saved += 1
                    all_articles.append(article)

            print(f"   Query '{query}': Saved {query_saved} new articles")
            time.sleep(3)

        print(f"\n=== COLLECTION COMPLETE ===")
        print(f"New articles collected: {len(all_articles)}")

        # Database summary
        try:
            total_in_db = len(self.db.get_articles_by_timerange(start_date, end_date))
            print(f"Total articles now in database: {total_in_db}")
        except Exception as e:
            print(f"Database summary failed: {e}")

        return all_articles

    def scrape_reddit_crypto_historical(self, subreddit: str, days_back: int = 365) -> List[NewsArticle]:
        """Reddit scraping (same as working version)"""
        articles = []
        base_url = f"https://www.reddit.com/r/{subreddit}/search.json"

        search_terms = ['news', 'announcement', 'update', 'breaking', 'alert', 'market']

        for term in search_terms:
            try:
                params = {
                    'q': f'{term} AND (bitcoin OR ethereum OR crypto OR altcoin)',
                    'restrict_sr': 'on',
                    'sort': 'new',
                    't': 'year',
                    'limit': 100
                }

                response = self.session.get(base_url, params=params, timeout=15)

                if response.status_code == 200:
                    data = response.json()

                    if 'data' in data and 'children' in data['data']:
                        for post in data['data']['children']:
                            post_data = post['data']
                            created = datetime.fromtimestamp(post_data['created_utc'])

                            if created >= datetime.now() - timedelta(days=days_back):
                                news_article = NewsArticle(
                                    id=f"reddit_{subreddit}_{post_data['id']}",
                                    title=post_data['title'],
                                    content=post_data.get('selftext', ''),
                                    url=post_data.get('url', f"https://reddit.com{post_data['permalink']}"),
                                    source=f"Reddit-{subreddit}",
                                    timestamp=created,
                                    author=post_data.get('author', ''),
                                    relevance_score=float(post_data.get('score', 0))
                                )
                                articles.append(news_article)

                time.sleep(2)

            except Exception as e:
                self.logger.error(f"Reddit error for r/{subreddit} term '{term}': {e}")

        return articles

# Usage function
def collect_fixed_historical_dataset(days_back: int = 365, twitter_bearer_token: str = None):
    """Collect with all fixes applied"""

    try:
        from scraper_config_utils import ScraperConfig
        config = ScraperConfig()
    except ImportError:
        class MinimalConfig:
            def __init__(self):
                self.database_path = "crypto_news.db"
        config = MinimalConfig()

    collector = FixedEnhancedCollector(config)
    articles = collector.run_fixed_enhanced_collection(days_back, twitter_bearer_token)

    print(f"\nSuccessfully collected {len(articles)} new historical articles!")
    return articles

def analyze_collected_data(db_path: str = "crypto_news.db"):
    """Analyze the collected historical data"""
    try:
        db = NewsDatabase(db_path)

        # Get all articles from database
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        articles = db.get_articles_by_timerange(start_date, end_date)

        print(f"\n=== DATABASE ANALYSIS ===")
        print(f"Total articles in database: {len(articles)}")

        # Analyze by source
        source_counts = {}
        for article in articles:
            source = article.source
            source_counts[source] = source_counts.get(source, 0) + 1

        print("\nArticles by source:")
        for source, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {source}: {count}")

        # Analyze by time period
        monthly_counts = {}
        for article in articles:
            month_key = article.timestamp.strftime('%Y-%m')
            monthly_counts[month_key] = monthly_counts.get(month_key, 0) + 1

        print("\nArticles by month:")
        for month in sorted(monthly_counts.keys())[-12:]:  # Last 12 months
            print(f"  {month}: {monthly_counts[month]}")

        # Average article length
        lengths = [len(article.content) for article in articles if article.content]
        avg_length = sum(lengths) / len(lengths) if lengths else 0

        print(f"\nContent statistics:")
        print(f"  Average content length: {avg_length:.0f} characters")
        print(f"  Articles with content: {len(lengths)}")
        print(f"  Articles without content: {len(articles) - len(lengths)}")

        return articles

    except Exception as e:
        print(f"Analysis failed: {e}")
        return []

def export_for_meta_model(output_format='csv', start_date=None, end_date=None, db_path="crypto_news.db"):
    """Export collected data in format suitable for meta-model training"""
    try:
        db = NewsDatabase(db_path)

        if not start_date:
            start_date = datetime.now() - timedelta(days=365)
        if not end_date:
            end_date = datetime.now()

        articles = db.get_articles_by_timerange(start_date, end_date)

        if output_format == 'json':
            # Export as JSON
            export_data = []
            for article in articles:
                export_data.append({
                    'id': article.id,
                    'title': article.title,
                    'content': article.content,
                    'url': article.url,
                    'source': article.source,
                    'timestamp': article.timestamp.isoformat(),
                    'author': article.author,
                    'sentiment': article.sentiment,
                    'relevance_score': article.relevance_score
                })

            with open('crypto_news_dataset.json', 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)

            print(f"Exported {len(export_data)} articles to crypto_news_dataset.json")

        elif output_format == 'csv':
            # Export as CSV for easier analysis
            try:
                import pandas as pd

                data = []
                for article in articles:
                    data.append({
                        'id': article.id,
                        'title': article.title,
                        'content': article.content[:500] if article.content else '',  # Truncate for CSV
                        'url': article.url,
                        'source': article.source,
                        'timestamp': article.timestamp,
                        'author': article.author,
                        'sentiment': article.sentiment,
                        'relevance_score': article.relevance_score,
                        'content_length': len(article.content) if article.content else 0
                    })

                df = pd.DataFrame(data)
                df.to_csv('crypto_news_dataset.csv', index=False, encoding='utf-8')

                print(f"Exported {len(data)} articles to crypto_news_dataset.csv")

            except ImportError:
                print("pandas not installed, using basic CSV export")

                # Basic CSV export without pandas
                import csv

                with open('crypto_news_dataset.csv', 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['id', 'title', 'content', 'url', 'source', 'timestamp', 'author', 'sentiment', 'relevance_score', 'content_length']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                    writer.writeheader()
                    for article in articles:
                        writer.writerow({
                            'id': article.id,
                            'title': article.title,
                            'content': article.content[:500] if article.content else '',
                            'url': article.url,
                            'source': article.source,
                            'timestamp': article.timestamp.isoformat(),
                            'author': article.author,
                            'sentiment': article.sentiment,
                            'relevance_score': article.relevance_score,
                            'content_length': len(article.content) if article.content else 0
                        })

                print(f"Exported {len(articles)} articles to crypto_news_dataset.csv (basic format)")

        return articles

    except Exception as e:
        print(f"Export failed: {e}")
        return []

if __name__ == "__main__":
    # Optional: Add your Twitter Bearer Token here
    dotenv_path=find_dotenv()
    if not dotenv_path:
        dotenv_path = "/mnt/gluster/exchange/keyring/.env"
    load_dotenv(dotenv_path=dotenv_path)
    TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

    collect_fixed_historical_dataset(
        days_back=365,
        twitter_bearer_token=TWITTER_BEARER_TOKEN
    )
    # Analyze results
    print("\n" + "="*50)
    analyze_collected_data()