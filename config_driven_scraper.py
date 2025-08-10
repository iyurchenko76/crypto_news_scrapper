import requests
import time
import feedparser
import yaml
import schedule
from datetime import datetime, timedelta
from typing import List, Dict
import logging
from bs4 import BeautifulSoup
import hashlib
import os

from crypto_news_scraper import NewsDatabase, NewsArticle


class ConfigDrivenScraper:
    """Unified scraper that uses YAML configuration for all settings"""
    
    def __init__(self, config_path: str = "crypto_scraper_config.yaml"):
        # Load configuration
        self.config = self.load_config(config_path)
        
        # Initialize database
        self.db = NewsDatabase(self.config['database_path'])
        
        # Setup HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Get enabled sources from config
        self.sources = [
            source for source in self.config['sources'] 
            if source.get('enabled', True)
        ]
        
        self.logger.info(f"Initialized with {len(self.sources)} enabled sources")
    
    def load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except FileNotFoundError:
            self.logger.error(f"Config file {config_path} not found")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing config file: {e}")
            raise
    
    def is_crypto_relevant(self, title: str, content: str) -> bool:
        """Check if article is crypto-relevant based on config keywords"""
        text = f"{title} {content}".lower()
        keywords = [kw.lower() for kw in self.config['crypto_keywords']]
        
        # Check for keyword matches
        matches = sum(1 for keyword in keywords if keyword in text)
        return matches > 0
    
    def is_valid_content(self, article: NewsArticle) -> bool:
        """Validate article content based on config rules"""
        content_length = len(article.content) if article.content else 0
        
        # Check content length
        if content_length < self.config['min_content_length']:
            return False
        if content_length > self.config['max_content_length']:
            return False
        
        # Check title
        if not article.title or len(article.title.strip()) < 10:
            return False
        
        return True
    
    def scrape_rss_source(self, source: dict, hours_back: int = None) -> List[NewsArticle]:
        """Scrape a single RSS source using config settings"""
        if hours_back is None:
            hours_back = 24  # Default
        
        articles = []
        
        try:
            self.logger.info(f"Scraping {source['name']}...")
            
            # Get RSS feed with config timeout
            timeout = self.config.get('request_timeout_seconds', 15)
            response = self.session.get(source['rss_url'], timeout=timeout)
            
            if response.status_code == 200:
                feed = feedparser.parse(response.content)
                
                for entry in feed.entries:
                    try:
                        # Extract basic data
                        title = getattr(entry, 'title', '').strip()
                        link = getattr(entry, 'link', '').strip()
                        
                        if not title or not link:
                            continue
                        
                        # Get content
                        content = ""
                        if hasattr(entry, 'content') and entry.content:
                            content = entry.content[0].value
                        elif hasattr(entry, 'summary'):
                            content = entry.summary
                        elif hasattr(entry, 'description'):
                            content = entry.description
                        
                        # Clean HTML
                        if content:
                            soup = BeautifulSoup(content, 'html.parser')
                            content = soup.get_text().strip()
                        
                        # Parse timestamp
                        timestamp = datetime.now()
                        if hasattr(entry, 'published_parsed') and entry.published_parsed:
                            try:
                                timestamp = datetime(*entry.published_parsed[:6])
                            except:
                                pass
                        
                        # Check if recent enough
                        cutoff_time = datetime.now() - timedelta(hours=hours_back)
                        if timestamp >= cutoff_time:
                            article = NewsArticle(
                                id=f"{source['name']}_{hashlib.md5(link.encode()).hexdigest()[:12]}",
                                title=title,
                                content=content,
                                url=link,
                                source=source['name'],
                                timestamp=timestamp,
                                author=getattr(entry, 'author', None)
                            )
                            
                            # Apply content validation and relevance filtering
                            if (self.is_crypto_relevant(title, content) and 
                                self.is_valid_content(article)):
                                articles.append(article)
                    
                    except Exception as e:
                        self.logger.warning(f"Error processing entry from {source['name']}: {e}")
                        continue
                
                self.logger.info(f"{source['name']}: found {len(articles)} relevant articles")
            
            else:
                self.logger.error(f"{source['name']}: HTTP {response.status_code}")
            
            # Apply rate limiting from config
            time.sleep(source.get('rate_limit_seconds', 1.0))
        
        except Exception as e:
            self.logger.error(f"Error scraping {source['name']}: {e}")
        
        return articles
    
    def scrape_cryptocompare(self, hours_back: int = 24) -> List[NewsArticle]:
        """Get news from CryptoCompare API"""
        articles = []
        
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours_back)
            
            url = "https://min-api.cryptocompare.com/data/v2/news/"
            params = {
                'lang': 'EN',
                'sortOrder': 'latest',
                'lTs': int(start_time.timestamp()),
                'hTs': int(end_time.timestamp()),
                'limit': 50
            }
            
            timeout = self.config.get('request_timeout_seconds', 15)
            response = self.session.get(url, params=params, timeout=timeout)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('Response') == 'Success' or data.get('Data'):
                    for item in data.get('Data', []):
                        try:
                            title = item.get('title', '')
                            content = item.get('body', '')
                            
                            article = NewsArticle(
                                id=f"CC_{item.get('id')}",
                                title=title,
                                content=content,
                                url=item.get('url', ''),
                                source=item.get('source_info', {}).get('name', 'CryptoCompare'),
                                timestamp=datetime.fromtimestamp(item.get('published_on', 0)),
                                category=item.get('categories', ''),
                                relevance_score=float(item.get('upvotes', 0))
                            )
                            
                            # Apply same filtering as RSS sources
                            if (self.is_crypto_relevant(title, content) and 
                                self.is_valid_content(article)):
                                articles.append(article)
                        
                        except Exception as e:
                            self.logger.warning(f"Error processing CryptoCompare item: {e}")
                            continue
                
                self.logger.info(f"CryptoCompare: found {len(articles)} relevant articles")
            
            else:
                self.logger.error(f"CryptoCompare API: HTTP {response.status_code}")
        
        except Exception as e:
            self.logger.error(f"CryptoCompare error: {e}")
        
        return articles
    
    def run_single_collection(self, hours_back: int = 24) -> dict:
        """Run a single collection cycle"""
        start_time = datetime.now()
        total_new = 0
        results = {}
        
        self.logger.info("=== Starting Collection Cycle ===")
        
        # Sort sources by priority from config
        sorted_sources = sorted(self.sources, key=lambda x: x.get('priority', 5))
        
        # Scrape RSS sources
        for source in sorted_sources:
            try:
                articles = self.scrape_rss_source(source, hours_back)
                
                # Save to database
                new_count = 0
                duplicate_count = 0
                
                for article in articles:
                    if self.db.save_article(article):
                        new_count += 1
                    else:
                        duplicate_count += 1
                
                total_new += new_count
                results[source['name']] = {
                    'found': len(articles),
                    'new': new_count,
                    'duplicates': duplicate_count
                }
                
                self.logger.info(f"{source['name']}: {new_count} new / {len(articles)} found / {duplicate_count} duplicates")
                
            except Exception as e:
                self.logger.error(f"Failed to process {source['name']}: {e}")
                results[source['name']] = {'found': 0, 'new': 0, 'duplicates': 0}
        
        # Scrape CryptoCompare
        try:
            cc_articles = self.scrape_cryptocompare(hours_back)
            cc_new = 0
            cc_duplicates = 0
            
            for article in cc_articles:
                if self.db.save_article(article):
                    cc_new += 1
                else:
                    cc_duplicates += 1
            
            total_new += cc_new
            results['CryptoCompare'] = {
                'found': len(cc_articles),
                'new': cc_new,
                'duplicates': cc_duplicates
            }
            
            self.logger.info(f"CryptoCompare: {cc_new} new / {len(cc_articles)} found / {cc_duplicates} duplicates")
        
        except Exception as e:
            self.logger.error(f"CryptoCompare failed: {e}")
            results['CryptoCompare'] = {'found': 0, 'new': 0, 'duplicates': 0}
        
        duration = (datetime.now() - start_time).total_seconds()
        
        self.logger.info("=== Collection Complete ===")
        self.logger.info(f"Total new articles: {total_new}")
        self.logger.info(f"Duration: {duration:.1f} seconds")
        
        return {
            'total_new': total_new,
            'duration': duration,
            'sources': results,
            'timestamp': start_time
        }
    
    def run_scheduled_collection(self):
        """Run scheduled collection using config intervals"""
        interval_seconds = self.config.get('update_interval_seconds', 300)
        
        self.logger.info(f"Starting scheduled collection every {interval_seconds} seconds")
        
        def job():
            try:
                result = self.run_single_collection()
                self.logger.info(f"Scheduled run complete: {result['total_new']} new articles")
            except Exception as e:
                self.logger.error(f"Scheduled run failed: {e}")
        
        # Schedule the job
        schedule.every(interval_seconds).seconds.do(job)
        
        # Run immediately first time
        job()
        
        # Keep running
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            self.logger.info("Scheduled collection stopped by user")
    
    def get_database_stats(self, hours: int = 24) -> dict:
        """Get database statistics"""
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
            
            articles = self.db.get_articles_by_timerange(start_time, end_time)
            
            source_counts = {}
            for article in articles:
                source_counts[article.source] = source_counts.get(article.source, 0) + 1
            
            return {
                'total_articles': len(articles),
                'time_range_hours': hours,
                'sources': source_counts,
                'config_sources': len(self.sources),
                'config_keywords': len(self.config['crypto_keywords'])
            }
        
        except Exception as e:
            self.logger.error(f"Error getting stats: {e}")
            return {'total_articles': 0, 'time_range_hours': hours, 'sources': {}}
    
    def export_recent_articles(self, hours: int = 24, format: str = 'csv'):
        """Export recent articles for meta-model"""
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        articles = self.db.get_articles_by_timerange(start_time, end_time)
        
        if format == 'csv':
            import csv
            filename = f'crypto_news_last_{hours}h.csv'
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['id', 'title', 'content', 'url', 'source', 'timestamp', 'author', 'relevance_score']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for article in articles:
                    writer.writerow({
                        'id': article.id,
                        'title': article.title,
                        'content': article.content,
                        'url': article.url,
                        'source': article.source,
                        'timestamp': article.timestamp.isoformat(),
                        'author': article.author,
                        'relevance_score': article.relevance_score
                    })
            
            self.logger.info(f"Exported {len(articles)} articles to {filename}")
            return filename
        
        return None

def main():
    """Main function with command line options"""
    import sys
    
    # Default config path
    config_path = "crypto_scraper_config.yaml"
    
    # Check if config file exists
    if not os.path.exists(config_path):
        print(f"Config file {config_path} not found!")
        print("Please create it or specify the correct path.")
        return
    
    scraper = ConfigDrivenScraper(config_path)
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "run":
            # Single run
            print("=== Current Database Stats ===")
            stats = scraper.get_database_stats(24)
            print(f"Articles in last 24h: {stats['total_articles']}")
            print(f"Enabled sources: {stats['config_sources']}")
            print(f"Crypto keywords: {stats['config_keywords']}")
            
            if stats['sources']:
                print("Top sources:")
                for source, count in sorted(stats['sources'].items(), key=lambda x: x[1], reverse=True)[:5]:
                    print(f"  {source}: {count}")
            
            print("\n=== Running Collection ===")
            result = scraper.run_single_collection(hours_back=48)
            
            print(f"\nResults:")
            print(f"Total new articles: {result['total_new']}")
            print(f"Duration: {result['duration']:.1f} seconds")
            
            print("\nPer-source breakdown:")
            for source, data in result['sources'].items():
                print(f"  {source}: {data['new']} new / {data['found']} found / {data['duplicates']} duplicates")
        
        elif command == "schedule":
            # Continuous scheduled runs
            scraper.run_scheduled_collection()
        
        elif command == "stats":
            # Show stats only
            stats = scraper.get_database_stats(24)
            print(f"Articles in last 24h: {stats['total_articles']}")
            print(f"Enabled sources: {stats['config_sources']}")
            
            if stats['sources']:
                print("\nSource breakdown:")
                for source, count in sorted(stats['sources'].items(), key=lambda x: x[1], reverse=True):
                    print(f"  {source}: {count}")
        
        elif command == "export":
            # Export recent articles
            hours = int(sys.argv[2]) if len(sys.argv) > 2 else 24
            filename = scraper.export_recent_articles(hours=hours)
            print(f"Exported to {filename}")
        
        else:
            print("Usage:")
            print("  python config_driven_scraper.py run      # Single collection run")
            print("  python config_driven_scraper.py schedule # Start scheduled collection")
            print("  python config_driven_scraper.py stats    # Show database stats")
            print("  python config_driven_scraper.py export [hours] # Export recent articles")
    
    else:
        # Default: single run
        result = scraper.run_single_collection()
        print(f"Collected {result['total_new']} new articles")

if __name__ == "__main__":
    main()
