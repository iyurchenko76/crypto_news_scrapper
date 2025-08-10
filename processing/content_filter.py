# File: src/processing/content_filter.py
"""Content filtering and validation"""
import re
from datetime import datetime
from typing import Dict, Any, Set

from core.models import NewsArticle
from utils.logger import get_logger

logger = get_logger(__name__)

class ContentFilter:
    """Filters and validates article content"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.crypto_keywords = self._load_crypto_keywords()
        self.blacklisted_domains = set(config.get('quality_control', {}).get('blacklisted_domains', []))
        self.min_content_length = config.get('min_content_length', 50)
        self.max_content_length = config.get('max_content_length', 50000)
        self.min_keyword_matches = config.get('quality_control', {}).get('required_keywords_count', 1)
        self.similarity_threshold = config.get('quality_control', {}).get('content_similarity_threshold', 0.8)

        # Cache for recently processed content hashes
        self.recent_hashes: Set[str] = set()
        self.hash_cache_size = 10000

    def _load_crypto_keywords(self) -> Set[str]:
        """Load crypto keywords for relevance filtering"""
        keywords = self.config.get('crypto_keywords', [])
        return {keyword.lower() for keyword in keywords}

    def is_valid_article(self, article: NewsArticle) -> bool:
        """Comprehensive article validation"""
        try:
            # Basic field validation
            if not self._validate_basic_fields(article):
                return False

            # Content length validation
            if not self._validate_content_length(article):
                return False

            # Domain blacklist check
            if self._is_blacklisted_domain(article.url):
                logger.debug(f"Article from blacklisted domain: {article.url}")
                return False

            # Crypto relevance check
            if not self._is_crypto_relevant(article):
                logger.debug(f"Article not crypto-relevant: {article.title[:50]}...")
                return False

            # Content quality check
            if not self._validate_content_quality(article):
                return False

            # Duplicate detection
            if self._is_duplicate_content(article):
                logger.debug(f"Duplicate content detected: {article.title[:50]}...")
                return False

            return True

        except Exception as e:
            logger.error(f"Error validating article {article.id}: {e}")
            return False

    def _validate_basic_fields(self, article: NewsArticle) -> bool:
        """Validate basic required fields"""
        if not article.title or len(article.title.strip()) < 10:
            return False

        if not article.url or not article.url.startswith(('http://', 'https://')):
            return False

        if not article.source:
            return False

        return True

    def _validate_content_length(self, article: NewsArticle) -> bool:
        """Validate content length"""
        content_length = len(article.content) if article.content else 0
        return self.min_content_length <= content_length <= self.max_content_length

    def _is_blacklisted_domain(self, url: str) -> bool:
        """Check if URL is from blacklisted domain"""
        try:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc.lower()
            return any(blacklisted in domain for blacklisted in self.blacklisted_domains)
        except Exception:
            return False

    def _is_crypto_relevant(self, article: NewsArticle) -> bool:
        """Check if article is crypto-relevant"""
        text = f"{article.title} {article.content or ''}".lower()

        # Count keyword matches
        matches = sum(1 for keyword in self.crypto_keywords if keyword in text)

        # Check tags for crypto relevance
        if article.tags:
            tag_text = " ".join(article.tags).lower()
            matches += sum(1 for keyword in self.crypto_keywords if keyword in tag_text)

        return matches >= self.min_keyword_matches

    def _validate_content_quality(self, article: NewsArticle) -> bool:
        """Validate content quality"""
        if not article.content:
            return True  # Some sources might only have titles

        content = article.content.strip()

        # Check for minimum word count
        word_count = len(content.split())
        min_words = self.config.get('quality_control', {}).get('minimum_word_count', 10)
        if word_count < min_words:
            return False

        # Check for excessive repetition
        if self._has_excessive_repetition(content):
            return False

        # Check for spam patterns
        if self._is_spam_content(content):
            return False

        return True

    def _has_excessive_repetition(self, content: str) -> bool:
        """Check for excessive repetition in content"""
        words = content.lower().split()
        if len(words) < 10:
            return False

        # Check for repeated phrases
        word_counts = {}
        for word in words:
            word_counts[word] = word_counts.get(word, 0) + 1

        # If any word appears more than 20% of total words, it's repetitive
        max_count = max(word_counts.values())
        return max_count > len(words) * 0.2

    def _is_spam_content(self, content: str) -> bool:
        """Check for spam patterns"""
        spam_patterns = [
            r'click here.*?to.*?',
            r'buy now.*?',
            r'limited time.*?offer',
            r'act now.*?',
            r'subscribe.*?to.*?newsletter',
            r'follow.*?us.*?on.*?social',
        ]

        content_lower = content.lower()
        spam_matches = sum(1 for pattern in spam_patterns if re.search(pattern, content_lower))

        # If multiple spam patterns found, likely spam
        return spam_matches >= 2

    def _is_duplicate_content(self, article: NewsArticle) -> bool:
        """Check for duplicate content using content hashing"""
        content_hash = article.get_content_hash()

        if content_hash in self.recent_hashes:
            return True

        # Add to recent hashes cache
        self.recent_hashes.add(content_hash)

        # Maintain cache size
        if len(self.recent_hashes) > self.hash_cache_size:
            # Remove oldest hashes (simplified approach)
            self.recent_hashes = set(list(self.recent_hashes)[-self.hash_cache_size//2:])

        return False

    async def enrich_article(self, article: NewsArticle) -> NewsArticle:
        """Enrich article with additional metadata"""
        try:
            # Calculate relevance score if not present
            if article.relevance_score is None:
                article.relevance_score = self._calculate_relevance_score(article)

            # Add content analysis metadata
            if article.content:
                article.metadata.update({
                    'word_count': len(article.content.split()),
                    'char_count': len(article.content),
                    'crypto_keyword_count': self._count_crypto_keywords(article)
                })

            # Add processing timestamp
            article.metadata['processed_at'] = datetime.now().isoformat()

            return article

        except Exception as e:
            logger.error(f"Error enriching article {article.id}: {e}")
            return article

    def _calculate_relevance_score(self, article: NewsArticle) -> float:
        """Calculate relevance score for article"""
        score = 0.0

        # Base score from crypto keyword density
        text = f"{article.title} {article.content or ''}".lower()
        keyword_matches = sum(1 for keyword in self.crypto_keywords if keyword in text)
        text_length = len(text.split())

        if text_length > 0:
            keyword_density = keyword_matches / text_length
            score += keyword_density * 100

        # Bonus for title containing crypto keywords
        title_matches = sum(1 for keyword in self.crypto_keywords if keyword in article.title.lower())
        score += title_matches * 10

        # Bonus for recent articles
        age_hours = (datetime.now() - article.timestamp).total_seconds() / 3600
        if age_hours < 24:
            score += (24 - age_hours) / 24 * 5

        # Normalize to 0-10 scale
        return min(10.0, max(0.0, score))

    def _count_crypto_keywords(self, article: NewsArticle) -> int:
        """Count crypto keywords in article"""
        text = f"{article.title} {article.content or ''}".lower()
        return sum(1 for keyword in self.crypto_keywords if keyword in text)