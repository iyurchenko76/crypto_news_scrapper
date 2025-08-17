# File: src/core/models.py
"""Enhanced data models with validation and hashing"""
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum

class SourceType(Enum):
    RSS = "rss"
    API = "api"
    WEB = "web"
    SOCIAL = "social"
    REDDIT = "reddit"
    GOOGLE_NEWS_RSS = "google_news_rss"
    GOOGLE_NEWS_WEB = "google_news_web"
    GOOGLE_NEWS_COMBINED = "google_news_combined"
    TELEGRAM_WEB = "telegram_web"
    TELEGRAM_API = "telegram_api"

@dataclass
class NewsArticle:
    """Enhanced news article model with validation"""
    id: str
    title: str
    content: str
    url: str
    source: str
    timestamp: datetime
    author: Optional[str] = None
    category: Optional[str] = None
    sentiment: Optional[float] = None
    relevance_score: Optional[float] = None
    source_type: SourceType = SourceType.RSS
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate and normalize data after initialization"""
        if not self.id:
            self.id = self.generate_id()

        # Normalize title and content
        self.title = self.title.strip() if self.title else ""
        self.content = self.content.strip() if self.content else ""

        # Validate required fields
        if not self.title or not self.url or not self.source:
            raise ValueError("Title, URL, and source are required fields")

    def generate_id(self) -> str:
        """Generate unique ID from URL and timestamp"""
        content_for_hash = f"{self.url}{self.timestamp}{self.source}"
        return hashlib.md5(content_for_hash.encode()).hexdigest()[:16]

    def get_content_hash(self) -> str:
        """Generate hash for content similarity detection"""
        content_for_hash = f"{self.title}{self.content}".lower().strip()
        return hashlib.sha256(content_for_hash.encode()).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'id': self.id,
            'title': self.title,
            'content': self.content,
            'url': self.url,
            'source': self.source,
            'timestamp': self.timestamp.isoformat(),
            'author': self.author,
            'category': self.category,
            'sentiment': self.sentiment,
            'relevance_score': self.relevance_score,
            'source_type': self.source_type.value,
            'tags': self.tags,
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NewsArticle':
        """Create instance from dictionary"""
        # Convert timestamp string back to datetime
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])

        # Convert source_type string back to enum
        if isinstance(data.get('source_type'), str):
            data['source_type'] = SourceType(data['source_type'])

        return cls(**data)