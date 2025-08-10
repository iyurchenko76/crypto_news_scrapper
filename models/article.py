# src/models/article.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

@dataclass
class Article:
    title: str
    url: str
    content: str
    published_at: datetime
    source: str
    author: Optional[str] = None
    tags: List[str] = None
    sentiment_score: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            'title': self.title,
            'url': self.url,
            'content': self.content,
            'published_at': self.published_at.isoformat(),
            'source': self.source,
            'author': self.author,
            'tags': self.tags or [],
            'sentiment_score': self.sentiment_score
        }