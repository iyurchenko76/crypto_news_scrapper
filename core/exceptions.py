# File: src/core/exceptions.py
"""Custom exceptions for the scraper"""

class ScraperError(Exception):
    """Base exception for scraper errors"""
    pass

class ConfigurationError(ScraperError):
    """Configuration-related errors"""
    pass

class ScrapingError(ScraperError):
    """Scraping operation errors"""
    pass

class DatabaseError(ScraperError):
    """Database operation errors"""
    pass

class ValidationError(ScraperError):
    """Data validation errors"""
    pass