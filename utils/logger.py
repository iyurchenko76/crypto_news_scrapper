# File: src/utils/logger.py
"""Enhanced logging configuration"""
import logging
import json
import sys
from datetime import datetime
from typing import Dict, Any

class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging"""

    def format(self, record):
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }

        # Add extra fields if present
        if hasattr(record, 'url'):
            log_entry['url'] = record.url
        if hasattr(record, 'duration'):
            log_entry['duration'] = record.duration
        if hasattr(record, 'source'):
            log_entry['source'] = record.source

        return json.dumps(log_entry)

def setup_logging(config: Dict[str, Any] = None) -> logging.Logger:
    """Setup logging configuration"""
    if config is None:
        config = {
            'level': 'INFO',
            'file_enabled': True,
            'file_path': 'scraper.log',
            'console_enabled': True,
            'format': 'standard'
        }

    # Get log level
    level = getattr(logging, config.get('level', 'INFO').upper())

    # Create logger
    logger = logging.getLogger('crypto_scraper')
    logger.setLevel(level)

    # Clear existing handlers
    logger.handlers.clear()

    # Choose formatter
    if config.get('format') == 'json':
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    # Console handler
    if config.get('console_enabled', True):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # File handler
    if config.get('file_enabled', True):
        file_handler = logging.FileHandler(config.get('file_path', 'scraper.log'))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

def get_logger(name: str) -> logging.Logger:
    """Get logger instance"""
    return logging.getLogger(f'crypto_scraper.{name}')