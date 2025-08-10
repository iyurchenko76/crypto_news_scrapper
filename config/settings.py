# File: src/config/settings.py
"""Configuration management and validation"""
from dataclasses import dataclass
from typing import Dict, Any, List

import yaml

from core.exceptions import ConfigurationError
from utils.logger import get_logger

logger = get_logger(__name__)

@dataclass
class DatabaseConfig:
    """Database configuration"""
    path: str
    max_connections: int = 10

@dataclass
class HTTPConfig:
    """HTTP client configuration"""
    connection_pool_size: int = 100
    connections_per_host: int = 10
    total_timeout: int = 30
    connect_timeout: int = 10
    read_timeout: int = 20
    max_retries: int = 3
    user_agent: str = "CryptoScraper/2.0"

@dataclass
class ScrapingConfig:
    """Scraping configuration"""
    update_interval_seconds: int = 300
    max_concurrent_sources: int = 5
    priority_delay_seconds: float = 1.0
    min_content_length: int = 50
    max_content_length: int = 50000

class ConfigManager:
    """Configuration manager with validation"""

    def __init__(self, config_path: str = "crypto_scraper_config.yaml"):
        self.config_path = config_path
        self._config: Dict[str, Any] = {}
        self._validated = False

    def load_config(self) -> Dict[str, Any]:
        """Load and validate configuration"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)

            self._validate_config()
            self._apply_defaults()

            logger.info(f"Configuration loaded successfully from {self.config_path}")
            return self._config

        except FileNotFoundError:
            raise ConfigurationError(f"Configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in configuration file: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {e}")

    def _validate_config(self):
        """Validate configuration structure and values"""
        required_keys = ['database_path', 'sources', 'crypto_keywords']

        for key in required_keys:
            if key not in self._config:
                raise ConfigurationError(f"Missing required configuration key: {key}")

        # Validate sources
        if not isinstance(self._config['sources'], list):
            raise ConfigurationError("'sources' must be a list")

        for i, source in enumerate(self._config['sources']):
            if not isinstance(source, dict):
                raise ConfigurationError(f"Source {i} must be a dictionary")

            if 'name' not in source:
                raise ConfigurationError(f"Source {i} missing required 'name' field")

            # Must have either rss_url or be API type or web archive or reddit
            if (not source.get('rss_url')
                    and source.get('source_type') != 'api'
                    and not source.get('source_type') != 'web'
                    and not source.get('source_type') != 'reddit' ):
                raise ConfigurationError(f"Source {source['name']} must have 'rss_url' or be API or 'web' or 'reddit' type")

        # Validate crypto keywords
        if not isinstance(self._config['crypto_keywords'], list):
            raise ConfigurationError("'crypto_keywords' must be a list")

        if len(self._config['crypto_keywords']) == 0:
            raise ConfigurationError("At least one crypto keyword must be specified")

        self._validated = True
        logger.info("Configuration validation passed")

    def _apply_defaults(self):
        """Apply default values for optional configuration"""
        defaults = {
            'update_interval_seconds': 300,
            'max_concurrent_sources': 5,
            'max_connections': 100,
            'connections_per_host': 10,
            'request_timeout_seconds': 30,
            'max_retries': 3,
            'min_content_length': 50,
            'max_content_length': 50000,
            'user_agent': 'CryptoScraper/2.0',
            'priority_delay_seconds': 1.0,
            'logging': {
                'level': 'INFO',
                'file_enabled': True,
                'file_path': 'scraper.log',
                'console_enabled': True,
                'format': 'standard'
            },
            'quality_control': {
                'duplicate_detection': True,
                'content_similarity_threshold': 0.8,
                'minimum_word_count': 10,
                'blacklisted_domains': [],
                'required_keywords_count': 1
            }
        }

        for key, value in defaults.items():
            if key not in self._config:
                self._config[key] = value
            elif isinstance(value, dict) and isinstance(self._config[key], dict):
                # Merge nested dictionaries
                for subkey, subvalue in value.items():
                    if subkey not in self._config[key]:
                        self._config[key][subkey] = subvalue

    def get_database_config(self) -> DatabaseConfig:
        """Get database configuration"""
        if not self._validated:
            raise ConfigurationError("Configuration not validated")

        return DatabaseConfig(
            path=self._config['database_path'],
            max_connections=self._config.get('max_connections', 10)
        )

    def get_http_config(self) -> HTTPConfig:
        """Get HTTP configuration"""
        if not self._validated:
            raise ConfigurationError("Configuration not validated")

        return HTTPConfig(
            connection_pool_size=self._config.get('max_connections', 100),
            connections_per_host=self._config.get('connections_per_host', 10),
            total_timeout=self._config.get('request_timeout_seconds', 30),
            max_retries=self._config.get('max_retries', 3),
            user_agent=self._config.get('user_agent', 'CryptoScraper/2.0')
        )

    def get_scraping_config(self) -> ScrapingConfig:
        """Get scraping configuration"""
        if not self._validated:
            raise ConfigurationError("Configuration not validated")

        return ScrapingConfig(
            update_interval_seconds=self._config.get('update_interval_seconds', 300),
            max_concurrent_sources=self._config.get('max_concurrent_sources', 5),
            priority_delay_seconds=self._config.get('priority_delay_seconds', 1.0),
            min_content_length=self._config.get('min_content_length', 50),
            max_content_length=self._config.get('max_content_length', 50000)
        )

    def get_enabled_sources(self) -> List[Dict[str, Any]]:
        """Get list of enabled sources"""
        if not self._validated:
            raise ConfigurationError("Configuration not validated")

        return [source for source in self._config['sources'] if source.get('enabled', True)]