# File: src/utils/http_client.py
"""Async HTTP client with retry logic and circuit breaker"""
import asyncio
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any

import aiohttp

from utils.logger import get_logger

logger = get_logger(__name__)

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

@dataclass
class RetryConfig:
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True

@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout: int = 60
    half_open_max_calls: int = 3

class CircuitBreaker:
    """Circuit breaker pattern implementation"""

    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
        self.half_open_calls = 0
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        async with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    logger.info("Circuit breaker transitioning to HALF_OPEN")
                else:
                    raise Exception("Circuit breaker is OPEN - calls blocked")

            if self.state == CircuitState.HALF_OPEN:
                if self.half_open_calls >= self.config.half_open_max_calls:
                    raise Exception("Circuit breaker HALF_OPEN limit exceeded")
                self.half_open_calls += 1

        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        except Exception as e:
            await self._on_failure()
            raise e

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset"""
        return (
                self.last_failure_time and
                time.time() - self.last_failure_time >= self.config.recovery_timeout
        )

    async def _on_success(self):
        """Handle successful call"""
        async with self._lock:
            self.failure_count = 0
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                logger.info("Circuit breaker reset to CLOSED")

    async def _on_failure(self):
        """Handle failed call"""
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.config.failure_threshold:
                self.state = CircuitState.OPEN
                logger.warning(f"Circuit breaker opened after {self.failure_count} failures")

class AsyncHTTPClient:
    """Advanced async HTTP client with retry logic and circuit breaker"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.circuit_breaker = CircuitBreaker(CircuitBreakerConfig(
            failure_threshold=config.get('circuit_breaker_threshold', 5),
            recovery_timeout=config.get('circuit_breaker_timeout', 60)
        ))
        self.retry_config = RetryConfig(
            max_retries=config.get('max_retries', 3),
            base_delay=config.get('base_delay', 1.0),
            max_delay=config.get('max_delay', 60.0)
        )

    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(
            limit=self.config.get('connection_pool_size', 100),
            limit_per_host=self.config.get('connections_per_host', 10),
            ttl_dns_cache=self.config.get('dns_cache_ttl', 300),
            use_dns_cache=True,
            enable_cleanup_closed=True
        )

        timeout = aiohttp.ClientTimeout(
            total=self.config.get('total_timeout', 30),
            connect=self.config.get('connect_timeout', 10),
            sock_read=self.config.get('read_timeout', 20)
        )

        headers = {
            'User-Agent': self.config.get('user_agent', 'CryptoScraper/2.0'),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }

        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=headers,
            raise_for_status=False
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    async def get_with_retry(self, url: str, **kwargs) -> Optional[str]:
        """Get URL with exponential backoff retry and circuit breaker"""
        return await self.circuit_breaker.call(self._get_with_retry_internal, url, **kwargs)

    async def _get_with_retry_internal(self, url: str, **kwargs) -> Optional[str]:
        """Internal retry logic"""
        last_exception = None

        for attempt in range(self.retry_config.max_retries + 1):
            try:
                async with self.session.get(url, **kwargs) as response:
                    if response.status == 200:
                        content = await response.text()
                        logger.debug(f"Successfully fetched {url} (attempt {attempt + 1})")
                        return content
                    elif response.status == 429:  # Rate limited
                        retry_after = int(response.headers.get('Retry-After', 60))
                        logger.warning(f"Rate limited on {url}, waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue
                    elif response.status >= 500:  # Server error, retry
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=f"Server error: {response.status}"
                        )
                    else:  # Client error, don't retry
                        logger.warning(f"Client error {response.status} for {url}")
                        return None

            except asyncio.TimeoutError as e:
                last_exception = e
                logger.warning(f"Timeout on {url} (attempt {attempt + 1})")
            except aiohttp.ClientError as e:
                last_exception = e
                logger.warning(f"Request error on {url} (attempt {attempt + 1}): {e}")
            except Exception as e:
                last_exception = e
                logger.error(f"Unexpected error on {url} (attempt {attempt + 1}): {e}")

            # Calculate delay for next attempt
            if attempt < self.retry_config.max_retries:
                delay = min(
                    self.retry_config.base_delay * (self.retry_config.exponential_base ** attempt),
                    self.retry_config.max_delay
                )

                # Add jitter to prevent thundering herd
                if self.retry_config.jitter:
                    delay *= (0.5 + random.random() * 0.5)

                logger.debug(f"Retrying {url} in {delay:.2f}s")
                await asyncio.sleep(delay)

        logger.error(f"Failed to fetch {url} after {self.retry_config.max_retries + 1} attempts: {last_exception}")
        return None