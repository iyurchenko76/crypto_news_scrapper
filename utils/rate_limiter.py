# File: src/utils/rate_limiter.py
"""Rate limiting implementations"""
import asyncio
import time

from utils.logger import get_logger

logger = get_logger(__name__)

class TokenBucketRateLimiter:
    """Token bucket algorithm for rate limiting"""

    def __init__(self, max_tokens: int, refill_rate: float):
        self.max_tokens = max_tokens
        self.tokens = float(max_tokens)
        self.refill_rate = refill_rate  # tokens per second
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> None:
        """Acquire tokens, waiting if necessary"""
        async with self._lock:
            await self._refill_tokens()

            if self.tokens >= tokens:
                self.tokens -= tokens
                return

            # Calculate wait time for required tokens
            tokens_needed = tokens - self.tokens
            wait_time = tokens_needed / self.refill_rate

            logger.debug(f"Rate limiter waiting {wait_time:.2f}s for {tokens} tokens")
            await asyncio.sleep(wait_time)

            # Refill again after waiting
            await self._refill_tokens()
            self.tokens = max(0, self.tokens - tokens)

    async def _refill_tokens(self):
        """Refill tokens based on elapsed time"""
        now = time.time()
        elapsed = now - self.last_refill

        # Add tokens based on elapsed time
        tokens_to_add = elapsed * self.refill_rate
        self.tokens = min(self.max_tokens, self.tokens + tokens_to_add)
        self.last_refill = now

class AdaptiveRateLimiter:
    """Rate limiter that adapts based on server responses"""

    def __init__(self, initial_rate: float = 1.0, min_rate: float = 0.1, max_rate: float = 10.0):
        self.current_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.success_count = 0
        self.failure_count = 0
        self.last_adjustment = time.time()
        self.adjustment_interval = 60  # Adjust every minute
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Acquire permission to make request"""
        await asyncio.sleep(1.0 / self.current_rate)

    async def record_success(self):
        """Record successful request"""
        async with self._lock:
            self.success_count += 1
            await self._maybe_adjust_rate()

    async def record_failure(self, is_rate_limit: bool = False):
        """Record failed request"""
        async with self._lock:
            self.failure_count += 1
            if is_rate_limit:
                # Immediate slowdown for rate limit errors
                self.current_rate = max(self.min_rate, self.current_rate * 0.5)
                logger.info(f"Rate limit detected, reducing rate to {self.current_rate:.2f} req/s")
            await self._maybe_adjust_rate()

    async def _maybe_adjust_rate(self):
        """Adjust rate based on success/failure ratio"""
        now = time.time()
        if now - self.last_adjustment < self.adjustment_interval:
            return

        total_requests = self.success_count + self.failure_count
        if total_requests < 10:  # Need minimum sample size
            return

        success_rate = self.success_count / total_requests
        old_rate = self.current_rate

        if success_rate > 0.95:  # Very high success rate
            self.current_rate = min(self.max_rate, self.current_rate * 1.2)
        elif success_rate < 0.8:  # Too many failures
            self.current_rate = max(self.min_rate, self.current_rate * 0.8)

        if abs(old_rate - self.current_rate) > 0.1:
            logger.info(f"Adjusted rate from {old_rate:.2f} to {self.current_rate:.2f} req/s "
                        f"(success rate: {success_rate:.2%})")

        # Reset counters
        self.success_count = 0
        self.failure_count = 0
        self.last_adjustment = now