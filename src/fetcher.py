"""HTTP fetcher with retry logic for Kleinanzeigen pages."""

import asyncio
import logging
import random
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import Config

logger = logging.getLogger(__name__)


class FetchError(Exception):
    """Exception raised when fetching fails."""
    pass


class Fetcher:
    """HTTP client with retry logic and rate limiting."""
    
    def __init__(self):
        """Initialize HTTP session with retry adapter."""
        self.session = requests.Session()
        self._lock = asyncio.Lock()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=Config.MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set headers
        self.session.headers.update({
            "User-Agent": Config.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0",
        })
    
    async def fetch(self, url: str, retry_count: int = 0) -> str:
        """
        Fetch a URL and return HTML content without blocking the event loop.
        
        Args:
            url: URL to fetch
            retry_count: Current retry attempt
            
        Returns:
            HTML content as string
            
        Raises:
            FetchError: If fetch fails after retries
        """
        async with self._lock:
            return await asyncio.to_thread(self._fetch_sync, url, retry_count)
    
    def _fetch_sync(self, url: str, retry_count: int = 0) -> str:
        """Synchronous fetch implementation executed in a thread."""
        try:
            logger.debug(f"Fetching: {url}")
            response = self.session.get(
                url, 
                timeout=Config.REQUEST_TIMEOUT
            )
            
            # Check for blocking
            if response.status_code == 403:
                logger.warning(f"403 Forbidden for {url} - possibly blocked")
                raise FetchError(f"Access denied (403) for {url}")
            
            if response.status_code == 429:
                logger.warning(f"429 Too Many Requests for {url}")
                raise FetchError(f"Rate limited (429) for {url}")
            
            response.raise_for_status()
            
            logger.debug(f"Fetched {len(response.text)} bytes from {url}")
            return response.text
            
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching {url}")
            if retry_count < Config.MAX_RETRIES:
                time.sleep(Config.RETRY_DELAY * (retry_count + 1))
                return self._fetch_sync(url, retry_count + 1)
            raise FetchError(f"Timeout after {Config.MAX_RETRIES} retries: {url}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {url}: {e}")
            if retry_count < Config.MAX_RETRIES:
                time.sleep(Config.RETRY_DELAY * (retry_count + 1))
                return self._fetch_sync(url, retry_count + 1)
            raise FetchError(f"Failed to fetch {url}: {e}")
    
    async def delay(self, min_sec: Optional[float] = None, max_sec: Optional[float] = None) -> None:
        """Async-friendly random delay helper."""
        min_delay = min_sec or Config.MIN_DELAY_BETWEEN_REQUESTS
        max_delay = max_sec or Config.MAX_DELAY_BETWEEN_REQUESTS
        delay = random.uniform(min_delay, max_delay)
        logger.debug(f"Sleeping for {delay:.2f} seconds")
        await asyncio.sleep(delay)
    
    def close(self) -> None:
        """Close the session."""
        self.session.close()
