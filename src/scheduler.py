"""Scheduler for periodic Kleinanzeigen checks - Multi-user version."""

import asyncio
import logging
import random
from datetime import datetime
from typing import Optional, Callable, Awaitable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from .config import Config
from .database import Database
from .fetcher import Fetcher, FetchError
from .parser import Parser, ListingDetails

logger = logging.getLogger(__name__)


class Scheduler:
    """Scheduler for periodic listing checks - Multi-user."""
    
    def __init__(
        self, 
        database: Database,
        send_message: Callable[[int, str], Awaitable[bool]]
    ):
        """
        Initialize scheduler.
        
        Args:
            database: Database instance
            send_message: Async function to send Telegram messages (chat_id, text)
        """
        self.db = database
        self.send_message = send_message
        self.fetcher = Fetcher()
        self.parser = Parser()
        self.scheduler = AsyncIOScheduler()
        self._is_running = False
    
    def start(self) -> None:
        """Start the scheduler with default interval."""
        self.scheduler.add_job(
            self._check_all_users,
            IntervalTrigger(minutes=Config.INTERVAL_MINUTES),
            id='check_all_users',
            name='Check all users queries',
            replace_existing=True
        )
        
        self.scheduler.start()
        logger.info(f"Scheduler started with base interval {Config.INTERVAL_MINUTES} minutes")
    
    def stop(self) -> None:
        """Stop the scheduler."""
        self.scheduler.shutdown(wait=False)
        self.fetcher.close()
        logger.info("Scheduler stopped")
    
    async def run_check_for_user(self, chat_id: int) -> dict:
        """
        Run a check cycle immediately for a specific user (/test command).
        Uses MAX_TEST_LISTINGS limit instead of MAX_NEW_PER_CYCLE.
        
        Args:
            chat_id: User's chat ID
            
        Returns:
            Dict with 'total' and 'new' counts
        """
        return await self._check_user(chat_id, max_limit=Config.MAX_TEST_LISTINGS)
    
    async def _check_all_users(self) -> None:
        """Execute check cycle for all users with enabled queries."""
        if self._is_running:
            logger.warning("Check cycle already running, skipping")
            return
        
        self._is_running = True
        logger.info("Starting check cycle for all users")
        
        try:
            # Get all queries grouped by user
            queries_by_user = self.db.get_all_enabled_queries_grouped()
            
            for chat_id, queries in queries_by_user.items():
                try:
                    # Check user's interval setting
                    user_interval = self.db.get_interval(chat_id)
                    last_check = self.db.get_last_check(chat_id)
                    
                    # Skip if user's custom interval hasn't elapsed
                    if last_check and user_interval > Config.INTERVAL_MINUTES:
                        last_time = datetime.fromisoformat(last_check['check_time'])
                        elapsed = (datetime.now() - last_time).total_seconds() / 60
                        if elapsed < user_interval:
                            logger.debug(f"Skipping user {chat_id}, interval not elapsed")
                            continue
                    
                    await self._check_user(chat_id)
                    
                    # Delay between users
                    await self.fetcher.delay(min_sec=1.0, max_sec=2.0)
                    
                except Exception as e:
                    logger.error(f"Error checking user {chat_id}: {e}")
                    continue
                    
        finally:
            self._is_running = False
            logger.info("Check cycle complete for all users")
    
    async def _check_user(self, chat_id: int, max_limit: Optional[int] = None) -> dict:
        """
        Check all queries for a specific user.
        
        Args:
            chat_id: User's chat ID
            max_limit: Override max listings per cycle (for /test command)
            
        Returns:
            Dict with results
        """
        queries = self.db.get_enabled_queries(chat_id)
        
        if not queries:
            return {"total": 0, "new": 0}
        
        logger.info(f"Checking {len(queries)} queries for user {chat_id}")
        
        total_found = 0
        new_found = 0
        errors = 0
        
        # Determine per-cycle limit (<=0 means unlimited)
        effective_limit = max_limit if max_limit is not None else Config.MAX_NEW_PER_CYCLE
        limit_enabled = effective_limit > 0
        
        for query in queries:
            try:
                per_query_limit = (
                    effective_limit - new_found if limit_enabled else None
                )
                found, new = await self._check_query(chat_id, query, per_query_limit)
                total_found += found
                new_found += new
                
                # Stop if we hit the limit across all queries
                if limit_enabled and new_found >= effective_limit:
                    break
            except Exception as e:
                logger.error(f"Error checking query {query['id']} for user {chat_id}: {e}")
                errors += 1
            
            # Delay between queries
            if query != queries[-1]:
                await self.fetcher.delay()
        
        # Record stats for user
        self.db.record_check(chat_id, total_found, new_found, errors)
        
        logger.info(
            f"User {chat_id}: found={total_found}, new={new_found}, errors={errors}"
        )
        
        return {
            "total": total_found,
            "new": new_found,
            "errors": errors
        }
    
    async def _check_query(
        self,
        chat_id: int,
        query: dict,
        max_remaining: Optional[int] = 30,
    ) -> tuple[int, int]:
        """
        Check a single search query for new listings.
        
        Args:
            chat_id: User's chat ID
            query: Query dict from database
            max_remaining: Maximum new listings to process for this query (None = unlimited)
            
        Returns:
            Tuple of (total_found, new_sent)
        """
        query_id = query['id']
        url = query['url']
        
        logger.info(f"Checking query {query_id} for user {chat_id}")
        
        # Fetch search page
        try:
            html = await self.fetcher.fetch(url)
        except FetchError as e:
            logger.error(f"Failed to fetch search page: {e}")
            raise
        
        # Parse listings
        previews = self.parser.parse_search_page(html)
        total_found = len(previews)
        
        if not previews:
            logger.info(f"No listings found for query {query_id}")
            return 0, 0
        
        # Process new listings
        new_count = 0
        sent_count = 0
        
        for preview in previews:
            # Check if already seen BY THIS USER
            if self.db.is_listing_seen(chat_id, preview.listing_id):
                continue
            
            new_count += 1
            
            # Check limit if enabled
            if max_remaining is not None and sent_count >= max_remaining:
                logger.info(f"Hit limit ({max_remaining}) for this query")
                break
            
            # Fetch and parse listing details
            try:
                details = await self._fetch_listing_details(preview, query_id)
                if details:
                    # Send message to user
                    success = await self._send_listing(chat_id, details)
                    if success:
                        sent_count += 1
                        # Mark as sent for this user
                        self.db.mark_listing_sent(
                            chat_id,
                            preview.listing_id,
                            preview.url,
                            query_id
                        )
                    
                    # Delay between messages
                    await asyncio.sleep(
                        random.uniform(
                            Config.MIN_DELAY_BETWEEN_MESSAGES,
                            Config.MAX_DELAY_BETWEEN_MESSAGES
                        )
                    )
            except Exception as e:
                logger.error(f"Error processing listing {preview.listing_id}: {e}")
                # Still mark as seen to avoid repeated errors
                self.db.mark_listing_sent(chat_id, preview.listing_id, preview.url, query_id)
            
            # Delay between listing page requests
            await self.fetcher.delay(min_sec=1.0, max_sec=2.0)
        
        logger.info(f"Query {query_id}: {total_found} found, {new_count} new, {sent_count} sent")
        return total_found, sent_count
    
    async def _fetch_listing_details(
        self, 
        preview, 
        query_id: int
    ) -> Optional[ListingDetails]:
        """
        Fetch and parse listing details page.
        
        Args:
            preview: ListingPreview from search results
            query_id: Query ID for context
            
        Returns:
            ListingDetails or None on error
        """
        try:
            html = await self.fetcher.fetch(preview.url)
            return self.parser.parse_listing_page(
                html, 
                preview.url, 
                preview.listing_id
            )
        except FetchError as e:
            logger.error(f"Failed to fetch listing {preview.listing_id}: {e}")
            # Return partial details with what we have
            return ListingDetails(
                listing_id=preview.listing_id,
                url=preview.url,
                title=preview.title,
                brand="—",
                year="—",
                price="—",
                location="—",
                plz="—"
            )
    
    async def _send_listing(self, chat_id: int, details: ListingDetails) -> bool:
        """
        Send a listing notification to a user.
        
        Args:
            chat_id: User's chat ID
            details: Parsed listing details
            
        Returns:
            True if sent successfully
        """
        message = details.format_message()
        
        for attempt in range(Config.MAX_RETRIES + 1):
            try:
                success = await self.send_message(chat_id, message)
                if success:
                    logger.debug(f"Sent listing {details.listing_id} to user {chat_id}")
                    return True
            except Exception as e:
                logger.warning(f"Send attempt {attempt + 1} failed: {e}")
                if attempt < Config.MAX_RETRIES:
                    await asyncio.sleep(Config.RETRY_DELAY)
        
        logger.error(f"Failed to send listing {details.listing_id} to user {chat_id}")
        return False
