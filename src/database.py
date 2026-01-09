"""SQLite database storage layer for Kleinanzeigen bot - Multi-user version."""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional
from contextlib import contextmanager

from .config import Config


class Database:
    """SQLite database handler for queries and seen listings - supports multiple users."""
    
    def __init__(self, db_path: Optional[str] = None):
        """Initialize database connection."""
        self.db_path = db_path or Config.DATABASE_PATH
        self._ensure_db_dir()
        self._init_schema()
    
    def _ensure_db_dir(self) -> None:
        """Ensure database directory exists."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
    
    @contextmanager
    def _get_connection(self):
        """Get database connection context manager."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _init_schema(self) -> None:
        """Initialize database schema."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Users table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    chat_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_active TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Search queries table - linked to user
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS queries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    url TEXT NOT NULL,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (chat_id) REFERENCES users(chat_id),
                    UNIQUE(chat_id, url)
                )
            """)
            
            # Seen listings table (deduplication) - per user
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS seen_listings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    listing_id TEXT NOT NULL,
                    chat_id INTEGER NOT NULL,
                    url TEXT NOT NULL,
                    query_id INTEGER,
                    first_seen_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    sent_at TEXT,
                    FOREIGN KEY (query_id) REFERENCES queries(id),
                    FOREIGN KEY (chat_id) REFERENCES users(chat_id),
                    UNIQUE(listing_id, chat_id)
                )
            """)
            
            # Settings table - per user
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_settings (
                    chat_id INTEGER NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY (chat_id, key),
                    FOREIGN KEY (chat_id) REFERENCES users(chat_id)
                )
            """)
            
            # Stats table for tracking - per user
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    check_time TEXT DEFAULT CURRENT_TIMESTAMP,
                    total_found INTEGER DEFAULT 0,
                    new_found INTEGER DEFAULT 0,
                    errors INTEGER DEFAULT 0,
                    FOREIGN KEY (chat_id) REFERENCES users(chat_id)
                )
            """)
    
    # ===== User Management =====
    
    def register_user(
        self, 
        chat_id: int, 
        username: Optional[str] = None, 
        first_name: Optional[str] = None
    ) -> None:
        """Register or update a user."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO users (chat_id, username, first_name, last_active)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_active = excluded.last_active
                """,
                (chat_id, username, first_name, datetime.now().isoformat())
            )
    
    def get_all_users_with_queries(self) -> list[dict]:
        """Get all users who have at least one enabled query."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT u.* FROM users u
                INNER JOIN queries q ON u.chat_id = q.chat_id
                WHERE q.enabled = 1
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def update_user_activity(self, chat_id: int) -> None:
        """Update user's last active timestamp."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET last_active = ? WHERE chat_id = ?",
                (datetime.now().isoformat(), chat_id)
            )
    
    # ===== Query Management =====
    
    def add_query(self, chat_id: int, url: str) -> int:
        """Add a search URL for a user. Returns query ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO queries (chat_id, url) VALUES (?, ?)",
                (chat_id, url)
            )
            return cursor.lastrowid
    
    def ensure_default_query(self, chat_id: int) -> None:
        """Ensure the user's default constant query is enabled and others disabled."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Disable any custom queries that might exist
            cursor.execute(
                "UPDATE queries SET enabled = 0 WHERE chat_id = ? AND url != ?",
                (chat_id, Config.SEARCH_URL)
            )
            # Enable or insert the default query
            cursor.execute(
                """
                INSERT INTO queries (chat_id, url, enabled)
                VALUES (?, ?, 1)
                ON CONFLICT(chat_id, url) DO UPDATE SET enabled = 1
                """,
                (chat_id, Config.SEARCH_URL)
            )
    
    def disable_user_queries(self, chat_id: int) -> None:
        """Disable all queries for a user (unsubscribe)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE queries SET enabled = 0 WHERE chat_id = ?",
                (chat_id,)
            )
    
    def has_enabled_query(self, chat_id: int) -> bool:
        """Check if user currently has the default query enabled."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM queries WHERE chat_id = ? AND enabled = 1 LIMIT 1",
                (chat_id,)
            )
            return cursor.fetchone() is not None
    
    def remove_query(self, chat_id: int, query_id: int) -> bool:
        """Remove a search URL by ID for a specific user. Returns True if removed."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM queries WHERE id = ? AND chat_id = ?", 
                (query_id, chat_id)
            )
            return cursor.rowcount > 0
    
    def get_query(self, chat_id: int, query_id: int) -> Optional[dict]:
        """Get a query by ID for a specific user."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM queries WHERE id = ? AND chat_id = ?", 
                (query_id, chat_id)
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_enabled_queries(self, chat_id: int) -> list[dict]:
        """Get all enabled search queries for a user."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM queries WHERE chat_id = ? AND enabled = 1 ORDER BY id",
                (chat_id,)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_all_queries(self, chat_id: int) -> list[dict]:
        """Get all search queries for a user."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM queries WHERE chat_id = ? ORDER BY id",
                (chat_id,)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_all_enabled_queries_grouped(self) -> dict[int, list[dict]]:
        """Get all enabled queries grouped by chat_id for scheduler."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM queries WHERE enabled = 1 ORDER BY chat_id, id"
            )
            result = {}
            for row in cursor.fetchall():
                row_dict = dict(row)
                chat_id = row_dict['chat_id']
                if chat_id not in result:
                    result[chat_id] = []
                result[chat_id].append(row_dict)
            return result
    
    def toggle_query(self, chat_id: int, query_id: int, enabled: bool) -> bool:
        """Enable or disable a query. Returns True if updated."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE queries SET enabled = ? WHERE id = ? AND chat_id = ?",
                (1 if enabled else 0, query_id, chat_id)
            )
            return cursor.rowcount > 0
    
    # ===== Listing Management =====
    
    def is_listing_seen(self, chat_id: int, listing_id: str) -> bool:
        """Check if a listing has been seen before by this user."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM seen_listings WHERE listing_id = ? AND chat_id = ?",
                (listing_id, chat_id)
            )
            return cursor.fetchone() is not None
    
    def mark_listing_sent(
        self, 
        chat_id: int,
        listing_id: str, 
        url: str, 
        query_id: Optional[int] = None
    ) -> None:
        """Mark a listing as sent for a user."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            cursor.execute(
                """
                INSERT OR REPLACE INTO seen_listings 
                (listing_id, chat_id, url, query_id, first_seen_at, sent_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (listing_id, chat_id, url, query_id, now, now)
            )
    
    def get_seen_listings_count(self, chat_id: int) -> int:
        """Get total count of seen listings for a user."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM seen_listings WHERE chat_id = ?",
                (chat_id,)
            )
            return cursor.fetchone()[0]
    
    # ===== Settings Management =====
    
    def get_setting(
        self, 
        chat_id: int, 
        key: str, 
        default: Optional[str] = None
    ) -> Optional[str]:
        """Get a setting value for a user."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT value FROM user_settings WHERE chat_id = ? AND key = ?", 
                (chat_id, key)
            )
            row = cursor.fetchone()
            return row[0] if row else default
    
    def set_setting(self, chat_id: int, key: str, value: str) -> None:
        """Set a setting value for a user."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO user_settings (chat_id, key, value) 
                VALUES (?, ?, ?)
                """,
                (chat_id, key, value)
            )
    
    def get_interval(self, chat_id: int) -> int:
        """Get check interval in minutes for a user."""
        value = self.get_setting(chat_id, "interval_minutes")
        return int(value) if value else Config.INTERVAL_MINUTES
    
    def set_interval(self, chat_id: int, minutes: int) -> None:
        """Set check interval in minutes for a user."""
        self.set_setting(chat_id, "interval_minutes", str(minutes))
    
    # ===== Stats Management =====
    
    def record_check(
        self, 
        chat_id: int,
        total_found: int = 0, 
        new_found: int = 0, 
        errors: int = 0
    ) -> None:
        """Record a check cycle stats for a user."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO stats (chat_id, total_found, new_found, errors)
                VALUES (?, ?, ?, ?)
                """,
                (chat_id, total_found, new_found, errors)
            )
    
    def get_last_check(self, chat_id: int) -> Optional[dict]:
        """Get the last check stats for a user."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM stats WHERE chat_id = ? ORDER BY id DESC LIMIT 1",
                (chat_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_stats_summary(self, chat_id: int) -> dict:
        """Get overall stats summary for a user."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Total checks
            cursor.execute(
                "SELECT COUNT(*) FROM stats WHERE chat_id = ?", 
                (chat_id,)
            )
            total_checks = cursor.fetchone()[0]
            
            # Total new found
            cursor.execute(
                "SELECT COALESCE(SUM(new_found), 0) FROM stats WHERE chat_id = ?",
                (chat_id,)
            )
            total_new = cursor.fetchone()[0]
            
            # Total errors
            cursor.execute(
                "SELECT COALESCE(SUM(errors), 0) FROM stats WHERE chat_id = ?",
                (chat_id,)
            )
            total_errors = cursor.fetchone()[0]
            
            return {
                "total_checks": total_checks,
                "total_new_found": total_new,
                "total_errors": total_errors
            }
