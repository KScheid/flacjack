"""
Asynchronous SQLite database interface using aiosqlite.
Manages metadata caching, stream manifest caching, and mirror performance history.
Uses a persistent connection with WAL mode for concurrent read support.
"""

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Optional
import aiosqlite

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "hifi_bridge.db"))
MAX_CACHE_ENTRIES = int(os.environ.get("MAX_CACHE_ENTRIES", "10000"))


class Database:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None
        self._write_lock: Optional[asyncio.Lock] = None
        self._loop = None
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)

    async def init_db(self):
        """Initializes persistent SQLite connection with WAL mode and creates tables."""
        current_loop = asyncio.get_running_loop()

        # If already initialized in the same event loop, skip
        if self._conn is not None and self._loop == current_loop:
            return

        # Close stale connection from a different event loop (e.g. test runner)
        if self._conn is not None:
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = None

        self._loop = current_loop
        self._write_lock = asyncio.Lock()
        self._conn = await aiosqlite.connect(self.db_path)

        # WAL mode allows concurrent reads while writing
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA busy_timeout=5000")
        await self._conn.execute("PRAGMA synchronous=NORMAL")

        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS search_cache (
                cache_key TEXT PRIMARY KEY,
                results_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL
            )
        """)
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_search_expires ON search_cache (expires_at)
        """)

        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS track_cache (
                track_id TEXT PRIMARY KEY,
                track_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL
            )
        """)
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_track_expires ON track_cache (expires_at)
        """)

        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS stream_cache (
                cache_key TEXT PRIMARY KEY,
                stream_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL
            )
        """)
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_stream_expires ON stream_cache (expires_at)
        """)

        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS mirror_metrics (
                url TEXT PRIMARY KEY,
                latency_ms REAL NOT NULL,
                status TEXT NOT NULL,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                last_checked REAL NOT NULL,
                notes TEXT
            )
        """)
        await self._conn.commit()
        logger.info(f"Database initialized at {self.db_path}")

    async def close(self):
        """Closes the persistent database connection."""
        if self._conn:
            try:
                await self._conn.close()
            except Exception as e:
                logger.warning(f"Error closing database: {e}")
            self._conn = None
            self._write_lock = None
            self._loop = None

    # ── Search Cache ──────────────────────────────────────────────────────────

    async def get_cached_search(self, query: str, quality: str, limit: int, offset: int) -> Optional[Dict[str, Any]]:
        cache_key = f"{query.lower().strip()}:{quality}:{limit}:{offset}"
        now = time.time()
        try:
            async with self._conn.execute(
                "SELECT results_json FROM search_cache WHERE cache_key = ? AND expires_at > ?",
                (cache_key, now)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return json.loads(row[0])
        except Exception as e:
            logger.warning(f"Error reading search cache: {e}")
        return None

    async def set_cached_search(self, query: str, quality: str, limit: int, offset: int, data: Dict[str, Any], ttl: int = 3600):
        cache_key = f"{query.lower().strip()}:{quality}:{limit}:{offset}"
        now = time.time()
        expires_at = now + ttl
        try:
            async with self._write_lock:
                await self._conn.execute(
                    "INSERT OR REPLACE INTO search_cache (cache_key, results_json, created_at, expires_at) VALUES (?, ?, ?, ?)",
                    (cache_key, json.dumps(data), now, expires_at)
                )
                await self._conn.commit()
        except Exception as e:
            logger.warning(f"Error writing search cache: {e}")

    # ── Track Metadata Cache ──────────────────────────────────────────────────

    async def get_cached_track(self, track_id: str) -> Optional[Dict[str, Any]]:
        now = time.time()
        try:
            async with self._conn.execute(
                "SELECT track_json FROM track_cache WHERE track_id = ? AND expires_at > ?",
                (str(track_id), now)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return json.loads(row[0])
        except Exception as e:
            logger.warning(f"Error reading track cache: {e}")
        return None

    async def set_cached_track(self, track_id: str, data: Dict[str, Any], ttl: int = 86400):
        now = time.time()
        expires_at = now + ttl
        try:
            async with self._write_lock:
                await self._conn.execute(
                    "INSERT OR REPLACE INTO track_cache (track_id, track_json, created_at, expires_at) VALUES (?, ?, ?, ?)",
                    (str(track_id), json.dumps(data), now, expires_at)
                )
                await self._conn.commit()
        except Exception as e:
            logger.warning(f"Error writing track cache: {e}")

    # ── Stream Resolution Cache ───────────────────────────────────────────────

    async def get_cached_stream(self, track_id: str, quality: str) -> Optional[Dict[str, Any]]:
        cache_key = f"{track_id}:{quality}"
        now = time.time()
        try:
            async with self._conn.execute(
                "SELECT stream_json FROM stream_cache WHERE cache_key = ? AND expires_at > ?",
                (cache_key, now)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return json.loads(row[0])
        except Exception as e:
            logger.warning(f"Error reading stream cache: {e}")
        return None

    async def set_cached_stream(self, track_id: str, quality: str, data: Dict[str, Any], ttl: int = 3600):
        cache_key = f"{track_id}:{quality}"
        now = time.time()
        expires_at = now + ttl
        try:
            async with self._write_lock:
                await self._conn.execute(
                    "INSERT OR REPLACE INTO stream_cache (cache_key, stream_json, created_at, expires_at) VALUES (?, ?, ?, ?)",
                    (cache_key, json.dumps(data), now, expires_at)
                )
                await self._conn.commit()
        except Exception as e:
            logger.warning(f"Error writing stream cache: {e}")

    # ── Mirror Metrics ────────────────────────────────────────────────────────

    async def save_mirror_metric(self, url: str, latency_ms: float, status: str, failures: int, last_checked: float, notes: Optional[str] = None):
        try:
            async with self._write_lock:
                await self._conn.execute(
                    "INSERT OR REPLACE INTO mirror_metrics (url, latency_ms, status, consecutive_failures, last_checked, notes) VALUES (?, ?, ?, ?, ?, ?)",
                    (url, latency_ms, status, failures, last_checked, notes)
                )
                await self._conn.commit()
        except Exception as e:
            logger.warning(f"Error saving mirror metric for {url}: {e}")

    async def load_mirror_metrics(self) -> Dict[str, Dict[str, Any]]:
        metrics: Dict[str, Dict[str, Any]] = {}
        try:
            async with self._conn.execute("SELECT url, latency_ms, status, consecutive_failures, last_checked, notes FROM mirror_metrics") as cursor:
                async for row in cursor:
                    metrics[row[0]] = {
                        "url": row[0],
                        "latency_ms": row[1],
                        "status": row[2],
                        "consecutive_failures": row[3],
                        "last_checked": row[4],
                        "notes": row[5]
                    }
        except Exception as e:
            logger.warning(f"Error loading mirror metrics: {e}")
        return metrics

    # ── Maintenance ───────────────────────────────────────────────────────────

    async def cleanup_expired(self) -> int:
        """Removes expired cache entries and enforces MAX_CACHE_ENTRIES limit per table."""
        now = time.time()
        total_deleted = 0
        try:
            async with self._write_lock:
                # Remove expired entries
                cur1 = await self._conn.execute("DELETE FROM search_cache WHERE expires_at <= ?", (now,))
                cur2 = await self._conn.execute("DELETE FROM track_cache WHERE expires_at <= ?", (now,))
                cur3 = await self._conn.execute("DELETE FROM stream_cache WHERE expires_at <= ?", (now,))
                total_deleted = cur1.rowcount + cur2.rowcount + cur3.rowcount

                # Enforce max cache size per table (LRU eviction by created_at)
                for table, key_col in [("search_cache", "cache_key"), ("track_cache", "track_id"), ("stream_cache", "cache_key")]:
                    count_cursor = await self._conn.execute(f"SELECT COUNT(*) FROM {table}")
                    count_row = await count_cursor.fetchone()
                    count = count_row[0] if count_row else 0
                    if count > MAX_CACHE_ENTRIES:
                        excess = count - MAX_CACHE_ENTRIES
                        await self._conn.execute(
                            f"DELETE FROM {table} WHERE {key_col} IN "
                            f"(SELECT {key_col} FROM {table} ORDER BY created_at ASC LIMIT ?)",
                            (excess,)
                        )
                        total_deleted += excess

                await self._conn.commit()
        except Exception as e:
            logger.warning(f"Error cleaning up cache: {e}")
        return total_deleted

    async def get_total_cache_entries(self) -> int:
        count = 0
        try:
            for table in ["search_cache", "track_cache", "stream_cache"]:
                async with self._conn.execute(f"SELECT COUNT(*) FROM {table}") as cur:
                    row = await cur.fetchone()
                    if row:
                        count += row[0]
        except Exception:
            pass
        return count


# Singleton instance
db = Database()
