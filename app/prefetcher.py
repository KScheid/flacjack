"""
Smart Lookahead Prefetcher & Local Audio Disk Cache for hifi-bridge.
Caches audio files locally for 0ms latency playback, immune to upstream rate limits.
Automatically pre-resolves and buffers the next tracks in queue/album in the background.
"""

import asyncio
import logging
import os
import re
import shutil
import time
from typing import Any, Dict, List, Optional, Set
import httpx

from app.database import db
from app.models import AddonTrack, QualityTier

logger = logging.getLogger(__name__)

CACHE_DIR = os.environ.get(
    "AUDIO_CACHE_DIR",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "audio_cache")
)
MAX_CACHE_GB = float(os.environ.get("MAX_CACHE_GB", "5.0"))
PREFETCH_LOOKAHEAD = int(os.environ.get("PREFETCH_LOOKAHEAD", "2"))
PREFETCH_ENABLED = os.environ.get("PREFETCH_ENABLED", "false").lower() in ("true", "1", "yes", "on")


class PrefetchManager:
    def __init__(
        self,
        cache_dir: str = CACHE_DIR,
        max_cache_gb: float = MAX_CACHE_GB,
        lookahead: int = PREFETCH_LOOKAHEAD,
        enabled: bool = PREFETCH_ENABLED,
    ):
        self.cache_dir = cache_dir
        self.max_cache_bytes = int(max_cache_gb * 1024 * 1024 * 1024)
        self.lookahead = lookahead
        self.enabled = enabled
        self._queue: asyncio.Queue = asyncio.Queue()
        self._queued_ids: Set[str] = set()
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False
        self._stream_client: Optional[httpx.AsyncClient] = None
        self._total_cache_hits = 0
        self._total_requests = 0

        os.makedirs(self.cache_dir, exist_ok=True)
        logger.info(
            f"[PREFETCH] Initialized audio cache at {self.cache_dir} "
            f"(Max size: {max_cache_gb} GB, Lookahead: {lookahead} tracks, Enabled: {enabled})"
        )

    async def start(self):
        """Starts the background prefetch worker."""
        if not self.enabled:
            logger.info("[PREFETCH] Background prefetching is disabled by config.")
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        logger.info("[PREFETCH] Background prefetch worker started.")

    async def stop(self):
        """Gracefully stops background worker and closes HTTP client."""
        self._running = False
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
        if self._stream_client and not self._stream_client.is_closed:
            await self._stream_client.aclose()
        logger.info("[PREFETCH] Background prefetch worker stopped.")

    # ── Cache Access & Query ──────────────────────────────────────────────────

    def get_cached_track_path(self, track_id: str) -> Optional[str]:
        """Returns file path if track is fully cached on disk."""
        clean_id = str(track_id).replace(".flac", "").strip()
        for ext in (".flac", ".m4a"):
            path = os.path.join(self.cache_dir, f"{clean_id}{ext}")
            if os.path.isfile(path) and os.path.getsize(path) > 10240:
                # Update atime for LRU tracking
                try:
                    os.utime(path, None)
                except Exception:
                    pass
                return path
        return None

    def record_request(self, is_hit: bool):
        """Tracks cache hit/miss statistics."""
        self._total_requests += 1
        if is_hit:
            self._total_cache_hits += 1

    def get_cache_stats(self) -> Dict[str, Any]:
        """Returns statistics on local disk cache."""
        total_bytes = 0
        total_files = 0
        cached_files = []

        try:
            if os.path.isdir(self.cache_dir):
                for f in os.scandir(self.cache_dir):
                    if f.is_file() and not f.name.startswith(".tmp_"):
                        size = f.stat().st_size
                        total_bytes += size
                        total_files += 1
                        cached_files.append({
                            "filename": f.name,
                            "id": f.name.rsplit(".", 1)[0],
                            "size_bytes": size,
                            "size_mb": round(size / (1024 * 1024), 2),
                            "last_accessed": f.stat().st_atime,
                        })
        except Exception as e:
            logger.warning(f"[PREFETCH] Error reading cache dir stats: {e}")

        # Sort newest accessed first
        cached_files.sort(key=lambda x: x["last_accessed"], reverse=True)

        hit_rate = (
            round((self._total_cache_hits / self._total_requests) * 100, 1)
            if self._total_requests > 0
            else 0.0
        )

        return {
            "enabled": self.enabled,
            "total_files": total_files,
            "total_bytes": total_bytes,
            "total_mb": round(total_bytes / (1024 * 1024), 2),
            "total_gb": round(total_bytes / (1024 * 1024 * 1024), 3),
            "max_gb": round(self.max_cache_bytes / (1024 * 1024 * 1024), 1),
            "usage_percent": (
                round((total_bytes / self.max_cache_bytes) * 100, 1)
                if self.max_cache_bytes > 0
                else 0.0
            ),
            "total_requests": self._total_requests,
            "cache_hits": self._total_cache_hits,
            "hit_rate_percent": hit_rate,
            "queue_depth": self._queue.qsize(),
            "recent_files": cached_files[:20],
        }

    def clear_cache(self) -> int:
        """Removes all cached audio files from disk."""
        deleted = 0
        try:
            for f in os.scandir(self.cache_dir):
                if f.is_file():
                    os.remove(f.path)
                    deleted += 1
            logger.info(f"[PREFETCH] Cleared {deleted} files from audio cache.")
        except Exception as e:
            logger.error(f"[PREFETCH] Error clearing audio cache: {e}")
        return deleted

    # ── Queue Prefetch Strategy ───────────────────────────────────────────────

    def queue_tracks(self, track_ids: List[str]):
        """Queues a list of track IDs for background prefetching."""
        if not self.enabled:
            return
        for tid in track_ids:
            clean_id = str(tid).replace(".flac", "").strip()
            if not clean_id or clean_id in self._queued_ids:
                continue
            if self.get_cached_track_path(clean_id):
                continue  # Already cached on disk
            self._queued_ids.add(clean_id)
            self._queue.put_nowait(clean_id)
            logger.info(f"[PREFETCH] Queued track {clean_id} for background lookahead caching.")

    async def queue_upcoming_from_context(self, current_track_id: str):
        """
        Inspects recent search results and cached albums to predict
        and queue the next upcoming tracks following current_track_id.
        """
        if not self.enabled:
            return
        clean_id = str(current_track_id).replace(".flac", "").strip()

        # Find recent searches in SQLite to locate this track in order
        upcoming_ids: List[str] = []
        try:
            async with db._conn.execute(
                "SELECT results_json FROM search_cache ORDER BY created_at DESC LIMIT 5"
            ) as cursor:
                async for row in cursor:
                    import json
                    data = json.loads(row[0])
                    tracks = data.get("tracks", [])
                    track_ids = [str(t.get("id")) for t in tracks if t.get("id")]
                    if clean_id in track_ids:
                        idx = track_ids.index(clean_id)
                        # Extract next `lookahead` tracks
                        next_slice = track_ids[idx + 1 : idx + 1 + self.lookahead]
                        upcoming_ids.extend(next_slice)
                        break
        except Exception as e:
            logger.debug(f"[PREFETCH] Context lookup note: {e}")

        if upcoming_ids:
            logger.info(f"[PREFETCH] Identified {len(upcoming_ids)} upcoming tracks in queue: {upcoming_ids}")
            self.queue_tracks(upcoming_ids)

    # ── Background Worker Loop ────────────────────────────────────────────────

    async def _worker_loop(self):
        """Continuously pulls track IDs from the queue and downloads them in background."""
        from app.mirror_manager import VERIFY_TLS
        self._stream_client = httpx.AsyncClient(
            verify=VERIFY_TLS,
            follow_redirects=True,
            timeout=httpx.Timeout(connect=5.0, read=90.0, write=5.0, pool=15.0),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )

        while self._running:
            try:
                track_id = await self._queue.get()
                self._queued_ids.discard(track_id)

                # Polite delay to prevent colliding with foreground playback requests
                await asyncio.sleep(2.0)

                # Skip if already downloaded in the meantime
                if self.get_cached_track_path(track_id):
                    self._queue.task_done()
                    continue

                await self._prefetch_track(track_id)
                self._queue.task_done()

                # Check and prune disk space if exceeding limit
                self._prune_cache_if_needed()

                # Sleep between downloads so we don't spike network, rate limits, or CPU
                await asyncio.sleep(2.0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[PREFETCH] Worker loop exception: {e}", exc_info=True)
                await asyncio.sleep(2.0)

    async def _prefetch_track(self, track_id: str):
        """Resolves and downloads a single track into the local audio cache."""
        from app.mirror_manager import mirror_manager
        from app.stream_resolver import stream_resolver

        logger.info(f"[PREFETCH] Starting background download for track {track_id}...")
        temp_path = os.path.join(self.cache_dir, f".tmp_{track_id}")

        try:
            # 1. Resolve candidate URLs
            candidate_urls: List[str] = []
            is_snowflake = bool(re.match(r'^\d{17,20}$', track_id))

            if is_snowflake:
                for m in mirror_manager.get_ranked_mirrors():
                    candidate_urls.append(f"{m}/track/{track_id}")
            else:
                resolved = await stream_resolver.resolve_stream(track_id, QualityTier.HI_RES_LOSSLESS.value)
                if resolved.url and resolved.url.startswith("http") and "/stream/audio/" not in resolved.url:
                    candidate_urls.append(resolved.url)
                for m in mirror_manager.get_ranked_mirrors():
                    candidate_urls.append(f"{m}/track/{track_id}")

            # 2. Try candidate mirrors
            success = False
            for target_url in candidate_urls:
                try:
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                        "Accept": "*/*",
                    }
                    async with self._stream_client.stream("GET", target_url, headers=headers) as resp:
                        if resp.status_code == 429:
                            logger.warning(f"[PREFETCH] Mirror {target_url} returned 429, backing off 2s...")
                            await asyncio.sleep(2.0)
                            continue

                        if resp.status_code not in (200, 206):
                            continue

                        ct = resp.headers.get("content-type", "").lower()
                        if ct.startswith("text/") or "html" in ct or "xml" in ct:
                            continue

                        with open(temp_path, "wb") as f:
                            async for chunk in resp.aiter_bytes(chunk_size=65536):
                                f.write(chunk)

                        success = True
                        break
                except Exception as e:
                    logger.debug(f"[PREFETCH] Candidate {target_url} failed: {e}")
                    continue

            if not success or not os.path.exists(temp_path) or os.path.getsize(temp_path) < 10240:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                logger.warning(f"[PREFETCH] Could not prefetch track {track_id} from any candidate.")
                return

            # 3. Detect container and rename to final
            with open(temp_path, "rb") as f:
                header = f.read(16)
            is_flac = header.startswith(b"fLaC")
            ext = ".flac" if is_flac else ".m4a"

            final_path = os.path.join(self.cache_dir, f"{track_id}{ext}")
            if os.path.exists(final_path):
                os.remove(final_path)
            os.rename(temp_path, final_path)

            size_mb = round(os.path.getsize(final_path) / (1024 * 1024), 2)
            logger.info(f"[PREFETCH] Successfully pre-cached track {track_id} ({size_mb} MB) -> {final_path}")

        except Exception as e:
            logger.error(f"[PREFETCH] Error prefetching track {track_id}: {e}")
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

    # ── LRU Eviction ──────────────────────────────────────────────────────────

    def _prune_cache_if_needed(self):
        """Deletes oldest accessed audio files if total cache exceeds MAX_CACHE_GB."""
        try:
            total_size = 0
            files = []
            for f in os.scandir(self.cache_dir):
                if f.is_file() and not f.name.startswith(".tmp_"):
                    stat = f.stat()
                    total_size += stat.st_size
                    files.append((stat.st_atime, stat.st_size, f.path))

            if total_size <= self.max_cache_bytes:
                return

            # Sort by oldest access time
            files.sort(key=lambda x: x[0])
            target_size = int(self.max_cache_bytes * 0.8)  # Prune to 80%

            pruned_count = 0
            pruned_bytes = 0
            for atime, size, path in files:
                if total_size <= target_size:
                    break
                try:
                    os.remove(path)
                    total_size -= size
                    pruned_bytes += size
                    pruned_count += 1
                except Exception:
                    pass

            logger.info(
                f"[PREFETCH] Cache pruning complete: Deleted {pruned_count} files "
                f"({round(pruned_bytes / (1024 * 1024), 1)} MB reclaimed)."
            )
        except Exception as e:
            logger.warning(f"[PREFETCH] Cache pruning error: {e}")


# Singleton instance
prefetcher = PrefetchManager()
