"""
Auto-Healing Mirror Pool Manager for hifi-bridge.
Fetches, ranks, health-checks, and orchestrates automatic failover
across upstream Hi-Fi API mirrors.
"""

import asyncio
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from typing import Any, Callable, Dict, List, Optional, Tuple
import httpx

from app.database import db
from app.models import MirrorMetrics, MirrorPoolSummary, MirrorStatus

logger = logging.getLogger(__name__)

INSTANCES_MD_URL = "https://raw.githubusercontent.com/monochrome-music/monochrome/main/INSTANCES.md"
VERIFY_TLS = os.environ.get("VERIFY_TLS", "false").lower() in ("true", "1", "yes")
MAX_CONCURRENT_UPSTREAM = int(os.environ.get("MAX_CONCURRENT_UPSTREAM", "20"))

FALLBACK_MIRRORS = [
    "https://tracks.monochrome.st",
    "https://monochrome.tf",
    "https://lossless.wtf",
    "https://if-it-runs-ship-it.lol",
    "https://api.monochrome.tf",
    "https://tidal.kinoplus.online",
]

BLACKLIST_DOMAINS = {"github.com", "rentry.co", "qqdl.site", "parklogic.com"}


class MirrorManager:
    def __init__(self):
        self.mirrors: Dict[str, MirrorMetrics] = {}
        self._lock = asyncio.Lock()
        self._client: Optional[httpx.AsyncClient] = None
        self._health_task: Optional[asyncio.Task] = None
        self._instances_task: Optional[asyncio.Task] = None
        self._running = False
        self._upstream_semaphore: Optional[asyncio.Semaphore] = None

        # Load initial fallbacks
        for url in FALLBACK_MIRRORS:
            self._register_mirror(url, is_custom=False)

        # Load extra mirrors from environment
        extra = os.environ.get("EXTRA_MIRRORS", "")
        if extra:
            for url in extra.split(","):
                u = url.strip().rstrip("/")
                if u:
                    self._register_mirror(u, is_custom=True)

    def _register_mirror(self, url: str, is_custom: bool = False, notes: Optional[str] = None):
        cleaned = url.strip().rstrip("/")
        if not cleaned or not cleaned.startswith(("http://", "https://")):
            return
        for bl in BLACKLIST_DOMAINS:
            if bl in cleaned:
                return
        if cleaned not in self.mirrors:
            self.mirrors[cleaned] = MirrorMetrics(
                url=cleaned,
                status=MirrorStatus.HEALTHY,
                latency_ms=9999.0,
                last_checked=0.0,
                consecutive_failures=0,
                is_custom=is_custom,
                notes=notes
            )

    async def get_client(self) -> httpx.AsyncClient:
        current_loop = asyncio.get_running_loop()
        if (
            self._client is None
            or self._client.is_closed
            or getattr(self, "_client_loop", None) != current_loop
        ):
            if self._client and not self._client.is_closed:
                try:
                    await self._client.aclose()
                except Exception:
                    pass
            self._client = httpx.AsyncClient(
                verify=VERIFY_TLS,
                follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (compatible; hifi-bridge/1.0; +https://github.com/monochrome-music)"},
                timeout=httpx.Timeout(connect=3.0, read=5.0, write=5.0, pool=10.0),
                limits=httpx.Limits(max_keepalive_connections=50, max_connections=100)
            )
            self._client_loop = current_loop
        return self._client

    async def start(self):
        """Starts background loops for discovery and health monitoring."""
        self._running = True
        self._upstream_semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPSTREAM)

        # Load any saved metrics from DB
        saved = await db.load_mirror_metrics()
        for url, data in saved.items():
            if url in self.mirrors:
                self.mirrors[url].latency_ms = data["latency_ms"]
                self.mirrors[url].status = MirrorStatus(data["status"])
                self.mirrors[url].consecutive_failures = data["consecutive_failures"]
                self.mirrors[url].last_checked = data["last_checked"]

        # Run immediate first passes
        await self.refresh_instances_from_github()
        await self.check_all_mirrors_health()

        # Start background schedules
        self._health_task = asyncio.create_task(self._health_check_loop())
        self._instances_task = asyncio.create_task(self._instances_refresh_loop())
        logger.info(f"MirrorManager started with {len(self.mirrors)} mirrors.")

    async def stop(self):
        """Stops background tasks and closes HTTP client."""
        self._running = False
        if self._health_task and not self._health_task.done():
            self._health_task.cancel()
        if self._instances_task and not self._instances_task.done():
            self._instances_task.cancel()
        if self._client and not self._client.is_closed:
            await self._client.aclose()
        self._upstream_semaphore = None
        logger.info("MirrorManager stopped.")

    async def _health_check_loop(self):
        """Background health check every 5 minutes."""
        while self._running:
            try:
                await asyncio.sleep(300)  # 5 minutes
                await self.check_all_mirrors_health()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}", exc_info=True)

    async def _instances_refresh_loop(self):
        """Background instances refresh from INSTANCES.md every 6 hours."""
        while self._running:
            try:
                await asyncio.sleep(21600)  # 6 hours
                await self.refresh_instances_from_github()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in instances refresh loop: {e}", exc_info=True)

    # ── Discovery from INSTANCES.md ──────────────────────────────────────────

    async def refresh_instances_from_github(self) -> List[str]:
        """Fetches and parses live upstream mirrors from INSTANCES.md."""
        logger.info(f"Fetching upstream instances list from {INSTANCES_MD_URL}...")
        client = await self.get_client()
        discovered: List[str] = []
        try:
            resp = await client.get(INSTANCES_MD_URL, timeout=8.0)
            if resp.status_code == 200:
                text = resp.text
                raw_urls = re.findall(r'https?://[a-zA-Z0-9\.\-:\/_]+', text)
                for raw in raw_urls:
                    cleaned = re.sub(r'[`\)\],]+$', '', raw).rstrip('/')
                    try:
                        domain = cleaned.split("://")[1].split("/")[0].lower()
                        if not any(b in domain for b in BLACKLIST_DOMAINS):
                            self._register_mirror(cleaned)
                            discovered.append(cleaned)
                    except Exception:
                        pass
                logger.info(f"Successfully refreshed instances from GitHub. Discovered {len(discovered)} mirrors.")
            else:
                logger.warning(f"Failed to fetch INSTANCES.md: HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"Exception fetching INSTANCES.md: {e}. Keeping existing mirrors.")
        return discovered

    # ── Health & Latency Checks ──────────────────────────────────────────────

    async def check_mirror_health(self, url: str) -> MirrorMetrics:
        """Lightweight probe check on a single mirror."""
        client = await self.get_client()
        metric = self.mirrors.get(url)
        if not metric:
            metric = MirrorMetrics(url=url)
            self.mirrors[url] = metric

        start = time.perf_counter()
        # Lighter probes: minimal search first, then root
        probe_paths = ["/search?q=a&limit=1", "/"]
        success = False
        latency_ms = 9999.0
        status_note = ""

        # Test with short 3s timeout
        for p in probe_paths:
            try:
                probe_url = f"{url}{p}"
                resp = await client.get(probe_url, timeout=3.0)
                elapsed = (time.perf_counter() - start) * 1000.0

                # Validate response: Must be JSON for API endpoints, not HTML / ad parking
                content_type = resp.headers.get("content-type", "").lower()
                body = resp.text[:1000].lower()
                is_ad_redirect = (
                    "redirecting..." in body or
                    "prebid-wrapper" in body or
                    "dfp-ad-container" in body or
                    "parklogic" in body or
                    "<!doctype html>" in body or
                    "text/html" in content_type
                )

                if is_ad_redirect:
                    metric.status = MirrorStatus.DEGRADED
                    metric.notes = "Ad redirect / non-API response"
                    metric.consecutive_failures += 1
                    break

                if resp.status_code == 200 and "application/json" in content_type:
                    success = True
                    latency_ms = round(elapsed, 2)
                    metric.status = MirrorStatus.HEALTHY
                    metric.latency_ms = latency_ms
                    metric.consecutive_failures = 0
                    metric.notes = f"OK ({p} {resp.status_code})"
                    break
                elif resp.status_code in (429, 502, 503, 504):
                    metric.status = MirrorStatus.DEGRADED
                    metric.notes = f"HTTP {resp.status_code}"
                    metric.consecutive_failures += 1
                    break
                elif resp.status_code == 404:
                    # Endpoint may differ, try next probe path
                    continue
            except httpx.TimeoutException:
                status_note = "Timeout (>3s)"
                break
            except Exception as e:
                status_note = str(e)[:60]
                break

        if not success and metric.status != MirrorStatus.DEGRADED:
            metric.consecutive_failures += 1
            if metric.consecutive_failures >= 3:
                metric.status = MirrorStatus.OFFLINE
            else:
                metric.status = MirrorStatus.DEGRADED
            metric.latency_ms = 9999.0
            metric.notes = status_note or "Probe failed"

        metric.last_checked = time.time()
        await db.save_mirror_metric(
            metric.url,
            metric.latency_ms,
            metric.status.value,
            metric.consecutive_failures,
            metric.last_checked,
            metric.notes
        )
        return metric

    async def check_all_mirrors_health(self):
        """Concurrently checks health of all mirrors in pool."""
        logger.info("Running health check on mirror pool...")
        tasks = [self.check_mirror_health(url) for url in list(self.mirrors.keys())]
        await asyncio.gather(*tasks, return_exceptions=True)
        summary = self.get_summary()
        logger.info(
            f"Health check complete: {summary.healthy} healthy, "
            f"{summary.degraded} degraded, {summary.offline} offline. "
            f"Fastest: {summary.fastest_mirror}"
        )

    # ── Ranked Mirror Selection ──────────────────────────────────────────────

    def get_ranked_mirrors(self) -> List[str]:
        """
        Returns mirrors ordered by priority:
        1. Healthy mirrors, sorted by lowest latency
        2. Degraded mirrors (ordered by fewest failures)
        (Offline mirrors excluded unless no other mirrors available)
        """
        healthy = [
            m for m in self.mirrors.values()
            if m.status == MirrorStatus.HEALTHY
        ]
        healthy.sort(key=lambda m: m.latency_ms)

        degraded = [
            m for m in self.mirrors.values()
            if m.status == MirrorStatus.DEGRADED
        ]
        degraded.sort(key=lambda m: (m.consecutive_failures, m.latency_ms))

        offline = [
            m for m in self.mirrors.values()
            if m.status == MirrorStatus.OFFLINE
        ]

        ranked = [m.url for m in healthy] + [m.url for m in degraded]
        if not ranked:
            ranked = [m.url for m in offline] + FALLBACK_MIRRORS
        return list(dict.fromkeys(ranked))

    # ── Rate Limiting ────────────────────────────────────────────────────────

    @asynccontextmanager
    async def rate_limit(self):
        """Context manager for rate-limiting upstream requests."""
        if self._upstream_semaphore:
            async with self._upstream_semaphore:
                yield
        else:
            yield

    # ── Failover Request Execution ───────────────────────────────────────────

    async def request_with_failover(
        self,
        path_template: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        validator: Optional[Callable[[httpx.Response], bool]] = None,
        timeout: float = 3.5,
    ) -> Tuple[httpx.Response, str]:
        """
        Executes an HTTP GET across ranked mirrors with automatic failover.
        Rate-limited by upstream semaphore when available.
        """
        async with self.rate_limit():
            return await self._failover_inner(path_template, params, headers, validator, timeout)

    async def _failover_inner(
        self,
        path_template: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        validator: Optional[Callable[[httpx.Response], bool]] = None,
        timeout: float = 3.5,
    ) -> Tuple[httpx.Response, str]:
        """Inner failover logic without rate limiting."""
        client = await self.get_client()
        ranked = self.get_ranked_mirrors()
        last_error = None

        for mirror_url in ranked:
            full_url = f"{mirror_url.rstrip('/')}{path_template}"
            try:
                resp = await client.get(
                    full_url,
                    params=params,
                    headers=headers,
                    timeout=timeout
                )

                # Check HTTP status
                if resp.status_code == 200:
                    # Check for ad redirect payload
                    text_head = resp.text[:500].lower() if resp.headers.get("content-type", "").startswith(("text/", "application/json")) else ""
                    if "redirecting..." in text_head or "prebid-wrapper" in text_head:
                        self.mark_mirror_failure(mirror_url, "Ad redirect detected")
                        continue

                    # Run custom validator if provided
                    if validator and not validator(resp):
                        self.mark_mirror_failure(mirror_url, "Validation failed")
                        continue

                    # Mirror succeeded
                    return resp, mirror_url

                elif resp.status_code in (429, 502, 503, 504):
                    self.mark_mirror_failure(mirror_url, f"HTTP {resp.status_code}")
                    continue
                elif resp.status_code == 404:
                    # 404 might mean endpoint path differs on this instance; try next
                    continue
                else:
                    self.mark_mirror_failure(mirror_url, f"HTTP {resp.status_code}")
                    continue

            except (httpx.TimeoutException, asyncio.TimeoutError):
                self.mark_mirror_failure(mirror_url, f"Timeout (>{timeout}s)")
                continue
            except Exception as e:
                self.mark_mirror_failure(mirror_url, f"Error: {e}")
                last_error = e
                continue

        error_msg = f"All {len(ranked)} Hi-Fi upstream mirrors failed. Last error: {last_error}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    def mark_mirror_failure(self, url: str, reason: str):
        """Penalizes a failing or slow mirror so it drops down the rankings."""
        if url in self.mirrors:
            self.mirrors[url].consecutive_failures += 1
            if self.mirrors[url].consecutive_failures >= 2:
                self.mirrors[url].status = MirrorStatus.DEGRADED
            self.mirrors[url].latency_ms += 1000.0  # Apply latency penalty
            self.mirrors[url].notes = reason
            logger.warning(f"Mirror penalized: {url} — {reason}")

    def get_summary(self) -> MirrorPoolSummary:
        """Returns statistics for health response."""
        healthy = sum(1 for m in self.mirrors.values() if m.status == MirrorStatus.HEALTHY)
        degraded = sum(1 for m in self.mirrors.values() if m.status == MirrorStatus.DEGRADED)
        offline = sum(1 for m in self.mirrors.values() if m.status == MirrorStatus.OFFLINE)
        ranked = self.get_ranked_mirrors()
        fastest = ranked[0] if ranked else None

        return MirrorPoolSummary(
            total=len(self.mirrors),
            healthy=healthy,
            degraded=degraded,
            offline=offline,
            fastest_mirror=fastest,
            mirrors=list(self.mirrors.values())
        )


# Singleton mirror manager instance
mirror_manager = MirrorManager()
