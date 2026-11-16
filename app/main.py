"""
hifi-bridge — Self-Hosted Hi-Fi Lossless Aggregator & BitChord Source Module.
Production-ready FastAPI microservice bridging public Hi-Fi lossless audio APIs
to the BitChord Android music client.
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from app.archiver import archiver
from app.dashboard import render_dashboard_html
from app.database import db
from app.mirror_manager import mirror_manager
from app.models import (
    AddonManifest,
    AddonSearchResponse,
    AddonSetting,
    AddonSettingOption,
    AddonStream,
    AddonTrack,
    AlbumDetail,
    ArtistDetail,
    HealthResponse,
    MirrorPoolSummary,
    QualityTier,
)
from app.prefetcher import prefetcher
from app.stream_resolver import stream_resolver

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("hifi-bridge")

START_TIME = time.time()
DEFAULT_QUALITY = os.environ.get("DEFAULT_QUALITY", QualityTier.HI_RES_LOSSLESS.value)


def get_request_base_url(request: Request) -> str:
    """Dynamically reconstructs client-facing base URL considering reverse proxies and headers."""
    forwarded_proto = request.headers.get("x-forwarded-proto")
    forwarded_host = request.headers.get("x-forwarded-host")
    if forwarded_host:
        proto = forwarded_proto or "http"
        return f"{proto}://{forwarded_host}"

    host = request.headers.get("host")
    if host:
        proto = forwarded_proto or request.url.scheme or "http"
        return f"{proto}://{host}"

    return str(request.base_url).rstrip("/")


def build_addon_manifest(base_url: str) -> AddonManifest:
    """Builds the BitChord-compatible AddonManifest descriptor."""
    return AddonManifest(
        id="flacjack",
        name="FlacJack 🥞",
        version="2.0.0",
        description="Fresh, warm stacks of lossless audio on demand (BitChord Source Module)",
        baseUrl=base_url,
        resources=["search", "stream"],
        settings=[
            AddonSetting(
                key="quality",
                default=DEFAULT_QUALITY,
                options=[
                    AddonSettingOption(
                        value=QualityTier.HI_RES_LOSSLESS.value,
                        label="Hi-Res Lossless (Up to 24-bit/192kHz FLAC)"
                    ),
                    AddonSettingOption(
                        value=QualityTier.LOSSLESS.value,
                        label="Lossless (16-bit/44.1kHz FLAC)"
                    ),
                    AddonSettingOption(
                        value=QualityTier.HIGH.value,
                        label="High (320kbps AAC)"
                    ),
                ]
            )
        ],
        allowDownloads=True,
        checkValidLossless=False,
        bitchordAuth=False,
    )


async def _cache_cleanup_loop():
    """Background task that evicts expired cache entries every 30 minutes."""
    while True:
        try:
            await asyncio.sleep(1800)  # 30 minutes
            deleted = await db.cleanup_expired()
            if deleted > 0:
                logger.info(f"Cache cleanup: removed {deleted} expired entries")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in cache cleanup loop: {e}", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown orchestration."""
    logger.info("Initializing hifi-bridge microservice...")
    await db.init_db()
    await mirror_manager.start()
    await prefetcher.start()
    cleanup_task = asyncio.create_task(_cache_cleanup_loop())
    yield
    logger.info("Shutting down hifi-bridge...")
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass
    await prefetcher.stop()
    await archiver.shutdown()
    await mirror_manager.stop()
    await db.close()


app = FastAPI(
    title="flacjack",
    description="Fresh, warm stacks of lossless audio on demand & BitChord Source Module",
    version="2.0.0",
    lifespan=lifespan,
)

# Enable CORS for cross-origin web apps and mobile webviews
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Health & Status Endpoints ────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["Monitoring"])
async def health_check():
    """System health check and live mirror pool summary."""
    uptime = time.time() - START_TIME
    summary = mirror_manager.get_summary()
    cache_count = await db.get_total_cache_entries()

    return HealthResponse(
        status="healthy" if summary.healthy > 0 else "degraded",
        version="1.0.0",
        uptime_seconds=round(uptime, 2),
        mirror_pool=summary,
        cache_entries=cache_count,
        archiving_enabled=archiver.enabled,
    )


@app.get("/mirrors", response_model=MirrorPoolSummary, tags=["Monitoring"])
async def get_mirrors():
    """Returns detailed statistics and latency metrics for all upstream mirrors."""
    return mirror_manager.get_summary()


@app.post("/mirrors/refresh", tags=["Monitoring"])
async def refresh_mirrors():
    """Manually triggers mirror list refresh from INSTANCES.md and re-runs health probes."""
    discovered = await mirror_manager.refresh_instances_from_github()
    await mirror_manager.check_all_mirrors_health()
    return {"status": "ok", "discovered_count": len(discovered), "mirrors": mirror_manager.get_summary()}


# ── BitChord Custom Module / Addon Protocol Endpoints ────────────────────────

@app.get("/manifest.json", response_model=AddonManifest, tags=["BitChord"])
async def get_manifest(request: Request):
    """Standard BitChord addon manifest discovery endpoint."""
    base_url = get_request_base_url(request)
    return build_addon_manifest(base_url)


@app.get("/bitchord-module.json", response_model=AddonManifest, tags=["BitChord"])
async def get_bitchord_module(request: Request):
    """
    Dynamic BitChord custom module configuration endpoint.
    Automatically populates baseUrl using request host / headers so importing works
    seamlessly over LAN, reverse proxy, or Tailscale.
    """
    base_url = get_request_base_url(request)
    return build_addon_manifest(base_url)


# ── Search & Track Metadata Endpoints ────────────────────────────────────────

@app.get("/search", response_model=AddonSearchResponse, tags=["Audio"])
async def search_tracks(
    request: Request,
    q: Optional[str] = Query(None, description="Search query string"),
    s: Optional[str] = Query(None, description="Alternative search query parameter (Monochrome)"),
    quality: str = Query(DEFAULT_QUALITY, description="Target audio quality tier"),
    limit: int = Query(25, ge=1, le=100, description="Max tracks to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    """
    Unified track search endpoint matching BitChord Addon protocol.
    Accepts both ?q= and ?s= parameters. Returns normalized AddonTrack rows.
    """
    query = (q or s or "").strip()
    if not query:
        return AddonSearchResponse(tracks=[])

    base_url = get_request_base_url(request)
    return await stream_resolver.search(
        query=query,
        quality=quality,
        limit=limit,
        offset=offset,
        base_url=base_url
    )


@app.get("/track/{track_id}", response_model=AddonTrack, tags=["Audio"])
@app.get("/track", response_model=AddonTrack, tags=["Audio"])
async def get_track(
    request: Request,
    track_id: Optional[str] = None,
    id: Optional[str] = Query(None, description="Track ID query parameter")
):
    """Fetches normalized metadata for a single track by ID."""
    clean_id = (track_id or id or "").strip()
    if not clean_id:
        raise HTTPException(status_code=400, detail="Missing track ID parameter")

    base_url = get_request_base_url(request)
    track = await stream_resolver.get_track(clean_id, base_url)
    if not track:
        raise HTTPException(status_code=404, detail=f"Track '{clean_id}' not found")
    return track


# ── Album & Artist Endpoints ─────────────────────────────────────────────────

@app.get("/album/{album_id}", response_model=AlbumDetail, tags=["Metadata"])
@app.get("/album", response_model=AlbumDetail, tags=["Metadata"])
async def get_album(
    request: Request,
    album_id: Optional[str] = None,
    id: Optional[str] = Query(None, description="Album ID query parameter")
):
    """Fetches album details and track list."""
    clean_id = (album_id or id or "").strip()
    if not clean_id:
        raise HTTPException(status_code=400, detail="Missing album ID parameter")

    base_url = get_request_base_url(request)
    return await stream_resolver.get_album(clean_id, base_url)


@app.get("/artist/{artist_id}", response_model=ArtistDetail, tags=["Metadata"])
@app.get("/artist", response_model=ArtistDetail, tags=["Metadata"])
async def get_artist(
    request: Request,
    artist_id: Optional[str] = None,
    id: Optional[str] = Query(None, description="Artist ID query parameter")
):
    """Fetches artist details and top tracks."""
    clean_id = (artist_id or id or "").strip()
    if not clean_id:
        raise HTTPException(status_code=400, detail="Missing artist ID parameter")

    base_url = get_request_base_url(request)
    return await stream_resolver.get_artist(clean_id, base_url)


# ── Stream Resolution & Binary Audio Proxy ───────────────────────────────────

@app.get("/stream/{track_id}", response_model=AddonStream, tags=["Playback"])
async def resolve_stream(
    track_id: str,
    request: Request,
    quality: str = Query(DEFAULT_QUALITY, description="Requested quality tier (HI_RES_LOSSLESS, LOSSLESS, HIGH)")
):
    """
    BitChord stream resolution endpoint. Returns JSON matching AddonStream schema
    with direct playable URL, codec, bit depth, and sample rate.
    """
    clean_id = track_id.strip()
    base_url = get_request_base_url(request)
    stream_info = await stream_resolver.resolve_stream(clean_id, quality=quality, base_url=base_url)

    # Trigger background archiving if enabled
    if archiver.enabled:
        track = await stream_resolver.get_track(clean_id, base_url)
        if track:
            target_url = stream_info.url
            if "/stream/audio/" in target_url:
                # Use the fastest mirror URL for direct download
                ranked = mirror_manager.get_ranked_mirrors()
                if ranked:
                    target_url = f"{ranked[0]}/track/{clean_id}"
            archiver.trigger_archive(track, target_url)

    return stream_info


@app.api_route("/stream/audio/{track_id}.flac", methods=["GET", "HEAD"], tags=["Playback"])
@app.api_route("/stream/audio/{track_id}", methods=["GET", "HEAD"], tags=["Playback"])
async def stream_audio_file(track_id: str, request: Request):
    """
    Serves the binary FLAC audio stream with full HTTP Range request support
    for scrubbing, seeking, and smooth ExoPlayer playback.
    Supports both GET and HEAD methods required by ExoPlayer.
    """
    return await stream_resolver.stream_audio_proxy(track_id, request)


# ── Operational & Cache APIs ──────────────────────────────────────────────────

@app.get("/api/status", tags=["Monitoring"])
async def api_status():
    """Returns real-time operational status, mirror health, and cache metrics for the WebUI."""
    uptime = time.time() - START_TIME
    summary = mirror_manager.get_summary()
    return {
        "status": "healthy" if summary.healthy > 0 else "degraded",
        "uptime_seconds": round(uptime, 2),
        "mirror_pool": summary,
        "cache": prefetcher.get_cache_stats(),
        "archiving_enabled": archiver.enabled,
    }


@app.post("/api/cache/clear", tags=["Monitoring"])
async def api_clear_cache():
    """Clears local audio disk cache."""
    deleted = prefetcher.clear_cache()
    return {"status": "ok", "deleted_files": deleted}


# ── WebUI Dashboard & Root Welcome ───────────────────────────────────────────

@app.get("/dashboard", response_class=HTMLResponse, tags=["General"])
async def web_dashboard(request: Request):
    """Embedded single-page management dashboard and in-browser lossless player."""
    base_url = get_request_base_url(request)
    return HTMLResponse(content=render_dashboard_html(base_url, version="2.0.0"))


@app.get("/", tags=["General"])
async def root_index(request: Request):
    """
    Root landing page.
    Returns embedded dark-mode Web Dashboard if requested by browser (HTML),
    or JSON summary if requested by API client.
    """
    base_url = get_request_base_url(request)
    accept = request.headers.get("accept", "")
    if "text/html" in accept:
        return HTMLResponse(content=render_dashboard_html(base_url, version="2.0.0"))

    return {
        "service": "flacjack",
        "description": "Fresh, warm stacks of lossless audio on demand & BitChord Source Module 🥞",
        "version": "2.0.0",
        "bitchord_module": f"{base_url}/bitchord-module.json",
        "manifest": f"{base_url}/manifest.json",
        "dashboard": f"{base_url}/dashboard",
        "health": f"{base_url}/health",
        "docs": f"{base_url}/docs",
    }
