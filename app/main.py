"""
hifi-bridge — Self-Hosted Hi-Fi Lossless Aggregator & BitChord Source Module.
Production-ready FastAPI microservice bridging public Hi-Fi lossless audio APIs
to the BitChord Android music client.
"""

import asyncio
import hmac
import ipaddress
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("hifi-bridge")

START_TIME = time.time()
DEFAULT_QUALITY = os.environ.get("DEFAULT_QUALITY", QualityTier.HI_RES_LOSSLESS.value)
SECRET_TOKEN = os.environ.get("SECRET_TOKEN", "").strip()
TAILSCALE_NETWORK = ipaddress.ip_network("100.64.0.0/10")


def is_lan_request(request: Request) -> bool:
    """Returns True only if the request originated from a private LAN, Tailscale, or loopback IP."""
    cf_ip = request.headers.get("cf-connecting-ip")
    if cf_ip:
        client_ip_str = cf_ip.strip()
    else:
        xff = request.headers.get("x-forwarded-for")
        if xff:
            client_ip_str = xff.split(",")[0].strip()
        else:
            client_ip_str = request.client.host if request.client else ""

    if client_ip_str in ("testclient", "localhost", "127.0.0.1", "::1"):
        return True

    try:
        ip = ipaddress.ip_address(client_ip_str)
        return ip.is_private or ip.is_loopback or (ip in TAILSCALE_NETWORK)
    except ValueError:
        return False


def get_request_base_url(request: Request, include_secret_prefix: bool = True) -> str:
    """Dynamically reconstructs client-facing base URL including secret prefix when configured."""
    forwarded_proto = request.headers.get("x-forwarded-proto")
    forwarded_host = request.headers.get("x-forwarded-host")
    if forwarded_host:
        proto = forwarded_proto or "https"
        origin = f"{proto}://{forwarded_host}"
    else:
        host = request.headers.get("host")
        if host:
            proto = forwarded_proto or request.url.scheme or "http"
            origin = f"{proto}://{host}"
        else:
            origin = str(request.base_url).rstrip("/")

    if include_secret_prefix and SECRET_TOKEN:
        return f"{origin}/s/{SECRET_TOKEN}"
    return origin


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
                        label="Hi-Res Lossless (Up to 24-bit/192kHz FLAC)",
                    ),
                    AddonSettingOption(
                        value=QualityTier.LOSSLESS.value,
                        label="Lossless (16-bit/44.1kHz FLAC)",
                    ),
                    AddonSettingOption(
                        value=QualityTier.HIGH.value,
                        label="High (320kbps AAC)",
                    ),
                ],
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
            await asyncio.sleep(1800)
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
    if SECRET_TOKEN:
        logger.info("SECRET_TOKEN is active: external BitChord routes require /s/<SECRET_TOKEN>/...")
    else:
        logger.warning("SECRET_TOKEN is not set: BitChord routes are accessible without token prefix.")

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
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def security_gate_middleware(request: Request, call_next):
    """
    1. Allows Docker local /health checks.
    2. Restricts WebUI (/, /dashboard) and admin APIs (/api/*, /mirrors*) to LAN IPs only.
    3. Enforces /s/<SECRET_TOKEN>/ prefix on BitChord & audio routes when SECRET_TOKEN is set.
    """
    path = request.url.path

    if path == "/health":
        return await call_next(request)

    lan = is_lan_request(request)

    # LAN-only WebUI & management routes
    if path in ("/", "/dashboard") or path.startswith("/api/") or path.startswith("/mirrors"):
        if not lan:
            return JSONResponse(status_code=404, content={"detail": "Not found"})
        return await call_next(request)

    # Token-protected BitChord & streaming routes
    if SECRET_TOKEN:
        expected_prefix = f"/s/{SECRET_TOKEN}"
        if path == expected_prefix or path.startswith(expected_prefix + "/"):
            return await call_next(request)
        # Allow unprefixed calls only from local LAN for quick testing
        if lan:
            return await call_next(request)
        return JSONResponse(status_code=403, content={"detail": "Forbidden"})

    return await call_next(request)


# ── Health & Status Endpoints ────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["Monitoring"])
async def health_check():
    """System health check and live mirror pool summary."""
    uptime = time.time() - START_TIME
    summary = mirror_manager.get_summary()
    cache_count = await db.get_total_cache_entries()

    return HealthResponse(
        status="healthy" if summary.healthy > 0 else "degraded",
        version="2.0.0",
        uptime_seconds=round(uptime, 2),
        mirror_pool=summary,
        cache_entries=cache_count,
        archiving_enabled=archiver.enabled,
    )


@app.get("/mirrors", response_model=MirrorPoolSummary, tags=["Monitoring"])
async def get_mirrors():
    return mirror_manager.get_summary()


@app.post("/mirrors/refresh", tags=["Monitoring"])
async def refresh_mirrors():
    discovered = await mirror_manager.refresh_instances_from_github()
    await mirror_manager.check_all_mirrors_health()
    return {"status": "ok", "discovered_count": len(discovered), "mirrors": mirror_manager.get_summary()}


# ── BitChord & Audio Router (Mounted at both root for LAN and /s/{token}) ────

bitchord_router = APIRouter()


@bitchord_router.get("/manifest.json", response_model=AddonManifest, tags=["BitChord"])
async def get_manifest(request: Request):
    base_url = get_request_base_url(request, include_secret_prefix=True)
    return build_addon_manifest(base_url)


@bitchord_router.get("/bitchord-module.json", response_model=AddonManifest, tags=["BitChord"])
async def get_bitchord_module(request: Request):
    base_url = get_request_base_url(request, include_secret_prefix=True)
    return build_addon_manifest(base_url)


@bitchord_router.get("/search", response_model=AddonSearchResponse, tags=["Audio"])
async def search_tracks(
    request: Request,
    q: Optional[str] = Query(None, description="Search query string"),
    s: Optional[str] = Query(None, description="Alternative search query parameter (Monochrome)"),
    quality: str = Query(DEFAULT_QUALITY, description="Target audio quality tier"),
    limit: int = Query(25, ge=1, le=100, description="Max tracks to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    query = (q or s or "").strip()
    if not query:
        return AddonSearchResponse(tracks=[])

    base_url = get_request_base_url(request, include_secret_prefix=True)
    return await stream_resolver.search(
        query=query,
        quality=quality,
        limit=limit,
        offset=offset,
        base_url=base_url,
    )


@bitchord_router.get("/track/{track_id}", response_model=AddonTrack, tags=["Audio"])
@bitchord_router.get("/track", response_model=AddonTrack, tags=["Audio"])
async def get_track(
    request: Request,
    track_id: Optional[str] = None,
    id: Optional[str] = Query(None, description="Track ID query parameter"),
):
    clean_id = (track_id or id or "").strip()
    if not clean_id:
        raise HTTPException(status_code=400, detail="Missing track ID parameter")

    base_url = get_request_base_url(request, include_secret_prefix=True)
    track = await stream_resolver.get_track(clean_id, base_url)
    if not track:
        raise HTTPException(status_code=404, detail=f"Track '{clean_id}' not found")
    return track


@bitchord_router.get("/album/{album_id}", response_model=AlbumDetail, tags=["Metadata"])
@bitchord_router.get("/album", response_model=AlbumDetail, tags=["Metadata"])
async def get_album(
    request: Request,
    album_id: Optional[str] = None,
    id: Optional[str] = Query(None, description="Album ID query parameter"),
):
    clean_id = (album_id or id or "").strip()
    if not clean_id:
        raise HTTPException(status_code=400, detail="Missing album ID parameter")

    base_url = get_request_base_url(request, include_secret_prefix=True)
    return await stream_resolver.get_album(clean_id, base_url)


@bitchord_router.get("/artist/{artist_id}", response_model=ArtistDetail, tags=["Metadata"])
@bitchord_router.get("/artist", response_model=ArtistDetail, tags=["Metadata"])
async def get_artist(
    request: Request,
    artist_id: Optional[str] = None,
    id: Optional[str] = Query(None, description="Artist ID query parameter"),
):
    clean_id = (artist_id or id or "").strip()
    if not clean_id:
        raise HTTPException(status_code=400, detail="Missing artist ID parameter")

    base_url = get_request_base_url(request, include_secret_prefix=True)
    return await stream_resolver.get_artist(clean_id, base_url)


@bitchord_router.get("/stream/{track_id}", response_model=AddonStream, tags=["Playback"])
async def resolve_stream(
    track_id: str,
    request: Request,
    quality: str = Query(DEFAULT_QUALITY, description="Requested quality tier (HI_RES_LOSSLESS, LOSSLESS, HIGH)"),
):
    clean_id = track_id.strip()
    base_url = get_request_base_url(request, include_secret_prefix=True)
    stream_info = await stream_resolver.resolve_stream(clean_id, quality=quality, base_url=base_url)

    if archiver.enabled:
        track = await stream_resolver.get_track(clean_id, base_url)
        if track:
            target_url = stream_info.url
            if "/stream/audio/" in target_url:
                ranked = mirror_manager.get_ranked_mirrors()
                if ranked:
                    target_url = f"{ranked[0]}/track/{clean_id}"
            archiver.trigger_archive(track, target_url)

    return stream_info


@bitchord_router.api_route("/stream/audio/{track_id}.flac", methods=["GET", "HEAD"], tags=["Playback"])
@bitchord_router.api_route("/stream/audio/{track_id}", methods=["GET", "HEAD"], tags=["Playback"])
async def stream_audio_file(track_id: str, request: Request):
    return await stream_resolver.stream_audio_proxy(track_id, request)


# Mount router at root (for LAN) and under /s/{SECRET_TOKEN} (for external BitChord)
app.include_router(bitchord_router)
if SECRET_TOKEN:
    app.include_router(bitchord_router, prefix=f"/s/{SECRET_TOKEN}")


# ── Operational & Cache APIs (LAN Only) ──────────────────────────────────────

@app.get("/api/status", tags=["Monitoring"])
async def api_status():
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
    deleted = prefetcher.clear_cache()
    return {"status": "ok", "deleted_files": deleted}


# ── WebUI Dashboard & Root Welcome (LAN Only) ────────────────────────────────

@app.get("/dashboard", response_class=HTMLResponse, tags=["General"])
async def web_dashboard(request: Request):
    base_url = get_request_base_url(request, include_secret_prefix=True)
    return HTMLResponse(content=render_dashboard_html(base_url, version="2.0.0"))


@app.get("/", tags=["General"])
async def root_index(request: Request):
    base_url = get_request_base_url(request, include_secret_prefix=True)
    accept = request.headers.get("accept", "")
    if "text/html" in accept:
        return HTMLResponse(content=render_dashboard_html(base_url, version="2.0.0"))

    return {
        "service": "flacjack",
        "version": "2.0.0",
        "bitchord_module": f"{base_url}/bitchord-module.json",
        "manifest": f"{base_url}/manifest.json",
        "dashboard": f"{get_request_base_url(request, include_secret_prefix=False)}/dashboard",
        "health": f"{get_request_base_url(request, include_secret_prefix=False)}/health",
    }
