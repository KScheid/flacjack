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
import re
import time
from urllib.parse import urlparse
from contextlib import asynccontextmanager
from typing import List, Optional, Set, Union
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
from app.stream_resolver import is_snowflake_id, sanitize_track_id, stream_resolver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("hifi-bridge")

START_TIME = time.time()
DEFAULT_QUALITY = os.environ.get("DEFAULT_QUALITY", QualityTier.HI_RES_LOSSLESS.value)
SECRET_TOKEN = os.environ.get("SECRET_TOKEN", "").strip()

# The token becomes part of every URL, so it has to be URL-safe. Anything else
# would silently break routing or the baseUrl handed to BitChord.
if SECRET_TOKEN and not re.fullmatch(r"(?!\.+$)[A-Za-z0-9._~\-]+", SECRET_TOKEN):
    raise RuntimeError(
        "SECRET_TOKEN may only contain letters, digits and the characters . _ ~ - "
        "(for example the output of: openssl rand -hex 24)"
    )


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("true", "1", "yes", "on")


# The WebUI dashboard and admin APIs (/, /dashboard, /api/*, /mirrors*). Off by
# default: the dashboard displays the token-bearing BitChord URL, so anything on
# the LAN that can load it can read the token.
WEBUI_ENABLED = _env_flag("WEBUI_ENABLED", False)

# Extra hostnames allowed to reach LAN-only routes, e.g. ALLOWED_HOSTS=nas.home,music.lan
# (see _host_is_trusted for what is allowed by default).
ALLOWED_HOSTS = {
    h.strip().lower().rstrip(".") for h in os.environ.get("ALLOWED_HOSTS", "").split(",") if h.strip()
}

# Browser origins allowed to make cross-origin requests. BitChord is a native
# app and needs no CORS, so none are allowed unless configured.
CORS_ORIGINS = [o.strip() for o in os.environ.get("CORS_ORIGINS", "").split(",") if o.strip()]


# ── Client IP / LAN detection ────────────────────────────────────────────────

IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
IPNetwork = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]

# Optional: narrow what counts as "LAN", e.g. LAN_CIDRS=192.168.50.0/24,172.17.0.0/16,100.64.0.0/10
# When set, ONLY these networks (plus loopback) count, so list Tailscale's
# 100.64.0.0/10 too if you want it. If requests reach this container through a
# reverse proxy, include the proxy's own address/subnet.
_LAN_CIDRS_RAW = os.environ.get("LAN_CIDRS", "").strip()
LAN_CIDRS_CONFIGURED = bool(_LAN_CIDRS_RAW)


def _parse_lan_cidrs(raw: str) -> List[IPNetwork]:
    networks: List[IPNetwork] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            networks.append(ipaddress.ip_network(part, strict=False))
        except ValueError:
            logger.warning(f"Ignoring invalid LAN_CIDRS entry: {part!r}")
    return networks


LAN_NETWORKS = _parse_lan_cidrs(_LAN_CIDRS_RAW)

# Default definition of "LAN" when LAN_CIDRS is not set. Listed explicitly because
# ipaddress.is_private also covers documentation and reserved ranges.
TAILSCALE_NETWORK = ipaddress.ip_network("100.64.0.0/10")
DEFAULT_LAN_NETWORKS: List[IPNetwork] = [
    TAILSCALE_NETWORK,  # Tailscale tailnet addresses
] + [
    ipaddress.ip_network(cidr)
    for cidr in (
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "169.254.0.0/16",
        "fc00::/7",
        "fe80::/10",
    )
]

_FORWARDED_FOR_RE = re.compile(r'for=\s*"?(\[[^\]]+\](?::\d+)?|[^;,"\s]+)', re.IGNORECASE)
_SINGLE_IP_HEADERS = ("cf-connecting-ip", "true-client-ip", "x-real-ip")


def _parse_ip(value: str) -> Optional[IPAddress]:
    """Parses an IP that may carry brackets or a port. Returns None if it is not an IP."""
    v = value.strip().strip('"').strip()
    if not v:
        return None
    if v.startswith("["):
        v = v[1:].split("]", 1)[0]
    elif v.count(":") == 1:  # IPv4 with port
        v = v.split(":", 1)[0]
    try:
        ip = ipaddress.ip_address(v)
    except ValueError:
        return None
    mapped = getattr(ip, "ipv4_mapped", None)  # ::ffff:1.2.3.4 -> 1.2.3.4
    return mapped if mapped is not None else ip


def _ip_is_lan(ip: IPAddress) -> bool:
    if ip.is_loopback:
        return True
    networks = LAN_NETWORKS if LAN_CIDRS_CONFIGURED else DEFAULT_LAN_NETWORKS
    return any(ip in net for net in networks)


def is_lan_request(request: Request) -> bool:
    """
    Returns True only if EVERY hop we know about is a LAN, Tailscale or loopback address:
    the TCP peer plus every address in the forwarding headers.

    Forwarding headers are client-controlled, so they are never allowed to make a
    request look more trusted, only less. A spoofed "X-Forwarded-For: 192.168.1.5"
    sent through a reverse proxy still carries the real public IP that the proxy
    appended, and that public hop fails the check. Anything unparseable fails closed.
    """
    peer = request.client.host if request.client else ""
    # Starlette's TestClient reports its peer as the literal "testclient". Only the
    # real connection can carry that value; a header saying "localhost" or
    # "testclient" is still rejected below because it does not parse as an IP.
    hops: List[str] = ["127.0.0.1" if peer == "testclient" else peer]

    for header in _SINGLE_IP_HEADERS:
        hops.extend(v for v in request.headers.getlist(header) if v.strip())

    for value in request.headers.getlist("x-forwarded-for"):
        hops.extend(part for part in value.split(",") if part.strip())

    for value in request.headers.getlist("forwarded"):
        hops.extend(_FORWARDED_FOR_RE.findall(value))

    for hop in hops:
        ip = _parse_ip(hop)
        if ip is None or not _ip_is_lan(ip):
            return False
    return True


_LOCAL_HOST_SUFFIXES = (".local", ".lan", ".home.arpa", ".internal", ".ts.net")


def _host_is_trusted(request: Request) -> bool:
    """
    Blocks DNS rebinding. A malicious web page can point its own domain at your
    server's LAN IP; the browser then sends LAN-looking requests and lets the page
    read the answers (including the token in the manifest). Those requests carry
    the attacker's domain in Host, so LAN-granted access requires a Host that an
    outsider cannot control: an IP literal, localhost, a single-label name ("nas"),
    a local-only suffix (.local, .lan, .home.arpa, .internal, .ts.net), or ALLOWED_HOSTS.
    """
    host = _first_header_value(request.headers.get("host")).lower()
    if host.startswith("["):
        return _parse_ip(host) is not None
    name = host.rsplit(":", 1)[0].rstrip(".") if host.count(":") <= 1 else host
    if not name:
        return False
    if name in ("localhost", "testserver", "testclient") or name in ALLOWED_HOSTS:
        return True
    if _parse_ip(name) is not None:
        return True
    if "." not in name:
        return True
    return name.endswith(_LOCAL_HOST_SUFFIXES)


def _origin_is_same_site(request: Request) -> bool:
    """CSRF guard for state-changing requests: a foreign page's form/fetch carries its own Origin."""
    origin = request.headers.get("origin")
    if not origin or origin == "null":
        return origin is None
    origin_host = urlparse(origin).netloc.lower()
    return origin_host == _first_header_value(request.headers.get("host")).lower()


def _has_valid_token_prefix(path: str) -> bool:
    """Constant-time check that the path starts with /s/<SECRET_TOKEN>."""
    if not SECRET_TOKEN or not path.startswith("/s/"):
        return False
    candidate = path[3:].split("/", 1)[0]
    return hmac.compare_digest(candidate.encode("utf-8"), SECRET_TOKEN.encode("utf-8"))


class _RedactTokenFilter(logging.Filter):
    """Keeps the secret token out of the access log (it is part of every request path)."""

    def filter(self, record: logging.LogRecord) -> bool:
        if SECRET_TOKEN:
            if isinstance(record.msg, str) and SECRET_TOKEN in record.msg:
                record.msg = record.msg.replace(SECRET_TOKEN, "***")
            if isinstance(record.args, tuple):
                record.args = tuple(
                    a.replace(SECRET_TOKEN, "***") if isinstance(a, str) else a for a in record.args
                )
        return True


logging.getLogger("uvicorn.access").addFilter(_RedactTokenFilter())


# ── Base URL reconstruction ──────────────────────────────────────────────────

_HOST_RE = re.compile(r"^(?:[A-Za-z0-9.\-]+|\[[0-9A-Fa-f:.]+\])(?::\d{1,5})?$")


def _first_header_value(value: Optional[str]) -> str:
    return value.split(",")[0].strip() if value else ""


def get_request_base_url(request: Request, include_secret_prefix: bool = True) -> str:
    """Dynamically reconstructs client-facing base URL including secret prefix when configured."""
    forwarded_proto = _first_header_value(request.headers.get("x-forwarded-proto")).lower()
    if forwarded_proto not in ("http", "https"):
        forwarded_proto = ""
    forwarded_host = _first_header_value(request.headers.get("x-forwarded-host"))
    host = _first_header_value(request.headers.get("host"))

    # Host headers are client-supplied and end up inside HTML and JSON, so only
    # accept values that actually look like a hostname[:port].
    if forwarded_host and _HOST_RE.match(forwarded_host):
        origin = f"{forwarded_proto or 'https'}://{forwarded_host}"
    elif host and _HOST_RE.match(host):
        origin = f"{forwarded_proto or request.url.scheme or 'http'}://{host}"
    else:
        server = request.scope.get("server")
        origin = f"http://{server[0]}:{server[1]}" if server else "http://localhost"

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


# ── Background work ──────────────────────────────────────────────────────────

# Strong references: asyncio only keeps weak references to tasks, so an
# un-referenced fire-and-forget task can be garbage collected mid-flight.
_background_tasks: Set[asyncio.Task] = set()


def _spawn_background(coro) -> None:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def _archive_in_background(clean_id: str, quality: str, base_url: str) -> None:
    """Looks up metadata and hands the track to the archiver without delaying playback."""
    try:
        # The URL handed to the client is our own proxy, which the archiver cannot
        # download from. Only snowflake IDs have a mirror endpoint that returns
        # audio; for every other ID <mirror>/track/<id> returns JSON, which must
        # never be archived as audio. With no direct URL the archiver copies from
        # the local audio cache instead.
        direct_url: Optional[str] = None
        if is_snowflake_id(clean_id):
            ranked = mirror_manager.get_ranked_mirrors()
            if ranked:
                direct_url = f"{ranked[0]}/track/{clean_id}"
        else:
            upstream = await stream_resolver.resolve_upstream(clean_id, quality)
            direct_url = upstream.url or None

        track = await stream_resolver.get_track(clean_id, base_url)
        if track:
            archiver.trigger_archive(track, clean_id, direct_url)
    except Exception as e:
        logger.warning(f"Archive trigger failed for track {clean_id}: {e}")


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
        if len(SECRET_TOKEN) < 16:
            logger.warning("SECRET_TOKEN is shorter than 16 characters; use a longer random value.")
    else:
        logger.warning("SECRET_TOKEN is not set: BitChord routes are accessible without token prefix.")
    if not WEBUI_ENABLED:
        logger.info("WebUI and admin APIs are disabled (set WEBUI_ENABLED=true to enable them on the LAN).")
    if SECRET_TOKEN:
        logger.info("BitChord module URL: https://<your-host>/s/<SECRET_TOKEN>/bitchord-module.json")
    if LAN_CIDRS_CONFIGURED:
        logger.info(f"LAN_CIDRS active: {', '.join(str(n) for n in LAN_NETWORKS) or 'loopback only'}")

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
    for task in list(_background_tasks):
        task.cancel()
    await prefetcher.stop()
    await archiver.shutdown()
    await stream_resolver.aclose()
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

# Previously allow_origins=["*"], which let ANY website open in a browser on your
# LAN fetch() this server and read the answers, including the token in the
# manifest. BitChord is a native app and is unaffected by CORS.
if CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_credentials=False,
        allow_methods=["GET", "HEAD"],
        allow_headers=["Range"],
    )


_ADMIN_PREFIXES = ("/api/", "/mirrors")


def _not_found() -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": "Not found"})


@app.middleware("http")
async def security_gate_middleware(request: Request, call_next):
    """
    1. Allows /health (Docker health checks); outsiders only get a bare status.
    2. WebUI (/, /dashboard) and admin APIs (/api/*, /mirrors*) exist only with
       WEBUI_ENABLED=true, and then only for LAN clients using a trusted Host,
       with a same-origin check on POSTs.
    3. Enforces /s/<SECRET_TOKEN>/ on BitChord & audio routes when SECRET_TOKEN is set.
       Unprefixed access is allowed from the LAN, again only with a trusted Host.
    Every refusal is a 404 so a probe cannot tell that the service exists.
    """
    path = request.url.path

    if path == "/health":
        return await call_next(request)

    lan = is_lan_request(request) and _host_is_trusted(request)

    if path in ("/", "/dashboard") or path.startswith(_ADMIN_PREFIXES):
        if not WEBUI_ENABLED or not lan:
            return _not_found()
        if request.method not in ("GET", "HEAD", "OPTIONS") and not _origin_is_same_site(request):
            return JSONResponse(status_code=403, content={"detail": "Cross-site request refused"})
        return await call_next(request)

    if SECRET_TOKEN:
        if _has_valid_token_prefix(path) or lan:
            return await call_next(request)
        return _not_found()

    return await call_next(request)


# ── Health & Status Endpoints ────────────────────────────────────────────────

@app.get("/health", tags=["Monitoring"])
async def health_check(request: Request):
    """System health check. Details (mirror pool, cache, uptime) are LAN-only."""
    if not (is_lan_request(request) and _host_is_trusted(request)):
        summary = mirror_manager.get_summary()
        return {"status": "healthy" if summary.healthy > 0 else "degraded"}
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
    clean_id = sanitize_track_id(track_id)
    base_url = get_request_base_url(request, include_secret_prefix=True)
    stream_info = await stream_resolver.resolve_stream(clean_id, quality=quality, base_url=base_url)

    if archiver.enabled:
        _spawn_background(_archive_in_background(clean_id, quality, base_url))

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

    plain_base_url = get_request_base_url(request, include_secret_prefix=False)
    return {
        "service": "flacjack",
        "version": "2.0.0",
        "bitchord_module": f"{base_url}/bitchord-module.json",
        "manifest": f"{base_url}/manifest.json",
        "dashboard": f"{plain_base_url}/dashboard",
        "health": f"{plain_base_url}/health",
    }
