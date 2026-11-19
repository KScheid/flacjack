"""
Stream Resolver and Normalizer for hifi-bridge.
Normalizes upstream track searches, handles DASH manifests,
provides quality fallback (HI_RES_LOSSLESS -> LOSSLESS -> HIGH),
and serves seamless FLAC audio streaming with HTTP Range support.
"""

import asyncio
import base64
import json
import logging
import os
import re
import uuid
import xml.etree.ElementTree as ET
from typing import Any, AsyncIterator, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, urlparse
import httpx
from fastapi import HTTPException, Request, Response
from fastapi.responses import StreamingResponse

from app.database import db
from app.mirror_manager import mirror_manager
from app.prefetcher import prefetcher
from app.models import (
    AddonSearchResponse,
    AddonStream,
    AddonTrack,
    AlbumDetail,
    ArtistDetail,
    QualityTier,
)

logger = logging.getLogger(__name__)

CACHE_TTL_SEARCH = int(os.environ.get("CACHE_TTL_SECONDS", "3600"))
CACHE_TTL_STREAM = int(os.environ.get("CACHE_TTL_STREAM", "3600"))
# A failed resolution is only remembered briefly, so one upstream hiccup does
# not make a track unplayable for a full hour.
CACHE_TTL_STREAM_FALLBACK = 60
MIN_AUDIO_BYTES = 10240
PROXY_STREAMS = os.environ.get("PROXY_STREAMS", "true").strip().lower() not in ("false", "0", "no", "off")
# How many times a dropped upstream connection is re-opened (from the byte it
# stopped at) before the client connection is given up on.
STREAM_RESUME_ATTEMPTS = max(0, int(os.environ.get("STREAM_RESUME_ATTEMPTS", "5")))
# Seconds without a single byte from upstream before the connection is treated
# as dead and resumed. Only counts while we are waiting on upstream, so a
# player that has paused reading (full buffer) never triggers it.
STREAM_STALL_TIMEOUT = float(os.environ.get("STREAM_STALL_TIMEOUT", "20"))
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
_CONTENT_RANGE_RE = re.compile(r"bytes\s+(\d+)-(\d+)/(\d+|\*)")

VALID_QUALITIES = {tier.value for tier in QualityTier}

_ID_RE = re.compile(r"[A-Za-z0-9_\-]{1,64}")
_SNOWFLAKE_RE = re.compile(r"\d{17,20}")
_RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)")
# Track-info payloads carry a "url" that points at a web page, not at audio.
_NON_AUDIO_HOSTS = {"tidal.com", "www.tidal.com", "listen.tidal.com"}

# (bitDepth, sampleRate, bitrate) reported when upstream does not say
_TIER_DEFAULTS: Dict[str, Tuple[float, float, float]] = {
    QualityTier.HI_RES_LOSSLESS.value: (24.0, 96000.0, 3000000.0),
    QualityTier.LOSSLESS.value: (16.0, 44100.0, 1411000.0),
    QualityTier.HIGH.value: (16.0, 44100.0, 320000.0),
    QualityTier.LOW.value: (16.0, 44100.0, 96000.0),
}


def sanitize_track_id(track_id: Any) -> str:
    """
    Strips a trailing audio extension and rejects anything that is not a plain ID.
    IDs end up in file names and upstream URLs, so invalid input is refused
    rather than silently rewritten into a different ID.
    """
    raw = str(track_id).strip()
    for ext in (".flac", ".m4a"):
        if raw.lower().endswith(ext):
            raw = raw[: -len(ext)]
            break
    if not _ID_RE.fullmatch(raw):
        raise HTTPException(status_code=400, detail="Invalid ID")
    return raw


def is_snowflake_id(track_id: str) -> bool:
    """17-20 digit IDs belong to the tracks.monochrome.st style backend."""
    return bool(_SNOWFLAKE_RE.fullmatch(track_id))


def normalize_quality(quality: Optional[str]) -> str:
    """Maps arbitrary client input onto a known quality tier."""
    candidate = str(quality or "").strip().upper()
    return candidate if candidate in VALID_QUALITIES else QualityTier.HI_RES_LOSSLESS.value


def cover_id_to_url(cover_id: Optional[str]) -> Optional[str]:
    """Converts a Tidal cover UUID to a high-resolution CDN URL."""
    if not cover_id or not isinstance(cover_id, str):
        return None
    if cover_id.startswith("http://") or cover_id.startswith("https://"):
        return cover_id
    formatted_path = cover_id.replace("-", "/")
    return f"https://resources.tidal.com/images/{formatted_path}/1280x1280.jpg"


def _as_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_audio_url(value: Any) -> bool:
    if not isinstance(value, str) or not value.startswith(("http://", "https://")):
        return False
    host = (urlparse(value).hostname or "").lower()
    return host not in _NON_AUDIO_HOSTS


def _internal_stream_url(base_url: str, track_id: str, quality: Optional[str] = None) -> str:
    url = f"{base_url.rstrip('/')}/stream/audio/{track_id}.flac" if base_url else f"/stream/audio/{track_id}.flac"
    if quality and quality != QualityTier.HI_RES_LOSSLESS.value:
        url += f"?quality={quality}"
    return url


def _build_stream(
    url: str,
    quality: str,
    *,
    codec: str = "flac",
    mime_type: Optional[str] = None,
    sample_rate: Optional[float] = None,
    bit_depth: Optional[float] = None,
    bitrate: Optional[float] = None,
    encrypted: bool = False,
) -> AddonStream:
    """Single place that builds AddonStream so every field agrees with the real tier."""
    codec = (codec or "flac").lower()
    is_flac = "flac" in codec
    default_depth, default_rate, default_bitrate = _TIER_DEFAULTS.get(
        quality, _TIER_DEFAULTS[QualityTier.LOSSLESS.value]
    )
    container = "flac" if is_flac else "mp4"
    return AddonStream(
        url=url,
        format="flac" if is_flac else "aac",
        quality=quality,
        streamQuality=quality,
        audioQuality=quality,
        codec=codec,
        fileCodec=codec,
        container=container,
        containerFormat=container,
        manifest="none",
        mimeType=mime_type or ("audio/flac" if is_flac else "audio/mp4"),
        encrypted=encrypted,
        sampleRate=sample_rate or default_rate,
        bitDepth=bit_depth or default_depth,
        bitrate=bitrate or default_bitrate,
    )


def _unwrap_payload(data: Any) -> Optional[Dict[str, Any]]:
    """Returns the useful dict from {"data": {...}}, a bare dict, or a legacy list of dicts."""
    if isinstance(data, dict):
        inner = data.get("data", data)
        if isinstance(inner, dict):
            return inner
        data = inner if isinstance(inner, list) else None
    if isinstance(data, list):
        merged: Dict[str, Any] = {}
        for part in data:
            if isinstance(part, dict):
                merged.update(part)
        return merged or None
    return None


class StreamResolver:
    def __init__(self):
        self._stream_client: Optional[httpx.AsyncClient] = None
        self._background_tasks: Set[asyncio.Task] = set()

    async def aclose(self) -> None:
        """Cancels background work and closes the upstream audio client."""
        for task in list(self._background_tasks):
            task.cancel()
        if self._stream_client is not None and not self._stream_client.is_closed:
            await self._stream_client.aclose()
        self._stream_client = None

    def _spawn(self, coro) -> None:
        """Fire-and-forget with a strong reference so the task cannot be garbage collected."""
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    # ── Search & Metadata Normalization ──────────────────────────────────────

    async def search(
        self,
        query: str,
        quality: str = QualityTier.HI_RES_LOSSLESS.value,
        limit: int = 25,
        offset: int = 0,
        base_url: str = "",
    ) -> AddonSearchResponse:
        cleaned_query = query.strip()
        if not cleaned_query:
            return AddonSearchResponse(tracks=[])
        quality = normalize_quality(quality)

        cached = await db.get_cached_search(cleaned_query, quality, limit, offset)
        if cached:
            tracks = []
            for t in cached.get("tracks", []):
                track_obj = AddonTrack(**t)
                if base_url:
                    track_obj.streamURL = _internal_stream_url(base_url, track_obj.id)
                tracks.append(track_obj)
            return AddonSearchResponse(tracks=tracks)

        tracks: List[AddonTrack] = []
        raw_items: List[Dict[str, Any]] = []
        upstream_paged = False

        # (path, params, whether upstream already applied limit/offset).
        # The query always travels in params so httpx encodes it; characters
        # such as & # + % in a search must never be spliced into the URL.
        strategies = [
            ("/search", {"q": cleaned_query}, False),
            ("/search", {"s": cleaned_query, "limit": limit, "offset": offset}, True),
            ("/search/", {"s": cleaned_query, "limit": limit, "offset": offset}, True),
        ]

        client = await mirror_manager.get_client()
        ranked_mirrors = mirror_manager.get_ranked_mirrors()

        async with mirror_manager.rate_limit():
            for mirror_url in ranked_mirrors:
                mirror_failed = False
                for path_template, params, paged in strategies:
                    if mirror_failed:
                        break
                    full_url = f"{mirror_url.rstrip('/')}{path_template}"
                    try:
                        resp = await client.get(full_url, params=params, timeout=4.0)
                        if resp.status_code == 200:
                            text_head = resp.text[:500].lower()
                            if "redirecting..." in text_head or "prebid-wrapper" in text_head:
                                mirror_manager.mark_mirror_failure(mirror_url, "Ad redirect")
                                mirror_failed = True
                                continue
                            try:
                                data = resp.json()
                                tracks, raw_items = self._normalize_search_response(data, base_url)
                                if tracks:
                                    upstream_paged = paged
                                    break
                            except Exception as e:
                                logger.warning(f"Failed to parse search JSON from {mirror_url}: {e}")
                                continue
                        elif resp.status_code in (429, 502, 503, 504):
                            mirror_manager.mark_mirror_failure(mirror_url, f"HTTP {resp.status_code}")
                            mirror_failed = True
                        elif resp.status_code == 404:
                            continue
                    except (httpx.TimeoutException, asyncio.TimeoutError):
                        mirror_manager.mark_mirror_failure(mirror_url, "Timeout")
                        mirror_failed = True
                    except Exception as e:
                        logger.debug(f"Strategy {path_template} on {mirror_url} failed: {e}")
                        mirror_failed = True

                if tracks:
                    break

        if not tracks:
            logger.warning(f"No results found for query '{cleaned_query}' across all mirrors.")
            return AddonSearchResponse(tracks=[])

        # If upstream already skipped `offset` rows, skipping them again here
        # would return an empty or shifted page.
        paged_tracks = tracks[:limit] if upstream_paged else tracks[offset : offset + limit]

        cache_payload = {
            "tracks": [t.model_dump() for t in paged_tracks],
            "total": len(tracks),
        }
        await db.set_cached_search(cleaned_query, quality, limit, offset, cache_payload, ttl=CACHE_TTL_SEARCH)

        for t in paged_tracks:
            await db.set_cached_track(t.id, t.model_dump(), ttl=86400)

        return AddonSearchResponse(tracks=paged_tracks, items=raw_items)

    def _normalize_search_response(
        self,
        data: Any,
        base_url: str = "",
    ) -> Tuple[List[AddonTrack], List[Dict[str, Any]]]:
        tracks: List[AddonTrack] = []
        raw_items: List[Dict[str, Any]] = []

        if isinstance(data, dict) and "tracks" in data and isinstance(data["tracks"], list):
            raw_items = [item for item in data["tracks"] if isinstance(item, dict)]
            for item in raw_items:
                track_id = str(item.get("trackId") or item.get("id") or "")
                if not track_id:
                    continue

                title = str(item.get("title") or "Unknown Title")
                artist_names = item.get("artistNames")
                artist = ""
                if isinstance(artist_names, list):
                    artist = ", ".join(str(a) for a in artist_names if a)
                if not artist:
                    raw_artist = item.get("artist")
                    artist = raw_artist if isinstance(raw_artist, str) and raw_artist else "Unknown Artist"
                album = str(item.get("albumTitle") or item.get("releaseTitle") or "")
                duration_ms = _as_float(item.get("duration"))
                duration_sec = round(duration_ms / 1000.0, 2) if duration_ms else None
                artwork = item.get("artwork") or item.get("cover")
                if not isinstance(artwork, str):
                    artwork = None
                stream_url = _internal_stream_url(base_url, track_id) if base_url else None
                isrc = item.get("isrc")

                track = AddonTrack(
                    id=track_id,
                    title=title,
                    artist=artist,
                    album=album,
                    duration=duration_sec,
                    artworkURL=artwork,
                    albumArtworkURL=artwork,
                    format="flac",
                    audioQuality=QualityTier.HI_RES_LOSSLESS.value,
                    audioMode="STEREO",
                    audioModes=["STEREO"],
                    streamURL=stream_url,
                    isrc=isrc if isinstance(isrc, str) else None,
                    trackNumber=_as_int(item.get("trackNumber")),
                )
                tracks.append(track)
            return tracks, raw_items

        items: Any = []
        if isinstance(data, dict):
            if "data" in data and isinstance(data["data"], dict) and "items" in data["data"]:
                items = data["data"]["items"]
            elif "items" in data and isinstance(data["items"], list):
                items = data["items"]
            elif "tracks" in data and isinstance(data["tracks"], dict) and "items" in data["tracks"]:
                items = data["tracks"]["items"]
        elif isinstance(data, list):
            items = data
        if not isinstance(items, list):
            items = []

        raw_items = [item for item in items if isinstance(item, dict)]
        for item in raw_items:
            t = item.get("item", item)
            if not isinstance(t, dict):
                continue

            track_id = str(t.get("id") or "")
            if not track_id:
                continue

            title = str(t.get("title") or "Unknown Title")
            artist = ""
            artists_list = t.get("artists")
            if isinstance(artists_list, list):
                artist = ", ".join(
                    str(a["name"]) for a in artists_list if isinstance(a, dict) and a.get("name")
                )
            if not artist:
                raw_artist = t.get("artist")
                if isinstance(raw_artist, dict):
                    artist = str(raw_artist.get("name") or "")
                elif isinstance(raw_artist, str):
                    artist = raw_artist
            artist = artist or "Unknown Artist"

            album_obj = t.get("album") if isinstance(t.get("album"), dict) else {}
            album_title = str(album_obj.get("title") or "")
            cover_id = album_obj.get("cover") or t.get("cover")
            artwork = cover_id_to_url(cover_id)

            duration_sec = _as_float(t.get("duration"))
            quality = t.get("audioQuality")
            if not isinstance(quality, str) or not quality:
                quality = "LOSSLESS"
            fmt = "flac" if "LOSSLESS" in quality else "aac"
            stream_url = _internal_stream_url(base_url, track_id) if base_url else None

            audio_modes = t.get("audioModes")
            if not isinstance(audio_modes, list) or not audio_modes:
                audio_modes = ["STEREO"]
            audio_modes = [str(m) for m in audio_modes]
            is_atmos = "DOLBY_ATMOS" in audio_modes
            isrc = t.get("isrc")

            track = AddonTrack(
                id=track_id,
                title=title,
                artist=artist,
                album=album_title,
                duration=duration_sec,
                artworkURL=artwork,
                albumArtworkURL=artwork,
                format=fmt,
                audioQuality=quality,
                audioMode="DOLBY_ATMOS" if is_atmos else "STEREO",
                audioModes=audio_modes,
                atmos=is_atmos,
                streamURL=stream_url,
                isrc=isrc if isinstance(isrc, str) else None,
                trackNumber=_as_int(t.get("trackNumber")),
            )
            tracks.append(track)

        return tracks, raw_items

    # ── Track Metadata Lookup ────────────────────────────────────────────────

    async def get_track(self, track_id: str, base_url: str = "") -> Optional[AddonTrack]:
        clean_id = sanitize_track_id(track_id)
        cached = await db.get_cached_track(clean_id)
        if cached:
            track_obj = AddonTrack(**cached)
            if base_url:
                track_obj.streamURL = _internal_stream_url(base_url, track_obj.id)
            return track_obj

        paths = [f"/info/?id={clean_id}", f"/info?id={clean_id}"]
        if not is_snowflake_id(clean_id):
            # For snowflake IDs <mirror>/track/<id> is the audio file itself;
            # fetching it here would pull the whole track into memory.
            paths.append(f"/track/{clean_id}")

        for path in paths:
            try:
                resp, _ = await mirror_manager.request_with_failover(path, timeout=3.5)
                if resp.status_code == 200:
                    item = _unwrap_payload(resp.json())
                    if not item:
                        continue
                    tracks, _ = self._normalize_search_response({"items": [item]}, base_url)
                    if tracks:
                        await db.set_cached_track(clean_id, tracks[0].model_dump())
                        return tracks[0]
            except Exception:
                continue

        return None

    # ── Stream Resolution & DASH Handling ────────────────────────────────────

    async def resolve_stream(
        self,
        track_id: str,
        quality: str = QualityTier.HI_RES_LOSSLESS.value,
        base_url: str = "",
    ) -> AddonStream:
        """
        The stream handed to clients. Its URL always points at our own audio
        proxy, never at the upstream CDN: signed CDN URLs expire and drop
        connections mid-track, and a client holding one has no way to recover,
        so it stalls and then skips to the next song. The proxy re-resolves
        stale URLs and resumes interrupted downloads transparently.
        Set PROXY_STREAMS=false to hand out the direct upstream URL instead.
        """
        clean_id = sanitize_track_id(track_id)
        quality = normalize_quality(quality)
        stream_obj = await self.resolve_upstream(clean_id, quality)
        if PROXY_STREAMS or not stream_obj.url or "/stream/audio/" in stream_obj.url:
            stream_obj.url = _internal_stream_url(base_url, clean_id, quality)
        return stream_obj

    async def resolve_upstream(
        self,
        track_id: str,
        quality: str = QualityTier.HI_RES_LOSSLESS.value,
        *,
        force_refresh: bool = False,
    ) -> AddonStream:
        """
        Resolves stream metadata plus, when one exists, a direct upstream audio URL.
        `url` is "" when there is no direct URL (snowflake IDs, failed resolution).
        `force_refresh` skips the cache, used when a cached URL has gone stale.
        """
        clean_id = sanitize_track_id(track_id)
        quality = normalize_quality(quality)

        if not force_refresh:
            cached = await db.get_cached_stream(clean_id, quality)
            if cached:
                stream_obj = AddonStream(**cached)
                if "/stream/audio/" in stream_obj.url:
                    stream_obj.url = ""  # entry written by an older version
                return stream_obj

        quality_chain = [quality]
        if quality == QualityTier.HI_RES_LOSSLESS.value:
            quality_chain.extend([QualityTier.LOSSLESS.value, QualityTier.HIGH.value])
        elif quality == QualityTier.LOSSLESS.value:
            quality_chain.append(QualityTier.HIGH.value)

        resolved_stream: Optional[AddonStream] = None
        ttl = CACHE_TTL_STREAM

        if is_snowflake_id(clean_id):
            # Snowflake IDs are served as plain files by <mirror>/track/<id>,
            # which is exactly what our audio proxy fetches.
            resolved_stream = _build_stream("", QualityTier.HI_RES_LOSSLESS.value)
        else:
            for tier in quality_chain:
                try:
                    candidate = await self._query_upstream_playback(clean_id, tier)
                except Exception:
                    continue
                # Only a directly playable URL counts. A tier that yields nothing
                # playable (e.g. segmented DASH) falls through to the next tier.
                if candidate and candidate.url:
                    resolved_stream = candidate
                    break

        if resolved_stream is None:
            fallback = _build_stream("", quality)
            fallback.bitDepth = 16.0
            fallback.sampleRate = 44100.0
            resolved_stream = fallback
            ttl = CACHE_TTL_STREAM_FALLBACK

        await db.set_cached_stream(clean_id, quality, resolved_stream.model_dump(), ttl=ttl)
        return resolved_stream

    async def _query_upstream_playback(
        self,
        track_id: str,
        quality: str,
    ) -> Optional[AddonStream]:
        endpoints = [
            f"/track/?id={track_id}&quality={quality}",
            f"/track?id={track_id}&quality={quality}",
            f"/track/{track_id}",
        ]

        for ep in endpoints:
            try:
                resp, _ = await mirror_manager.request_with_failover(ep, timeout=4.0)
                if resp.status_code != 200:
                    continue
                payload = _unwrap_payload(resp.json())
                if not payload:
                    continue
                stream = self._parse_playback_payload(payload, track_id, quality)
                if stream:
                    return stream
            except Exception:
                continue
        return None

    def _parse_playback_payload(
        self,
        payload: Dict[str, Any],
        track_id: str,
        quality: str,
    ) -> Optional[AddonStream]:
        """Returns a stream with a directly playable URL, or None if the payload has none."""
        # Upstream reports the tier it actually granted, which can be lower than requested.
        granted = payload.get("audioQuality")
        if isinstance(granted, str) and granted.upper() in VALID_QUALITIES:
            quality = granted.upper()
        sample_rate = _as_float(payload.get("sampleRate"))
        bit_depth = _as_float(payload.get("bitDepth"))
        lossless = quality in (QualityTier.HI_RES_LOSSLESS.value, QualityTier.LOSSLESS.value)

        for key in ("OriginalTrackUrl", "url"):
            direct_url = payload.get(key)
            if _is_audio_url(direct_url):
                codec = "flac" if ".flac" in direct_url or lossless else "aac"
                return _build_stream(
                    direct_url, quality, codec=codec, sample_rate=sample_rate, bit_depth=bit_depth
                )

        manifest_b64 = payload.get("manifest")
        manifest_mime = payload.get("manifestMimeType") or ""
        if not manifest_b64 or not isinstance(manifest_b64, str):
            return None

        try:
            decoded_text = base64.b64decode(manifest_b64).decode("utf-8", errors="ignore")
        except Exception as e:
            logger.warning(f"Failed to decode manifest for track {track_id}: {e}")
            return None

        if manifest_mime == "application/dash+xml" or "<MPD" in decoded_text:
            return self._parse_dash_manifest(decoded_text, track_id, quality, sample_rate, bit_depth)

        try:
            manifest_json = json.loads(decoded_text)
        except ValueError:
            return None
        if not isinstance(manifest_json, dict):
            return None

        urls = manifest_json.get("urls")
        if not isinstance(urls, list) or not urls or not _is_audio_url(urls[0]):
            return None

        # Tidal's JSON manifest names this field "codecs"
        codec = manifest_json.get("codecs") or manifest_json.get("codec")
        if not isinstance(codec, str) or not codec:
            codec = "flac" if lossless else "aac"
        mime = manifest_json.get("mimeType")
        encryption = manifest_json.get("encryptionType")
        return _build_stream(
            urls[0],
            quality,
            codec=codec,
            mime_type=mime if isinstance(mime, str) and mime else None,
            sample_rate=sample_rate,
            bit_depth=bit_depth,
            encrypted=isinstance(encryption, str) and encryption.upper() != "NONE",
        )

    def _parse_dash_manifest(
        self,
        xml_text: str,
        track_id: str,
        quality: str,
        sample_rate: Optional[float] = None,
        bit_depth: Optional[float] = None,
    ) -> Optional[AddonStream]:
        """
        Returns a stream only when the MPD points at one complete file (BaseURL).
        Segmented DASH cannot be served by the progressive proxy, so it returns
        None and the caller falls back to the next quality tier.
        """
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logger.warning(f"Error parsing DASH XML for track {track_id}: {e}")
            return None

        def local(tag: str) -> str:
            return tag.rsplit("}", 1)[-1]

        segmented = any(local(el.tag) in ("SegmentTemplate", "SegmentList") for el in root.iter())
        base_urls = [el for el in root.iter() if local(el.tag) == "BaseURL" and el.text]
        if segmented or not base_urls:
            logger.debug(f"Track {track_id}: DASH manifest at {quality} is segmented, skipping tier")
            return None

        direct_url = base_urls[0].text.strip()
        if not _is_audio_url(direct_url):
            return None

        codec = "flac"
        bitrate = None
        for el in root.iter():
            if local(el.tag) == "Representation":
                codec = el.get("codecs") or codec
                bitrate = _as_float(el.get("bandwidth"))
                sample_rate = _as_float(el.get("audioSamplingRate")) or sample_rate
                break

        return _build_stream(
            direct_url,
            quality,
            codec=codec,
            sample_rate=sample_rate,
            bit_depth=bit_depth,
            bitrate=bitrate,
        )

    # ── Live Audio Proxy Streaming with HTTP Range Support & Disk Caching ────

    @staticmethod
    def _content_disposition(
        track_meta: Optional[Dict[str, Any]],
        fallback_title: str,
        ext: str,
    ) -> Optional[str]:
        """
        Builds a Content-Disposition value that is always valid latin-1.
        HTTP headers cannot carry raw non-latin-1 text, so the real name goes
        into the percent-encoded filename* parameter.
        """
        if not track_meta:
            return None
        artist = str(track_meta.get("artist") or "Unknown")
        title = str(track_meta.get("title") or fallback_title)
        display = re.sub(r"[\x00-\x1f\x7f]+", " ", f"{artist} - {title}").strip()[:180] + ext
        ascii_name = re.sub(r"[^A-Za-z0-9 \-\.\(\),_]", "_", display)
        return f"inline; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(display, safe='')}"

    @staticmethod
    def _parse_range(range_header: Optional[str], file_size: int) -> Optional[Tuple[int, int]]:
        """
        Returns (start, end) for a single satisfiable byte range, None when the
        header is absent or unsupported (serve the whole file), raises 416 otherwise.
        """
        if not range_header:
            return None
        match = _RANGE_RE.fullmatch(range_header.strip())
        if not match or (not match.group(1) and not match.group(2)):
            return None

        if match.group(1):
            start = int(match.group(1))
            end = int(match.group(2)) if match.group(2) else file_size - 1
        else:  # suffix range: the last N bytes
            suffix_length = int(match.group(2))
            start = max(file_size - suffix_length, 0) if suffix_length > 0 else file_size
            end = file_size - 1

        if start >= file_size or end < start:
            raise HTTPException(
                status_code=416,
                detail="Requested range not satisfiable",
                headers={"Content-Range": f"bytes */{file_size}"},
            )
        return start, min(end, file_size - 1)

    def _serve_local_audio_file(
        self,
        file_path: str,
        request: Request,
        content_disposition: Optional[str] = None,
    ) -> Response:
        file_size = os.path.getsize(file_path)
        media_type = "audio/flac" if file_path.endswith(".flac") else "audio/mp4"

        response_headers = {
            "Content-Type": media_type,
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=31536000, immutable",
            "X-HiFi-Cache": "HIT",
        }
        if content_disposition:
            response_headers["Content-Disposition"] = content_disposition

        byte_range = self._parse_range(request.headers.get("range"), file_size)
        if byte_range:
            start, end = byte_range
            status_code = 206
            response_headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        else:
            start, end = 0, file_size - 1
            status_code = 200
        content_length = end - start + 1
        response_headers["Content-Length"] = str(content_length)

        if request.method == "HEAD":
            return Response(status_code=status_code, headers=response_headers)

        def iter_file():
            with open(file_path, "rb") as f:
                f.seek(start)
                remaining = content_length
                while remaining > 0:
                    chunk = f.read(min(65536, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    yield chunk

        return StreamingResponse(iter_file(), status_code=status_code, headers=response_headers)

    @staticmethod
    def _plan_cache_tee(
        upstream_resp: httpx.Response,
        range_header: Optional[str],
    ) -> Tuple[bool, Optional[int]]:
        """Decides whether this response is the complete file and how long it should be."""
        if range_header is not None and range_header.strip() != "bytes=0-":
            return False, None

        if upstream_resp.status_code == 200:
            length = upstream_resp.headers.get("Content-Length", "")
            return True, int(length) if length.isdigit() else None

        match = re.match(r"bytes 0-(\d+)/(\d+)", upstream_resp.headers.get("Content-Range", ""))
        if match and int(match.group(1)) == int(match.group(2)) - 1:
            return True, int(match.group(2))
        return False, None

    @staticmethod
    def _finalize_cache_file(
        handle: Any,
        temp_path: str,
        track_id: str,
        complete: bool,
    ) -> None:
        """
        Closes the temp file and either promotes it into the cache or deletes it.
        Fully synchronous and never raises, so it is safe to run from a `finally`
        block even while the request task is being cancelled.
        """
        try:
            handle.close()
        except Exception:
            pass
        try:
            if complete:
                with open(temp_path, "rb") as hf:
                    header = hf.read(12)
                if header.startswith(b"fLaC"):
                    ext = ".flac"
                elif header[4:8] == b"ftyp":
                    ext = ".m4a"
                else:
                    ext = None  # not audio we recognise: never let it into the cache
                if ext:
                    final_path = os.path.join(prefetcher.cache_dir, f"{track_id}{ext}")
                    os.replace(temp_path, final_path)
                    logger.info(
                        f"[CACHE] Saved {track_id}{ext} to local audio disk cache "
                        f"({os.path.getsize(final_path) // 1024} KB)"
                    )
                    # The prefetch worker only prunes while PREFETCH_ENABLED is on,
                    # so enforce MAX_CACHE_GB here too or the cache grows forever.
                    prune = getattr(prefetcher, "_prune_cache_if_needed", None)
                    if prune:
                        try:
                            asyncio.get_running_loop().run_in_executor(None, prune)
                        except RuntimeError:
                            pass
                    return
                logger.warning(f"[CACHE] Not caching {track_id}: unrecognised audio container")
        except Exception as e:
            logger.warning(f"[CACHE] Could not finalize cache file for {track_id}: {e}")
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass

    # ── Upstream connections ────────────────────────────────────────────────

    def _get_stream_client(self) -> httpx.AsyncClient:
        if self._stream_client is None or self._stream_client.is_closed:
            from app.mirror_manager import VERIFY_TLS

            self._stream_client = httpx.AsyncClient(
                verify=VERIFY_TLS,
                follow_redirects=True,
                # `read` is the longest gap between two chunks before the
                # connection counts as stalled and gets resumed.
                timeout=httpx.Timeout(connect=5.0, read=STREAM_STALL_TIMEOUT, write=5.0, pool=30.0),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
            )
        return self._stream_client

    @staticmethod
    def _is_non_audio(content_type: str) -> bool:
        """HTML parking pages, XML S3 errors and JSON API replies are not audio."""
        ct = content_type.lower()
        return ct.startswith("text/") or "html" in ct or "xml" in ct or "json" in ct

    async def _open_upstream(self, url: str, headers: Dict[str, str]) -> Optional[httpx.Response]:
        """Opens a streaming GET. Returns the response only if it is audio with status 200/206."""
        client = self._get_stream_client()
        resp: Optional[httpx.Response] = None
        try:
            for attempt in range(2):
                resp = await client.send(client.build_request("GET", url, headers=headers), stream=True)
                if resp.status_code == 429 and attempt == 0:
                    logger.warning(f"Audio upstream {url} returned 429, backing off 2.0s...")
                    await resp.aclose()
                    resp = None
                    await asyncio.sleep(2.0)
                    continue
                break
            if resp is None or resp.status_code not in (200, 206):
                logger.debug(f"Audio upstream {url} returned status {resp.status_code if resp else '?'}")
                return None
            content_type = resp.headers.get("Content-Type", "audio/flac")
            if self._is_non_audio(content_type):
                logger.warning(f"Audio proxy: skipping {url} — non-audio content-type '{content_type}'")
                return None
            ok, resp = resp, None  # caller owns it now
            return ok
        except Exception as e:
            logger.debug(f"Audio upstream {url} failed: {e}")
            return None
        finally:
            if resp is not None:
                try:
                    await resp.aclose()
                except Exception:
                    pass

    async def _candidate_urls(self, clean_id: str, quality: str, *, refresh: bool) -> List[str]:
        """
        Where the audio for a track can be fetched. Snowflake IDs are plain files
        at <mirror>/track/<id>; every other ID needs a resolved direct URL, since
        for those <mirror>/track/<id> is a JSON API reply, not audio.
        """
        if is_snowflake_id(clean_id):
            return list(dict.fromkeys(f"{m}/track/{clean_id}" for m in mirror_manager.get_ranked_mirrors()))
        try:
            resolved = await self.resolve_upstream(clean_id, quality, force_refresh=refresh)
        except Exception as e:
            logger.debug(f"Stream resolution failed for {clean_id}: {e}")
            return []
        return [resolved.url] if resolved.url.startswith("http") else []

    async def _open_first_available(
        self,
        clean_id: str,
        quality: str,
        headers: Dict[str, str],
    ) -> Tuple[Optional[httpx.Response], str]:
        """
        Tries the cached sources first, then re-resolves once. A cached direct URL
        is a signed CDN link that can expire long before the cache entry does.
        """
        tried: Set[str] = set()
        for refresh in (False, True):
            if refresh and is_snowflake_id(clean_id):
                break
            for url in await self._candidate_urls(clean_id, quality, refresh=refresh):
                if url in tried:
                    continue
                tried.add(url)
                resp = await self._open_upstream(url, headers)
                if resp is not None:
                    return resp, url
            if not refresh and tried:
                logger.info(f"[STREAM] Cached sources for {clean_id} failed, re-resolving")
        return None, ""

    @staticmethod
    def _response_span(resp: httpx.Response) -> Tuple[int, Optional[int], Optional[int]]:
        """(first byte offset, last byte offset or None, total size or None) of a response body."""
        if resp.status_code == 206:
            match = _CONTENT_RANGE_RE.match(resp.headers.get("Content-Range", "").strip())
            if match:
                total = int(match.group(3)) if match.group(3).isdigit() else None
                return int(match.group(1)), int(match.group(2)), total
            return 0, None, None
        length = resp.headers.get("Content-Length", "")
        if length.isdigit():
            return 0, int(length) - 1, int(length)
        return 0, None, None

    async def _reopen_at(
        self,
        clean_id: str,
        quality: str,
        base_headers: Dict[str, str],
        last_url: str,
        position: int,
        end: Optional[int],
        total: Optional[int],
        refresh: bool,
    ) -> Tuple[Optional[httpx.Response], str, int]:
        """
        Re-opens the track at byte `position`. Returns (response, url, bytes to
        discard from the start of the response). A different source is only
        accepted when it is provably the same file (same total size), because
        splicing bytes from another quality tier would corrupt the audio.
        """
        headers = dict(base_headers)
        headers["Range"] = f"bytes={position}-{'' if end is None else end}"

        urls = [last_url] if last_url else []
        if refresh:
            urls += await self._candidate_urls(clean_id, quality, refresh=True)
        for url in dict.fromkeys(urls):
            resp = await self._open_upstream(url, headers)
            if resp is None:
                continue
            first, _, resp_total = self._response_span(resp)
            same_file = url == last_url or (total is not None and resp_total == total)
            if same_file and resp.status_code == 206 and first <= position:
                return resp, url, position - first
            if same_file and resp.status_code == 200:
                return resp, url, position  # range ignored: whole file again
            await resp.aclose()
        return None, last_url, 0

    async def _proxy_body(
        self,
        upstream_resp: httpx.Response,
        source_url: str,
        clean_id: str,
        quality: str,
        base_headers: Dict[str, str],
        tee_to_cache: bool,
        expected_total_bytes: Optional[int],
    ) -> AsyncIterator[bytes]:
        """
        Streams upstream audio to the client, optionally writing a copy to the disk cache.

        If upstream drops, stalls or ends early, the request is re-opened from the
        exact byte that was reached, so the client sees one continuous response
        instead of a broken one. Without this, a player stalls on the broken
        connection, retries, and when that retry also fails skips the track.
        """
        handle = None
        temp_path = ""
        bytes_written = 0
        completed_cleanly = False
        current: Optional[httpx.Response] = upstream_resp
        position, end, total = self._response_span(upstream_resp)
        skip = 0
        attempts = 0
        resumed_at = position

        if tee_to_cache:
            temp_path = os.path.join(prefetcher.cache_dir, f".tmp_proxy_{clean_id}_{uuid.uuid4().hex[:8]}")
            try:
                handle = open(temp_path, "wb")
            except OSError as e:
                # A cache problem must never stop playback.
                logger.warning(f"[CACHE] Cannot open cache file for {clean_id}: {e}")
                handle = None

        try:
            while True:
                error: Optional[BaseException] = None
                try:
                    async for chunk in current.aiter_bytes(chunk_size=65536):
                        if skip:
                            if len(chunk) <= skip:
                                skip -= len(chunk)
                                continue
                            chunk = chunk[skip:]
                            skip = 0
                        if end is not None:
                            chunk = chunk[: max(0, end + 1 - position)]
                        if not chunk:
                            break
                        if handle is not None:
                            try:
                                await asyncio.to_thread(handle.write, chunk)
                                bytes_written += len(chunk)
                            except OSError as e:
                                logger.warning(f"[CACHE] Disk write failed for {clean_id}, caching disabled: {e}")
                                self._finalize_cache_file(handle, temp_path, clean_id, complete=False)
                                handle = None
                        position += len(chunk)
                        yield chunk
                        if end is not None and position > end:
                            break
                except httpx.HTTPError as e:
                    error = e

                if end is not None and position > end:
                    completed_cleanly = True
                    break
                if error is None and end is None:
                    completed_cleanly = True  # no length to check against
                    break
                reason = error or "upstream closed the connection early"

                # Retries are budgeted per outage: a resume that delivered real
                # progress resets the count, so a long track with a few separate
                # hiccups is not given up on.
                if position - resumed_at >= 256 * 1024:
                    attempts = 0

                old, current = current, None
                try:
                    await old.aclose()
                except Exception:
                    pass

                while current is None:
                    if attempts >= STREAM_RESUME_ATTEMPTS:
                        logger.error(
                            f"[STREAM] Giving up on {clean_id} at byte {position}"
                            f"{f'/{total}' if total else ''} after {attempts} resume attempts ({reason})"
                        )
                        raise RuntimeError(f"Upstream audio for {clean_id} could not be resumed")
                    attempts += 1
                    logger.warning(
                        f"[STREAM] Upstream for {clean_id} interrupted at byte {position}"
                        f"{f'/{total}' if total else ''} ({reason!r}); "
                        f"resuming (attempt {attempts}/{STREAM_RESUME_ATTEMPTS})"
                    )
                    await asyncio.sleep(min(0.5 * (2 ** (attempts - 1)), 4.0))
                    current, source_url, skip = await self._reopen_at(
                        clean_id, quality, base_headers, source_url, position, end, total,
                        refresh=attempts > 1,
                    )
                    reason = "re-open failed"
                resumed_at = position
                logger.info(f"[STREAM] Resumed {clean_id} at byte {position}")
        finally:
            # File cleanup first and without awaiting: if the client disconnected,
            # this task may be cancelled and any await here could be interrupted.
            if handle is not None:
                complete = (
                    completed_cleanly
                    and bytes_written > MIN_AUDIO_BYTES
                    and (expected_total_bytes is None or bytes_written >= expected_total_bytes)
                )
                self._finalize_cache_file(handle, temp_path, clean_id, complete)
            if current is not None:
                await current.aclose()

    async def stream_audio_proxy(
        self,
        track_id: str,
        request: Request,
    ) -> Response:
        clean_id = sanitize_track_id(track_id)
        quality = normalize_quality(request.query_params.get("quality"))
        track_meta = await db.get_cached_track(clean_id)

        # ── 1. Check local audio cache on disk (CACHE HIT) ────────────────────
        cached_path = prefetcher.get_cached_track_path(clean_id)
        if cached_path:
            try:
                ext = os.path.splitext(cached_path)[1] or ".flac"
                response = self._serve_local_audio_file(
                    cached_path, request, self._content_disposition(track_meta, clean_id, ext)
                )
            except OSError as e:
                # File vanished between the check and the read (pruned / cleared)
                logger.warning(f"[CACHE] Cached file for {clean_id} unreadable, using upstream: {e}")
            else:
                logger.info(f"[CACHE HIT] Serving track {clean_id} directly from local disk ({cached_path})")
                prefetcher.record_request(is_hit=True)
                self._spawn(prefetcher.queue_upcoming_from_context(clean_id))
                return response

        prefetcher.record_request(is_hit=False)
        self._spawn(prefetcher.queue_upcoming_from_context(clean_id))

        # ── 2. Stream from upstream mirror (CACHE MISS) ───────────────────────
        base_headers = {
            "User-Agent": _UA,
            "Accept": "*/*",
            # Byte counts and Content-Length are forwarded as-is, so the body
            # must not be transparently decompressed on the way through.
            "Accept-Encoding": "identity",
        }
        forward_headers = dict(base_headers)
        range_header = request.headers.get("range")
        if range_header:
            forward_headers["Range"] = range_header
        elif request.method == "HEAD":
            forward_headers["Range"] = "bytes=0-0"

        upstream_resp, source_url = await self._open_first_available(clean_id, quality, forward_headers)
        if upstream_resp is None:
            raise HTTPException(status_code=502, detail="All upstream audio sources failed")

        handed_off = False
        try:
            status_code = upstream_resp.status_code
            audio_content_type = upstream_resp.headers.get("Content-Type", "audio/flac")
            content_type = audio_content_type.lower()

            response_headers = {
                "Content-Type": audio_content_type,
                "Accept-Ranges": "bytes",
                "Cache-Control": "public, max-age=31536000, immutable",
                "X-HiFi-Cache": "MISS",
            }
            if "Content-Length" in upstream_resp.headers:
                response_headers["Content-Length"] = upstream_resp.headers["Content-Length"]
            if "Content-Range" in upstream_resp.headers:
                response_headers["Content-Range"] = upstream_resp.headers["Content-Range"]

            ext = ".m4a" if ("mp4" in content_type or "m4a" in content_type or "aac" in content_type) else ".flac"
            disposition = self._content_disposition(track_meta, clean_id, ext)
            if disposition:
                response_headers["Content-Disposition"] = disposition

            if request.method == "HEAD":
                head_status = status_code
                size_unknown = False
                if not range_header and status_code == 206:
                    # We probed with bytes=0-0; report the full size, not 1 byte.
                    total_match = re.search(r"/(\d+)\s*$", response_headers.pop("Content-Range", ""))
                    if total_match:
                        response_headers["Content-Length"] = total_match.group(1)
                    else:
                        response_headers.pop("Content-Length", None)
                        size_unknown = True
                    head_status = 200
                head_response = Response(status_code=head_status, headers=response_headers)
                if size_unknown and "content-length" in head_response.headers:
                    del head_response.headers["content-length"]
                return head_response

            tee_to_cache, expected_total_bytes = self._plan_cache_tee(upstream_resp, range_header)
            body = self._proxy_body(
                upstream_resp, source_url, clean_id, quality, base_headers, tee_to_cache, expected_total_bytes
            )
            handed_off = True  # the streaming body now owns and closes the response
            return StreamingResponse(body, status_code=status_code, headers=response_headers)
        finally:
            if not handed_off:
                try:
                    await upstream_resp.aclose()
                except Exception:
                    pass

    # ── Album Details Lookup ─────────────────────────────────────────────────

    async def _fetch_first_json(self, endpoints: List[str]) -> Optional[Dict[str, Any]]:
        for ep in endpoints:
            try:
                resp, _ = await mirror_manager.request_with_failover(ep, timeout=4.0)
                if resp.status_code == 200:
                    item = _unwrap_payload(resp.json())
                    if item:
                        return item
            except Exception:
                continue
        return None

    async def get_album(self, album_id: str, base_url: str = "") -> AlbumDetail:
        clean_album_id = sanitize_track_id(album_id)
        item = await self._fetch_first_json(
            [
                f"/album/?id={clean_album_id}",
                f"/album?id={clean_album_id}",
                f"/release/{clean_album_id}",
            ]
        )
        if not item:
            raise HTTPException(status_code=404, detail="Album not found across mirrors")

        tracks, _ = self._normalize_search_response(item, base_url)

        artist_obj = item.get("artist")
        artist_name = ""
        if isinstance(artist_obj, dict):
            artist_name = str(artist_obj.get("name") or "")
        elif isinstance(artist_obj, str):
            artist_name = artist_obj
        artist_name = artist_name or str(item.get("artistName") or "") or "Unknown Artist"
        artist_id = artist_obj.get("id") if isinstance(artist_obj, dict) else None
        release_date = item.get("releaseDate")
        audio_quality = item.get("audioQuality")

        return AlbumDetail(
            id=clean_album_id,
            title=str(item.get("title") or "Unknown Album"),
            artist=artist_name,
            artistId=str(artist_id) if artist_id else None,
            cover=cover_id_to_url(item.get("cover")),
            releaseDate=release_date if isinstance(release_date, str) else None,
            numberOfTracks=len(tracks),
            duration=_as_float(item.get("duration")),
            audioQuality=audio_quality if isinstance(audio_quality, str) and audio_quality else "LOSSLESS",
            tracks=tracks,
        )

    # ── Artist Details Lookup ────────────────────────────────────────────────

    async def get_artist(self, artist_id: str, base_url: str = "") -> ArtistDetail:
        clean_artist_id = sanitize_track_id(artist_id)
        item = await self._fetch_first_json(
            [
                f"/artist/?id={clean_artist_id}",
                f"/artist?id={clean_artist_id}",
            ]
        )
        if not item:
            raise HTTPException(status_code=404, detail="Artist not found across mirrors")

        tracks, _ = self._normalize_search_response(item, base_url)

        albums = item.get("albums")
        if isinstance(albums, dict):
            albums = albums.get("items")
        if not isinstance(albums, list):
            albums = []
        bio = item.get("bio")

        return ArtistDetail(
            id=clean_artist_id,
            name=str(item.get("name") or "Unknown Artist"),
            picture=cover_id_to_url(item.get("picture")),
            bio=bio if isinstance(bio, str) else None,
            topTracks=tracks,
            albums=[a for a in albums if isinstance(a, dict)],
        )


stream_resolver = StreamResolver()
