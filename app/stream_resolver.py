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
from typing import Any, Dict, List, Optional, Tuple
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


def sanitize_track_id(track_id: str) -> str:
    """Strips file extensions and prevents path traversal characters."""
    raw = str(track_id).replace(".flac", "").replace(".m4a", "").strip()
    clean = re.sub(r"[^a-zA-Z0-9_\-]", "", raw)
    if not clean:
        raise HTTPException(status_code=400, detail="Invalid track ID")
    return clean


def cover_id_to_url(cover_id: Optional[str]) -> Optional[str]:
    """Converts a Tidal cover UUID to a high-resolution CDN URL."""
    if not cover_id:
        return None
    if cover_id.startswith("http://") or cover_id.startswith("https://"):
        return cover_id
    formatted_path = cover_id.replace("-", "/")
    return f"https://resources.tidal.com/images/{formatted_path}/1280x1280.jpg"


class StreamResolver:
    def __init__(self):
        self._stream_client: Optional[httpx.AsyncClient] = None

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

        cached = await db.get_cached_search(cleaned_query, quality, limit, offset)
        if cached:
            tracks = []
            for t in cached.get("tracks", []):
                track_obj = AddonTrack(**t)
                if base_url:
                    track_obj.streamURL = f"{base_url.rstrip('/')}/stream/audio/{track_obj.id}.flac"
                tracks.append(track_obj)
            return AddonSearchResponse(tracks=tracks)

        tracks: List[AddonTrack] = []
        raw_items: List[Dict[str, Any]] = []

        strategies = [
            ("/search", {"q": cleaned_query}),
            ("/search", {"s": cleaned_query, "limit": limit, "offset": offset}),
            ("/search/?s=" + cleaned_query, {"limit": limit, "offset": offset}),
            ("/search?s=" + cleaned_query, {"limit": limit, "offset": offset}),
        ]

        client = await mirror_manager.get_client()
        ranked_mirrors = mirror_manager.get_ranked_mirrors()

        async with mirror_manager.rate_limit():
            for mirror_url in ranked_mirrors:
                mirror_failed = False
                for path_template, params in strategies:
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

        paged_tracks = tracks[offset : offset + limit] if offset > 0 else tracks[:limit]

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
            raw_items = data["tracks"]
            for item in raw_items:
                track_id = str(item.get("trackId") or item.get("id") or "")
                if not track_id:
                    continue

                title = item.get("title") or "Unknown Title"
                artist_names = item.get("artistNames") or []
                artist = ", ".join(artist_names) if artist_names else (item.get("artist") or "Unknown Artist")
                album = item.get("albumTitle") or item.get("releaseTitle") or ""
                duration_ms = item.get("duration") or 0
                duration_sec = round(float(duration_ms) / 1000.0, 2) if duration_ms > 0 else None
                artwork = item.get("artwork") or item.get("cover")
                stream_url = f"{base_url.rstrip('/')}/stream/audio/{track_id}.flac" if base_url else None

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
                    isrc=item.get("isrc"),
                    trackNumber=item.get("trackNumber"),
                )
                tracks.append(track)
            return tracks, raw_items

        items = []
        if isinstance(data, dict):
            if "data" in data and isinstance(data["data"], dict) and "items" in data["data"]:
                items = data["data"]["items"]
            elif "items" in data and isinstance(data["items"], list):
                items = data["items"]
            elif "tracks" in data and isinstance(data["tracks"], dict) and "items" in data["tracks"]:
                items = data["tracks"]["items"]
        elif isinstance(data, list):
            items = data

        raw_items = items
        for item in items:
            t = item.get("item", item) if isinstance(item, dict) else item
            if not isinstance(t, dict):
                continue

            track_id = str(t.get("id") or "")
            if not track_id:
                continue

            title = t.get("title") or "Unknown Title"
            artists_list = t.get("artists") or []
            if artists_list and isinstance(artists_list, list):
                artist = ", ".join([a.get("name", "") for a in artists_list if isinstance(a, dict) and a.get("name")])
            else:
                artist = (
                    t.get("artist", {}).get("name")
                    if isinstance(t.get("artist"), dict)
                    else (t.get("artist") or "Unknown Artist")
                )

            album_obj = t.get("album") if isinstance(t.get("album"), dict) else {}
            album_title = album_obj.get("title") or ""
            cover_id = album_obj.get("cover") or t.get("cover")
            artwork = cover_id_to_url(cover_id)

            duration_sec = float(t.get("duration", 0)) if t.get("duration") else None
            quality = t.get("audioQuality") or "LOSSLESS"
            fmt = "flac" if "LOSSLESS" in quality else "aac"
            stream_url = f"{base_url.rstrip('/')}/stream/audio/{track_id}.flac" if base_url else None

            audio_modes = t.get("audioModes", ["STEREO"])
            is_atmos = "DOLBY_ATMOS" in audio_modes

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
                isrc=t.get("isrc"),
                trackNumber=t.get("trackNumber"),
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
                track_obj.streamURL = f"{base_url.rstrip('/')}/stream/audio/{track_obj.id}.flac"
            return track_obj

        for path in [f"/info/?id={clean_id}", f"/info?id={clean_id}", f"/track/{clean_id}"]:
            try:
                resp, _ = await mirror_manager.request_with_failover(path, timeout=3.5)
                if resp.status_code == 200:
                    data = resp.json()
                    item = data.get("data", data)
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
        clean_id = sanitize_track_id(track_id)

        cached = await db.get_cached_stream(clean_id, quality)
        if cached:
            stream_obj = AddonStream(**cached)
            if base_url and "/stream/audio/" in stream_obj.url:
                stream_obj.url = f"{base_url.rstrip('/')}/stream/audio/{clean_id}.flac"
            return stream_obj

        quality_chain = [quality]
        if quality == QualityTier.HI_RES_LOSSLESS.value:
            quality_chain.extend([QualityTier.LOSSLESS.value, QualityTier.HIGH.value])
        elif quality == QualityTier.LOSSLESS.value:
            quality_chain.append(QualityTier.HIGH.value)

        resolved_stream = None
        is_snowflake = bool(re.match(r"^\d{17,20}$", clean_id))

        if is_snowflake:
            stream_endpoint = (
                f"{base_url.rstrip('/')}/stream/audio/{clean_id}.flac"
                if base_url
                else f"/stream/audio/{clean_id}.flac"
            )
            resolved_stream = AddonStream(
                url=stream_endpoint,
                format="flac",
                quality=QualityTier.HI_RES_LOSSLESS.value,
                streamQuality=QualityTier.HI_RES_LOSSLESS.value,
                audioQuality=QualityTier.HI_RES_LOSSLESS.value,
                codec="flac",
                fileCodec="flac",
                container="flac",
                manifest="none",
                mimeType="audio/flac",
                sampleRate=96000.0,
                bitDepth=24.0,
                bitrate=3000000.0,
                audioMode="STEREO",
                encrypted=False,
            )
        else:
            for tier in quality_chain:
                try:
                    resolved_stream = await self._query_upstream_playback(clean_id, tier, base_url)
                    if resolved_stream and resolved_stream.url:
                        break
                except Exception:
                    continue

        if not resolved_stream or not resolved_stream.url:
            stream_endpoint = (
                f"{base_url.rstrip('/')}/stream/audio/{clean_id}.flac"
                if base_url
                else f"/stream/audio/{clean_id}.flac"
            )
            resolved_stream = AddonStream(
                url=stream_endpoint,
                format="flac",
                quality=quality,
                codec="flac",
                mimeType="audio/flac",
                bitDepth=16.0,
                sampleRate=44100.0,
                manifest="none",
                encrypted=False,
                error=None,
            )

        await db.set_cached_stream(clean_id, quality, resolved_stream.model_dump(), ttl=CACHE_TTL_STREAM)
        return resolved_stream

    async def _query_upstream_playback(
        self,
        track_id: str,
        quality: str,
        base_url: str = "",
    ) -> Optional[AddonStream]:
        endpoints = [
            f"/track/?id={track_id}&quality={quality}",
            f"/track?id={track_id}&quality={quality}",
            f"/track/{track_id}",
        ]

        for ep in endpoints:
            try:
                resp, mirror_url = await mirror_manager.request_with_failover(ep, timeout=4.0)
                if resp.status_code == 200:
                    data = resp.json()
                    item = data.get("data", data)
                    return self._parse_playback_payload(item, track_id, quality, mirror_url, base_url)
            except Exception:
                continue
        return None

    def _parse_playback_payload(
        self,
        payload: Dict[str, Any],
        track_id: str,
        quality: str,
        mirror_url: str,
        base_url: str,
    ) -> AddonStream:
        if "url" in payload and payload["url"]:
            direct_url = payload["url"]
            fmt = (
                "flac"
                if ".flac" in direct_url
                or quality in (QualityTier.HI_RES_LOSSLESS.value, QualityTier.LOSSLESS.value)
                else "aac"
            )
            return AddonStream(
                url=direct_url,
                format=fmt,
                quality=quality,
                codec=fmt,
                mimeType="audio/flac" if fmt == "flac" else "audio/aac",
                bitDepth=24.0 if quality == QualityTier.HI_RES_LOSSLESS.value else 16.0,
                sampleRate=96000.0 if quality == QualityTier.HI_RES_LOSSLESS.value else 44100.0,
                manifest="none",
                encrypted=False,
            )

        manifest_b64 = payload.get("manifest")
        manifest_mime = payload.get("manifestMimeType", "")

        if manifest_b64 and isinstance(manifest_b64, str):
            try:
                decoded_bytes = base64.b64decode(manifest_b64)
                decoded_text = decoded_bytes.decode("utf-8", errors="ignore")

                if manifest_mime == "application/dash+xml" or "<MPD" in decoded_text:
                    return self._parse_dash_manifest(decoded_text, track_id, quality, base_url)

                try:
                    manifest_json = json.loads(decoded_text)
                    urls = manifest_json.get("urls", [])
                    if urls and isinstance(urls, list) and len(urls) > 0:
                        direct_url = urls[0]
                        mime = manifest_json.get("mimeType", "audio/flac")
                        codec = manifest_json.get("codec", "flac")
                        return AddonStream(
                            url=direct_url,
                            format="flac" if "flac" in codec else "aac",
                            quality=quality,
                            codec=codec,
                            mimeType=mime,
                            bitDepth=24.0 if quality == QualityTier.HI_RES_LOSSLESS.value else 16.0,
                            sampleRate=96000.0 if quality == QualityTier.HI_RES_LOSSLESS.value else 44100.0,
                            manifest="none",
                            encrypted=False,
                        )
                except json.JSONDecodeError:
                    pass

            except Exception as e:
                logger.warning(f"Failed to decode manifest for track {track_id}: {e}")

        stream_endpoint = (
            f"{base_url.rstrip('/')}/stream/audio/{track_id}.flac"
            if base_url
            else f"/stream/audio/{track_id}.flac"
        )
        return AddonStream(
            url=stream_endpoint,
            format="flac",
            quality=quality,
            codec="flac",
            mimeType="audio/flac",
            bitDepth=24.0 if quality == QualityTier.HI_RES_LOSSLESS.value else 16.0,
            sampleRate=96000.0 if quality == QualityTier.HI_RES_LOSSLESS.value else 44100.0,
            manifest="none",
            encrypted=False,
        )

    def _parse_dash_manifest(
        self,
        xml_text: str,
        track_id: str,
        quality: str,
        base_url: str,
    ) -> AddonStream:
        try:
            root = ET.fromstring(xml_text)
            ns = {"mpd": "urn:mpeg:dash:schema:mpd:2011"}

            base_urls = root.findall(".//mpd:BaseURL", ns) or root.findall(".//BaseURL")
            if base_urls and base_urls[0].text:
                direct_url = base_urls[0].text.strip()
                if direct_url.startswith(("http://", "https://")):
                    return AddonStream(
                        url=direct_url,
                        format="flac",
                        quality=quality,
                        codec="flac",
                        mimeType="audio/flac",
                        manifest="none",
                        encrypted=False,
                    )

            reps = root.findall(".//mpd:Representation", ns) or root.findall(".//Representation")
            bitrate = None
            sample_rate = 44100.0
            if reps:
                bw = reps[0].get("bandwidth")
                if bw:
                    bitrate = float(bw)
                sr = reps[0].get("audioSamplingRate")
                if sr:
                    sample_rate = float(sr)

            stream_endpoint = (
                f"{base_url.rstrip('/')}/stream/audio/{track_id}.flac"
                if base_url
                else f"/stream/audio/{track_id}.flac"
            )
            return AddonStream(
                url=stream_endpoint,
                format="flac",
                quality=quality,
                codec="flac",
                mimeType="audio/flac",
                bitDepth=24.0 if quality == QualityTier.HI_RES_LOSSLESS.value else 16.0,
                sampleRate=sample_rate,
                bitrate=bitrate or 1411000.0,
                manifest="none",
                encrypted=False,
            )
        except Exception as e:
            logger.warning(f"Error parsing DASH XML for track {track_id}: {e}")
            stream_endpoint = (
                f"{base_url.rstrip('/')}/stream/audio/{track_id}.flac"
                if base_url
                else f"/stream/audio/{track_id}.flac"
            )
            return AddonStream(
                url=stream_endpoint,
                format="flac",
                quality=quality,
                codec="flac",
                mimeType="audio/flac",
                manifest="none",
                encrypted=False,
            )

    # ── Live Audio Proxy Streaming with HTTP Range Support & Disk Caching ────

    def _serve_local_audio_file(
        self,
        file_path: str,
        request: Request,
        disposition_filename: Optional[str] = None,
    ) -> Response:
        file_size = os.path.getsize(file_path)
        is_flac = file_path.endswith(".flac")
        media_type = "audio/flac" if is_flac else "audio/mp4"

        response_headers = {
            "Content-Type": media_type,
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=31536000, immutable",
            "X-HiFi-Cache": "HIT",
        }
        if disposition_filename:
            safe_name = re.sub(r"[^\w\s\-\.\(\),]", "_", disposition_filename)
            response_headers["Content-Disposition"] = f'inline; filename="{safe_name}"'

        range_header = request.headers.get("range")

        if request.method == "HEAD":
            if range_header:
                match = re.match(r"bytes=(\d+)-(\d*)", range_header)
                if match:
                    start = int(match.group(1))
                    if start >= file_size:
                        raise HTTPException(status_code=416, detail="Requested range not satisfiable")
                    end = min(int(match.group(2)), file_size - 1) if match.group(2) else file_size - 1
                    response_headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
                    response_headers["Content-Length"] = str(end - start + 1)
                    return Response(status_code=206, headers=response_headers, media_type=media_type)
            response_headers["Content-Length"] = str(file_size)
            return Response(status_code=200, headers=response_headers, media_type=media_type)

        if range_header:
            match = re.match(r"bytes=(\d+)-(\d*)", range_header)
            if match:
                start = int(match.group(1))
                end = int(match.group(2)) if match.group(2) else file_size - 1
                if start >= file_size:
                    raise HTTPException(status_code=416, detail="Requested range not satisfiable")
                end = min(end, file_size - 1)
                content_length = end - start + 1
                response_headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
                response_headers["Content-Length"] = str(content_length)

                def iter_range():
                    with open(file_path, "rb") as f:
                        f.seek(start)
                        remaining = content_length
                        while remaining > 0:
                            chunk = f.read(min(65536, remaining))
                            if not chunk:
                                break
                            remaining -= len(chunk)
                            yield chunk

                return StreamingResponse(
                    iter_range(),
                    status_code=206,
                    headers=response_headers,
                    media_type=media_type,
                )

        response_headers["Content-Length"] = str(file_size)

        def iter_full():
            with open(file_path, "rb") as f:
                while chunk := f.read(65536):
                    yield chunk

        return StreamingResponse(
            iter_full(),
            status_code=200,
            headers=response_headers,
            media_type=media_type,
        )

    async def stream_audio_proxy(
        self,
        track_id: str,
        request: Request,
    ) -> Response:
        clean_id = sanitize_track_id(track_id)

        # ── 1. Check local audio cache on disk (CACHE HIT) ────────────────────
        cached_path = prefetcher.get_cached_track_path(clean_id)
        if cached_path:
            logger.info(f"[CACHE HIT] Serving track {clean_id} directly from local disk ({cached_path})")
            prefetcher.record_request(is_hit=True)
            asyncio.create_task(prefetcher.queue_upcoming_from_context(clean_id))
            track_meta = await db.get_cached_track(clean_id)
            disposition = None
            if track_meta:
                artist_name = track_meta.get("artist", "Unknown")
                track_title = track_meta.get("title", clean_id)
                disposition = f"{artist_name} - {track_title}.flac"
            return self._serve_local_audio_file(cached_path, request, disposition)

        prefetcher.record_request(is_hit=False)
        asyncio.create_task(prefetcher.queue_upcoming_from_context(clean_id))

        # ── 2. Stream from upstream mirror (CACHE MISS) ───────────────────────
        if self._stream_client is None or self._stream_client.is_closed:
            from app.mirror_manager import VERIFY_TLS

            self._stream_client = httpx.AsyncClient(
                verify=VERIFY_TLS,
                follow_redirects=True,
                timeout=httpx.Timeout(connect=5.0, read=120.0, write=5.0, pool=30.0),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
            )
        client = self._stream_client

        candidate_urls: List[str] = []
        is_snowflake = bool(re.match(r"^\d{17,20}$", clean_id))

        if is_snowflake:
            for mirror_url in mirror_manager.get_ranked_mirrors():
                candidate_urls.append(f"{mirror_url}/track/{clean_id}")
        else:
            resolved = await self.resolve_stream(clean_id, QualityTier.HI_RES_LOSSLESS.value)
            if resolved.url and resolved.url.startswith("http") and "/stream/audio/" not in resolved.url:
                candidate_urls.append(resolved.url)
            for mirror_url in mirror_manager.get_ranked_mirrors():
                candidate_urls.append(f"{mirror_url}/track/{clean_id}")

        seen = set()
        unique_urls = []
        for u in candidate_urls:
            if u not in seen:
                seen.add(u)
                unique_urls.append(u)
        candidate_urls = unique_urls

        forward_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "*/*",
        }
        range_header = request.headers.get("range")
        if range_header:
            forward_headers["Range"] = range_header
        elif request.method == "HEAD":
            forward_headers["Range"] = "bytes=0-0"

        for target_url in candidate_urls:
            try:
                req = client.build_request("GET", target_url, headers=forward_headers)
                upstream_resp = await client.send(req, stream=True)

                if upstream_resp.status_code == 429:
                    logger.warning(f"Audio proxy candidate {target_url} returned 429, backing off 2.0s...")
                    await upstream_resp.aclose()
                    await asyncio.sleep(2.0)
                    req = client.build_request("GET", target_url, headers=forward_headers)
                    upstream_resp = await client.send(req, stream=True)

                if upstream_resp.status_code in (200, 206):
                    content_type = upstream_resp.headers.get("Content-Type", "").lower()
                    if (
                        content_type.startswith("text/")
                        or "html" in content_type
                        or "xml" in content_type
                    ):
                        logger.warning(f"Audio proxy: Skipping {target_url} — non-audio content-type '{content_type}'")
                        await upstream_resp.aclose()
                        continue

                    audio_content_type = upstream_resp.headers.get("Content-Type", "audio/flac")
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

                    track_meta = await db.get_cached_track(clean_id)
                    if track_meta:
                        artist_name = track_meta.get("artist", "Unknown")
                        track_title = track_meta.get("title", clean_id)
                        safe_name = re.sub(r"[^\w\s\-\.\(\),]", "_", f"{artist_name} - {track_title}")
                        response_headers["Content-Disposition"] = f'inline; filename="{safe_name}.flac"'

                    # Fix HEAD response Content-Length when probing via bytes=0-0
                    if request.method == "HEAD":
                        await upstream_resp.aclose()
                        if not range_header and upstream_resp.status_code == 206:
                            cr = upstream_resp.headers.get("Content-Range", "")
                            total_match = re.search(r"/(\d+)$", cr)
                            if total_match:
                                response_headers["Content-Length"] = total_match.group(1)
                            response_headers.pop("Content-Range", None)
                            return Response(
                                content=b"",
                                status_code=200,
                                headers=response_headers,
                                media_type=audio_content_type,
                            )
                        return Response(
                            content=b"",
                            status_code=upstream_resp.status_code,
                            headers=response_headers,
                            media_type=audio_content_type,
                        )

                    # Only tee to disk cache when streaming from byte 0 with no open-ended byte limit
                    is_full_stream = range_header is None or range_header == "bytes=0-"
                    expected_total_bytes: Optional[int] = None
                    if is_full_stream:
                        if upstream_resp.status_code == 200 and "Content-Length" in upstream_resp.headers:
                            expected_total_bytes = int(upstream_resp.headers["Content-Length"])
                        elif upstream_resp.status_code == 206 and "Content-Range" in upstream_resp.headers:
                            m = re.match(r"bytes 0-(\d+)/(\d+)", upstream_resp.headers["Content-Range"])
                            if m and int(m.group(1)) == int(m.group(2)) - 1:
                                expected_total_bytes = int(m.group(2))
                            else:
                                is_full_stream = False

                    temp_cache_file = (
                        os.path.join(prefetcher.cache_dir, f".tmp_proxy_{clean_id}_{uuid.uuid4().hex[:8]}")
                        if is_full_stream
                        else None
                    )
                    cache_handle = open(temp_cache_file, "wb") if temp_cache_file else None

                    async def stream_chunks():
                        bytes_written = 0
                        completed_cleanly = False
                        try:
                            async for chunk in upstream_resp.aiter_bytes(chunk_size=65536):
                                if cache_handle:
                                    await asyncio.to_thread(cache_handle.write, chunk)
                                    bytes_written += len(chunk)
                                yield chunk
                            completed_cleanly = True
                        finally:
                            await upstream_resp.aclose()
                            if cache_handle:
                                cache_handle.close()
                                is_complete = (
                                    completed_cleanly
                                    and bytes_written > 10240
                                    and (expected_total_bytes is None or bytes_written >= expected_total_bytes)
                                )
                                if is_complete and temp_cache_file and os.path.exists(temp_cache_file):
                                    with open(temp_cache_file, "rb") as hf:
                                        hdr = hf.read(16)
                                    ext = ".flac" if hdr.startswith(b"fLaC") else ".m4a"
                                    final_file = os.path.join(prefetcher.cache_dir, f"{clean_id}{ext}")
                                    os.replace(temp_cache_file, final_file)
                                    logger.info(
                                        f"[CACHE] Saved {clean_id}{ext} to local audio disk cache "
                                        f"({bytes_written // 1024} KB)"
                                    )
                                elif temp_cache_file and os.path.exists(temp_cache_file):
                                    try:
                                        os.remove(temp_cache_file)
                                    except Exception:
                                        pass

                    return StreamingResponse(
                        stream_chunks(),
                        status_code=upstream_resp.status_code,
                        headers=response_headers,
                        media_type=audio_content_type,
                    )
                else:
                    logger.debug(f"Audio proxy candidate {target_url} returned status {upstream_resp.status_code}")
                    await upstream_resp.aclose()
                    continue
            except Exception as e:
                logger.debug(f"Audio proxy attempt failed for {target_url}: {e}")
                continue

        raise HTTPException(status_code=502, detail="All upstream audio sources failed")

    # ── Album Details Lookup ─────────────────────────────────────────────────

    async def get_album(self, album_id: str, base_url: str = "") -> AlbumDetail:
        clean_album_id = sanitize_track_id(album_id)
        endpoints = [
            f"/album/?id={clean_album_id}",
            f"/album?id={clean_album_id}",
            f"/release/{clean_album_id}",
        ]
        data = None
        for ep in endpoints:
            try:
                resp, _ = await mirror_manager.request_with_failover(ep, timeout=4.0)
                if resp.status_code == 200:
                    data = resp.json()
                    break
            except Exception:
                continue

        if not data:
            raise HTTPException(status_code=404, detail="Album not found across mirrors")

        item = data.get("data", data)
        tracks, _ = self._normalize_search_response(item, base_url)

        title = item.get("title") or "Unknown Album"
        artist_obj = item.get("artist") or {}
        artist_name = (
            artist_obj.get("name") if isinstance(artist_obj, dict) else (item.get("artistName") or "Unknown Artist")
        )
        cover = cover_id_to_url(item.get("cover"))

        return AlbumDetail(
            id=str(clean_album_id),
            title=title,
            artist=artist_name,
            artistId=str(artist_obj.get("id")) if isinstance(artist_obj, dict) and artist_obj.get("id") else None,
            cover=cover,
            releaseDate=item.get("releaseDate"),
            numberOfTracks=len(tracks),
            duration=float(item.get("duration", 0)) if item.get("duration") else None,
            audioQuality=item.get("audioQuality", "LOSSLESS"),
            tracks=tracks,
        )

    # ── Artist Details Lookup ────────────────────────────────────────────────

    async def get_artist(self, artist_id: str, base_url: str = "") -> ArtistDetail:
        clean_artist_id = sanitize_track_id(artist_id)
        endpoints = [
            f"/artist/?id={clean_artist_id}",
            f"/artist?id={clean_artist_id}",
        ]
        data = None
        for ep in endpoints:
            try:
                resp, _ = await mirror_manager.request_with_failover(ep, timeout=4.0)
                if resp.status_code == 200:
                    data = resp.json()
                    break
            except Exception:
                continue

        if not data:
            raise HTTPException(status_code=404, detail="Artist not found across mirrors")

        item = data.get("data", data)
        tracks, _ = self._normalize_search_response(item, base_url)
        name = item.get("name") or "Unknown Artist"
        picture = cover_id_to_url(item.get("picture"))

        return ArtistDetail(
            id=str(clean_artist_id),
            name=name,
            picture=picture,
            bio=item.get("bio"),
            topTracks=tracks,
            albums=item.get("albums", []),
        )


stream_resolver = StreamResolver()
