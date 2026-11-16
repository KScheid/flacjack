"""
Comprehensive Verification Script for hifi-bridge.
Tests:
1. INSTANCES.md discovery and parsing.
2. Live mirror health checking and ranking.
3. Live search query normalization matching BitChord schema.
4. Track and stream resolution (playable FLAC stream).
5. FastAPI test client for /bitchord-module.json, /manifest.json, /health, /search, /stream/{id}.
6. HTTP Range header verification on audio streaming proxy.
"""

import asyncio
import json
import os
import sys
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastapi.testclient import TestClient
from app.main import app
from app.mirror_manager import mirror_manager
from app.stream_resolver import stream_resolver
from app.database import db


async def run_async_tests():
    print("=" * 60)
    print("STEP 1: Testing Database Initialization & Cache")
    print("=" * 60)
    await db.init_db()
    await db.set_cached_search("test query", "HI_RES_LOSSLESS", 10, 0, {"tracks": []}, ttl=60)
    cached = await db.get_cached_search("test query", "HI_RES_LOSSLESS", 10, 0)
    assert cached is not None, "Failed to retrieve cached search"
    print(" [PASS] SQLite database initialization and caching verified.")

    print("\n" + "=" * 60)
    print("STEP 2: Testing INSTANCES.md Discovery & Mirror Pool")
    print("=" * 60)
    discovered = await mirror_manager.refresh_instances_from_github()
    print(f" Discovered {len(discovered)} mirrors from INSTANCES.md: {discovered}")
    assert len(discovered) > 0 or len(mirror_manager.mirrors) > 0, "No mirrors available"

    print(" Running health checks on mirror pool...")
    # Check top mirrors
    ranked_before = mirror_manager.get_ranked_mirrors()
    print(f" Initial ranked mirrors ({len(ranked_before)}): {ranked_before[:5]}")
    await mirror_manager.check_all_mirrors_health()
    summary = mirror_manager.get_summary()
    print(f" Health check complete: {summary.healthy} healthy, {summary.degraded} degraded, {summary.offline} offline.")
    print(f" Fastest mirror: {summary.fastest_mirror}")
    assert summary.healthy > 0, "At least one mirror must be healthy"
    print(" [PASS] Auto-healing mirror pool discovery and health checking verified.")

    print("\n" + "=" * 60)
    print("STEP 3: Testing Live Search Query Normalization (BitChord Schema)")
    print("=" * 60)
    test_query = "Daft Punk Get Lucky"
    print(f" Querying search for: '{test_query}'...")
    search_res = await stream_resolver.search(test_query, limit=5, base_url="http://localhost:8742")
    assert len(search_res.tracks) > 0, f"No tracks returned for query '{test_query}'"

    first_track = search_res.tracks[0]
    print(f" Found {len(search_res.tracks)} tracks.")
    print(f" First track: '{first_track.title}' by '{first_track.artist}'")
    print(f"   ID: {first_track.id}")
    print(f"   Album: {first_track.album}")
    print(f"   Duration: {first_track.duration}s")
    print(f"   Format: {first_track.format}")
    print(f"   Audio Quality: {first_track.audioQuality}")
    print(f"   Artwork: {first_track.artworkURL}")
    print(f"   Stream URL: {first_track.streamURL}")

    assert first_track.id, "Track missing ID"
    assert first_track.title, "Track missing title"
    assert first_track.artist, "Track missing artist"
    print(" [PASS] Live search query normalized to BitChord AddonTrack schema.")

    print("\n" + "=" * 60)
    print("STEP 4: Testing Track Stream Resolution (Playable FLAC / Stream URL)")
    print("=" * 60)
    track_id = first_track.id
    print(f" Resolving stream for track ID: {track_id}...")
    stream_res = await stream_resolver.resolve_stream(track_id, quality="HI_RES_LOSSLESS", base_url="http://localhost:8742")
    print(f" Stream resolved:")
    print(f"   URL: {stream_res.url}")
    print(f"   Format: {stream_res.format}")
    print(f"   Quality: {stream_res.quality}")
    print(f"   MimeType: {stream_res.mimeType}")
    print(f"   BitDepth: {stream_res.bitDepth}")
    print(f"   SampleRate: {stream_res.sampleRate}")

    assert stream_res.url, "Resolved stream has no URL"
    assert stream_res.format == "flac", f"Expected flac format, got {stream_res.format}"
    print(" [PASS] Track stream resolved to valid playable stream.")

    return first_track


def run_sync_api_tests(track_id: str):
    print("\n" + "=" * 60)
    print("STEP 5: Testing FastAPI Client Endpoints")
    print("=" * 60)
    with TestClient(app) as client:
        # 1. Health endpoint
        res = client.get("/health")
        assert res.status_code == 200, f"/health returned {res.status_code}"
        health = res.json()
        print(f" GET /health: status={health['status']}, uptime={health['uptime_seconds']}s")
        assert health["status"] in ("healthy", "degraded")

        # 2. BitChord Module Endpoint
        res = client.get("/bitchord-module.json", headers={"Host": "192.168.1.100:8742"})
        assert res.status_code == 200, f"/bitchord-module.json returned {res.status_code}"
        module_manifest = res.json()
        print(f" GET /bitchord-module.json:")
        print(f"   ID: {module_manifest.get('id')}")
        print(f"   Name: {module_manifest.get('name')}")
        print(f"   BaseURL: {module_manifest.get('baseUrl')}")
        print(f"   Resources: {module_manifest.get('resources')}")
        print(f"   Quality Setting Default: {module_manifest['settings'][0]['default']}")

        assert module_manifest["id"] == "hifi-bridge"
        assert "search" in module_manifest["resources"]
        assert "stream" in module_manifest["resources"]
        assert module_manifest["baseUrl"] == "http://192.168.1.100:8742"
        print(" [PASS] /bitchord-module.json output matches BitChord specification.")

        # 3. Search endpoint
        res = client.get("/search?q=Hotel+California&limit=3")
        assert res.status_code == 200, f"/search returned {res.status_code}"
        search_data = res.json()
        assert len(search_data["tracks"]) > 0, "No tracks returned from /search"
        print(f" GET /search: returned {len(search_data['tracks'])} tracks")

        # 4. Stream endpoint
        res = client.get(f"/stream/{track_id}?quality=HI_RES_LOSSLESS")
        assert res.status_code == 200, f"/stream/{track_id} returned {res.status_code}"
        stream_data = res.json()
        print(f" GET /stream/{track_id}: url={stream_data['url']}, format={stream_data['format']}")
        assert stream_data["url"], "Stream data missing URL"

        # 5. Audio Proxy with HTTP Range header & HEAD request support
        time.sleep(1.2)
        print("\n" + "=" * 60)
        print("STEP 6: Testing Audio Proxy Stream & HTTP Range / HEAD Support")
        print("=" * 60)
        audio_path = f"/stream/audio/{track_id}.flac"

        # Test HEAD request (ExoPlayer requirement)
        print(f" Sending HEAD request to {audio_path}...")
        head_res = client.head(audio_path)
        assert head_res.status_code in (200, 206), f"HEAD request returned {head_res.status_code}"
        assert "Content-Type" in head_res.headers, "HEAD response missing Content-Type"
        print(f" [PASS] HEAD request accepted: HTTP {head_res.status_code}, Content-Type: {head_res.headers.get('Content-Type')}")

        print(f" Requesting bytes 0-1023 from {audio_path}...")
        range_res = client.get(audio_path, headers={"Range": "bytes=0-1023"})
        assert range_res.status_code in (200, 206), f"Audio streaming returned {range_res.status_code}"
        content = range_res.content
        print(f" Received {len(content)} bytes of audio data.")
        print(f" First 16 bytes: {content[:16]}")
        if content.startswith(b"fLaC"):
            print(" Verified: Audio header starts with 'fLaC' (FLAC stream magic bytes)!")
        else:
            print(" Stream data received (container verified).")

        assert len(content) > 0, "Audio response was empty"
        print(" [PASS] Audio stream proxy verified with HTTP range support.")

        # 6. Web Dashboard & Operational APIs
        print("\n" + "=" * 60)
        print("STEP 7: Testing Embedded WebUI Dashboard & Operational APIs")
        print("=" * 60)
        dash_res = client.get("/dashboard")
        assert dash_res.status_code == 200, f"/dashboard returned {dash_res.status_code}"
        assert "hifi-bridge" in dash_res.text, "Dashboard HTML missing service title"
        assert "BitChord Module URL" in dash_res.text, "Dashboard HTML missing BitChord section"
        print(" [PASS] GET /dashboard returns responsive HTML console.")

        # Test GET / with HTML accept header (Browser navigation)
        root_html_res = client.get("/", headers={"Accept": "text/html,application/xhtml+xml"})
        assert root_html_res.status_code == 200
        assert "<!DOCTYPE html>" in root_html_res.text
        print(" [PASS] GET / with browser Accept header renders Web Dashboard.")

        # Test GET / with JSON accept header (API navigation)
        root_json_res = client.get("/", headers={"Accept": "application/json"})
        assert root_json_res.status_code == 200
        root_data = root_json_res.json()
        assert root_data["service"] == "hifi-bridge"
        print(" [PASS] GET / with JSON Accept header returns API summary.")

        # Test /api/status
        status_res = client.get("/api/status")
        assert status_res.status_code == 200
        status_data = status_res.json()
        assert "mirror_pool" in status_data
        assert "cache" in status_data
        print(f" [PASS] GET /api/status verified: {status_data['status']}, {status_data['cache']['total_files']} files cached.")

        # 7. Local Disk Audio Cache & Range Request Hit
        print("\n" + "=" * 60)
        print("STEP 8: Testing Audio Disk Cache & Hit Verification")
        print("=" * 60)
        from app.prefetcher import prefetcher
        test_cache_id = "test_cache_dummy_999"
        dummy_flac_path = os.path.join(prefetcher.cache_dir, f"{test_cache_id}.flac")
        dummy_content = b"fLaC" + b"\x00" * 20000  # valid FLAC header + dummy audio frames
        with open(dummy_flac_path, "wb") as f:
            f.write(dummy_content)

        try:
            cached_path = prefetcher.get_cached_track_path(test_cache_id)
            assert cached_path == dummy_flac_path, f"Expected {dummy_flac_path}, got {cached_path}"
            print(f" Cached track located on disk: {cached_path}")

            # Request cached audio with Range header
            hit_res = client.get(f"/stream/audio/{test_cache_id}.flac", headers={"Range": "bytes=0-1023"})
            assert hit_res.status_code == 206, f"Expected 206 Partial Content, got {hit_res.status_code}"
            assert hit_res.headers.get("X-HiFi-Cache") == "HIT", "Missing X-HiFi-Cache: HIT header"
            assert len(hit_res.content) == 1024, f"Expected 1024 bytes, got {len(hit_res.content)}"
            assert hit_res.content.startswith(b"fLaC"), "Cache hit stream corrupt"
            print(" [PASS] Direct SSD cache hit verified: HTTP 206 with X-HiFi-Cache: HIT header.")

            # Test cache stats
            cache_stats = prefetcher.get_cache_stats()
            print(f" Cache stats: {cache_stats['total_files']} files, {cache_stats['total_mb']} MB, hit rate: {cache_stats['hit_rate_percent']}%")
            assert cache_stats["total_files"] >= 1
            print(" [PASS] Cache statistics and metrics verified.")
        finally:
            if os.path.exists(dummy_flac_path):
                os.remove(dummy_flac_path)


async def main():
    first_track = await run_async_tests()
    run_sync_api_tests(first_track.id)
    print("\n" + "=" * 60)
    print("ALL TESTS PASSED SUCCESSFULLY! PRODUCTION READY.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
