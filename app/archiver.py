"""
Background FLAC and Audio Archiver for hifi-bridge.
When ENABLE_FLAC_ARCHIVE=true, copies or downloads streamed tracks,
inspects container (FLAC vs MP4/AAC), embeds metadata tags
(Title, Artist, Album, Track Number, Cover Art) using mutagen,
and organizes them into /music/{Artist}/{Album}/{TrackNumber} - {Title}.[flac|m4a].
"""

import asyncio
import logging
import os
import re
import shutil
import uuid
from typing import Optional, Set
import httpx
from mutagen.flac import FLAC, Picture
from mutagen.mp4 import MP4, MP4Cover

from app.models import AddonTrack

logger = logging.getLogger(__name__)

ENABLE_FLAC_ARCHIVE = os.environ.get("ENABLE_FLAC_ARCHIVE", "false").lower() in ("true", "1", "yes", "on")
MUSIC_DIR = os.environ.get("MUSIC_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "music"))


def sanitize_filename(name: str) -> str:
    """Removes or replaces characters not allowed in file or folder names."""
    if not name:
        return "Unknown"
    cleaned = re.sub(r'[\\/*?:"<>|]', "_", name).strip()
    cleaned = cleaned.rstrip(". ")
    return cleaned if cleaned else "Unknown"


def _tag_and_save_sync(temp_path: str, is_flac: bool, is_mp4: bool, track: AddonTrack, artist: str, album: str, title: str, track_num: int, cover_bytes: Optional[bytes]):
    """Embeds Vorbis/MP4 metadata tags and artwork synchronously in worker thread."""
    if is_flac:
        try:
            audio = FLAC(temp_path)
            audio["TITLE"] = track.title or title
            audio["ARTIST"] = track.artist or artist
            audio["ALBUM"] = track.album or album
            audio["TRACKNUMBER"] = str(track_num)
            if track.isrc:
                audio["ISRC"] = track.isrc
            audio["ORGANIZATION"] = "flacjack"

            if cover_bytes:
                pic = Picture()
                pic.data = cover_bytes
                pic.type = 3  # Front cover
                pic.mime = "image/jpeg" if cover_bytes.startswith(b"\xff\xd8") else "image/png"
                pic.desc = "Front Cover"
                audio.add_picture(pic)

            audio.save()
        except Exception as e:
            logger.warning(f"Archiver: FLAC tagging note: {e}")

    elif is_mp4:
        try:
            audio = MP4(temp_path)
            if audio.tags is None:
                audio.add_tags()
            audio.tags["\xa9nam"] = [track.title or title]
            audio.tags["\xa9ART"] = [track.artist or artist]
            audio.tags["\xa9alb"] = [track.album or album]
            audio.tags["trkn"] = [(track_num, 0)]
            if cover_bytes:
                fmt = MP4Cover.FORMAT_JPEG if cover_bytes.startswith(b"\xff\xd8") else MP4Cover.FORMAT_PNG
                audio.tags["covr"] = [MP4Cover(cover_bytes, imageformat=fmt)]
            audio.save()
        except Exception as e:
            logger.warning(f"Archiver: MP4 tagging note: {e}")


class FlacArchiver:
    def __init__(self, music_dir: str = MUSIC_DIR, enabled: bool = ENABLE_FLAC_ARCHIVE):
        self.music_dir = music_dir
        self.enabled = enabled
        self._semaphore = asyncio.Semaphore(2)  # Max 2 concurrent background archivers
        self._active_tasks: Set[asyncio.Task] = set()
        if self.enabled:
            os.makedirs(self.music_dir, exist_ok=True)
            logger.info(f"Audio Archiver enabled. Destination directory: {self.music_dir}")

    def trigger_archive(self, track: AddonTrack, track_id_or_url: str, direct_audio_url: Optional[str] = None):
        """Fires background archiving task if archiver is enabled."""
        if not self.enabled:
            return
        if track_id_or_url.startswith("http://") or track_id_or_url.startswith("https://"):
            actual_url = track_id_or_url
            actual_id = track.id
        else:
            actual_url = direct_audio_url
            actual_id = track_id_or_url
        task = asyncio.create_task(self._archive_task(track, actual_id, actual_url))
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)

    async def shutdown(self):
        """Waits for in-progress archive tasks to complete gracefully."""
        if self._active_tasks:
            count = len(self._active_tasks)
            logger.info(f"Archiver: Waiting for {count} active archive task(s) to finish...")
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
            logger.info("Archiver: All archive tasks completed.")

    async def _archive_task(self, track: AddonTrack, track_id: str, direct_audio_url: Optional[str] = None):
        """Copies from local audio cache or downloads audio, embeds tags, and organizes to library."""
        from app.prefetcher import prefetcher
        async with self._semaphore:
            temp_path = None
            try:
                artist = sanitize_filename(track.artist or "Unknown Artist")
                album = sanitize_filename(track.album or "Unknown Album")
                title = sanitize_filename(track.title or "Unknown Title")
                track_num = track.trackNumber or 1

                folder_path = os.path.join(self.music_dir, artist, album)
                await asyncio.to_thread(os.makedirs, folder_path, exist_ok=True)

                unique_tok = uuid.uuid4().hex[:8]
                temp_path = os.path.join(folder_path, f".tmp_arch_{track_id}_{unique_tok}")

                # 1. Download artwork first (small payload)
                cover_bytes: Optional[bytes] = None
                artwork_url = track.artworkURL or track.albumArtworkURL

                if artwork_url and artwork_url.startswith("http"):
                    try:
                        async with httpx.AsyncClient(verify=False, follow_redirects=True, timeout=10.0) as client:
                            cover_resp = await client.get(artwork_url)
                            if cover_resp.status_code == 200:
                                cover_bytes = cover_resp.content
                    except Exception as e:
                        logger.warning(f"Archiver: Failed to download cover art: {e}")

                # 2. Acquire audio: Prefer copying directly from local cache!
                cached_audio = prefetcher.get_cached_track_path(track_id)
                if cached_audio and os.path.isfile(cached_audio):
                    logger.info(f"Archiver: Copying '{artist} - {title}' directly from local cache ({cached_audio})")
                    await asyncio.to_thread(shutil.copy2, cached_audio, temp_path)
                elif direct_audio_url and direct_audio_url.startswith("http") and "/stream/audio/" not in direct_audio_url:
                    logger.info(f"Archiver: Downloading audio for '{artist} - {title}' from {direct_audio_url}...")
                    async with httpx.AsyncClient(verify=False, follow_redirects=True, timeout=60.0) as client:
                        async with client.stream("GET", direct_audio_url) as resp:
                            if resp.status_code not in (200, 206):
                                logger.warning(f"Archiver: Download failed for {title} (HTTP {resp.status_code})")
                                return
                            with open(temp_path, "wb") as f:
                                async for chunk in resp.aiter_bytes(chunk_size=65536):
                                    await asyncio.to_thread(f.write, chunk)
                else:
                    logger.info(f"Archiver: Track '{title}' not cached yet; will archive on next playback.")
                    return

                if not os.path.exists(temp_path) or os.path.getsize(temp_path) < 10240:
                    logger.warning(f"Archiver: Incomplete or empty file for {title}")
                    return

                # 3. Detect container
                def _inspect_header():
                    with open(temp_path, "rb") as f:
                        return f.read(16)
                header = await asyncio.to_thread(_inspect_header)
                is_flac = header.startswith(b"fLaC")
                is_mp4 = b"ftyp" in header

                ext = ".flac" if is_flac else (".m4a" if is_mp4 else ".flac")
                filename = f"{track_num:02d} - {title}{ext}"
                final_path = os.path.join(folder_path, filename)

                if os.path.exists(final_path) and os.path.getsize(final_path) > 10240:
                    logger.debug(f"Archiver: Track already exists: {final_path}")
                    if os.path.exists(temp_path):
                        await asyncio.to_thread(os.remove, temp_path)
                    return

                # 4. Embed metadata tags offloaded to thread pool
                await asyncio.to_thread(
                    _tag_and_save_sync,
                    temp_path, is_flac, is_mp4, track, artist, album, title, track_num, cover_bytes
                )

                # 5. Move to final library path
                if os.path.exists(final_path):
                    await asyncio.to_thread(os.remove, final_path)
                await asyncio.to_thread(os.rename, temp_path, final_path)
                logger.info(f"Archiver: Successfully archived '{artist} - {title}{ext}' ({os.path.getsize(final_path):,} bytes)")

            except Exception as e:
                logger.error(f"Archiver: Exception archiving track {track.id}: {e}", exc_info=True)
            finally:
                if temp_path and os.path.exists(temp_path):
                    try:
                        await asyncio.to_thread(os.remove, temp_path)
                    except Exception:
                        pass


# Singleton archiver instance
archiver = FlacArchiver()
