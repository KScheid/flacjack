"""
Pydantic data models for hifi-bridge.
Includes schemas for BitChord custom module/addon protocol,
Monochrome/HiFi upstream normalization, and mirror pool monitoring.
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field


class QualityTier(str, Enum):
    HI_RES_LOSSLESS = "HI_RES_LOSSLESS"  # 24-bit / up to 192kHz FLAC
    LOSSLESS = "LOSSLESS"                # 16-bit / 44.1kHz FLAC
    HIGH = "HIGH"                        # 320kbps AAC / MP3
    LOW = "LOW"                          # 96kbps AAC


class MirrorStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    OFFLINE = "offline"


# ── BitChord Addon Protocol Models ───────────────────────────────────────

class AddonSettingOption(BaseModel):
    value: Union[str, int, float, bool]
    label: Optional[str] = None


class AddonSetting(BaseModel):
    key: str
    default: Optional[Union[str, int, float, bool]] = None
    options: List[AddonSettingOption] = Field(default_factory=list)


class AddonManifest(BaseModel):
    id: str = "hifi-bridge"
    name: str = "Hi-Fi Bridge (Monochrome Lossless)"
    version: str = "2.0.0"
    description: str = "Self-hosted Hi-Fi Lossless Aggregator & BitChord Audio Source"
    baseUrl: Optional[str] = None
    resources: List[str] = Field(default_factory=lambda: ["search", "stream"])
    settings: List[AddonSetting] = Field(default_factory=list)
    allowDownloads: bool = True
    checkValidLossless: bool = False
    bitchordAuth: bool = False


class AddonTrack(BaseModel):
    id: str
    title: str
    artist: str
    album: str = ""
    duration: Optional[float] = None  # In seconds
    artworkURL: Optional[str] = None
    albumArtworkURL: Optional[str] = None
    format: str = "flac"
    audioQuality: str = "HI_RES_LOSSLESS"
    audioMode: Optional[str] = "STEREO"
    audioModes: List[str] = Field(default_factory=lambda: ["STEREO"])
    atmos: Optional[bool] = False
    streamURL: Optional[str] = None
    isrc: Optional[str] = None
    trackNumber: Optional[int] = None


class AddonSearchResponse(BaseModel):
    tracks: List[AddonTrack] = Field(default_factory=list)
    items: Optional[List[Dict[str, Any]]] = None  # Generic fallback for third-party clients


class AddonStream(BaseModel):
    url: str = ""
    format: str = "flac"
    quality: str = "HI_RES_LOSSLESS"
    streamQuality: Optional[str] = "HI_RES_LOSSLESS"
    audioQuality: Optional[str] = "HI_RES_LOSSLESS"
    codec: Optional[str] = "flac"
    fileCodec: Optional[str] = "flac"
    container: Optional[str] = "flac"
    containerFormat: Optional[str] = "flac"
    manifest: Optional[str] = "none"
    mediaType: Optional[str] = None
    mimeType: Optional[str] = "audio/flac"
    encrypted: Optional[bool] = False
    sampleRate: Optional[float] = 96000.0
    bitDepth: Optional[float] = 24.0
    bitrate: Optional[float] = 3000000.0
    audioMode: Optional[str] = "STEREO"
    audioModes: List[str] = Field(default_factory=lambda: ["STEREO"])
    error: Optional[str] = None


# ── Album and Artist Models ───────────────────────────────────────────────

class AlbumDetail(BaseModel):
    id: str
    title: str
    artist: str
    artistId: Optional[str] = None
    cover: Optional[str] = None
    releaseDate: Optional[str] = None
    numberOfTracks: int = 0
    duration: Optional[float] = None
    audioQuality: str = "LOSSLESS"
    tracks: List[AddonTrack] = Field(default_factory=list)


class ArtistDetail(BaseModel):
    id: str
    name: str
    picture: Optional[str] = None
    bio: Optional[str] = None
    topTracks: List[AddonTrack] = Field(default_factory=list)
    albums: List[Dict[str, Any]] = Field(default_factory=list)


# ── Mirror Monitoring & Health Models ─────────────────────────────────────

class MirrorMetrics(BaseModel):
    url: str
    status: MirrorStatus = MirrorStatus.HEALTHY
    latency_ms: float = 0.0
    last_checked: float = 0.0
    consecutive_failures: int = 0
    is_custom: bool = False
    notes: Optional[str] = None


class MirrorPoolSummary(BaseModel):
    total: int
    healthy: int
    degraded: int
    offline: int
    fastest_mirror: Optional[str] = None
    mirrors: List[MirrorMetrics] = Field(default_factory=list)


class HealthResponse(BaseModel):
    status: str
    version: str = "1.0.0"
    uptime_seconds: float
    mirror_pool: MirrorPoolSummary
    cache_entries: int
    archiving_enabled: bool
