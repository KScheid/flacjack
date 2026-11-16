# `FlacJack` 🥞 — Fresh, Warm Stacks of Lossless Audio on Demand

Production-ready, self-hosted Python (FastAPI) microservice bridging public Hi-Fi lossless audio APIs (Monochrome) to the **BitChord** Android music client and any modern web browser.

```
┌─────────────────────────────────┐
│     BitChord Android Client     │
│   (Plays 24-bit / 16-bit FLAC)  │
└────────────────┬────────────────┘
                 │ HTTPS / HTTP (Addon Protocol)
                 ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        `FlacJack` 🥞 Microservice                      │
│  ├─ Embedded WebUI Dashboard (Dark Mode, Latency, In-Browser Player)   │
│  ├─ Dynamic BitChord Module (/bitchord-module.json)                   │
│  ├─ Smart Lookahead Queue Prefetcher & Local SSD Audio Disk Cache      │
│  ├─ Auto-Healing Mirror Pool & Latency Health Probing                  │
│  ├─ Stream Resolver & DASH Manifest Decoding                           │
│  ├─ Audio Proxy with HTTP Range (Seeking) & ExoPlayer HEAD Probing     │
│  ├─ SQLite Metadata & Search Cache (aiosqlite WAL)                    │
│  └─ Optional Background FLAC Archiver (mutagen)                        │
└────────┬───────────────────────────────┬───────────────────────────────┘
         │ Instant 0ms Seek / Cache Hits │ Background Lookahead Buffering
         ▼                               ▼
┌─────────────────────────┐     ┌────────────────────────────────────────┐
│   Local SSD Cache       │     │  Upstream Hi-Fi Mirrors (Monochrome)   │
│   /data/audio_cache/    │     │  • tracks.monochrome.st                │
│   (Rate Limit Immune)   │     │  • INSTANCES.md mirror pool            │
└─────────────────────────┘     └────────────────────────────────────────┘
```

---

## Key Features

1. **Smart Lookahead Queue Prefetcher & Local SSD Disk Cache (`prefetcher.py`):**
   - **0ms Playback & Zero Lag:** Caches audio tracks directly onto local SSD storage (`/data/audio_cache`).
   - **Rate Limit Immunity:** Buffers the next 2 tracks of your active album or playlist in the background. Playback never stalls or drops due to Cloudflare rate limits (`429` / `403`).
   - **Automatic LRU Pruning:** Enforces a configurable cache ceiling (e.g., `MAX_CACHE_GB=10`). When storage hits the limit, least-recently played tracks are pruned automatically.
   - **Full HTTP Range & HEAD Support:** Supports ExoPlayer's media probing (`HEAD` requests) and byte-range seeking (`bytes=start-end`) for scrubbing.

2. **Embedded WebUI Dashboard (`dashboard.py`):**
   - Single-page responsive dark-mode management console accessible at `http://<SERVER-IP>:8742/`.
   - **Live Monitoring:** Real-time uptime counter, healthy mirror pool status, and mirror latency ranking.
   - **SSD Cache Metrics:** Total cached tracks, storage consumed, cache hit rate percentage, and active prefetch queue depth.
   - **Built-in Lossless Audio Player:** Search tracks and preview full lossless FLAC audio directly in your browser.
   - **1-Click Actions:** Copy BitChord module URL, force re-probe upstream mirrors, or clear local disk cache.

3. **Auto-Healing Mirror Pool (`mirror_manager.py`):**
   - Discovers live upstream mirrors from `monochrome-music/monochrome`'s `INSTANCES.md` and maintains ranked fallback mirrors.
   - Probes mirror health every 5 minutes with lightweight JSON checks, blacklisting expired/parked domains (such as old `*.qqdl.site` domains).
   - Seamless failover: if a mirror times out or returns 429/502, requests automatically back off and fail over to healthy mirrors.

4. **BitChord Custom Module Generator (`/bitchord-module.json` & `/manifest.json`):**
   - Strict conformity with BitChord's native Addon specifications (`resources: ["search", "stream"]`).
   - Dynamically parses incoming `Host`, `X-Forwarded-*`, and Tailscale MagicDNS headers so that importing works whether on LAN, Tailscale (`monolith...ts.net:8742`), or behind a reverse proxy (Caddy, Nginx).

5. **Optimized for Docker & Unraid OS:**
   - Multi-stage slim Docker image with `ffmpeg`, `curl`, and `gosu`.
   - Built-in support for Unraid user mapping (`PUID=99`, `PGID=100`) so permissions on `/mnt/user/appdata/hifi-bridge` and `/mnt/user/music` remain intact.
   - Pre-configured Unraid Docker labels (`net.unraid.docker.webui`, `net.unraid.docker.icon`) for 1-click WebUI access from the Unraid dashboard.
   - Includes ready-to-use Unraid XML template (`unraid-template.xml`).

---

## Deployment on Unraid OS

### Method A: Docker Compose (Recommended)

1. On Unraid, install the **Docker Compose Manager** plugin from Community Applications (CA) if you haven't already.
2. In Unraid terminal or Docker Compose Manager, create a stack with the following `docker-compose.yml`:
   ```yaml
   services:
     hifi-bridge:
       image: hifi-bridge:latest
       build:
         context: /mnt/user/appdata/hifi-bridge
       container_name: hifi-bridge
       restart: unless-stopped
       ports:
         - "8742:8742"
       environment:
         - PUID=99
         - PGID=100
         - PORT=8742
         - HOST=0.0.0.0
         - DEFAULT_QUALITY=HI_RES_LOSSLESS
         - AUDIO_CACHE_DIR=/data/audio_cache
         - MAX_CACHE_GB=10
         - PREFETCH_LOOKAHEAD=2
         - PREFETCH_ENABLED=true
         - ENABLE_FLAC_ARCHIVE=false
         - CACHE_TTL_SECONDS=3600
         - DB_PATH=/data/hifi_bridge.db
         - MUSIC_DIR=/music
       volumes:
         - /mnt/user/appdata/hifi-bridge/data:/data
         - /mnt/user/music:/music
       labels:
         net.unraid.docker.icon: "https://raw.githubusercontent.com/walkxcode/dashboard-icons/main/svg/music.svg"
         net.unraid.docker.webui: "http://[IP]:[PORT:8742]/"
   ```
3. Start the container:
   ```bash
   docker compose up -d
   ```

### Method B: Unraid Container Template (GUI)

1. Copy `unraid-template.xml` from this repository to your Unraid flash drive:
   `/boot/config/plugins/dockerMan/templates-user/my-hifi-bridge.xml`
2. In the Unraid WebUI, go to **Docker** ➔ **Add Container** ➔ select **hifi-bridge** from the template dropdown.
3. Click **Apply**.

---

## How to Connect to BitChord

1. Open **BitChord** on your Android device.
2. Navigate to **Settings** ➔ **Audio Sources** (or **Manage Sources**).
3. Tap **Add Source** (or paste custom source).
4. Enter your `hifi-bridge` module URL:
   - **Over Tailscale:** `https://monolith.tail6dbdcb.ts.net:8742/bitchord-module.json`
   - **Over Home LAN:** `http://192.168.1.xxx:8742/bitchord-module.json`
5. BitChord will query the manifest and activate **Hi-Fi Bridge (Monochrome Lossless)**.
6. Under source quality settings, pick your preferred tier:
   - `HI_RES_LOSSLESS` (24-bit FLAC)
   - `LOSSLESS` (16-bit FLAC)
   - `HIGH` (320kbps AAC)

---

## Configuration Reference

| Environment Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `8742` | HTTP port to listen on. |
| `HOST` | `0.0.0.0` | Bind IP address. |
| `PUID` | `99` | Unraid user ID (`nobody`). |
| `PGID` | `100` | Unraid group ID (`users`). |
| `DEFAULT_QUALITY` | `HI_RES_LOSSLESS` | Default quality tier (`HI_RES_LOSSLESS`, `LOSSLESS`, `HIGH`). |
| `PREFETCH_ENABLED` | `true` | Enables background lookahead queue prefetching. |
| `PREFETCH_LOOKAHEAD` | `2` | Number of upcoming tracks to prefetch in the background. |
| `AUDIO_CACHE_DIR` | `/data/audio_cache` | Path for cached audio files on local disk. |
| `MAX_CACHE_GB` | `10` | Maximum disk space (in GB) before LRU eviction. |
| `ENABLE_FLAC_ARCHIVE`| `false` | When true, permanently saves and tags FLACs to `MUSIC_DIR`. |
| `MUSIC_DIR` | `/music` | Destination directory for archived FLAC tracks. |
| `DB_PATH` | `/data/hifi_bridge.db` | Path to SQLite metadata database. |
| `CACHE_TTL_SECONDS` | `3600` | Search metadata cache TTL in seconds. |
| `MAX_CONCURRENT_UPSTREAM` | `20` | Max simultaneous requests sent to upstream mirrors. |
| `VERIFY_TLS` | `false` | Whether to enforce strict TLS verification for upstream mirrors. |

---

## Troubleshooting & FAQ

- **"Outdated Addon" badge in BitChord:** In BitChord v1.9.1+, self-hosted addons display an "Outdated Addon" label because official addons require a proprietary cryptographic signature. This is cosmetic only; all streaming and search functionality works 100%.
- **Random playback pauses between songs:** ExoPlayer requires valid HTTP `HEAD` responses and byte-range seeking before streaming audio. `hifi-bridge` natively handles `HEAD` probing and Range responses (`206 Partial Content`).
- **Cloudflare Rate Limiting (429):** The smart lookahead prefetcher and disk cache ensure that once you start listening, subsequent songs are already stored on local disk and stream at 0ms latency with zero upstream API calls.
