"""
Embedded Web Dashboard for hifi-bridge.
Lightweight, responsive dark-mode management console with zero external dependencies.
Ideal for Unraid Docker WebUI, local management, mirror monitoring, and in-browser testing.
"""

import json
from urllib.parse import urlparse

def render_dashboard_html(base_url: str, version: str = "2.0.0") -> str:
    """Generates the single-page HTML/CSS/JS dashboard."""
    # Search and playback go through the token-prefixed routes, which work
    # whether or not ALLOW_LAN_WITHOUT_TOKEN is on.
    api_prefix = json.dumps(urlparse(base_url).path.rstrip("/"))
    return f"""<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>FlacJack 🥞 — Lossless Audio Gateway</title>
  <link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>🥞</text></svg>">
  <script src="https://cdn.tailwindcss.com"></script>
  <script>
    tailwind.config = {{
      darkMode: 'class',
      theme: {{
        extend: {{
          colors: {{
            brand: {{
              50: '#f0fdf4',
              500: '#22c55e',
              600: '#16a34a',
              700: '#15803d',
              900: '#14532d',
            }}
          }}
        }}
      }}
    }}
  </script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
    body {{ font-family: 'Inter', sans-serif; }}
    code, pre, .font-mono {{ font-family: 'JetBrains Mono', monospace; }}
    .glass {{ background: rgba(24, 24, 27, 0.7); backdrop-filter: blur(12px); border: 1px solid rgba(255, 255, 255, 0.08); }}
    .glass-card {{ background: rgba(30, 30, 36, 0.6); backdrop-filter: blur(8px); border: 1px solid rgba(255, 255, 255, 0.06); }}
  </style>
</head>
<body class="bg-zinc-950 text-zinc-100 min-h-screen flex flex-col antialiased selection:bg-brand-500 selection:text-black">

  <!-- Top Navigation -->
  <header class="sticky top-0 z-50 glass border-b border-zinc-800/80 px-4 lg:px-8 py-3.5">
    <div class="max-w-7xl mx-auto flex items-center justify-between">
      <div class="flex items-center space-x-3">
        <div class="w-9 h-9 rounded-xl bg-gradient-to-tr from-amber-500 to-yellow-400 flex items-center justify-center shadow-lg shadow-amber-500/20">
          <span class="text-xl">🥞</span>
        </div>
        <div>
          <div class="flex items-center space-x-2">
            <h1 class="text-lg font-bold tracking-tight text-white">FlacJack</h1>
            <span class="text-xs font-semibold px-2 py-0.5 rounded-full bg-zinc-800 text-zinc-400 border border-zinc-700/60">v{version}</span>
            <span id="status-pill" class="text-xs font-medium px-2 py-0.5 rounded-full bg-emerald-950 text-emerald-400 border border-emerald-800/80 flex items-center gap-1">
              <span class="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse"></span> ONLINE
            </span>
          </div>
          <p class="text-xs text-zinc-400 hidden sm:block">Fresh, warm stacks of lossless audio on demand & BitChord Source</p>
        </div>
      </div>
      <div class="flex items-center space-x-3">
        <a href="/docs" target="_blank" class="text-xs font-medium text-zinc-400 hover:text-white px-3 py-1.5 rounded-lg bg-zinc-900 border border-zinc-800 hover:border-zinc-700 transition">API Docs</a>
        <a href="/health" target="_blank" class="text-xs font-medium text-zinc-400 hover:text-white px-3 py-1.5 rounded-lg bg-zinc-900 border border-zinc-800 hover:border-zinc-700 transition">Health JSON</a>
      </div>
    </div>
  </header>

  <!-- Main Container -->
  <main class="flex-1 max-w-7xl w-full mx-auto p-4 lg:p-8 space-y-6">

    <!-- BitChord Quick Connect Banner -->
    <div class="glass-card rounded-2xl p-6 bg-gradient-to-r from-emerald-950/40 via-zinc-900/60 to-zinc-900/40 border-emerald-900/30 shadow-xl">
      <div class="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div class="space-y-1">
          <div class="flex items-center gap-2">
            <span class="text-emerald-400 font-semibold text-sm uppercase tracking-wider">BitChord Module URL</span>
            <span class="text-xs px-2 py-0.5 rounded bg-emerald-900/40 text-emerald-300 border border-emerald-700/40">Ready to Import</span>
          </div>
          <p class="text-sm text-zinc-300">Paste this URL into <strong>BitChord</strong> ➔ <strong>Settings</strong> ➔ <strong>Audio Sources</strong> ➔ <strong>Add Source</strong>:</p>
          <div class="flex items-center gap-2 pt-2">
            <input id="bitchord-url-input" type="text" readonly value="{base_url}/bitchord-module.json" 
              class="w-full md:w-[480px] bg-zinc-950/80 border border-zinc-700/80 text-emerald-400 font-mono text-xs sm:text-sm px-3.5 py-2 rounded-xl focus:outline-none select-all shadow-inner">
            <button onclick="copyBitChordUrl()" id="copy-btn"
              class="px-4 py-2 rounded-xl bg-emerald-600 hover:bg-emerald-500 text-white font-medium text-xs sm:text-sm transition flex items-center gap-1.5 shadow-lg shadow-emerald-900/30 whitespace-nowrap">
              <span>📋</span> <span id="copy-btn-text">Copy URL</span>
            </button>
          </div>
        </div>
        <div class="text-xs text-zinc-400 bg-zinc-950/60 p-4 rounded-xl border border-zinc-800/80 space-y-1.5 min-w-[240px]">
          <div class="font-medium text-zinc-200">✨ Client Capabilities</div>
          <div>• 24-bit / 96–192kHz Hi-Res FLAC</div>
          <div>• 0ms Local Lookahead Prefetching</div>
          <div>• Scrubbing & Seeking (HTTP Range)</div>
        </div>
      </div>
    </div>

    <!-- Metrics Grid -->
    <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
      <div class="glass-card rounded-2xl p-4 space-y-1">
        <div class="text-xs font-medium text-zinc-400 flex items-center justify-between">
          <span>Uptime</span>
          <span>⏱️</span>
        </div>
        <div id="metric-uptime" class="text-xl font-bold text-white font-mono">--</div>
        <div class="text-[11px] text-zinc-500">Service active</div>
      </div>

      <div class="glass-card rounded-2xl p-4 space-y-1">
        <div class="text-xs font-medium text-zinc-400 flex items-center justify-between">
          <span>Audio Disk Cache</span>
          <span>💾</span>
        </div>
        <div id="metric-cache-size" class="text-xl font-bold text-white font-mono">-- MB</div>
        <div id="metric-cache-detail" class="text-[11px] text-emerald-400 font-medium">0 tracks (0% of 5 GB)</div>
      </div>

      <div class="glass-card rounded-2xl p-4 space-y-1">
        <div class="text-xs font-medium text-zinc-400 flex items-center justify-between">
          <span>Cache Hit Rate</span>
          <span>⚡</span>
        </div>
        <div id="metric-hit-rate" class="text-xl font-bold text-emerald-400 font-mono">--%</div>
        <div id="metric-requests" class="text-[11px] text-zinc-500">0 requests served</div>
      </div>

      <div class="glass-card rounded-2xl p-4 space-y-1">
        <div class="text-xs font-medium text-zinc-400 flex items-center justify-between">
          <span>Mirror Pool</span>
          <span>🌐</span>
        </div>
        <div id="metric-mirrors" class="text-xl font-bold text-white font-mono">-- / --</div>
        <div id="metric-fastest" class="text-[11px] text-zinc-400 truncate">Fastest: Loading...</div>
      </div>
    </div>

    <!-- In-Browser Search & Player -->
    <div class="glass-card rounded-2xl p-6 space-y-4">
      <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <h2 class="text-base font-semibold text-white flex items-center gap-2">
            <span>🎧</span> <span>Live Audio Stream Test</span>
          </h2>
          <p class="text-xs text-zinc-400">Search any track to verify search indexing, metadata resolution, and Hi-Res FLAC playback directly in your browser.</p>
        </div>
      </div>

      <!-- Search Input -->
      <form id="search-form" onsubmit="performSearch(event)" class="flex gap-2">
        <input type="text" id="search-input" placeholder="Search track, artist, or album (e.g. Daft Punk, Hotel California, Kendrick Lamar)..." 
          class="flex-1 bg-zinc-900 border border-zinc-700/80 text-white text-sm px-4 py-2.5 rounded-xl focus:outline-none focus:border-brand-500 transition shadow-inner">
        <button type="submit" id="search-button"
          class="px-5 py-2.5 bg-zinc-800 hover:bg-zinc-700 text-white font-medium text-sm rounded-xl transition flex items-center gap-2">
          <span>🔍</span> <span>Search</span>
        </button>
      </form>

      <!-- Player Bar (Hidden until playing) -->
      <div id="audio-player-container" class="hidden p-4 rounded-xl bg-zinc-900 border border-zinc-800 space-y-2">
        <div class="flex items-center justify-between text-xs text-zinc-400">
          <div class="flex items-center gap-2">
            <span id="player-track-art" class="w-8 h-8 rounded bg-zinc-800 overflow-hidden flex items-center justify-center text-xs">🎵</span>
            <div>
              <span id="player-track-title" class="font-semibold text-zinc-200 block text-sm">--</span>
              <span id="player-track-artist" class="text-zinc-400 block text-xs">--</span>
            </div>
          </div>
          <div class="flex items-center gap-2">
            <span id="player-quality-tag" class="px-2 py-0.5 rounded bg-emerald-950 text-emerald-400 border border-emerald-800/80 font-mono text-[10px]">HI_RES_LOSSLESS</span>
          </div>
        </div>
        <audio id="audio-element" controls class="w-full h-10 rounded-lg outline-none"></audio>
      </div>

      <!-- Search Results List -->
      <div id="search-results" class="space-y-2 max-h-[380px] overflow-y-auto pr-1"></div>
    </div>

    <!-- Two Columns: Mirrors Monitor & Local Audio Cache -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">

      <!-- Upstream Mirror Health -->
      <div class="glass-card rounded-2xl p-6 space-y-4 flex flex-col">
        <div class="flex items-center justify-between">
          <div>
            <h2 class="text-base font-semibold text-white flex items-center gap-2">
              <span>🌐</span> <span>Upstream Mirror Pool</span>
            </h2>
            <p class="text-xs text-zinc-400">Self-healing pool with automatic failover and latency ranking.</p>
          </div>
          <button onclick="refreshMirrors()" id="refresh-mirrors-btn"
            class="text-xs px-3 py-1.5 rounded-lg bg-zinc-900 hover:bg-zinc-800 text-zinc-300 border border-zinc-700/80 transition flex items-center gap-1">
            <span>🔄</span> <span>Probe Now</span>
          </button>
        </div>

        <div class="flex-1 overflow-x-auto">
          <table class="w-full text-left text-xs">
            <thead>
              <tr class="text-zinc-400 border-b border-zinc-800/80">
                <th class="pb-2.5 font-medium">Mirror Host</th>
                <th class="pb-2.5 font-medium">Status</th>
                <th class="pb-2.5 font-medium text-right">Latency</th>
                <th class="pb-2.5 font-medium text-right">Notes</th>
              </tr>
            </thead>
            <tbody id="mirrors-table-body" class="divide-y divide-zinc-800/60 font-mono">
              <tr><td colspan="4" class="py-4 text-center text-zinc-500 font-sans">Loading mirrors...</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Local Audio Disk Cache -->
      <div class="glass-card rounded-2xl p-6 space-y-4 flex flex-col">
        <div class="flex items-center justify-between">
          <div>
            <h2 class="text-base font-semibold text-white flex items-center gap-2">
              <span>⚡</span> <span>Prefetched & Cached Tracks</span>
            </h2>
            <p class="text-xs text-zinc-400">Audio stored locally for instant 0ms playback and zero rate limits.</p>
          </div>
          <button onclick="clearCache()" 
            class="text-xs px-3 py-1.5 rounded-lg bg-zinc-900 hover:bg-red-950/60 text-zinc-300 hover:text-red-300 border border-zinc-700/80 hover:border-red-800/60 transition flex items-center gap-1">
            <span>🗑️</span> <span>Clear</span>
          </button>
        </div>

        <div class="flex-1 overflow-x-auto max-h-[300px] overflow-y-auto pr-1">
          <table class="w-full text-left text-xs">
            <thead>
              <tr class="text-zinc-400 border-b border-zinc-800/80">
                <th class="pb-2.5 font-medium">Track ID / File</th>
                <th class="pb-2.5 font-medium text-right">Size</th>
                <th class="pb-2.5 font-medium text-right">Action</th>
              </tr>
            </thead>
            <tbody id="cache-table-body" class="divide-y divide-zinc-800/60 font-mono">
              <tr><td colspan="3" class="py-4 text-center text-zinc-500 font-sans">Scanning cache...</td></tr>
            </tbody>
          </table>
        </div>
      </div>

    </div>

  </main>

  <!-- Footer -->
  <footer class="border-t border-zinc-900 px-4 py-4 text-center text-xs text-zinc-400">
    <div class="max-w-7xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-2">
      <div>hifi-bridge microservice • Docker & Unraid Ready</div>
      <div class="text-zinc-400">Tailscale HTTPS • Let's Encrypt Certified</div>
    </div>
  </footer>

  <!-- Dashboard JavaScript -->
  <script>
    function copyBitChordUrl() {{
      const input = document.getElementById('bitchord-url-input');
      navigator.clipboard.writeText(input.value).then(() => {{
        const btnText = document.getElementById('copy-btn-text');
        btnText.innerText = 'Copied!';
        setTimeout(() => {{ btnText.innerText = 'Copy URL'; }}, 2000);
      }});
    }}

    async function loadStats() {{
      try {{
        const res = await fetch('/api/status');
        const data = await res.json();

        // Uptime
        const upSec = Math.floor(data.uptime_seconds);
        const hrs = Math.floor(upSec / 3600);
        const mins = Math.floor((upSec % 3600) / 60);
        document.getElementById('metric-uptime').innerText = `${{hrs}}h ${{mins}}m`;

        // Cache stats
        const cache = data.cache || {{}};
        document.getElementById('metric-cache-size').innerText = `${{cache.total_mb || 0}} MB`;
        document.getElementById('metric-cache-detail').innerText = `${{cache.total_files || 0}} tracks (${{cache.usage_percent || 0}}% of ${{cache.max_gb || 5}} GB)`;
        document.getElementById('metric-hit-rate').innerText = `${{cache.hit_rate_percent || 0}}%`;
        document.getElementById('metric-requests').innerText = `${{cache.cache_hits || 0}} hits / ${{cache.total_requests || 0}} requests`;

        // Mirror stats
        const pool = data.mirror_pool || {{}};
        document.getElementById('metric-mirrors').innerText = `${{pool.healthy || 0}} / ${{pool.total || 0}}`;
        document.getElementById('metric-fastest').innerText = pool.fastest_mirror ? `Fastest: ${{pool.fastest_mirror.replace('https://', '')}}` : 'No mirrors active';

        // Render mirror table
        renderMirrorTable(pool.mirrors || []);

        // Render cache table
        renderCacheTable(cache.recent_files || []);

      }} catch (err) {{
        console.error('Failed to load status:', err);
      }}
    }}

    function renderMirrorTable(mirrors) {{
      const tbody = document.getElementById('mirrors-table-body');
      if (!mirrors || mirrors.length === 0) {{
        tbody.innerHTML = '<tr><td colspan="4" class="py-4 text-center text-zinc-500 font-sans">No mirrors configured</td></tr>';
        return;
      }}
      tbody.innerHTML = mirrors.map(m => {{
        let badgeColor = 'bg-zinc-800 text-zinc-400 border-zinc-700';
        if (m.status === 'healthy') badgeColor = 'bg-emerald-950 text-emerald-400 border-emerald-800/80';
        else if (m.status === 'degraded') badgeColor = 'bg-amber-950 text-amber-400 border-amber-800/80';
        else if (m.status === 'offline') badgeColor = 'bg-red-950 text-red-400 border-red-800/80';

        const latColor = m.latency_ms < 500 ? 'text-emerald-400' : (m.latency_ms < 1500 ? 'text-amber-400' : 'text-zinc-500');
        const latText = m.latency_ms < 9000 ? `${{Math.round(m.latency_ms)}} ms` : 'timeout';
        const displayUrl = esc(m.url.replace('https://', ''));

        return `
          <tr class="hover:bg-zinc-900/40 transition">
            <td class="py-2 text-zinc-200 truncate max-w-[180px]" title="${{esc(m.url)}}">${{displayUrl}}</td>
            <td class="py-2"><span class="px-2 py-0.5 rounded text-[10px] uppercase font-bold border ${{badgeColor}}">${{esc(m.status)}}</span></td>
            <td class="py-2 text-right font-medium ${{latColor}}">${{latText}}</td>
            <td class="py-2 text-right text-[11px] text-zinc-500 truncate max-w-[120px]" title="${{esc(m.notes)}}">${{esc(m.notes) || '—'}}</td>
          </tr>
        `;
      }}).join('');
    }}

    function renderCacheTable(files) {{
      const tbody = document.getElementById('cache-table-body');
      if (!files || files.length === 0) {{
        tbody.innerHTML = '<tr><td colspan="3" class="py-4 text-center text-zinc-500 font-sans">No tracks cached yet. Play or search music to prefetch!</td></tr>';
        return;
      }}
      window._cachedFiles = files;
      tbody.innerHTML = files.map((f, i) => {{
        return `
          <tr class="hover:bg-zinc-900/40 transition">
            <td class="py-2 text-zinc-300 truncate max-w-[180px]" title="${{esc(f.filename)}}">${{esc(f.filename)}}</td>
            <td class="py-2 text-right text-zinc-400">${{esc(f.size_mb)}} MB</td>
            <td class="py-2 text-right">
              <button onclick="playCached(${{i}})" 
                class="px-2 py-0.5 text-[11px] bg-zinc-800 hover:bg-emerald-600 text-zinc-300 hover:text-white rounded transition">▶ Play</button>
            </td>
          </tr>
        `;
      }}).join('');
    }}

    async function performSearch(e) {{
      e.preventDefault();
      const query = document.getElementById('search-input').value.trim();
      if (!query) return;

      const btn = document.getElementById('search-button');
      const resultsDiv = document.getElementById('search-results');
      btn.disabled = true;
      btn.innerHTML = '<span>⏳</span> <span>Searching...</span>';
      resultsDiv.innerHTML = '<div class="py-8 text-center text-zinc-500 text-sm">Querying upstream Hi-Fi pool...</div>';

      try {{
        const res = await fetch(`${{API_PREFIX}}/search?q=${{encodeURIComponent(query)}}&limit=10`);
        const data = await res.json();
        const tracks = data.tracks || [];

        if (tracks.length === 0) {{
          resultsDiv.innerHTML = '<div class="py-8 text-center text-zinc-500 text-sm">No tracks found. Try another search query!</div>';
        }} else {{
          window._searchTracks = tracks;
          resultsDiv.innerHTML = tracks.map((t, i) => {{
            const dur = t.duration ? `${{Math.floor(t.duration / 60)}}:${{String(Math.floor(t.duration % 60)).padStart(2, '0')}}` : '--:--';
            const artUrl = safeUrl(t.artworkURL || t.albumArtworkURL);
            const artImg = artUrl ? `<img src="${{esc(artUrl)}}" class="w-10 h-10 rounded object-cover">` : `<div class="w-10 h-10 rounded bg-zinc-800 flex items-center justify-center text-xs">🎵</div>`;

            return `
              <div class="flex items-center justify-between p-3 rounded-xl bg-zinc-900/60 hover:bg-zinc-800/80 border border-zinc-800/80 transition gap-3">
                <div class="flex items-center gap-3 min-w-0">
                  ${{artImg}}
                  <div class="min-w-0">
                    <div class="font-medium text-sm text-zinc-100 truncate">${{esc(t.title)}}</div>
                    <div class="text-xs text-zinc-400 truncate">${{esc(t.artist)}} ${{t.album ? '• ' + esc(t.album) : ''}}</div>
                  </div>
                </div>
                <div class="flex items-center gap-3 flex-shrink-0">
                  <span class="text-xs text-zinc-500 font-mono hidden sm:inline">${{dur}}</span>
                  <span class="text-[10px] px-2 py-0.5 rounded bg-emerald-950 text-emerald-400 border border-emerald-800/80 font-mono hidden sm:inline">${{esc(t.audioQuality || 'FLAC')}}</span>
                  <button onclick="playSearch(${{i}})"
                    class="px-3 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-medium transition flex items-center gap-1 shadow">
                    <span>▶</span> <span>Play</span>
                  </button>
                </div>
              </div>
            `;
          }}).join('');
        }}
      }} catch (err) {{
        resultsDiv.innerHTML = `<div class="py-8 text-center text-red-400 text-sm">Search error: ${{esc(err.message)}}</div>`;
      }} finally {{
        btn.disabled = false;
        btn.innerHTML = '<span>🔍</span> <span>Search</span>';
      }}
    }}

    const API_PREFIX = {api_prefix};

    // Everything shown here comes from third-party mirrors: always escape it.
    function esc(value) {{
      return String(value ?? '').replace(/[&<>"'`]/g, c => ({{
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;', '`': '&#96;'
      }})[c]);
    }}

    function safeUrl(value) {{
      return typeof value === 'string' && /^https?:[/][/]/i.test(value) ? value : '';
    }}

    function playSearch(i) {{
      const t = (window._searchTracks || [])[i];
      if (t) playTrack(t.id, t.title, t.artist, safeUrl(t.artworkURL || t.albumArtworkURL));
    }}

    function playCached(i) {{
      const f = (window._cachedFiles || [])[i];
      if (f) playTrack(f.id, `Cached Track ${{f.id}}`, 'Local Cache', '');
    }}

    function playTrack(trackId, title, artist, artUrl) {{
      const container = document.getElementById('audio-player-container');
      const audio = document.getElementById('audio-element');
      const trackTitle = document.getElementById('player-track-title');
      const trackArtist = document.getElementById('player-track-artist');
      const trackArt = document.getElementById('player-track-art');

      container.classList.remove('hidden');
      trackTitle.innerText = title;
      trackArtist.innerText = artist;

      if (artUrl) {{
        const img = document.createElement('img');
        img.src = artUrl;
        img.className = 'w-full h-full object-cover';
        trackArt.replaceChildren(img);
      }} else {{
        trackArt.innerHTML = '🎵';
      }}

      audio.src = `${{API_PREFIX}}/stream/audio/${{encodeURIComponent(trackId)}}.flac`;
      audio.play();

      // Refresh stats after a moment to reflect caching
      setTimeout(loadStats, 2000);
    }}

    async function refreshMirrors() {{
      const btn = document.getElementById('refresh-mirrors-btn');
      btn.innerText = 'Probing...';
      try {{
        await fetch('/mirrors/refresh', {{ method: 'POST' }});
        await loadStats();
      }} finally {{
        btn.innerText = '🔄 Probe Now';
      }}
    }}

    async function clearCache() {{
      if (!confirm('Clear all cached audio files from disk?')) return;
      await fetch('/api/cache/clear', {{ method: 'POST' }});
      await loadStats();
    }}

    // Poll stats every 10 seconds
    loadStats();
    setInterval(loadStats, 10000);
  </script>
</body>
</html>
"""
