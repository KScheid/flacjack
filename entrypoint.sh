#!/bin/sh
set -e

PUID=${PUID:-99}
PGID=${PGID:-100}

# Adjust user/group IDs if specified differently
if [ "$PGID" != "100" ] && [ "$(id -g appuser 2>/dev/null)" != "$PGID" ]; then
    groupmod -o -g "$PGID" appgroup 2>/dev/null || groupadd -g "$PGID" appgroup 2>/dev/null || true
fi

if [ "$PUID" != "99" ] && [ "$(id -u appuser 2>/dev/null)" != "$PUID" ]; then
    usermod -o -u "$PUID" -g "$PGID" appuser 2>/dev/null || useradd -u "$PUID" -g "$PGID" -d /home/appuser appuser 2>/dev/null || true
fi

# Ensure storage directories exist and have proper permissions for Unraid
mkdir -p /data/audio_cache /music
chown -R "$PUID:$PGID" /data /music /home/appuser 2>/dev/null || true

# Execute command via gosu if running as root
if [ "$(id -u)" = "0" ]; then
    exec gosu "$PUID:$PGID" "$@"
fi

exec "$@"
