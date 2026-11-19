"""
FLAC completeness check.

Mirrors sometimes end a download early without an error and without having
said how big the file is. The partial file still starts with a valid header
that claims the full duration, so a player plays a few seconds, fails to seek
past the end of the data and skips the track. Every later play of that track
from the disk cache fails the same way.

This module decides whether a FLAC byte stream really ends with its final
audio frame:

  * STREAMINFO (the first metadata block) gives the total sample count.
  * Scanning backwards from the end, the last frame header is found and the
    frame is verified with its CRC-16, so we know the file does not end
    mid-frame.
  * That frame's first sample + its block size must equal the total.

Only the first few bytes and the last few hundred KB are needed, so it works
on a file on disk and on a stream as it passes through.
"""

from typing import Optional, Tuple

# The final frame plus some slack. Even 24-bit/192 kHz frames are far smaller.
TAIL_BYTES = 256 * 1024
HEAD_BYTES = 64


def _crc8_table():
    table = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = ((c << 1) ^ 0x07) & 0xFF if c & 0x80 else (c << 1) & 0xFF
        table.append(c)
    return table


def _crc16_table():
    table = []
    for i in range(256):
        c = i << 8
        for _ in range(8):
            c = ((c << 1) ^ 0x8005) & 0xFFFF if c & 0x8000 else (c << 1) & 0xFFFF
        table.append(c)
    return table


_CRC8 = _crc8_table()
_CRC16 = _crc16_table()


def _crc8(data: bytes) -> int:
    c = 0
    for b in data:
        c = _CRC8[c ^ b]
    return c


def _crc16(data: bytes) -> int:
    c = 0
    table = _CRC16
    for b in data:
        c = ((c << 8) & 0xFFFF) ^ table[(c >> 8) ^ b]
    return c


def parse_streaminfo(head: bytes) -> Optional[Tuple[int, int, int]]:
    """Returns (total_samples, min_blocksize, max_frame_size) from the start of a FLAC file, or None."""
    if len(head) < 42 or head[:4] != b"fLaC" or (head[4] & 0x7F) != 0:
        return None
    info = head[8:42]
    min_block = int.from_bytes(info[0:2], "big")
    max_frame = int.from_bytes(info[7:10], "big")
    total_samples = ((info[13] & 0x0F) << 32) | int.from_bytes(info[14:18], "big")
    return total_samples, min_block, max_frame


def _read_utf8_number(data: bytes, pos: int) -> Optional[Tuple[int, int]]:
    """FLAC's UTF-8-style coded frame/sample number. Returns (value, new position)."""
    if pos >= len(data):
        return None
    first = data[pos]
    if first < 0x80:
        return first, pos + 1
    if first == 0xFE:
        length, value = 7, 0
    else:
        ones = 0
        while first & (0x80 >> ones):
            ones += 1
        if ones < 2 or ones > 6:
            return None
        length, value = ones, first & (0xFF >> (ones + 1))
    if pos + length > len(data):
        return None
    for b in data[pos + 1 : pos + length]:
        if b & 0xC0 != 0x80:
            return None
        value = (value << 6) | (b & 0x3F)
    return value, pos + length


def _parse_frame_header(data: bytes, pos: int, min_block: int) -> Optional[Tuple[int, int]]:
    """Returns (first sample of the frame, block size) for a valid frame header at pos."""
    if pos + 6 > len(data) or data[pos] != 0xFF or data[pos + 1] not in (0xF8, 0xF9):
        return None
    variable = data[pos + 1] == 0xF9
    block_code = data[pos + 2] >> 4
    rate_code = data[pos + 2] & 0x0F
    channels = data[pos + 3] >> 4
    size_code = (data[pos + 3] >> 1) & 0x07
    if block_code == 0 or rate_code == 15 or channels > 10 or size_code == 3 or data[pos + 3] & 0x01:
        return None

    coded = _read_utf8_number(data, pos + 4)
    if coded is None:
        return None
    number, p = coded

    if block_code == 1:
        block_size = 192
    elif 2 <= block_code <= 5:
        block_size = 576 << (block_code - 2)
    elif block_code == 6:
        if p + 1 > len(data):
            return None
        block_size = data[p] + 1
        p += 1
    elif block_code == 7:
        if p + 2 > len(data):
            return None
        block_size = int.from_bytes(data[p : p + 2], "big") + 1
        p += 2
    else:
        block_size = 256 << (block_code - 8)

    if rate_code == 12:
        p += 1
    elif rate_code in (13, 14):
        p += 2
    if p + 1 > len(data) or _crc8(data[pos:p]) != data[p]:
        return None

    first_sample = number if variable else number * (min_block or block_size)
    return first_sample, block_size


def check_flac_bytes(head: bytes, tail: bytes) -> Optional[bool]:
    """
    head: the first bytes of the file (at least 42). tail: the last bytes.
    True: complete. False: truncated. None: cannot tell (not FLAC, unknown
    length, or no frame found in the tail), in which case callers should not
    treat the file as broken.
    """
    info = parse_streaminfo(head)
    if info is None:
        return None
    total_samples, min_block, max_frame = info
    if total_samples == 0:
        return None  # encoder did not record the length

    # Some taggers append a 128-byte ID3v1 tag after the audio.
    if len(tail) >= 128 and tail[-128:-125] == b"TAG":
        tail = tail[:-128]
    if len(tail) < 8:
        return False

    stored_crc = int.from_bytes(tail[-2:], "big")
    body = tail[:-2]
    pos = len(body)
    # A complete final frame starts no further back than the largest frame in
    # the file (recorded in STREAMINFO when the encoder knew it).
    window = (max_frame + 64) if max_frame else 128 * 1024
    stop = max(0, len(body) - window)
    while True:
        pos = body.rfind(b"\xff", stop, pos)
        if pos < 0:
            break
        parsed = _parse_frame_header(tail, pos, min_block)
        if parsed is None:
            continue
        if _crc16(body[pos:]) == stored_crc:
            first_sample, block_size = parsed
            return first_sample + block_size == total_samples
        # A valid-looking header whose frame does not check out: either a false
        # sync inside audio data, or the real last frame cut short. Keep looking;
        # if no frame closes the stream cleanly, it is truncated.
    # A complete file always ends with a full frame inside the window, so a
    # FLAC with a known length and no closing frame is cut short.
    return False


def check_flac_file(path: str) -> Optional[bool]:
    """check_flac_bytes for a file on disk."""
    try:
        with open(path, "rb") as f:
            head = f.read(HEAD_BYTES)
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - TAIL_BYTES))
            tail = f.read()
    except OSError:
        return None
    return check_flac_bytes(head, tail)
