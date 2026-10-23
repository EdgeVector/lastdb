"""Inspect only the actual generated native payload bytes for boundary evidence."""

import hashlib
from pathlib import Path
import shutil
import struct
import subprocess
import zlib

BLOCK_BYTES = 4096


def decode(frame, decoder):
    stored = struct.unpack_from("<I", frame, 28)[0]
    expected = struct.unpack_from("<I", frame, 24)[0]
    if (frame[:8] != b"LDBLK1\0\0" or len(frame) != 80 + stored
            or not 1 <= expected <= BLOCK_BYTES or frame[11] != 0):
        raise RuntimeError("A generated payload frame has an unsupported shape")
    check = bytearray(frame)
    check[64:68] = b"\0" * 4
    if zlib.crc32(check) != struct.unpack_from("<I", frame, 64)[0]:
        raise RuntimeError("The generated payload frame checksum differs")
    if frame[10] == 0:
        raw = frame[80:]
    elif frame[10] == 1:
        result = subprocess.run([str(decoder), "-d", "-q", "-c"], input=frame[80:],
                                capture_output=True, timeout=5, check=True)
        raw = result.stdout
    else:
        raise RuntimeError("The generated payload frame codec is unsupported")
    if len(raw) != expected:
        raise RuntimeError("The decoded generated frame length differs")
    return raw


def stream(home, offered, decoder):
    parts, output, ordinal, object_id = [], bytearray(), 0, None
    paths = sorted(name for name in offered if name.startswith("packs/") and "/map/" not in name)
    if len(paths) < 2 or len({Path(name).parts[1] for name in paths}) != 1:
        raise RuntimeError("The offered payload requires one object with multiple physical parts")
    for number, relative in enumerate(paths):
        path = home / relative
        with path.open("rb") as source:
            header = source.read(128)
            part = struct.unpack_from("<I", header, 40)[0]
            first = struct.unpack_from("<Q", header, 48)[0]
            blocks = struct.unpack_from("<Q", header, 56)[0]
            if (header[:8] != b"LDPART1\0" or part != number or first != ordinal
                    or struct.unpack_from("<I", header, 44)[0] != path.stat().st_size):
                raise RuntimeError("The actual generated payload part boundary differs")
            object_id = header[16:32] if object_id is None else object_id
            if header[16:32] != object_id:
                raise RuntimeError("The offered parts do not belong to the same payload object")
            for _ in range(blocks):
                prefix = source.read(80)
                size = struct.unpack_from("<I", prefix, 12)[0]
                if (not 80 < size <= BLOCK_BYTES + 80 or prefix[32:48] != object_id
                        or struct.unpack_from("<Q", prefix, 16)[0] != ordinal):
                    raise RuntimeError("The generated frame identity or ordinal differs")
                raw = decode(prefix + source.read(size - 80), decoder)
                output.extend(raw)
                ordinal += 1
                if len(output) > 16 * 1024 * 1024:
                    raise RuntimeError("Decoded generated payload exceeds 16 MiB")
            if source.read(1):
                raise RuntimeError("A generated payload part has bytes outside its frames")
        parts.append({"path": relative, "part": part, "first_block": first, "blocks": blocks})
    return bytes(output), parts


def inspect(home, offered, collection, keys, expected):
    binary = shutil.which("zstd")
    if binary is None:
        raise RuntimeError("The native byte capture requires the installed zstd command")
    decoder = Path(binary).resolve(strict=True)
    raw, parts = stream(home, offered, decoder)
    lookup = {value: number for number, value in enumerate(keys)}
    cursor, rows = 0, []
    while cursor < len(raw):
        start = cursor
        header = raw[cursor:cursor + 96]
        if header[:8] != b"LDREC1\0\0" or len(header) != 96:
            raise RuntimeError("The generated stream does not contain a complete native record")
        key_bytes = struct.unpack_from("<I", header, 12)[0]
        body_bytes = struct.unpack_from("<Q", header, 16)[0]
        if key_bytes > 65535 or body_bytes > 1024 * 1024:
            raise RuntimeError("An observed generated record exceeds its finite key or body bound")
        cursor += 96
        key = raw[cursor:cursor + key_bytes].decode()
        cursor += key_bytes
        body = raw[cursor:cursor + body_bytes]
        cursor += body_bytes
        if (key not in lookup or len(body) != body_bytes or body.hex() != expected[key]
                or hashlib.sha256(key.encode()).digest() != header[40:72]
                or hashlib.sha256(collection.encode()).digest()[:16] != header[72:88]):
            raise RuntimeError("An actual generated record differs from the exact offered key and body")
        touched = [part["part"] for part in parts
                   if start < (part["first_block"] + part["blocks"]) * BLOCK_BYTES
                   and cursor > part["first_block"] * BLOCK_BYTES]
        rows.append({"number": lookup[key], "key_bytes": key_bytes, "body_bytes": body_bytes,
                     "body_sha256": hashlib.sha256(body).hexdigest(),
                     "decoded_start": start, "decoded_end": cursor, "data_parts": touched})
    if len(rows) != len(keys) or len({row["number"] for row in rows}) != len(keys):
        raise RuntimeError("The actual native stream does not contain each offered key exactly once")
    crossing = [row["number"] for row in rows if len(row["data_parts"]) > 1]
    if not crossing:
        raise RuntimeError("No actual generated record crosses a physical payload part")
    return {"data_parts": parts, "records": rows, "spanning_record_numbers": crossing,
            "block_bytes": BLOCK_BYTES, "decoded_bytes": len(raw),
            "decoder": str(decoder), "decoder_sha256": hashlib.sha256(decoder.read_bytes()).hexdigest(),
            "evidence": "Actual native frames and full record headers, keys, and body bytes; no computed-order assumption"}
