#!/usr/bin/env python3
"""Accept one TCP connection and never reply (hung HTTP peer for timeout probes).

Prints `host:port` on stdout, then blocks. SIGTERM/SIGINT exits.
"""
from __future__ import annotations

import socket
import sys


def main() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    host, port = sock.getsockname()
    print(f"{host}:{port}", flush=True)
    try:
        conn, _ = sock.accept()
        # Keep the socket open without reading or writing until killed.
        while True:
            data = conn.recv(4096)
            if not data:
                break
    except KeyboardInterrupt:
        return 0
    finally:
        sock.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
