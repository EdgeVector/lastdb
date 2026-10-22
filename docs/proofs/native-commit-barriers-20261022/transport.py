#!/usr/bin/env python3
"""Measure real, sequential LastDB HTTP writes through an owned Unix socket.

Run against a generated isolated home only. No database files are copied.
The caller starts/stops the real daemon and seeds DurableWriteDemo separately.
"""

import argparse
import hashlib
import http.client
import json
import math
from pathlib import Path
import socket
import statistics
import time


class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=60)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def request(client, route, payload=None):
    start_unix_us = time.time_ns() // 1000
    started = time.perf_counter_ns()
    encoded = None if payload is None else json.dumps(payload).encode()
    client.request("GET" if payload is None else "POST", route, encoded,
                   {"Content-Type": "application/json", "X-LastDB-Client": "durable-latency-proof"})
    response = client.getresponse()
    data = json.loads(response.read())
    elapsed = (time.perf_counter_ns() - started) / 1_000_000
    client.last_window = (start_unix_us, time.time_ns() // 1000)
    return response.status, data, elapsed

