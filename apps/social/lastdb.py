"""Social's connection to the local LastDB owner socket."""

from contextlib import closing
import http.client
import json
from pathlib import Path
import socket
import subprocess
import tempfile


class SocialError(Exception):
    pass


class LastDBError(SocialError):
    def __init__(self, method, path, status, body):
        self.status = status
        self.code = body.get("error") if isinstance(body, dict) else None
        detail = self.code or (body if isinstance(body, str) else "request failed")
        super().__init__(f"{method} {path} returned HTTP {status} ({detail})")


class LocalConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=95)
        self.path = str(path)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


class LastDB:
    def __init__(self, home, executable):
        self.home = home.resolve()
        if self.home == (Path.home() / ".lastdb").resolve():
            raise SocialError("this Social prototype requires a separate LastDB home")
        self.socket = self.home / "data" / "folddb.sock"
        self.executable = executable.resolve()

    def call(self, method, path, body=None):
        payload = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        headers = {"X-LastDB-Client": "social", "Content-Type": "application/json"}
        with closing(LocalConnection(self.socket)) as connection:
            connection.request(method, path, payload, headers)
            with connection.getresponse() as response:
                raw = response.read(8 * 1024 * 1024 + 1)
                status = response.status
        if len(raw) > 8 * 1024 * 1024:
            raise SocialError("LastDB reply exceeds the Social response limit")
        try:
            result = json.loads(raw)
        except (ValueError, UnicodeError):
            result = raw.decode(errors="replace").strip()
        if status != 200 or not isinstance(result, dict) or result.get("ok") is not True:
            raise LastDBError(method, path, status, result)
        return result

    def command(self, *arguments):
        command = [str(self.executable), "--data-dir", str(self.home), *arguments]
        result = subprocess.run(command, capture_output=True, text=True, timeout=20)
        if result.returncode:
            raise SocialError(result.stderr.strip() or f"LastDB {arguments[0]} failed")
        return json.loads(result.stdout)

    def require_sessions(self):
        if self.call("GET", "/api/version").get("capabilities", {}).get("database_sessions_v1") is not True:
            raise SocialError("Social needs a LastDB build with database sessions")

    def verify_contact(self, path, expected_key):
        return self.command("share", "verify-contact", str(path), "--recipient-key", expected_key)

    def verify_saved_contact(self, code, expected_key):
        with tempfile.TemporaryDirectory(prefix="social-contact-") as directory:
            path = Path(directory) / "recipient.contact"
            path.write_text(code, encoding="ascii")
            return self.verify_contact(path, expected_key)

    def declare_private(self, schema):
        return self.call("POST", "/api/schemas/declare", {
            "namespace": "social-graph", "schema": schema,
        })["definition_id"]

    def get_record(self, schema, key):
        rows = self.call("POST", "/api/query", {
            "schema_name": schema, "filter": {"HashKey": key},
        })["results"]
        return rows[0] if rows else None

    def write_record(self, schema, definition, key, fields, previous):
        return self.call("POST", "/api/mutation", {
            "type": "mutation", "schema": schema, "definition_id": definition,
            "key_value": {"hash": key, "range": None}, "alternates": {},
            "record": fields, "mutation_type": "replace" if previous else "create",
            "expected": {"type": "revision", "revision": previous["revision"]}
            if previous else {"type": "absent"}, "durability": "durable",
        })

    def own_key(self):
        with tempfile.TemporaryDirectory(prefix="social-contact-") as directory:
            path = Path(directory) / "publisher.contact"
            return self.command("share", "contact", "--out", str(path))["recipient_public_key"]
