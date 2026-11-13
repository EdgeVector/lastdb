"""Serve Social's owner interface on a private loopback browser session."""

from http.client import HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import secrets
import subprocess
from urllib.parse import parse_qs, urlencode, urlsplit
import webbrowser

from app import SocialApp
from lastdb import SocialError


ASSETS = Path(__file__).resolve().parent / "web"
STATIC = {"/": ("index.html", "text/html"), "/app.js": ("app.js", "text/javascript"),
          "/styles.css": ("styles.css", "text/css")}
MAX_BODY = 16_384


class SocialServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = False

    def __init__(self, node):
        super().__init__(("127.0.0.1", 0), Handler)
        self.app = SocialApp(node)
        self.origin = f"http://127.0.0.1:{self.server_port}"
        self.session = secrets.token_urlsafe(32)
        self.url = f"{self.origin}/#session={self.session}"


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def setup(self):
        self.request.settimeout(10)
        super().setup()

    def send_payload(self, status, content_type, body):
        self.send_response(status)
        self.send_header("Content-Type", content_type + "; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
        self.end_headers()
        self.wfile.write(body)

    def json(self, value, status=200):
        self.send_payload(status, "application/json", json.dumps(value).encode())

    def do_GET(self):
        target = urlsplit(self.path)
        asset = STATIC.get(target.path)
        if asset is not None and self.same_host():
            filename, content_type = asset
            self.send_payload(200, content_type, (ASSETS / filename).read_bytes())
            return
        self.api(target, self.get)

    def do_POST(self):
        self.api(urlsplit(self.path), self.post)

    def same_host(self):
        return self.headers.get("Host") == self.server.origin.removeprefix("http://")

    def api(self, target, handler):
        token = self.headers.get("X-Social-Session", "")
        origin = self.headers.get("Origin")
        if (not self.same_host() or (origin is not None and origin != self.server.origin)
                or not token.isascii() or not secrets.compare_digest(token, self.server.session)):
            self.json({"error": "Social session is required"}, 403)
            return
        try:
            result = handler(target)
        except (SocialError, ValueError, KeyError, TypeError, UnicodeError) as error:
            self.json({"error": str(error)}, 400)
        except (OSError, HTTPException, subprocess.SubprocessError):
            self.json({"error": "The local LastDB connection is unavailable"}, 503)
        else:
            self.json(result)

    def get(self, target):
        query = parse_qs(target.query)
        app = self.server.app
        match target.path:
            case "/api/home":
                return app.home()
            case "/api/person":
                return app.person(query["nodeKey"][0])
            case "/api/database":
                return app.database(query["databaseId"][0])
            case _:
                raise SocialError("unknown Social operation")

    def post(self, target):
        if self.headers.get_content_type() != "application/json":
            raise SocialError("expected JSON")
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 < length <= MAX_BODY:
            raise SocialError("invalid request size")
        args = json.loads(self.rfile.read(length))
        if not isinstance(args, dict):
            raise SocialError("expected a Social request")
        app = self.server.app
        match target.path:
            case "/api/remember":
                return app.remember(args)
            case "/api/create":
                return app.create(args["name"])
            case "/api/invite":
                return app.invite(args)
            case "/api/revoke":
                return app.revoke(args)
            case "/api/retry":
                return app.retry(args["databaseId"])
            case "/api/accept":
                return app.accept(args["receipt"], args["publisher"], args.get("name"))
            case "/api/picker":
                return app.picker(args["spec"], args["intent"])
            case "/api/pick":
                return app.approve_pick(args["spec"], args["intent"], args["databaseId"])
            case _:
                raise SocialError("unknown Social operation")


def run_server(node, open_browser, database_id=None):
    with SocialServer(node) as server:
        if database_id is not None:
            server.app.selected(database_id)
            query = urlencode({"databaseId": database_id})
            server.url = f"{server.origin}/?{query}#session={server.session}"
        print(server.url, flush=True)
        if open_browser:
            webbrowser.open(server.url)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
