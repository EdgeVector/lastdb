"""Exercise Social's owner interface and selected-database sessions in DEV."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from http.client import HTTPConnection
import json
from pathlib import Path
import subprocess
import sys
from urllib.parse import parse_qs, urlsplit

from node import Connection
from signed_files import retry, summary
from support import decode_json, require


ROOT = Path(__file__).resolve().parents[2]
SPEC = {"name": "social", "version": 1}


@contextmanager
def window(node, database=None):
    command = [sys.executable, "-B", str(ROOT / "apps/social/social"), "--data-dir", str(node.home),
               "--lastdb", str(node.programs.directory / "lastdb"), "open", "--no-browser"]
    if database:
        command.extend(("--database", database))
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                          env=node.programs.environment) as process:
        try:
            import select
            require(bool(select.select([process.stdout], [], [], 20)[0]), "social_window_start")
            target = urlsplit(process.stdout.readline(4096).decode().strip())
            require(target.hostname == "127.0.0.1" and target.port, "social_loopback")
            if database:
                require(parse_qs(target.query).get("databaseId") == [database], "social_settings_deep_link")
            token = parse_qs(target.fragment).get("session", [None])[0]
            require(bool(token), "social_window_session")
            with closing(HTTPConnection(target.hostname, target.port, timeout=10)) as connection:
                connection.request("GET", target.path)
                with connection.getresponse() as reply:
                    require(reply.status == 200 and b"<title>Social</title>" in reply.read(65537),
                            "social_page_served")
            yield target.port, token
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


def call(window, path, body=None, *, authorized=True, status=200):
    port, token = window
    headers = {"Content-Type": "application/json"}
    if authorized:
        headers["X-Social-Session"] = token
    with closing(HTTPConnection("127.0.0.1", port, timeout=100)) as connection:
        connection.request("GET" if body is None else "POST", path,
                           None if body is None else json.dumps(body).encode(), headers)
        with connection.getresponse() as reply:
            require(reply.status == status, f"social_app_{path.split('?')[0]}_http_{reply.status}")
            return decode_json(reply.read(4 * 1024 * 1024))


def scoped(node, selection, app, method, path, body=None, status=200):
    headers = {"X-LastDB-Client": app, "X-LastDB-Database-Session": selection["databaseSession"],
               "Content-Type": "application/json"}
    with closing(Connection(node.socket)) as connection:
        connection.request(method, path, None if body is None else json.dumps(body).encode(), headers)
        with connection.getresponse() as reply:
            require(reply.status == status, f"social_content_{app}_http_{reply.status}")
            return decode_json(reply.read(65537)) if status == 200 else None


def open_content_apps(recipient, browser, database):
    args = {"spec": SPEC, "intent": "read", "databaseId": database}
    choices = call(browser, "/api/picker", args)
    require(any(row["databaseId"] == database for row in choices), "social_picker_received_database")
    require(call(browser, "/api/pick", {**args, "databaseId": None}) is None, "social_picker_cancel")
    selections = [(name, call(browser, "/api/pick", args)) for name in ("album-viewer", "photo-journal")]
    require(all(selection["databaseId"] == database and selection["spec"] == SPEC
                for _, selection in selections), "social_picker_shared_database")
    for app, selection in selections:
        scoped(recipient, selection, app, "POST", "/api/query", {}, status=403)
        scoped(recipient, selection, app, "GET", f"/api/databases/{database}/share/config", status=403)
        scoped(recipient, selection, app, "POST", f"/api/databases/{database}/mutation", {}, status=403)
    return selections


def read_album(node, selections, database, key, title=None):
    path = f"/api/databases/{database}/query"
    query = {"schema_name": "social/Album", "filter": {"HashKey": key}}
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda item: scoped(node, item[1], item[0], "POST", path, query), selections))
    if title is None:
        return all(reply["results"] == [] for reply in results)
    return all(len(reply["results"]) == 1 and reply["results"][0]["fields"]["title"] == title
               for reply in results)


def observe_access(browser, database, state, revision=None):
    def matches(result):
        access = result["database"]["selfAccess"]
        return (access["state"] == state and access["confirmedAtMs"] and not access["stale"]
                and (revision is None or access["revision"] == str(revision)))
    retry(f"social_recipient_access_{state}", lambda: call(browser, f"/api/database?databaseId={database}"), matches)


def report_content_flow():
    message = ("Two content clients selected the same database, opened the same albums, and were refused "
               "on personal data, the roster, and reader writes. Both retained cached albums after revoke "
               "and caught up after regrant.")
    print(message, flush=True)
    summary(message)
