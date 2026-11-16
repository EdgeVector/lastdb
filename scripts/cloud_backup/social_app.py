"""Run Social's owner commands and selected-database sessions in DEV."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
import os
from pathlib import Path
import subprocess

from node import Connection
from support import OperationError, decode_json, require, retry, summary


ROOT = Path(__file__).resolve().parents[2]
SPEC = {"name": "social", "version": 1}
GROUPS = ("person", "circle", "db", "grant", "request")


def source(environment):
    """The pinned EdgeVector/social clone the workflow checked out."""
    path = environment.get("SOCIAL_SOURCE")
    require(bool(path) and Path(path, "src/social").is_file(), "social_source_missing")
    return Path(path)


def command(node):
    environment = node.programs.environment
    entrypoint = environment.get("SOCIAL_COMMAND") or str(source(environment) / "src/social")
    return [entrypoint, "--data-dir", str(node.home), "--lastdb", str(node.programs.directory / "lastdb")]


def sdk(node, action, **body):
    result = subprocess.run(
        ["node", os.environ["SOCIAL_SDK_OPERATION"], action, *command(node)],
        input=json.dumps(body).encode(), capture_output=True, env=node.programs.environment, timeout=190,
    )
    require(result.returncode == 0, f"social_sdk_{action}")
    return decode_json(result.stdout)


def install(publisher, root, revision):
    programs = publisher.programs
    require(len(revision) == 40 and all(char in "0123456789abcdef" for char in revision), "social_install_revision")
    home = root / "os-home"
    home.mkdir(mode=0o700)
    extra = {"HOME": str(home), "LASTDB_APPS_ROOT": str(root / "apps"),
             "PATH": f"{programs.directory}:{os.environ['PATH']}"}
    catalog = programs.environment.get("SOCIAL_INSTALL_REGISTRY") or "dev"
    require(catalog in ("dev", "prod"), "social_install_registry")
    index = decode_json((ROOT / "lastdb_node/environments.json").read_bytes())["environments"][catalog]["registry_index"]
    receipt = programs.command(publisher.home, [
        "app", "install", "social-graph", "--channel", "next", "--index", index, "--accept-install", "--json",
    ], extra=extra, timeout=150)
    require(isinstance(receipt, list) and len(receipt) == 1 and receipt[0]["sha"] == revision,
            "social_install_exact_source")
    link = home / ".local/bin/social"
    managed = root / "apps/social-graph/current/source/src/social"
    require(link.is_symlink() and link.resolve(strict=True) == managed.resolve(strict=True), "social_install_command")
    programs.environment["SOCIAL_COMMAND"] = str(link)
    summary(f"Installed Social from the signed {catalog.upper()} registry at `{revision}` and used its managed command link.")


def social(node, *arguments, stdin=None, refusal=None, refusals=()):
    """Run one Social command and decode its JSON result."""
    label = "_".join(arguments[:2] if arguments[0] in GROUPS else arguments[:1]).replace("-", "_")
    try:
        result = subprocess.run([*command(node), *arguments],
                                input=None if stdin is None else json.dumps(stdin).encode(),
                                capture_output=True, env=node.programs.environment, timeout=180)
    except subprocess.TimeoutExpired as error:
        raise OperationError(f"social_{label}_deadline") from error
    if refusal is not None:
        require(result.returncode == 1 and not result.stdout and refusal in result.stderr,
                f"social_{label}_expected_refusal")
        return None
    if result.returncode:
        # An invitation is held in memory and must never enter workflow output.
        for fragment, code in refusals:
            require(fragment.encode() not in result.stderr, code)
        detail = result.stderr.decode(errors="replace").partition(" returned HTTP ")[2][:3]
        suffix = f"_http_{detail}" if detail.isascii() and detail.isdigit() else ""
        raise OperationError(f"social_{label}_failed{suffix}")
    return decode_json(result.stdout)


def pick(node, spec, intent, database):
    """Issue an owner-approved data session for one database."""
    return social(node, "pick-database", "--spec", spec["name"], "--version", str(spec["version"]),
                  "--intent", intent, "--database", database, "--session")


def scoped(node, selection, app, method, path, body=None, status=200):
    headers = {"X-LastDB-Client": app, "X-LastDB-Database-Session": selection["databaseSession"],
               "Content-Type": "application/json"}
    with closing(Connection(node.socket)) as connection:
        connection.request(method, path, None if body is None else json.dumps(body).encode(), headers)
        with connection.getresponse() as reply:
            require(reply.status == status, f"social_content_{app}_http_{reply.status}")
            return decode_json(reply.read(65537)) if status == 200 else None


def open_content_apps(recipient, database):
    choices = social(recipient, "pick-database", "--spec", SPEC["name"], "--version", str(SPEC["version"]),
                     "--intent", "read")
    require(any(row["databaseId"] == database for row in choices), "social_picker_received_database")
    selections = [(name, pick(recipient, SPEC, "read", database)) for name in ("album-viewer", "photo-journal")]
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


def observe_access(node, database, state, revision=None, *, role="read", write_schemas=None):
    def matches(result):
        access = result["database"]["selfAccess"]
        return (access["state"] == state and access["confirmedAtMs"] and not access["stale"]
                and (revision is None or access["revision"] == str(revision))
                and (state != "active" or access["role"] == role)
                and (write_schemas is None or access.get("writeSchemas") == write_schemas))
    retry(f"social_recipient_access_{state}", lambda: social(node, "db", "show", database), matches)


def report_content_flow():
    message = ("Two content clients selected the same database, opened the same albums, and were refused "
               "on personal data, the roster, and reader writes. Both retained cached albums after revoke "
               "and caught up after regrant.")
    print(message, flush=True)
    summary(message)
