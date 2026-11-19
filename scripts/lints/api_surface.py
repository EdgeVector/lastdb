#!/usr/bin/env python3
"""Tie the lastdbd socket surface to API_VERSION.

The Rust constants are the only hand-edited interface version numbers:

  lastdb_node/src/lastdb_uds/uds_router.rs   API_VERSION, CAPABILITIES_JSON, EXTENSIONS_JSON
  lastdb_node/src/version.rs                 MIN_SUPPORTED_API_VERSION,
                                             APP_API_VERSION, MIN_SUPPORTED_APP_API_VERSION

This script derives two committed files from the source:

  lastdb_node/api-surface.json                 routes, capabilities, versions
  lastdb_app_sdk/src/apiVersion.generated.ts   the SDK's copy of the versions

`--write` regenerates both. `--check` fails when either is out of date, when
the SDK names a route the node does not serve, or (with `--base <rev>`) when
the surface changed against the base revision without the required version
change:

  * any added or removed route or capability requires a higher API_VERSION;
  * an app API extension, a route under `/v1/x/`, is listed by name and is
    not a route for these rules: adding one changes no version, and removing
    one is refused, because an installed app that names it can no longer run.
    `EXTENSIONS_JSON` must name exactly those extensions; edit that constant
    when this check says it does not;
  * a removed capability, or a removed route that the base SDK used, also
    requires MIN_SUPPORTED_API_VERSION to rise above the previous minimum.
    The floor is the first version whose installed apps do not call the
    removed routes. Equality with API_VERSION is not required: a later
    version can still install an app that never called those routes.

Limit: this is a source check of route strings and capability names. It does
not see a change in request keys or in the meaning of a reply. It does check
that lastdb_node/app-api.json lists exactly the app API paths this node
serves: every route in `routes` that starts with `/v1/`, and `/v1/x/<name>`
for each name in `extensions`. `app_routes` is the SDK `/api` list and is not
part of that file. `info.version` must equal `api_version`. The record-home
operation checks the bodies. The SDK types for those bodies are generated
and checked by scripts/lints/app_api_types.py.
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ROUTER = "lastdb_node/src/lastdb_uds/uds_router.rs"
VERSION = "lastdb_node/src/version.rs"
# The socket server: the router and the request executors. Other trees hold
# client paths for the cloud service, which are not this node's surface.
SERVER_DIRS = ("lastdb_node/src/lastdb_uds", "lastdb_node/src/exec")
SDK_ROUTES = "lastdb_app_sdk/src/databases.ts"
SURFACE = "lastdb_node/api-surface.json"
APP_API = "lastdb_node/app-api.json"
SDK_GENERATED = "lastdb_app_sdk/src/apiVersion.generated.ts"

ROUTE_LITERAL = re.compile(r'"(/health|/api/[A-Za-z0-9_/-]*|/v1/[A-Za-z0-9_/-]*)"')
SDK_ROUTE_LITERAL = re.compile(r"'((?:/api|/v1)/[A-Za-z0-9_/-]*)'")
EXTENSION_PREFIX = "/v1/x/"


def constant(path, name):
    match = re.search(rf"pub const {name}: u32 = ([0-9]+);", (ROOT / path).read_text())
    if match is None:
        raise SystemExit(f"api-surface: {path} has no literal `pub const {name}: u32`")
    return int(match[1])


def capabilities():
    text = (ROOT / ROUTER).read_text()
    match = re.search(r'const CAPABILITIES_JSON: &str = r#"(.*?)"#;', text, re.S)
    if match is None:
        raise SystemExit(f"api-surface: {ROUTER} has no CAPABILITIES_JSON raw string")
    return sorted(name for name, on in json.loads(match[1]).items() if on is True)


def extensions_json():
    """The names in `EXTENSIONS_JSON`, in the order the node reports them."""
    text = (ROOT / ROUTER).read_text()
    match = re.search(r'const EXTENSIONS_JSON: &str = r#"(.*?)"#;', text, re.S)
    if match is None:
        raise SystemExit(f"api-surface: {ROUTER} has no EXTENSIONS_JSON raw string")
    names = json.loads(match[1])
    if not isinstance(names, list) or any(not isinstance(name, str) for name in names):
        raise SystemExit(f"api-surface: EXTENSIONS_JSON in {ROUTER} is not a JSON array of names")
    return names


def norm(route):
    return route if route == "/" else route.rstrip("/")


def scoped(route):
    """The database-scoped form of an `/api/<path>` route, or None."""
    return "/api/databases/{database}/" + route[len("/api/"):] if route.startswith("/api/") else None


def served(route, routes, extensions=()):
    return (
        route in routes
        or scoped(route) in routes
        or (route.startswith(EXTENSION_PREFIX) and route[len(EXTENSION_PREFIX):] in extensions)
    )


def node_routes(app_routes):
    """The served routes, and the names of the app API extensions."""
    routes, source = set(), ""
    for directory in SERVER_DIRS:
        for path in sorted((ROOT / directory).rglob("*.rs")):  # walk-ok: two repo source dirs
            source += path.read_text()
    found = {norm(r) for r in ROUTE_LITERAL.findall(source)}
    extensions = sorted(r[len(EXTENSION_PREFIX):] for r in found if r.startswith(EXTENSION_PREFIX))
    routes.update(r for r in found if not r.startswith(EXTENSION_PREFIX))
    # A route served only under a database scope appears in the source without
    # the `/api/` prefix. Record the ones the SDK uses in their scoped form.
    for route in app_routes:
        if route.startswith("/api/") and route not in routes and f'"{route[len("/api/"):]}"' in source:
            routes.add(scoped(route))
    return sorted(routes), extensions


def sdk_routes():
    text = (ROOT / SDK_ROUTES).read_text()
    block = re.search(r"export const LASTDB_API_ROUTES = \{(.*?)\} as const;", text, re.S)
    if block is None:
        raise SystemExit(f"api-surface: {SDK_ROUTES} has no LASTDB_API_ROUTES block")
    return sorted({norm(r) for r in SDK_ROUTE_LITERAL.findall(block[1])})


def current_surface():
    app_routes = sdk_routes()
    routes, extensions = node_routes(app_routes)
    return {
        "api_version": constant(ROUTER, "API_VERSION"),
        "min_supported_api_version": constant(VERSION, "MIN_SUPPORTED_API_VERSION"),
        "app_api_version": constant(VERSION, "APP_API_VERSION"),
        "min_supported_app_api_version": constant(VERSION, "MIN_SUPPORTED_APP_API_VERSION"),
        "capabilities": capabilities(),
        "routes": routes,
        "extensions": extensions,
        "app_routes": app_routes,
    }


def surface_text(surface):
    return json.dumps(surface, indent=2) + "\n"


def sdk_text(surface):
    return (
        "// GENERATED by scripts/lints/api_surface.py --write. Do not edit.\n"
        "// Source: lastdb_node/src/lastdb_uds/uds_router.rs and lastdb_node/src/version.rs.\n"
        "\n"
        "/** Current `lastdbd` Unix-socket protocol `API_VERSION`. */\n"
        f"export const LASTDB_SDK_API_VERSION = {surface['api_version']};\n"
        "\n"
        "/** Minimum protocol `api_version` supported by `lastdbd`. */\n"
        f"export const LASTDB_MIN_SUPPORTED_API_VERSION = {surface['min_supported_api_version']};\n"
    )


def base_surface(rev):
    shown = subprocess.run(
        ["git", "-C", str(ROOT), "show", f"{rev}:{SURFACE}"],
        capture_output=True, text=True, check=False,
    )
    if shown.returncode != 0:
        return None
    return json.loads(shown.stdout)


def compare(base, now):
    errors = []
    api, base_api = now["api_version"], base["api_version"]
    low, base_low = now["min_supported_api_version"], base["min_supported_api_version"]
    if api < base_api:
        errors.append(f"API_VERSION went down: {base_api} -> {api}")
    if low < base_low:
        errors.append(f"MIN_SUPPORTED_API_VERSION went down: {base_low} -> {low}")
    changes = []
    for kind in ("routes", "capabilities"):
        old, new = set(base[kind]), set(now[kind])
        changes += [f"+ {kind[:-1]} {item}" for item in sorted(new - old)]
        changes += [f"- {kind[:-1]} {item}" for item in sorted(old - new)]
    if changes and api <= base_api:
        errors.append(
            f"the socket surface changed but API_VERSION is still {api}; "
            f"set API_VERSION to {base_api + 1} in {ROUTER}:\n    " + "\n    ".join(changes)
        )
    removed = sorted(set(base.get("extensions", [])) - set(now["extensions"]))
    if removed:
        errors.append(
            "an app API extension was removed; an installed app that names it can no longer run. "
            "Keep serving it: " + ", ".join(removed)
        )
    lost = set(base["routes"]) - set(now["routes"])
    gone = {r for r in base["app_routes"] if r in lost or scoped(r) in lost}
    broken = sorted(gone
                    | (set(base["capabilities"]) - set(now["capabilities"])))
    if broken and low <= base_low:
        errors.append(
            "an app-facing route or capability was removed, so a client from the previous "
            "minimum can no longer work; raise MIN_SUPPORTED_API_VERSION above "
            f"{base_low} in {VERSION}. The floor is the first version whose installed "
            "apps do not call the removed routes:\n    "
            + "\n    ".join(broken)
        )
    return errors, changes


def app_api_paths(surface):
    """Paths the OpenAPI file must list: /v1 routes plus /v1/x/<extension>."""
    paths = {route for route in surface["routes"] if route.startswith("/v1/")}
    paths.update(EXTENSION_PREFIX + name for name in surface["extensions"])
    return paths


def app_api_errors(surface):
    """The OpenAPI file matches the served app API paths and api_version.

    `app_routes` is the SDK list of `/api` routes. The app socket does not
    serve those, so they are not required in the OpenAPI file. `--write` does
    not rewrite the file.
    """
    file = ROOT / APP_API
    if not file.is_file():
        return [f"{APP_API} is missing"]
    try:
        spec = json.loads(file.read_text())
    except json.JSONDecodeError as error:
        return [f"{APP_API} is not JSON: {error}"]
    declared = set((spec.get("paths") or {}))
    expected = app_api_paths(surface)
    errors = []
    missing = sorted(expected - declared)
    extra = sorted(declared - expected)
    if missing:
        errors.append(f"{APP_API} omits app API paths: " + ", ".join(missing))
    if extra:
        errors.append(f"{APP_API} has paths the node does not serve: " + ", ".join(extra))
    version = str((spec.get("info") or {}).get("version"))
    api = str(surface["api_version"])
    if version != api:
        errors.append(f"{APP_API} info.version is {version}; api_version is {api}")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true", help="regenerate the committed files")
    mode.add_argument("--check", action="store_true", help="fail when a rule is broken")
    parser.add_argument("--base", help="git revision to compare the surface against")
    args = parser.parse_args()

    surface = current_surface()
    outputs = {SURFACE: surface_text(surface), SDK_GENERATED: sdk_text(surface)}
    if args.write:
        for path, text in outputs.items():
            (ROOT / path).write_text(text)
            print(f"api-surface: wrote {path}")
        errors = app_api_errors(surface)
        for error in errors:
            print(f"api-surface: FAIL {error}", file=sys.stderr)
        return 1 if errors else 0

    errors = app_api_errors(surface)
    if surface["min_supported_api_version"] > surface["api_version"]:
        errors.append("MIN_SUPPORTED_API_VERSION is above API_VERSION")
    if surface["min_supported_app_api_version"] > surface["app_api_version"]:
        errors.append("MIN_SUPPORTED_APP_API_VERSION is above APP_API_VERSION")
    missing = sorted(
        r for r in surface["app_routes"] if not served(r, surface["routes"], surface["extensions"])
    )
    if missing:
        errors.append("the SDK names routes the node does not serve: " + ", ".join(missing))
    reported = extensions_json()
    if reported != surface["extensions"]:
        errors.append(
            "EXTENSIONS_JSON does not name exactly the served /v1/x/ routes "
            f"{json.dumps(surface['extensions'])}; edit EXTENSIONS_JSON in {ROUTER}"
        )
    for path, text in outputs.items():
        file = ROOT / path
        if not file.is_file() or file.read_text() != text:
            errors.append(f"{path} is out of date; run: python3 scripts/lints/api_surface.py --write")
    if args.base:
        base = base_surface(args.base)
        if base is None:
            print(f"api-surface: {args.base} has no {SURFACE}; no base comparison")
        else:
            found, changes = compare(base, surface)
            errors += found
            print(f"api-surface: {len(changes)} surface change(s) against {args.base}")
    if errors:
        for error in errors:
            print(f"api-surface: FAIL {error}", file=sys.stderr)
        return 1
    print(f"api-surface: OK api_version={surface['api_version']} "
          f"min_supported={surface['min_supported_api_version']} "
          f"app_api_version={surface['app_api_version']} "
          f"min_supported_app_api={surface['min_supported_app_api_version']} "
          f"routes={len(surface['routes'])} capabilities={len(surface['capabilities'])} "
          f"extensions={len(surface['extensions'])} "
          f"app_api={len(app_api_paths(surface))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
