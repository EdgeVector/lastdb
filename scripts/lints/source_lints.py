#!/usr/bin/env python3
"""Run source lints across workspace crates in a single pass.

Replaces the legacy shell suite (tracing-egress, redaction, byte-slice,
spawn-instrument, no-hardcoded-urls, workspace-edition-inheritance, and
schema-core-statics) while discovering workspace crates dynamically from
Cargo.toml `[workspace].members`.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

EGRESS_RE = re.compile(r"reqwest::(?:Client|ClientBuilder)::(?:new|default|builder)\(\)")
SENSITIVE = "password|token|api_key|secret|auth_token|email|phone|ssn|user_hash|user_id"
REDACT_FIELD_RE = re.compile(rf"tracing::(?:info|warn|debug|error|trace)!.*?(?:{SENSITIVE})\s*=\s*[^,]")
REDACT_RHS_RE = re.compile(rf"(?:{SENSITIVE})\s*=[^,]*")
REDACT_CALL_RE = re.compile(r"redact(?:_id)?!\(")
ISOLATED_RE = re.compile(
    r"\b(?:filter|range_filter|hash_range_filter|record_key|key_value|payload)\b\s*=\s*(?:\{:\?\}|\?[A-Za-z_&])"
)
BYTE_SLICE_RE = re.compile(r"\[\.\.[^\]]*\.len\(\)\.min\([0-9]+\)\]")
SPAWN_ASYNC_RE = re.compile(r"tokio::spawn\(\s*async|^\s*async\s+(?:move\s+)?\{|^\s*async\s*\{", re.M)
SPAWN_OK_RE = re.compile(r"\.instrument\(|\.in_current_span\(\)|// lint:spawn-bare-ok")
CHAR_LIT_RE = re.compile(r"^'(?:\\.|[^'\\])'")
STATICS_RAW_RE = re.compile(r"static\s+mut|lazy_static")
STATICS_CODE_RE = re.compile(r"\b(?:OnceLock|LazyLock)\b")
ALLOW_URL_PREFIXES = (
    "lastdb_node/environments.json",
    "target/",
    "scripts/lints/source_lints.py",
    ".git/",
    ".claude/worktrees/",
)


def _strip_toml_comments(text: str) -> str:
    out: list[str] = []
    for line in text.splitlines():
        quote, escaped, cut = "", False, len(line)
        for idx, ch in enumerate(line):
            if escaped:
                escaped = False
            elif quote:
                escaped = ch == "\\" and quote == '"'
                if ch == quote and not escaped:
                    quote = ""
            elif ch in ('"', "'"):
                quote = ch
            elif ch == "#":
                cut = idx
                break
        out.append(line[:cut])
    return "\n".join(out)


def _workspace_members(manifest: Path) -> list[Path]:
    cleaned = _strip_toml_comments(manifest.read_text(encoding="utf-8"))
    ws = re.search(r"(?m)^\s*\[workspace\]\s*$", cleaned)
    if not ws:
        return []
    rest = cleaned[ws.end() :]
    nxt = re.search(r"(?m)^\s*\[", rest)
    mem = re.search(r"(?ms)^\s*members\s*=\s*\[(.*?)\]", rest[: nxt.start()] if nxt else rest)
    if not mem:
        return []
    base = manifest.parent
    members: list[Path] = []
    for q1, q2 in re.findall(r'"([^"]+)"|\'([^\']+)\'', mem.group(1)):
        entry = q1 or q2
        if "*" in entry or "?" in entry:
            members.extend(p for p in sorted(base.glob(entry)) if (p / "Cargo.toml").is_file())
        else:
            members.append(base / entry)
    return members


def _resolve_src_dirs(repo: Path, scopes: list[str]) -> list[Path]:
    if not scopes:
        members = _workspace_members(repo / "Cargo.toml")
        if not members:
            raise SystemExit("ERROR: no workspace members found in Cargo.toml")
        for m in members:
            if not (m / "src").is_dir():
                raise SystemExit(f"ERROR: workspace crate src missing: {m.relative_to(repo)}/src")
        return [m / "src" for m in members]
    src_dirs: list[Path] = []
    for raw in scopes:
        d = repo / raw
        if not d.is_dir():
            raise SystemExit(f"source-lints: scope directory not found: {raw}")
        nested = [d / "src"] if (d / "src").is_dir() else [p for p in sorted(d.glob("*/src")) if p.is_dir()]
        if not nested:
            raise SystemExit(f"source-lints: no {raw}/src or {raw}/*/src directories found")
        src_dirs.extend(nested)
    return src_dirs


def _iter_src_files(src_dirs: list[Path], repo: Path) -> list[tuple[str, list[str]]]:
    collected: list[tuple[str, list[str]]] = []
    for src_dir in src_dirs:
        for root, dirnames, filenames in os.walk(src_dir):
            dirnames[:] = sorted(d for d in dirnames if d not in ("target", "binaries"))
            for fname in sorted(filenames):
                fpath = Path(root) / fname
                collected.append((fpath.relative_to(repo).as_posix(), fpath.read_text("utf-8", "replace").splitlines()))
    return collected


def _spawn_call_text(lines: list[str], start: int, limit: int = 200) -> str:
    depth, opened, in_block, out = 0, False, False, []
    for line in lines[start : start + limit]:
        out.append(line)
        in_str, j, n = False, 0, len(line)
        while j < n:
            ch, nxt = line[j], (line[j + 1] if j + 1 < n else "")
            if in_block:
                if ch == "*" and nxt == "/":
                    in_block, j = False, j + 2
                else:
                    j += 1
            elif in_str:
                if ch == "\\":
                    j += 2
                else:
                    in_str = ch != '"'
                    j += 1
            elif ch == "/" and nxt == "/":
                break
            elif ch == "/" and nxt == "*":
                in_block, j = True, j + 2
            elif ch == '"':
                in_str, j = True, j + 1
            elif ch == "'" and CHAR_LIT_RE.match(line[j:]):
                j += CHAR_LIT_RE.match(line[j:]).end()  # type: ignore[union-attr]
            else:
                if ch == "(":
                    depth, opened = depth + 1, True
                elif ch == ")":
                    depth -= 1
                    if opened and depth == 0:
                        return "\n".join(out)
                j += 1
    return "\n".join(out)


def _has_override(lines: list[str], idx: int, marker: str) -> bool:
    return marker in lines[idx] or (idx > 0 and marker in lines[idx - 1])


def _scan_line(
    rel: str, lines: list[str], i: int, counts: list[int], errs: dict[str, list[str]]
) -> None:
    line, lineno = lines[i], i + 1
    if EGRESS_RE.search(line):
        counts[0] += 1
        if not any("// trace-egress:" in p for p in lines[max(0, i - 3) : i]):
            errs["egress"].append(
                f"ERROR: {rel}:{lineno} — reqwest::Client construction without // trace-egress: classifier in preceding 3 lines"
            )
    if REDACT_FIELD_RE.search(line) and not _has_override(lines, i, "lint:redaction-ok"):
        rhs = REDACT_RHS_RE.search(line)
        if not (rhs and REDACT_CALL_RE.search(rhs.group(0))):
            errs["redact"].append(
                f"ERROR: {rel}:{lineno} — sensitive field emitted without redact!() / redact_id!()\n    {line}"
            )
    if ISOLATED_RE.search(line) and not _has_override(lines, i, "lint:redaction-ok"):
        errs["redact"].append(
            f"ERROR: {rel}:{lineno} — query-filter value Debug-emitted without isolated-namespace redaction\n    {line}\n"
            "    Route it through fold_db::app_isolation::redact_debug_if_isolated(&schema_name, &filter)"
        )
    if BYTE_SLICE_RE.search(line) and not _has_override(lines, i, "lint:byte-slice-ok"):
        errs["slice"].append(f"ERROR: {rel}:{lineno} — UTF-8-unsafe byte-index slice on a string\n    {line}")
    if "tokio::spawn(" in line and not line.lstrip().startswith(("//", "*", "/*")):
        if SPAWN_ASYNC_RE.search("\n".join(lines[i : i + 3])):
            counts[1] += 1
            prev_ok = i > 0 and "// lint:spawn-bare-ok" in lines[i - 1]
            if not (prev_ok or SPAWN_OK_RE.search(_spawn_call_text(lines, i))):
                errs["spawn"].append(
                    f"ERROR: {rel}:{lineno} — tokio::spawn(async ...) without .instrument() / .in_current_span() / override inside the spawn call"
                )


def _check_src_trees(files: list[tuple[str, list[str]]], num_trees: int) -> bool:
    counts = [0, 0]
    errs: dict[str, list[str]] = {"egress": [], "redact": [], "slice": [], "spawn": []}
    for rel, lines in files:
        for i in range(len(lines)):
            _scan_line(rel, lines, i, counts, errs)
    summaries = (
        ("egress", f"lint-tracing-egress: ok — all {counts[0]} reqwest construction sites in {num_trees} src tree(s) are classified."),
        ("redact", f"lint-redaction: ok — no unredacted sensitive-field tracing call sites in {num_trees} src tree(s)."),
        ("slice", f"lint-byte-slice: ok — no UTF-8-unsafe byte-index slice sites in {num_trees} src tree(s)."),
        ("spawn", f"lint-spawn-instrument: ok — all {counts[1]} tokio::spawn(async) sites in {num_trees} src tree(s) are instrumented or explicitly marked."),
    )
    ok = True
    for key, summary in summaries:
        if errs[key]:
            ok = False
            for msg in errs[key]:
                print(msg, file=sys.stderr)
        else:
            print(summary)
    return ok


def _tracked_files(repo: Path) -> list[str]:
    proc = subprocess.run(["git", "-C", str(repo), "ls-files", "-z"], capture_output=True, check=False)
    if proc.returncode == 0:
        return [p.decode("utf-8", "replace") for p in proc.stdout.split(b"\0") if p]
    found: list[str] = []
    for root, dirnames, filenames in os.walk(repo):
        dirnames[:] = sorted(d for d in dirnames if d not in (".git", "target"))
        found.extend((Path(root) / f).relative_to(repo).as_posix() for f in sorted(filenames))
    return found


def _check_no_hardcoded_urls(repo: Path, tracked: list[str]) -> bool:
    env_data = json.loads((repo / "lastdb_node/environments.json").read_text(encoding="utf-8"))
    hosts = sorted({
        re.sub(r"^https?://", "", val).split("/", 1)[0]
        for env_map in env_data.get("environments", {}).values()
        for key, val in env_map.items()
        if key != "region" and isinstance(val, str) and val.startswith("http")
    })
    encoded = [(h, h.encode("utf-8")) for h in hosts]
    errors = 0
    for rel in tracked:
        if rel.startswith(ALLOW_URL_PREFIXES) or not (repo / rel).is_file():
            continue
        raw = (repo / rel).read_bytes()
        for host, hbytes in encoded:
            if hbytes in raw:
                errors += 1
                print(f"lint-no-hardcoded-urls: FAIL\n  ./{rel} contains hardcoded gateway hostname '{host}'.", file=sys.stderr)
                for idx, line in enumerate(raw.decode("utf-8", "replace").splitlines(), 1):
                    if host in line:
                        print(f"    {idx}:{line}", file=sys.stderr)
    if errors:
        return False
    print("lint-no-hardcoded-urls: ok")
    return True


def _check_edition_inheritance(repo: Path, tracked: list[str]) -> bool:
    ws_manifests = [
        repo / rel for rel in tracked
        if (rel == "Cargo.toml" or rel.endswith("/Cargo.toml"))
        and (repo / rel).is_file()
        and re.search(r"(?m)^\s*\[workspace\]", (repo / rel).read_text(encoding="utf-8"))
    ]
    if not ws_manifests:
        raise SystemExit("lint-workspace-edition-inheritance: no workspace manifests found")
    errors = checked = 0
    for ws_manifest in ws_manifests:
        for member_dir in _workspace_members(ws_manifest):
            manifest = member_dir / "Cargo.toml"
            rel = manifest.relative_to(repo).as_posix()
            text = _strip_toml_comments(manifest.read_text(encoding="utf-8"))
            checked += 1
            if re.search(r"(?m)^\s*edition\s*=", text):
                print(f"lint-workspace-edition-inheritance: FAIL\n  {rel} pins a literal Rust edition.", file=sys.stderr)
                errors += 1
            if not re.search(r"(?m)^\s*edition\.workspace\s*=\s*true\s*$", text):
                print(f"lint-workspace-edition-inheritance: FAIL\n  {rel} must declare edition.workspace = true.", file=sys.stderr)
                errors += 1
    if errors:
        return False
    print(f"lint-workspace-edition-inheritance: ok - {checked} workspace member manifest(s) inherit edition")
    return True


def _check_schema_core_statics(repo: Path) -> bool:
    hits: list[str] = []
    for path in sorted((repo / "schema_service/crates/core").rglob("*.rs")):
        rel = path.relative_to(repo).as_posix()
        for idx, line in enumerate(path.read_text("utf-8", "replace").splitlines(), 1):
            is_comment = line.lstrip().startswith(("//", "*", "/*"))
            if STATICS_RAW_RE.search(line) or (not is_comment and STATICS_CODE_RE.search(line)):
                hits.append(f"{rel}:{idx}:{line}")
    if hits:
        print("\n".join(hits), file=sys.stderr)
        print("FAIL: schema_service/crates/core must not use static mut, lazy_static, OnceLock, or LazyLock", file=sys.stderr)
        return False
    print("OK: schema_service/crates/core has no static mut or lazy_static")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", action="append", default=[], help="limit src/ checks to <dir>")
    parser.add_argument("--strict", action="store_true", help="compat flag (strict is always on)")
    parser.add_argument("--schema-core-statics", action="store_true", help="run only schema_core statics gate")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    if args.schema_core_statics:
        return 0 if _check_schema_core_statics(repo) else 1
    src_dirs = _resolve_src_dirs(repo, args.scope)
    ok = _check_src_trees(_iter_src_files(src_dirs, repo), len(src_dirs))
    if not args.scope:
        tracked = _tracked_files(repo)
        ok = _check_no_hardcoded_urls(repo, tracked) and ok
        ok = _check_edition_inheritance(repo, tracked) and ok
        ok = _check_schema_core_statics(repo) and ok
    if not ok:
        print("ERROR: one or more source lints failed", file=sys.stderr)
        return 1
    print("source lints: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
