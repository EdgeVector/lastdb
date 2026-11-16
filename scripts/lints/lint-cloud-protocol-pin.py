#!/usr/bin/env python3
"""Fail when this client would be rejected by the deployed storage service.

The service is `cloud_backup_protocol` pinned by EdgeVector/exemem-infra main's
`fold` gitlink. When the protocol tree SHA matches the pin, exit immediately.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))
from protocol_pin.wire import Field, Schema, TypeDef, Usage, norm_code  # noqa: E402

PIN_API = "https://api.github.com/repos/EdgeVector/exemem-infra/contents/fold?ref=main"
SERVICE_TARBALL_API = "https://api.github.com/repos/EdgeVector/exemem-infra/tarball/main"
FIX = (
    "Land and deploy the exemem-infra pin bump first, or gate the client "
    "so it does not send the incompatible field, variant, or shape."
)


def _git(repo: Path, *args: str, text: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=text)


def incompatibilities(old: Schema, new: Schema, usage: Usage, variant_live: Callable[[str, str, str], bool] | None = None) -> list[str]:
    """Wire changes that make the pinned service reject this client, or vice versa."""
    errors: list[str] = []
    seen: set[tuple[str, str, str]] = set()
    is_live = variant_live or (lambda _path, _name, _wire: True)
    for root in sorted(usage.request_roots):
        if new.get(root) is not None:
            _request(old, new, usage, f"ty({root})", f"ty({root})", errors, seen, root)
    for root in sorted(usage.response_roots):
        if old.get(root) is not None and new.get(root) is None:
            errors.append(f"removed response type {root}")
        elif old.get(root) is not None and new.get(root) is not None:
            _response(old, new, is_live, f"ty({root})", f"ty({root})", errors, seen, root)
    return list(dict.fromkeys(errors))


def _check_header(old_ty: TypeDef, new_ty: TypeDef, old_b: str, new_b: str, errors: list[str], where: str) -> bool:
    if old_ty.de_note != new_ty.de_note:
        errors.append(f"{where} deserialization changed ({new_ty.path})")
    if old_b != new_b:
        errors.append(f"{where} bound changed from {old_b or 'none'} to {new_b or 'none'}")
    if old_ty.note != new_ty.note:
        errors.append(f"{where} scalar validation changed ({new_ty.path})")
    if old_ty.custom or new_ty.custom:
        if old_ty.custom != new_ty.custom:
            errors.append(f"{where} serialization changed ({new_ty.path})")
        return False
    if (old_ty.repr, old_ty.tag, old_ty.content) != (new_ty.repr, new_ty.tag, new_ty.content):
        errors.append(f"{where} wire representation changed from {old_ty.repr} to {new_ty.repr}")
        return False
    return True


def _as_inner(old_ty: TypeDef, new_ty: TypeDef) -> bool:
    return old_ty.repr in {"transparent", "newtype"} and new_ty.repr in {"transparent", "newtype"} and len(old_ty.fields) == 1 and len(new_ty.fields) == 1


def _request(old_s: Schema, new_s: Schema, usage: Usage, old_enc: str, new_enc: str, errors: list[str], seen: set, where: str) -> None:
    if ("req", old_enc, new_enc) in seen:
        return
    seen.add(("req", old_enc, new_enc))
    peeled = _peel(old_enc, new_enc, errors, where)
    if peeled is None:
        return
    old_ty, old_b = _named(old_s, peeled[0])
    new_ty, new_b = _named(new_s, peeled[1])
    if new_ty is None:
        return
    if old_ty is None:
        errors.append(f"{where} uses {new_ty.path}, which the deployed service does not define")
        return
    if not _check_header(old_ty, new_ty, old_b, new_b, errors, where):
        return
    if _as_inner(old_ty, new_ty):
        _request(old_s, new_s, usage, old_ty.fields[0].encoded, new_ty.fields[0].encoded, errors, seen, where)
    elif new_ty.kind == "enum":
        old_vars = {v.wire: v for v in old_ty.variants}
        for v in new_ty.variants:
            if (new_ty.path, v.name) not in usage.sent_variants:
                continue
            ov = old_vars.get(v.wire)
            if ov is None and not old_ty.other_ok:
                errors.append(f"client sends {new_ty.path}::{v.name}, which the deployed service rejects")
            elif ov is not None:
                _request_fields(old_s, new_s, usage, ov, v, new_ty.path, v.name, old_ty.deny, errors, seen, f"{where}::{v.name}")
    else:
        _request_fields(old_s, new_s, usage, old_ty, new_ty, new_ty.path, None, old_ty.deny, errors, seen, where)


def _response(old_s: Schema, new_s: Schema, is_live: Callable[[str, str, str], bool], old_enc: str, new_enc: str, errors: list[str], seen: set, where: str) -> None:
    if ("res", old_enc, new_enc) in seen:
        return
    seen.add(("res", old_enc, new_enc))
    peeled = _peel(old_enc, new_enc, errors, where)
    if peeled is None:
        return
    old_ty, old_b = _named(old_s, peeled[0])
    new_ty, new_b = _named(new_s, peeled[1])
    if old_ty is None or new_ty is None:
        if old_ty is not None and new_ty is None:
            errors.append(f"{where} no longer accepts {old_ty.path}")
        return
    if not _check_header(old_ty, new_ty, old_b, new_b, errors, where):
        return
    if _as_inner(old_ty, new_ty):
        _response(old_s, new_s, is_live, old_ty.fields[0].encoded, new_ty.fields[0].encoded, errors, seen, where)
    elif old_ty.kind == "enum":
        new_vars = {v.wire: v for v in new_ty.variants}
        for v in old_ty.variants:
            nv = new_vars.get(v.wire)
            if nv is None and not new_ty.other_ok and is_live(old_ty.path, v.name, v.wire):
                errors.append(f"removed {old_ty.path}::{v.name}; the deployed service can still send it")
            elif nv is not None:
                _response_fields(old_s, new_s, is_live, v, nv, new_ty.deny, errors, seen, f"{where}::{v.name}")
    else:
        _response_fields(old_s, new_s, is_live, old_ty, new_ty, new_ty.deny, errors, seen, where)


def _flat(schema: Schema, holder) -> list[Field]:
    if not isinstance(holder, TypeDef):
        return list(holder.fields)
    found: list[Field] = []
    for item in holder.fields:
        inner, _ = _named(schema, item.encoded) if item.flatten else (None, "")
        found.extend(inner.fields if inner is not None and inner.kind == "struct" else [item])
    return found


def _request_fields(old_s: Schema, new_s: Schema, usage: Usage, old_ty, new_ty, owner: str, variant: str | None, deny: bool, errors: list[str], seen: set, where: str) -> None:
    old_f = {f.wire: f for f in _flat(old_s, old_ty)}
    new_f = {f.wire: f for f in _flat(new_s, new_ty)}
    for wire, fld in new_f.items():
        if not usage.emits(owner, variant, fld):
            continue
        if wire not in old_f:
            if deny:
                errors.append(f"{where}.{fld.wire} is serialized by the client and the deployed type denies unknown fields")
        else:
            _request(old_s, new_s, usage, old_f[wire].encoded, fld.encoded, errors, seen, f"{where}.{fld.wire}")
    for wire, fld in old_f.items():
        if not (fld.default or fld.skip == "never"):
            fresh = new_f.get(wire)
            if fresh is None or not usage.emits(owner, variant, fresh):
                errors.append(f"{where} no longer sends required field {wire}")


def _response_fields(old_s: Schema, new_s: Schema, is_live: Callable[[str, str, str], bool], old_ty, new_ty, deny: bool, errors: list[str], seen: set, where: str) -> None:
    old_f = {f.wire: f for f in _flat(old_s, old_ty)}
    new_f = {f.wire: f for f in _flat(new_s, new_ty)}
    for wire, fld in old_f.items():
        if fld.skip == "never":
            continue
        fresh = new_f.get(wire)
        if fresh is None:
            if deny:
                errors.append(f"removed {where}.{wire}; the deployed service can still send it and this client denies unknown fields")
        else:
            _response(old_s, new_s, is_live, fld.encoded, fresh.encoded, errors, seen, f"{where}.{wire}")
    for wire, fld in new_f.items():
        if wire not in old_f and not fld.default:
            errors.append(f"client requires {where}.{wire}, which the deployed service does not send")


def _peel(old_enc: str, new_enc: str, errors: list[str], where: str) -> tuple[str, str] | None:
    while True:
        if old_enc == new_enc and old_enc.startswith("p:"):
            return None
        w_old = next((p for p in ("list(", "opt(") if old_enc.startswith(p) and old_enc.endswith(")")), "")
        w_new = next((p for p in ("list(", "opt(") if new_enc.startswith(p) and new_enc.endswith(")")), "")
        if w_old and w_old == w_new:
            old_enc, new_enc = old_enc[len(w_old) : -1], new_enc[len(w_new) : -1]
            continue
        for prefix, label in (("map(", "map shape changed"), ("tup(", "tuple shape changed")):
            if old_enc.startswith(prefix) or new_enc.startswith(prefix):
                if old_enc != new_enc:
                    errors.append(f"{where} {label}")
                return None
        if old_enc.startswith("p:") or new_enc.startswith("p:"):
            if old_enc != new_enc:
                errors.append(f"{where} shape changed from {old_enc} to {new_enc}")
            return None
        return old_enc, new_enc


def _named(schema: Schema, encoded: str) -> tuple[TypeDef | None, str]:
    if not (encoded.startswith("ty(") and encoded.endswith(")")):
        return None, ""
    body = encoded[3:-1]
    path, sep, bound = body.partition(",")
    td = schema.get(path)
    if td is None and sep:
        td, bound = schema.get(body), ""
    return td, bound


def _make_variant_live(pinned: Schema, load_service: Callable[[], list[str] | None]) -> Callable[[str, str, str], bool]:
    from protocol_pin.ast_scan import proto_non_self_sources
    svc_cache: list[list[str] | None] = []
    proto_cache: dict[str, list[str]] = {}

    def check(enum_path: str, vname: str, vwire: str) -> bool:
        pat = re.compile(rf"\b{re.escape(vname)}\b|\"{re.escape(vwire)}\"")
        ename = enum_path.split("::")[-1]
        if any(pat.search(src) for src in proto_cache.setdefault(ename, proto_non_self_sources(pinned, ename))):
            return True
        if not svc_cache:
            svc_cache.append(load_service())
        return True if not svc_cache[0] else any(pat.search(src) for src in svc_cache[0])

    return check


def main() -> None:
    argparse.ArgumentParser(description=__doc__).parse_args()
    repo = Path(__file__).resolve().parents[2]
    sha = pin_sha()
    if not (re.fullmatch(r"[0-9a-f]{40}", sha) and _git(repo, "cat-file", "-e", f"{sha}^{{commit}}").returncode == 0):
        fetch = _git(repo, "fetch", "--depth", "1", "origin", sha)
        if fetch.returncode != 0:
            raise SystemExit(f"could not fetch pinned lastdb commit {sha}: {fetch.stderr.strip()}")
    pin_proc = _git(repo, "rev-parse", "--verify", f"{sha}:cloud_backup_protocol")
    if pin_proc.returncode != 0:
        raise SystemExit(f"pinned commit {sha} has no cloud_backup_protocol tree")
    pin_tree = pin_proc.stdout.strip()

    clean = _git(repo, "status", "--porcelain", "--", "cloud_backup_protocol")
    if clean.returncode == 0 and not clean.stdout.strip():
        head_proc = _git(repo, "rev-parse", "--verify", "HEAD:cloud_backup_protocol")
        if head_proc.returncode == 0 and head_proc.stdout.strip() == pin_tree:
            print(f"cloud-protocol-pin: cloud_backup_protocol tree matches pinned {sha} ({pin_tree})")
            return

    from protocol_pin.ast_scan import collect_usage, load_protocol

    head = load_protocol(repo)
    with tempfile.TemporaryDirectory(prefix="protocol-pin-") as tmp:
        arc = _git(repo, "archive", sha, "cloud_backup_protocol", text=False)
        if arc.returncode != 0:
            raise SystemExit(f"pinned commit {sha} has no cloud_backup_protocol tree")
        subprocess.run(["tar", "-x", "-C", tmp], input=arc.stdout, check=True)
        pinned = load_protocol(Path(tmp))
        errors = incompatibilities(pinned, head, collect_usage(repo, head), _make_variant_live(pinned, _remote_service_sources))
    _report(sha, errors)


def _report(sha: str, errors: list[str]) -> None:
    if errors:
        print(f"cloud_backup_protocol is incompatible with the storage service\npinned by exemem-infra main (fold @{sha}).\n")
        for error in errors:
            print(f"- {error}")
        print(f"\n{FIX}")
        sys.exit(1)
    print(f"cloud_backup_protocol matches the storage service pinned by exemem-infra main (fold @{sha}).")


def _gh_tokens() -> list[str]:
    seen: list[str] = []
    for name in ("GITHUB_TOKEN", "GH_TOKEN", "GH_PAT"):
        val = os.environ.get(name, "").strip()
        if val and val not in seen:
            seen.append(val)
    return [*seen, ""]


def _gh_open(url: str, token: str):
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "lastdb-protocol-pin"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30)


def _remote_service_sources() -> list[str] | None:
    for token in _gh_tokens():
        try:
            with _gh_open(SERVICE_TARBALL_API, token) as resp, tarfile.open(fileobj=io.BytesIO(resp.read()), mode="r:gz") as tar:
                sources = [norm_code(f.read().decode("utf-8")) for m in tar.getmembers() if m.isfile() and "/exemem_service/" in m.name and m.name.endswith(".rs") and (f := tar.extractfile(m)) is not None]
            if sources:
                return sources
        except (OSError, tarfile.TarError, UnicodeDecodeError):
            continue
    return None


def pin_sha() -> str:
    last = "no response"
    for token in _gh_tokens():
        try:
            with _gh_open(PIN_API, token) as resp:
                payload = json.load(resp)
        except urllib.error.HTTPError as exc:
            last = f"HTTP {exc.code}"
            continue
        except urllib.error.URLError as exc:
            last = str(exc.reason)
            continue
        if payload.get("type") == "submodule" and payload.get("sha"):
            return payload["sha"]
        last = f"unexpected type {payload.get('type')}"
    raise SystemExit(
        f"Could not read EdgeVector/exemem-infra main's fold gitlink ({last}). "
        "The storage-service pin is that submodule. This check fails closed until the pin can be read."
    )


if __name__ == "__main__":
    main()
