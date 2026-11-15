"""Compare a client protocol with the one the deployed service can accept."""
from __future__ import annotations

from protocol_pin.wire import Field, Schema, TypeDef, Usage


def incompatibilities(old: Schema, new: Schema, usage: Usage) -> list[str]:
    """Wire changes that make the pinned service reject this client, or vice versa."""
    errors: list[str] = []
    seen: set[tuple[str, str, str]] = set()
    for root in sorted(usage.request_roots):
        if new.get(root) is not None:
            _request(old, new, usage, f"ty({root})", f"ty({root})", errors, seen, root)
    for root in sorted(usage.response_roots):
        if old.get(root) is not None and new.get(root) is None:
            errors.append(f"removed response type {root}")
        elif old.get(root) is not None and new.get(root) is not None:
            _response(old, new, f"ty({root})", f"ty({root})", errors, seen, root)
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


def _response(old_s: Schema, new_s: Schema, old_enc: str, new_enc: str, errors: list[str], seen: set, where: str) -> None:
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
        _response(old_s, new_s, old_ty.fields[0].encoded, new_ty.fields[0].encoded, errors, seen, where)
    elif old_ty.kind == "enum":
        new_vars = {v.wire: v for v in new_ty.variants}
        for v in old_ty.variants:
            nv = new_vars.get(v.wire)
            if nv is None and not new_ty.other_ok:
                errors.append(f"removed {old_ty.path}::{v.name}; the deployed service can still send it")
            elif nv is not None:
                _response_fields(old_s, new_s, v, nv, new_ty.deny, errors, seen, f"{where}::{v.name}")
    else:
        _response_fields(old_s, new_s, old_ty, new_ty, new_ty.deny, errors, seen, where)


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


def _response_fields(old_s: Schema, new_s: Schema, old_ty, new_ty, deny: bool, errors: list[str], seen: set, where: str) -> None:
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
            _response(old_s, new_s, fld.encoded, fresh.encoded, errors, seen, f"{where}.{wire}")
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
