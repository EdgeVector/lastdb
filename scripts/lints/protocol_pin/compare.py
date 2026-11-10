"""Compare a client protocol with the one the deployed service can accept."""

from protocol_pin.extract import Field, Schema, TypeDef, Variant


def incompatibilities(old, new, usage):
    """Wire changes that make the pinned service reject this client, or the reverse."""
    errors = []
    seen = set()
    for root in sorted(usage.request_roots):
        if new.get(root) is None:
            continue
        _request(old, new, usage, _ty(root), _ty(root), errors, seen, root)
    for root in sorted(usage.response_roots):
        if old.get(root) is None or new.get(root) is None:
            if old.get(root) is not None and new.get(root) is None:
                errors.append(f"removed response type {root}")
            continue
        _response(old, new, _ty(root), _ty(root), errors, seen, root)
    return _unique(errors)


def _request(old_s, new_s, usage, old_enc, new_enc, errors, seen, where):
    key = ("req", old_enc, new_enc)
    if key in seen:
        return
    seen.add(key)
    peeled = _peel(old_enc, new_enc, errors, where)
    if peeled is None:
        return
    old_enc, new_enc = peeled
    old_ty, old_bound = _named(old_s, old_enc)
    new_ty, new_bound = _named(new_s, new_enc)
    if new_ty is None:
        return
    if old_ty is None:
        errors.append(f"{where} uses {new_ty.path}, which the deployed service does not define")
        return
    _notes(old_ty, new_ty, old_bound, new_bound, errors, where)
    if old_ty.custom or new_ty.custom:
        if old_ty.custom != new_ty.custom:
            errors.append(f"{where} serialization changed ({new_ty.path})")
        return
    if old_ty.repr != new_ty.repr or old_ty.tag != new_ty.tag or old_ty.content != new_ty.content:
        errors.append(
            f"{where} wire representation changed from {old_ty.repr} to {new_ty.repr}"
        )
        return
    if _as_inner(old_ty, new_ty):
        _request(
            old_s, new_s, usage, old_ty.fields[0].encoded, new_ty.fields[0].encoded,
            errors, seen, where,
        )
        return
    if new_ty.kind == "enum":
        _request_enum(old_s, new_s, usage, old_ty, new_ty, errors, seen, where)
        return
    _request_fields(
        old_s, new_s, usage, old_ty, new_ty, new_ty.path, None, old_ty.deny, errors, seen, where
    )


def _response(old_s, new_s, old_enc, new_enc, errors, seen, where):
    key = ("res", old_enc, new_enc)
    if key in seen:
        return
    seen.add(key)
    peeled = _peel(old_enc, new_enc, errors, where)
    if peeled is None:
        return
    old_enc, new_enc = peeled
    old_ty, old_bound = _named(old_s, old_enc)
    new_ty, new_bound = _named(new_s, new_enc)
    if old_ty is None or new_ty is None:
        if old_ty is not None and new_ty is None:
            errors.append(f"{where} no longer accepts {old_ty.path}")
        return
    _notes(old_ty, new_ty, old_bound, new_bound, errors, where)
    if old_ty.custom or new_ty.custom:
        if old_ty.custom != new_ty.custom:
            errors.append(f"{where} serialization changed ({new_ty.path})")
        return
    if old_ty.repr != new_ty.repr or old_ty.tag != new_ty.tag or old_ty.content != new_ty.content:
        errors.append(
            f"{where} wire representation changed from {old_ty.repr} to {new_ty.repr}"
        )
        return
    if _as_inner(old_ty, new_ty):
        _response(
            old_s, new_s, old_ty.fields[0].encoded, new_ty.fields[0].encoded,
            errors, seen, where,
        )
        return
    if old_ty.kind == "enum":
        _response_enum(old_s, new_s, old_ty, new_ty, errors, seen, where)
        return
    _response_fields(old_s, new_s, old_ty, new_ty, new_ty.deny, errors, seen, where)


def _request_enum(old_s, new_s, usage, old_ty, new_ty, errors, seen, where):
    old_vars = {item.wire: item for item in old_ty.variants}
    for variant in new_ty.variants:
        if (new_ty.path, variant.name) not in usage.sent_variants:
            continue
        old_var = old_vars.get(variant.wire)
        if old_var is None:
            if not old_ty.other_ok:
                errors.append(
                    f"client sends {new_ty.path}::{variant.name}, "
                    "which the deployed service rejects"
                )
            continue
        _request_fields(
            old_s, new_s, usage, old_var, variant, new_ty.path, variant.name,
            old_ty.deny, errors, seen, f"{where}::{variant.name}",
        )


def _response_enum(old_s, new_s, old_ty, new_ty, errors, seen, where):
    new_vars = {item.wire: item for item in new_ty.variants}
    for variant in old_ty.variants:
        new_var = new_vars.get(variant.wire)
        if new_var is None:
            if not new_ty.other_ok:
                errors.append(
                    f"removed {old_ty.path}::{variant.name}; "
                    "the deployed service can still send it"
                )
            continue
        _response_fields(
            old_s, new_s, variant, new_var, new_ty.deny, errors, seen,
            f"{where}::{variant.name}",
        )


def _request_fields(old_s, new_s, usage, old_ty, new_ty, owner, variant, deny, errors, seen, where):
    old_fields = {item.wire: item for item in _flat(old_s, old_ty)}
    new_fields = {item.wire: item for item in _flat(new_s, new_ty)}
    for wire, field in new_fields.items():
        if not usage.emits(owner, variant, field):
            continue
        if wire not in old_fields:
            if deny:
                errors.append(
                    f"{where}.{field.wire} is serialized by the client and "
                    "the deployed type denies unknown fields"
                )
            continue
        _request(
            old_s, new_s, usage, old_fields[wire].encoded, field.encoded,
            errors, seen, f"{where}.{field.wire}",
        )
    for wire, field in old_fields.items():
        fresh = new_fields.get(wire)
        if field.default or field.skip == "never":
            continue
        sent = fresh is not None and usage.emits(owner, variant, fresh)
        if not sent:
            errors.append(f"{where} no longer sends required field {wire}")


def _response_fields(old_s, new_s, old_ty, new_ty, deny, errors, seen, where):
    old_fields = {item.wire: item for item in _flat(old_s, old_ty)}
    new_fields = {item.wire: item for item in _flat(new_s, new_ty)}
    for wire, field in old_fields.items():
        if field.skip == "never":
            continue
        fresh = new_fields.get(wire)
        if fresh is None:
            if deny:
                errors.append(
                    f"removed {where}.{wire}; the deployed service can still send it "
                    "and this client denies unknown fields"
                )
            continue
        _response(old_s, new_s, field.encoded, fresh.encoded, errors, seen, f"{where}.{wire}")
    for wire, field in new_fields.items():
        if wire in old_fields or field.default:
            continue
        errors.append(
            f"client requires {where}.{wire}, which the deployed service does not send"
        )


def _as_inner(old_ty, new_ty):
    return (
        old_ty.repr in {"transparent", "newtype"}
        and new_ty.repr in {"transparent", "newtype"}
        and len(old_ty.fields) == 1
        and len(new_ty.fields) == 1
    )


def _flat(schema, typedef):
    if not isinstance(typedef, TypeDef):
        return list(typedef.fields)
    found = []
    for item in typedef.fields:
        if not item.flatten:
            found.append(item)
            continue
        inner, _bound = _named(schema, item.encoded)
        if inner is not None and inner.kind == "struct":
            found.extend(inner.fields)
        else:
            found.append(item)
    return found


def _notes(old_ty, new_ty, old_bound, new_bound, errors, where):
    if old_ty.de_note != new_ty.de_note:
        errors.append(f"{where} deserialization changed ({new_ty.path})")
    if old_bound != new_bound:
        errors.append(
            f"{where} bound changed from {old_bound or 'none'} to {new_bound or 'none'}"
        )
    if old_ty.note != new_ty.note:
        errors.append(f"{where} scalar validation changed ({new_ty.path})")


def _peel(old_enc, new_enc, errors, where):
    while True:
        if old_enc == new_enc and old_enc.startswith("p:"):
            return None
        if _wrap(old_enc) and _wrap(old_enc) == _wrap(new_enc):
            old_enc, new_enc = _inner(old_enc), _inner(new_enc)
            continue
        if old_enc.startswith("map(") or new_enc.startswith("map("):
            if old_enc != new_enc:
                errors.append(f"{where} map shape changed")
            return None
        if old_enc.startswith("tup(") or new_enc.startswith("tup("):
            if old_enc != new_enc:
                errors.append(f"{where} tuple shape changed")
            return None
        if old_enc.startswith("p:") or new_enc.startswith("p:"):
            if old_enc != new_enc:
                errors.append(f"{where} shape changed from {old_enc} to {new_enc}")
            return None
        return old_enc, new_enc


def _wrap(encoded):
    for prefix in ("list(", "opt("):
        if encoded.startswith(prefix) and encoded.endswith(")"):
            return prefix
    return ""


def _inner(encoded):
    return encoded[encoded.find("(") + 1:-1]


def _named(schema, encoded):
    if not encoded.startswith("ty(") or not encoded.endswith(")"):
        return None, ""
    body = encoded[3:-1]
    path, separator, bound = body.partition(",")
    typedef = schema.get(path)
    if typedef is None and separator:
        typedef = schema.get(body)
        bound = ""
    return typedef, bound


def _ty(path):
    return f"ty({path})"


def _unique(errors):
    found = []
    for error in errors:
        if error not in found:
            found.append(error)
    return found


def self_check():
    """Fail the linter if the compatibility rules themselves drift."""
    base = _schema(_begin(["version"], deny=True), _enum(["Active"]))
    same = _schema(_begin(["version"], deny=True), _enum(["Active"]))
    added = _schema(_begin(["version", "extra"], deny=True), _enum(["Active"]))
    skipped = _schema(_begin(["version", "note"], deny=True, skip_last=True), _enum(["Active"]))
    variant = _schema(_begin(["version"], deny=True), _enum(["Active", "Extra"]))
    open_old = _schema(_begin(["version"], deny=False), _enum(["Active"]))
    open_new = _schema(
        _begin(["version", "extra"], deny=False, default_last=True), _enum(["Active"])
    )
    usage = _usage(sent_fields={("Begin", None, "extra"), ("Begin", None, "note")}, sent_variants={("State", "Extra")})
    quiet = _usage()
    checks = [
        (incompatibilities(base, same, usage) == [], "identical schemas"),
        (any("extra" in item for item in incompatibilities(base, added, usage)), "sent field"),
        (incompatibilities(open_old, open_new, usage) == [], "unknown field allowed"),
        (incompatibilities(base, skipped, quiet) == [], "skipped field"),
        (any("note" in item for item in incompatibilities(base, skipped, usage)), "populated skip field"),
        (any("required field extra" in item for item in incompatibilities(added, base, usage)), "removed field"),
        (any("Extra" in item for item in incompatibilities(base, variant, usage)), "sent variant"),
        (incompatibilities(base, variant, quiet) == [], "unsent variant"),
    ]
    failed = [label for ok, label in checks if not ok]
    if failed:
        raise SystemExit("protocol pin self-check failed: " + ", ".join(failed))


def _begin(names, deny, skip_last=False, default_last=False):
    fields = []
    for index, name in enumerate(names):
        last = index == len(names) - 1
        fields.append(Field(
            name=name, wire=name, encoded="p:string",
            skip="Option::is_none" if last and skip_last else None,
            default=last and (skip_last or default_last),
        ))
    return TypeDef(path="Begin", kind="struct", deny=deny, repr="struct", fields=fields)


def _enum(names):
    return TypeDef(
        path="State", kind="enum", deny=True, repr="external",
        variants=[Variant(name=name, wire=name.lower(), kind="unit") for name in names],
    )


def _schema(*types):
    schema = Schema(types={item.path: item for item in types}, files={})
    return schema


def _usage(sent_fields=(), sent_variants=()):
    from protocol_pin.usage import Usage

    usage = Usage()
    usage.sent_fields = set(sent_fields)
    usage.sent_variants = set(sent_variants)
    usage.request_roots = {"Begin", "State"}
    usage.response_roots = {"Begin", "State"}
    return usage
