"""Turn parsed Rust items into comparable wire encodings."""

from protocol_pin.extract import Field, TypeDef, Variant
from protocol_pin.rust import PRIMITIVES, norm_code, split_generic, strip_ref


def build_exports(modules, by_name):
    """Public names of each module, including `pub use` re-exports."""
    cache = {}

    def walk(viewer, segments):
        current = viewer
        index = 0
        if segments and segments[0] in {"crate", "super", "self"}:
            if segments[0] == "crate":
                current = ""
            elif segments[0] == "super":
                current = viewer.rsplit("::", 1)[0] if "::" in viewer else ""
            index = 1
        for name in segments[index:]:
            sub = modules.get(current, {}).get("subs", {}).get(name)
            if sub is None:
                return None
            current = sub["path"]
        return current

    def exports(path, stack):
        if path in cache:
            return cache[path]
        if path in stack:
            return {}
        stack.add(path)
        module = modules[path]
        names = {}
        for name, record in module["defs"].items():
            if record["public"] and (record["wire"] or name in module["impls"]):
                names[name] = record["path"]
        for use in module["uses"]:
            if not use["public"]:
                continue
            for local, segments in use["names"]:
                if local == "*":
                    target = walk(path, segments)
                    if target is not None:
                        for star_name, star_path in exports(target, stack).items():
                            names.setdefault(star_name, star_path)
                    continue
                leaf = segments[-1] if segments else local
                if leaf in by_name:
                    names[local] = by_name[leaf]
        stack.remove(path)
        cache[path] = names
        return names

    return {path: exports(path, set()) for path in modules}


def materialize(record, module, by_name, consts):
    flags = record["flags"]
    if record["kind"] == "type_item":
        encoded = encode(record["alias"], by_name, consts)
        return TypeDef(
            path=record["path"], kind="alias", alias=_alias_target(encoded),
            alias_of=encoded, repr="alias",
        )
    fields = [_encode_field(raw, by_name, consts) for raw in record["raw_fields"]]
    variants = [
        Variant(
            name=raw["name"], wire=raw["wire"], kind=raw["kind"], other=raw["other"],
            fields=[_encode_field(item, by_name, consts) for item in raw["fields"]],
        )
        for raw in record["raw_variants"]
    ]
    kind = "enum" if record["kind"] == "enum_item" else "struct"
    if record["kind"] == "macro":
        kind = "scalar"
    repr_name = _repr(flags, fields, kind)
    return TypeDef(
        path=record["path"],
        kind=kind,
        deny=flags["deny"],
        transparent=flags["transparent"] or repr_name in {"transparent", "newtype"},
        repr=repr_name,
        tag=flags["tag"],
        content=flags["content"],
        fields=fields,
        variants=variants,
        other_ok=any(item.other for item in variants),
        note=record.get("note", ""),
    )


def _repr(flags, fields, kind):
    if flags["transparent"]:
        return "transparent"
    if flags["untagged"]:
        return "untagged"
    if flags["tag"] and flags["content"]:
        return "adjacent"
    if flags["tag"]:
        return "internal"
    if kind == "enum":
        return "external"
    if kind == "scalar":
        return "string"
    if len(fields) == 1 and fields[0].name.startswith("_"):
        return "newtype"
    if fields and all(field.name.startswith("_") for field in fields):
        return "tuple"
    if not fields and kind == "struct":
        return "unit"
    return "struct"


def _encode_field(raw, by_name, consts):
    flags = raw["flags"]
    return Field(
        name=raw["name"],
        wire=raw["wire"],
        encoded=encode(raw["ty"], by_name, consts),
        skip=flags["skip"],
        default=flags["default"],
        flatten=flags["flatten"],
    )


def _alias_target(encoded):
    if encoded.startswith("ty(") and encoded.endswith(")"):
        return encoded[3:-1].split(",")[0]
    return encoded


def encode(text, by_name, consts):
    """Encode a Rust type as a wire shape. Short names are unique in this crate."""
    text = strip_ref(text)
    if text.startswith("(") and text.endswith(")"):
        return _tuple(text, by_name, consts)
    base, args = split_generic(text)
    encoded_args = [encode(arg, by_name, consts) for arg in args]
    wrapped = _wrapped(base, encoded_args)
    if wrapped is not None:
        return wrapped
    if base.isdigit():
        return "const:" + base
    if base in PRIMITIVES:
        return f"p:{PRIMITIVES[base]}"
    if base == "Value" or base.endswith("::Value"):
        return "p:json"
    bound = _bound(encoded_args)
    leaf = base.split("::")[-1]
    if leaf in consts and leaf not in by_name:
        literal = consts[leaf].strip()
        return "const:" + (literal if literal.isdigit() else norm_code(literal))
    if leaf in by_name:
        path = by_name[leaf]
        return f"ty({path},{bound})" if bound else f"ty({path})"
    return "unk(" + text + ")"


def _tuple(text, by_name, consts):
    inner = text[1:-1].strip()
    if not inner:
        return "p:null"
    parts = [encode(part, by_name, consts) for part in split_generic(f"T<{inner}>")[1]]
    return "tup(" + ",".join(parts) + ")"


def _wrapped(base, encoded_args):
    if base in {"Vec", "BTreeSet", "HashSet", "BinaryHeap"} and encoded_args:
        return f"list({encoded_args[0]})"
    if base == "Option" and encoded_args:
        return f"opt({encoded_args[0]})"
    if base in {"Box", "Arc", "Rc", "Cow"} and encoded_args:
        return encoded_args[0]
    if base in {"BTreeMap", "HashMap"} and len(encoded_args) == 2:
        return f"map({encoded_args[0]},{encoded_args[1]})"
    return None


def _bound(encoded_args):
    if not encoded_args:
        return ""
    bound = encoded_args[0]
    if bound.startswith("p:"):
        return bound[2:]
    if bound.startswith("const:"):
        return bound[len("const:"):]
    return bound
