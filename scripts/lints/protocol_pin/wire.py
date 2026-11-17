"""Serde wire model, Rust type encoding, and skip-predicate helpers."""
from __future__ import annotations

from dataclasses import dataclass, field
import re

PRIMITIVES = {"bool": "bool", "char": "string", "str": "string", "String": "string", "f32": "float", "f64": "float"}
for _t in ("u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32", "i64", "i128", "isize"):
    PRIMITIVES[_t] = "integer"
SKIP_AT_EMPTY = ("is_none", "is_empty", "is_default")


@dataclass
class Field:
    name: str
    wire: str
    encoded: str
    skip: str | None = None
    default: bool = False
    flatten: bool = False


@dataclass
class Variant:
    name: str
    wire: str
    fields: list[Field] = field(default_factory=list)
    other: bool = False


@dataclass
class TypeDef:
    path: str
    kind: str
    deny: bool = False
    repr: str = "struct"
    tag: str | None = None
    content: str | None = None
    fields: list[Field] = field(default_factory=list)
    variants: list[Variant] = field(default_factory=list)
    alias: str | None = None
    custom: str | None = None
    de_note: str | None = None
    other_ok: bool = False
    note: str = ""


@dataclass
class Schema:
    types: dict[str, TypeDef]
    files: dict[str, str]
    by_name: dict[str, str] = field(default_factory=dict)
    exports: dict[str, dict[str, str]] = field(default_factory=dict)

    def canonical(self, path: str) -> str:
        seen: set[str] = set()
        while path in self.types and self.types[path].kind == "alias" and path not in seen:
            seen.add(path)
            path = self.types[path].alias or path
        return path

    def get(self, path: str) -> TypeDef | None:
        return self.types.get(self.canonical(path))


class Usage:
    def __init__(self) -> None:
        self.sent_fields: set[tuple[str, str | None, str]] = set()
        self.sent_variants: set[tuple[str, str]] = set()
        self.request_roots: set[str] = set()
        self.response_roots: set[str] = set()

    def emits(self, path: str, variant: str | None, fld: Field) -> bool:
        return False if fld.skip == "never" else (True if not fld.skip else (path, variant, fld.name) in self.sent_fields)


def norm_code(text: str) -> str:
    return " ".join(re.sub(r"//[^\n]*", " ", re.sub(r"/\*.*?\*/", " ", text, flags=re.S)).split())


def rename_ident(name: str, style: str | None) -> str:
    if not style or style == "PascalCase":
        return name
    snake = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)).lower()
    parts = [p for p in snake.split("_") if p]
    styles = {
        "snake_case": "_".join(parts),
        "camelCase": parts[0] + "".join(p.title() for p in parts[1:]) if parts else name,
        "lowercase": snake.replace("_", ""),
        "UPPERCASE": snake.replace("_", "").upper(),
        "SCREAMING_SNAKE_CASE": "_".join(parts).upper(),
        "kebab-case": "-".join(parts),
    }
    return styles.get(style, name)


def top_items(inner: str) -> list[str]:
    items, buf, depth = [], [], 0
    for ch in inner:
        depth = depth + 1 if ch in "(<" else (max(0, depth - 1) if ch in ")>" else depth)
        if ch == "," and depth == 0:
            items.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if "".join(buf).strip():
        items.append("".join(buf).strip())
    return items


def merge_serde(texts: list[str]) -> dict:
    flags = {
        "deny": False, "transparent": False, "untagged": False, "other": False,
        "default": False, "skip": None, "flatten": False, "rename": None,
        "rename_all": None, "tag": None, "content": None,
    }
    bool_map = {"deny_unknown_fields": "deny", "transparent": "transparent", "untagged": "untagged", "other": "other", "flatten": "flatten"}
    kv_map = {"rename_all": "rename_all", "rename": "rename", "tag": "tag", "content": "content", "skip_serializing_if": "skip"}
    for text in texts:
        m = re.search(r"#\s*\[\s*serde\s*\((.*)\)\s*\]", text, flags=re.S) if "serde" in text else None
        for item in top_items(m.group(1)) if m else ():
            if item in bool_map:
                flags[bool_map[item]] = True
            elif item in ("skip", "skip_serializing"):
                flags["skip"] = "never"
            elif item == "default" or item.startswith("default "):
                flags["default"] = True
            elif "=" in item:
                k, v = (s.strip().strip('"') for s in item.split("=", 1))
                if k in kv_map:
                    flags[kv_map[k]] = v
                elif k == "default":
                    flags["default"] = True
    return flags


def empty_for_skip(expr: str, predicate: str | None) -> bool:
    if not predicate or predicate == "never":
        return False
    text, tail = " ".join(expr.split()), predicate.split("::")[-1]
    if tail not in SKIP_AT_EMPTY and not predicate.endswith(SKIP_AT_EMPTY):
        return False
    if tail == "is_none" or predicate.endswith("is_none"):
        return text == "None"
    if tail == "is_empty" or predicate.endswith("is_empty"):
        return text in {"Vec::new()", "vec![]", "String::new()", '""'}
    return text in {"Default::default()", "None", "Vec::new()", "vec![]", '""', "0", "false"}


def encode(text: str, by_name: dict[str, str], consts: dict[str, str]) -> str:
    text = " ".join(text.split())
    while text.startswith("&"):
        text = re.sub(r"^'[A-Za-z0-9_]+\s+", "", text[1:].strip())
    if text.startswith("(") and text.endswith(")"):
        inner = text[1:-1].strip()
        return "p:null" if not inner else "tup(" + ",".join(encode(p, by_name, consts) for p in top_items(inner)) + ")"
    base, args = (text.split("<", 1)[0].strip(), top_items(text.split("<", 1)[1][:-1])) if "<" in text and text.endswith(">") else (text, [])
    ea = [encode(a, by_name, consts) for a in args]
    if base in {"Vec", "BTreeSet", "HashSet", "BinaryHeap"} and ea:
        return f"list({ea[0]})"
    if base == "Option" and ea:
        return f"opt({ea[0]})"
    if base in {"Box", "Arc", "Rc", "Cow"} and ea:
        return ea[0]
    if base in {"BTreeMap", "HashMap"} and len(ea) == 2:
        return f"map({ea[0]},{ea[1]})"
    if base.isdigit():
        return "const:" + base
    if base in PRIMITIVES:
        return f"p:{PRIMITIVES[base]}"
    if base == "Value" or base.endswith("::Value"):
        return "p:json"
    b0 = ea[0] if ea else ""
    bound = b0[2:] if b0.startswith("p:") else (b0[6:] if b0.startswith("const:") else b0)
    leaf = base.split("::")[-1]
    if leaf in consts and leaf not in by_name:
        lit = consts[leaf].strip()
        return "const:" + (lit if lit.isdigit() else norm_code(lit))
    return (f"ty({by_name[leaf]},{bound})" if bound else f"ty({by_name[leaf]})") if leaf in by_name else f"unk({text})"


def materialize(rec: dict, by_name: dict[str, str], consts: dict[str, str]) -> TypeDef:
    flags = rec["flags"]
    if rec["kind"] == "type_item":
        enc = encode(rec["alias"], by_name, consts)
        return TypeDef(path=rec["path"], kind="alias", alias=enc[3:-1].split(",")[0] if enc.startswith("ty(") and enc.endswith(")") else enc, repr="alias")
    ef = lambda r: Field(name=r["name"], wire=r["wire"], encoded=encode(r["ty"], by_name, consts), skip=r["flags"]["skip"], default=r["flags"]["default"], flatten=r["flags"]["flatten"])
    fields = [ef(r) for r in rec["raw_fields"]]
    variants = [Variant(name=r["name"], wire=r["wire"], other=r["other"], fields=[ef(i) for i in r["fields"]]) for r in rec["raw_variants"]]
    kind = "enum" if rec["kind"] == "enum_item" else ("scalar" if rec["kind"] == "macro" else "struct")
    rname = ("transparent" if flags["transparent"] else "untagged" if flags["untagged"] else "adjacent" if flags["tag"] and flags["content"] else "internal" if flags["tag"] else "external" if kind == "enum" else "string" if kind == "scalar" else "newtype" if len(fields) == 1 and fields[0].name.startswith("_") else "tuple" if fields and all(f.name.startswith("_") for f in fields) else "unit" if not fields and kind == "struct" else "struct")
    return TypeDef(path=rec["path"], kind=kind, deny=flags["deny"], repr=rname, tag=flags["tag"], content=flags["content"], fields=fields, variants=variants, other_ok=any(v.other for v in variants), note=rec.get("note", ""))


def build_exports(modules: dict[str, dict], by_name: dict[str, str]) -> dict[str, dict[str, str]]:
    cache: dict[str, dict[str, str]] = {}

    def exports(path: str, stack: set[str]) -> dict[str, str]:
        if path in cache or path in stack:
            return cache.get(path, {})
        stack.add(path)
        mod = modules[path]
        names = {n: r["path"] for n, r in mod["defs"].items() if r["public"] and (r["wire"] or n in mod["impls"])}
        for use in mod["uses"]:
            for local, segs in use["names"] if use["public"] else ():
                if local == "*":
                    cur: str | None = "" if segs and segs[0] == "crate" else ((path.rsplit("::", 1)[0] if "::" in path else "") if segs and segs[0] == "super" else path)
                    for seg in segs[1 if segs and segs[0] in {"crate", "super", "self"} else 0 :]:
                        sub = modules.get(cur or "", {}).get("subs", {}).get(seg)
                        cur = sub["path"] if sub else None
                        if cur is None:
                            break
                    for sname, spath in (exports(cur, stack).items() if cur is not None else ()):
                        names.setdefault(sname, spath)
                elif (segs[-1] if segs else local) in by_name:
                    names[local] = by_name[segs[-1] if segs else local]
        stack.remove(path)
        cache[path] = names
        return names

    return {p: exports(p, set()) for p in modules}
