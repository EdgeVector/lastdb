"""Load the serde wire model of one cloud_backup_protocol tree."""

from dataclasses import dataclass, field
from pathlib import Path

import tree_sitter_rust as ts_rust
from tree_sitter import Language, Parser

from protocol_pin.rust import derive_serde, merge_serde, norm_code, rename_ident

PARSER = Parser(Language(ts_rust.language()))


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
    kind: str
    fields: list = field(default_factory=list)
    other: bool = False


@dataclass
class TypeDef:
    path: str
    kind: str
    deny: bool = False
    transparent: bool = False
    repr: str = "struct"
    tag: str | None = None
    content: str | None = None
    fields: list = field(default_factory=list)
    variants: list = field(default_factory=list)
    alias: str | None = None
    alias_of: str | None = None
    custom: str | None = None
    de_note: str | None = None
    other_ok: bool = False
    note: str = ""


@dataclass
class Schema:
    types: dict
    files: dict

    def canonical(self, path):
        seen = set()
        while path in self.types and self.types[path].kind == "alias":
            if path in seen:
                break
            seen.add(path)
            path = self.types[path].alias
        return path

    def get(self, path):
        path = self.canonical(path)
        return self.types.get(path)


def load_protocol(root):
    """Parse cloud_backup_protocol under `root` (a repo root or the crate dir)."""
    root = Path(root)
    crate = root / "cloud_backup_protocol" if (root / "cloud_backup_protocol").is_dir() else root
    src = crate / "src" / "lib.rs"
    if not src.is_file():
        raise SystemExit(f"cloud_backup_protocol source not found under {root}")
    modules = {}
    _read_module(modules, "", src)
    return _link(modules)


def _read_module(modules, path, file):
    source = file.read_bytes()
    tree = PARSER.parse(source)
    module = {
        "path": path,
        "file": str(file),
        "source": source,
        "subs": {},
        "defs": {},
        "consts": {},
        "uses": [],
        "impls": {},
    }
    modules[path] = module
    pending = []
    for node in tree.root_node.children:
        if node.type == "attribute_item":
            pending.append(node)
            continue
        if node.type in {"line_comment", "block_comment"}:
            continue
        if node.type == "mod_item":
            _take_mod(modules, module, node, file)
        elif node.type == "use_declaration":
            module["uses"].append(_use_record(node))
        elif node.type == "const_item":
            name = node.child_by_field_name("name")
            value = node.child_by_field_name("value")
            if name is not None:
                module["consts"][name.text.decode()] = value.text.decode() if value else ""
        elif node.type in {"struct_item", "enum_item", "type_item"}:
            _take_type(module, node, pending)
        elif node.type == "impl_item":
            _take_impl(module, node)
        elif node.type == "expression_statement":
            _take_macro(module, node)
        pending = []


def _take_mod(modules, module, node, file):
    name = node.child_by_field_name("name").text.decode()
    body = node.child_by_field_name("body")
    child = f"{module['path']}::{name}" if module["path"] else name
    public = _vis(node) == "pub"
    module["subs"][name] = {"path": child, "public": public}
    if body is not None:
        return
    base = file.parent if file.name in {"lib.rs", "mod.rs"} else file.parent / file.stem
    candidate = base / f"{name}.rs"
    nested = base / name / "mod.rs"
    target = candidate if candidate.is_file() else nested
    if target.is_file():
        _read_module(modules, child, target)


def _vis(node):
    vis = node.child_by_field_name("visibility")
    if vis is None:
        for child in node.children:
            if child.type == "visibility_modifier":
                vis = child
                break
    return " ".join(vis.text.decode().split()) if vis is not None else ""


def _use_record(node):
    public = _vis(node) == "pub"
    names = []

    def walk(item, prefix):
        kind = item.type
        if kind == "scoped_use_list":
            head = [child for child in item.children if child.type not in {":", "::"}]
            path = _path_of(head[0]) if head else []
            for child in head[1:]:
                walk(child, path)
        elif kind == "use_list":
            for child in item.children:
                if child.type not in {",", "{", "}"}:
                    walk(child, prefix)
        elif kind == "use_wildcard":
            path = _path_of(item)
            names.append(("*", prefix + path[:-1] if path and path[-1] == "*" else prefix + path))
        elif kind == "use_as_clause":
            ids = [child.text.decode() for child in item.children if child.type == "identifier"]
            if ids:
                names.append((ids[-1], prefix + [ids[0]]))
        elif kind == "scoped_identifier":
            path = _path_of(item)
            names.append((path[-1], prefix + path))
        elif kind == "identifier":
            ident = item.text.decode()
            names.append((ident, prefix + [ident]))
        elif kind == "crate" or kind == "super" or kind == "self":
            names.append((item.text.decode(), prefix + [item.text.decode()]))

    for child in node.children:
        if child.type not in {"use", ";", "visibility_modifier"}:
            walk(child, [])
    return {"public": public, "names": names}


def _path_of(node):
    if node.type in {"identifier", "crate", "super", "self"}:
        return [node.text.decode()]
    if node.type == "scoped_identifier":
        parts = []
        for child in node.children:
            if child.type == "scoped_identifier":
                parts.extend(_path_of(child))
            elif child.type in {"identifier", "crate", "super", "self", "type_identifier"}:
                parts.append(child.text.decode())
        return parts
    if node.type == "use_wildcard":
        parts = []
        for child in node.children:
            if child.type in {"identifier", "scoped_identifier", "crate", "super", "self"}:
                parts.extend(_path_of(child))
            elif child.text.decode() == "*":
                parts.append("*")
        return parts
    return [node.text.decode()]


def _attr_texts(nodes):
    return [node.text.decode() for node in nodes]


def _take_type(module, node, pending):
    if any("cfg(test)" in text.text.decode() for text in pending):
        return
    name = node.child_by_field_name("name").text.decode()
    texts = _attr_texts(pending)
    flags = merge_serde(texts)
    path = f"{module['path']}::{name}" if module["path"] else name
    record = {
        "path": path,
        "name": name,
        "public": _vis(node) == "pub",
        "internal": _vis(node).startswith("pub") or _vis(node) == "",
        "kind": node.type,
        "flags": flags,
        "wire": derive_serde(texts),
        "raw_fields": [],
        "raw_variants": [],
        "alias": None,
    }
    if node.type == "type_item":
        record["alias"] = node.child_by_field_name("type").text.decode()
        record["wire"] = True
    elif node.type == "struct_item":
        record["raw_fields"] = _fields(node.child_by_field_name("body"), flags.get("rename_all"))
    elif node.type == "enum_item":
        record["raw_variants"] = _variants(node, flags.get("rename_all"))
    module["defs"][name] = record


def _fields(body, rename_all):
    if body is None:
        return []
    found = []
    pending = []
    positional = 0
    for node in body.children:
        if node.type == "attribute_item":
            pending.append(node)
            continue
        if node.type in {"line_comment", "block_comment", ",", "{", "}", "(", ")"}:
            continue
        flags = merge_serde(_attr_texts(pending))
        pending = []
        if node.type == "field_declaration":
            ident = node.child_by_field_name("name")
            ty_node = node.child_by_field_name("type")
            name = ident.text.decode() if ident is not None else f"_{positional}"
            ty = ty_node.text.decode() if ty_node is not None else node.text.decode()
        elif node.is_named:
            name = f"_{positional}"
            ty = node.text.decode()
        else:
            continue
        if name.startswith("_"):
            positional += 1
        wire = flags["rename"] or rename_ident(name, rename_all)
        found.append({"name": name, "wire": wire, "ty": ty, "flags": flags})
    return found


def _variants(node, rename_all):
    body = node.child_by_field_name("body")
    found = []
    pending = []
    if body is None:
        return found
    for child in body.children:
        if child.type == "attribute_item":
            pending.append(child)
            continue
        if child.type in {"line_comment", "block_comment", ",", "{", "}"}:
            continue
        if child.type != "enum_variant":
            pending = []
            continue
        flags = merge_serde(_attr_texts(pending))
        pending = []
        name = child.child_by_field_name("name").text.decode()
        variant_body = child.child_by_field_name("body")
        kind = "unit"
        fields = []
        if variant_body is not None and variant_body.type == "field_declaration_list":
            kind = "struct"
            fields = _fields(variant_body, None)
        elif variant_body is not None:
            kind = "tuple"
            fields = _fields(variant_body, None)
        wire = flags["rename"] or rename_ident(name, rename_all)
        found.append({
            "name": name,
            "wire": wire,
            "kind": kind,
            "fields": fields,
            "other": flags["other"],
        })
    return found


def _take_impl(module, node):
    trait = node.child_by_field_name("trait")
    self_ty = node.child_by_field_name("type")
    if trait is None or self_ty is None:
        return
    trait_name = trait.text.decode().split("<", 1)[0]
    if trait_name not in {"Serialize", "Deserialize"}:
        return
    owner = self_ty.text.decode().split("<", 1)[0].strip().split("::")[-1]
    bucket = module["impls"].setdefault(owner, {})
    bucket[trait_name] = norm_code(node.text.decode())


def _take_macro(module, node):
    text = node.text.decode().lstrip()
    if not text.startswith("string_primitive!"):
        return
    parts = text.split(",")
    name = parts[0].split("!", 1)[-1].strip(" (\n")
    validator = parts[1].strip() if len(parts) > 1 else ""
    module["defs"][name] = {
        "path": f"{module['path']}::{name}" if module["path"] else name,
        "name": name,
        "public": True,
        "internal": True,
        "kind": "macro",
        "flags": merge_serde(["#[serde(transparent)]"]),
        "wire": True,
        "raw_fields": [{"name": "0", "wire": "0", "ty": "String", "flags": merge_serde("")}],
        "raw_variants": [],
        "alias": None,
        "note": validator,
    }


def _link(modules):
    from protocol_pin.shape import build_exports, materialize

    schema = Schema(types={}, files={path: mod["file"] for path, mod in modules.items()})
    by_name, consts = _index(modules)
    schema.by_name = by_name
    for module in modules.values():
        for name, record in module["defs"].items():
            if not record["wire"] and name not in module["impls"]:
                continue
            schema.types[record["path"]] = materialize(record, module, by_name, consts)
        _apply_impls(module, schema)
    schema.exports = build_exports(modules, by_name)
    return schema


def _index(modules):
    """Map each wire type and const by its short name. Both are unique in this crate."""
    by_name = {}
    consts = {}
    dupes = []
    for module in modules.values():
        for name, value in module["consts"].items():
            if name in consts and consts[name] != value:
                dupes.append(name)
            consts[name] = value
        for name, record in module["defs"].items():
            if not record["wire"] and name not in module["impls"]:
                continue
            if name in by_name:
                dupes.append(name)
            by_name[name] = record["path"]
    if dupes:
        listed = ", ".join(sorted(set(dupes)))
        raise SystemExit(f"cloud_backup_protocol wire names are not unique: {listed}")
    return by_name, consts


def _apply_impls(module, schema):
    for name, impls in module["impls"].items():
        record = module["defs"].get(name)
        if record is None:
            continue
        typedef = schema.types.get(record["path"])
        if typedef is None:
            continue
        ser = impls.get("Serialize")
        de = impls.get("Deserialize")
        if ser:
            typedef.custom = ser
            typedef.repr = "custom"
        if de:
            typedef.de_note = de
