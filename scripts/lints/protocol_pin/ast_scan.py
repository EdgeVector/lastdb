"""Tree-sitter Rust scanners for protocol extraction and client wire usage."""
from __future__ import annotations

from pathlib import Path

import tree_sitter_rust as ts_rust
from tree_sitter import Language, Parser

from protocol_pin.wire import (
    Schema, Usage, build_exports, empty_for_skip, materialize, merge_serde, norm_code, rename_ident,
)

PARSER = Parser(Language(ts_rust.language()))
SKIP_DIRS = {"target", ".git", "node_modules", "vendor"}


def _vis(node) -> str:
    vis = node.child_by_field_name("visibility") or next((c for c in node.children if c.type == "visibility_modifier"), None)
    return " ".join(vis.text.decode().split()) if vis is not None else ""


def _path_of(node) -> list[str]:
    if node.type in {"scoped_identifier", "use_wildcard"}:
        parts: list[str] = []
        for child in node.children:
            if child.type in {"scoped_identifier", "identifier", "crate", "super", "self", "type_identifier"}:
                parts.extend(_path_of(child) if child.type == "scoped_identifier" else [child.text.decode()])
            elif node.type == "use_wildcard" and child.text.decode() == "*":
                parts.append("*")
        return parts
    return [node.text.decode()]


def parse_use(node) -> dict:
    names: list[tuple[str, list[str]]] = []

    def walk(item, prefix: list[str]) -> None:
        kind = item.type
        if kind == "scoped_use_list":
            head = [c for c in item.children if c.type not in {":", "::"}]
            for child in head[1:]:
                walk(child, _path_of(head[0]) if head else [])
        elif kind == "use_list":
            for child in (c for c in item.children if c.type not in {",", "{", "}"}):
                walk(child, prefix)
        elif kind == "use_wildcard":
            path = _path_of(item)
            names.append(("*", prefix + (path[:-1] if path and path[-1] == "*" else path)))
        elif kind == "use_as_clause":
            ids = [c.text.decode() for c in item.children if c.type == "identifier"]
            if ids:
                names.append((ids[-1], prefix + [ids[0]]))
        elif kind == "scoped_identifier":
            path = _path_of(item)
            names.append((path[-1], prefix + path))
        elif kind in {"identifier", "crate", "super", "self"}:
            names.append((item.text.decode(), prefix + [item.text.decode()]))

    for child in (c for c in node.children if c.type not in {"use", ";", "visibility_modifier"}):
        walk(child, [])
    return {"public": _vis(node) == "pub", "names": names}


def load_protocol(root: str | Path) -> Schema:
    root = Path(root)
    crate = root / "cloud_backup_protocol" if (root / "cloud_backup_protocol").is_dir() else root
    src = crate / "src" / "lib.rs"
    if not src.is_file():
        raise SystemExit(f"cloud_backup_protocol source not found under {root}")
    modules: dict[str, dict] = {}
    _read_module(modules, "", src)
    return _link(modules)


def _read_module(modules: dict[str, dict], path: str, file: Path) -> None:
    tree = PARSER.parse(file.read_bytes())
    mod = {"path": path, "file": str(file), "subs": {}, "defs": {}, "consts": {}, "uses": [], "impls": {}}
    modules[path] = mod
    pending = []
    for node in tree.root_node.children:
        if node.type == "attribute_item":
            pending.append(node)
            continue
        if node.type in {"line_comment", "block_comment"}:
            continue
        if node.type == "mod_item":
            name = node.child_by_field_name("name").text.decode()
            child = f"{path}::{name}" if path else name
            mod["subs"][name] = {"path": child, "public": _vis(node) == "pub"}
            if node.child_by_field_name("body") is None:
                base = file.parent if file.name in {"lib.rs", "mod.rs"} else file.parent / file.stem
                target = base / f"{name}.rs" if (base / f"{name}.rs").is_file() else base / name / "mod.rs"
                if target.is_file():
                    _read_module(modules, child, target)
        elif node.type == "use_declaration":
            mod["uses"].append(parse_use(node))
        elif node.type == "const_item":
            name, val = node.child_by_field_name("name"), node.child_by_field_name("value")
            if name is not None:
                mod["consts"][name.text.decode()] = val.text.decode() if val else ""
        elif node.type in {"struct_item", "enum_item", "type_item"}:
            _take_type(mod, node, pending)
        elif node.type == "impl_item":
            trait, self_ty = node.child_by_field_name("trait"), node.child_by_field_name("type")
            if trait is not None and self_ty is not None:
                tname = trait.text.decode().split("<", 1)[0]
                if tname in {"Serialize", "Deserialize"}:
                    owner = self_ty.text.decode().split("<", 1)[0].strip().split("::")[-1]
                    mod["impls"].setdefault(owner, {})[tname] = norm_code(node.text.decode())
        elif node.type == "expression_statement":
            text = node.text.decode().lstrip()
            if text.startswith("string_primitive!"):
                parts = text.split(",")
                mname = parts[0].split("!", 1)[-1].strip(" (\n")
                mod["defs"][mname] = {
                    "path": f"{path}::{mname}" if path else mname, "name": mname, "public": True,
                    "kind": "macro", "flags": merge_serde(["#[serde(transparent)]"]), "wire": True,
                    "raw_fields": [{"name": "0", "wire": "0", "ty": "String", "flags": merge_serde([])}],
                    "raw_variants": [], "alias": None, "note": parts[1].strip() if len(parts) > 1 else "",
                }
        pending = []


def _fields(body, rename_all: str | None) -> list[dict]:
    found, pending, pos = [], [], 0
    for node in body.children if body is not None else ():
        if node.type == "attribute_item":
            pending.append(node)
            continue
        if node.type in {"line_comment", "block_comment", ",", "{", "}", "(", ")"}:
            continue
        flags = merge_serde([n.text.decode() for n in pending])
        pending = []
        if node.type == "field_declaration":
            ident, ty_node = node.child_by_field_name("name"), node.child_by_field_name("type")
            name = ident.text.decode() if ident is not None else f"_{pos}"
            ty = ty_node.text.decode() if ty_node is not None else node.text.decode()
        elif node.is_named:
            name, ty = f"_{pos}", node.text.decode()
        else:
            continue
        if name.startswith("_"):
            pos += 1
        found.append({"name": name, "wire": flags["rename"] or rename_ident(name, rename_all), "ty": ty, "flags": flags})
    return found


def _take_type(mod: dict, node, pending: list) -> None:
    texts = [n.text.decode() for n in pending]
    if any("cfg(test)" in t for t in texts):
        return
    name = node.child_by_field_name("name").text.decode()
    flags, blob = merge_serde(texts), " ".join(texts)
    rec = {
        "path": f"{mod['path']}::{name}" if mod["path"] else name,
        "name": name, "public": _vis(node) == "pub", "kind": node.type, "flags": flags,
        "wire": node.type == "type_item" or "Serialize" in blob or "Deserialize" in blob,
        "raw_fields": [], "raw_variants": [], "alias": None,
    }
    if node.type == "type_item":
        rec["alias"] = node.child_by_field_name("type").text.decode()
    elif node.type == "struct_item":
        rec["raw_fields"] = _fields(node.child_by_field_name("body"), flags.get("rename_all"))
    elif node.type == "enum_item":
        body = node.child_by_field_name("body")
        vpend = []
        for child in body.children if body is not None else ():
            if child.type == "attribute_item":
                vpend.append(child)
            elif child.type == "enum_variant":
                vflags = merge_serde([n.text.decode() for n in vpend])
                vpend = []
                vname = child.child_by_field_name("name").text.decode()
                vbody = child.child_by_field_name("body")
                vkind = "struct" if vbody is not None and vbody.type == "field_declaration_list" else ("tuple" if vbody is not None else "unit")
                rec["raw_variants"].append({
                    "name": vname, "wire": vflags["rename"] or rename_ident(vname, flags.get("rename_all")),
                    "kind": vkind, "fields": _fields(vbody, None) if vbody else [], "other": vflags["other"],
                })
            elif child.type not in {"line_comment", "block_comment", ",", "{", "}"}:
                vpend = []
    mod["defs"][name] = rec


def _link(modules: dict[str, dict]) -> Schema:
    by_name, consts, dupes = {}, {}, []
    for mod in modules.values():
        for k, v in mod["consts"].items():
            if k in consts and consts[k] != v:
                dupes.append(k)
            consts[k] = v
        for k, r in mod["defs"].items():
            if r["wire"] or k in mod["impls"]:
                if k in by_name:
                    dupes.append(k)
                by_name[k] = r["path"]
    if dupes:
        raise SystemExit(f"cloud_backup_protocol wire names are not unique: {', '.join(sorted(set(dupes)))}")
    schema = Schema(types={}, files={p: m["file"] for p, m in modules.items()}, by_name=by_name)
    for mod in modules.values():
        for name, rec in mod["defs"].items():
            if rec["wire"] or name in mod["impls"]:
                schema.types[rec["path"]] = materialize(rec, by_name, consts)
        for name, impls in mod["impls"].items():
            rec = mod["defs"].get(name)
            td = schema.types.get(rec["path"]) if rec else None
            if td and impls.get("Serialize"):
                td.custom, td.repr = impls["Serialize"], "custom"
            if td and impls.get("Deserialize"):
                td.de_note = impls["Deserialize"]
    schema.exports = build_exports(modules, by_name)
    return schema


def collect_usage(repo: str | Path, schema: Schema) -> Usage:
    usage, repo = Usage(), Path(repo)
    by_file = {Path(p).resolve(): mod for mod, p in schema.files.items()}
    for path in repo.rglob("*.rs"):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        module = by_file.get(path.resolve())
        tree = PARSER.parse(path.read_bytes())
        if tree.root_node.has_error and module is None:
            continue
        uses = [parse_use(n) for n in tree.root_node.children if n.type == "use_declaration"]
        names = {n: schema.canonical(p) for n, p in schema.by_name.items()} if module is not None else _client_names(schema, uses)
        if names or module is not None:
            _walk_usage(tree.root_node, names, schema, usage)
    for roots in (usage.request_roots, usage.response_roots):
        roots.update(schema.canonical(p) for p in list(roots) if schema.canonical(p) in schema.types)
    return usage


def _client_names(schema: Schema, uses: list[dict]) -> dict[str, str]:
    found: dict[str, str] = {}
    for use in uses:
        for local, segs in use["names"]:
            if not segs or segs[0] != "cloud_backup_protocol":
                continue
            body = [p for p in segs[1:] if p != "*"]
            if local == "*":
                for name, path in schema.exports.get("::".join(body), {}).items():
                    found[name] = schema.canonical(path)
            elif (body[-1] if body else local) in schema.by_name:
                found[local] = schema.canonical(schema.by_name[body[-1] if body else local])
    return found


def _in_pattern(node) -> bool:
    cur = node.parent
    while cur is not None:
        if cur.type.endswith("_pattern") or cur.type == "match_pattern":
            return True
        cur = cur.parent
    return False


def _type_name(text: str, names: dict[str, str], schema: Schema) -> str | None:
    cleaned = " ".join(text.replace("&", " ").split()).split("<", 1)[0].strip()
    path = names.get(cleaned.split("::")[-1]) or names.get(cleaned)
    return schema.canonical(path) if path else None


def _add_response(text: str, names: dict[str, str], schema: Schema, usage: Usage) -> None:
    raw = text.replace("Option<", " ").replace("Vec<", " ").replace("Result<", " ")
    for tok in raw.replace("<", " ").replace(">", " ").replace(",", " ").split():
        path = _type_name(tok, names, schema)
        if path and schema.get(path) is not None:
            usage.response_roots.add(path)


def _type_and_variant(text: str, names: dict[str, str], schema: Schema) -> tuple[str | None, str | None]:
    parts = text.split("::")
    if len(parts) == 1:
        p = names.get(parts[0])
        return (schema.canonical(p) if p else None, None)
    owner = names.get(parts[-2])
    if owner is None:
        return None, None
    td = schema.get(owner)
    if td is None or td.kind != "enum":
        return schema.canonical(owner), None
    return td.path, (parts[-1] if any(v.name == parts[-1] for v in td.variants) else None)


def _roots_in_fn(fn, names: dict[str, str], schema: Schema, usage: Usage) -> None:
    params: dict[str, str] = {}
    listed = fn.child_by_field_name("parameters")
    for child in listed.children if listed is not None else ():
        if child.type == "parameter":
            ident, ty = child.child_by_field_name("pattern"), child.child_by_field_name("type")
            path = _type_name(ty.text.decode(), names, schema) if ident is not None and ty is not None else None
            if path and ident is not None:
                params[ident.text.decode()] = path
    text = fn.text.decode()
    if ".post(" in text or "canonical_bytes(" in text:
        usage.request_roots.update(params.values())
        ret = fn.child_by_field_name("return_type")
        if ret is not None:
            _add_response(ret.text.decode(), names, schema, usage)
    stack = [fn]
    while stack:
        node = stack.pop()
        if node.type == "let_declaration":
            ty, val = node.child_by_field_name("type"), node.child_by_field_name("value")
            if ty is not None and val is not None and (".post(" in val.text.decode() or "from_slice" in val.text.decode()):
                _add_response(ty.text.decode(), names, schema, usage)
        stack.extend(node.children)


def _struct_expr(node, names: dict[str, str], schema: Schema, usage: Usage) -> None:
    name = node.child_by_field_name("name")
    if _in_pattern(node) or name is None:
        return
    path, variant = _type_and_variant(name.text.decode(), names, schema)
    td = schema.get(path) if path else None
    body = node.child_by_field_name("body") if td is not None else None
    if td is not None and variant:
        usage.sent_variants.add((td.path, variant))
    if body is None or td is None:
        return
    fmap = {f.name: f for f in td.fields} if variant is None else next(({f.name: f for f in v.fields} for v in td.variants if v.name == variant), {})
    for child in body.children:
        if child.type not in {"shorthand_field_initializer", "field_initializer"}:
            continue
        ident = child.child_by_field_name("field") or child.child_by_field_name("name") or next((c for c in child.children if c.type in {"field_identifier", "identifier"}), None)
        fname = ident.text.decode() if ident is not None else None
        val = child.child_by_field_name("value") if child.type == "field_initializer" else None
        fld = fmap.get(fname) if fname else None
        if fld is not None and not empty_for_skip(fname if val is None else val.text.decode(), fld.skip):
            usage.sent_fields.add((td.path, variant, fname))  # type: ignore[arg-type]


def _walk_usage(node, names: dict[str, str], schema: Schema, usage: Usage) -> None:
    if node.type == "function_item":
        _roots_in_fn(node, names, schema, usage)
    if node.type == "struct_expression":
        _struct_expr(node, names, schema, usage)
    elif node.type == "call_expression":
        fn = node.child_by_field_name("function")
        if fn is not None and fn.type == "scoped_identifier" and not _in_pattern(node):
            p, v = _type_and_variant(fn.text.decode(), names, schema)
            if p and v and schema.get(p) is not None:
                usage.sent_variants.add((schema.canonical(p), v))
    elif node.type == "scoped_identifier" and not _in_pattern(node):
        if node.parent is None or node.parent.type not in {"call_expression", "struct_expression"}:
            p, v = _type_and_variant(node.text.decode(), names, schema)
            if p and v and schema.get(p) is not None:
                usage.sent_variants.add((schema.canonical(p), v))
    for child in node.children:
        _walk_usage(child, names, schema, usage)


def proto_non_self_sources(pinned: Schema, enum_name: str) -> list[str]:
    chunks: list[str] = []
    for path in sorted(set(pinned.files.values())):
        for child in PARSER.parse(Path(path).read_bytes()).root_node.children:
            if child.type in {"line_comment", "block_comment"}:
                continue
            if child.type == "enum_item" and (n := child.child_by_field_name("name")) and n.text.decode() == enum_name:
                continue
            if child.type == "impl_item" and (t := child.child_by_field_name("type")) and t.text.decode().split("<", 1)[0].strip().split("::")[-1] == enum_name:
                body = child.child_by_field_name("body")
                for item in body.children if body is not None else ():
                    params = item.child_by_field_name("parameters") if item.type == "function_item" else None
                    if item.type == "function_item" and not (params and any(p.type == "self_parameter" for p in params.children)):
                        chunks.append(norm_code(item.text.decode()))
                continue
            chunks.append(norm_code(child.text.decode()))
    return chunks
