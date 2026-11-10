"""What the client serializes, and which protocol types cross the wire."""

from pathlib import Path

import tree_sitter_rust as ts_rust
from tree_sitter import Language, Parser

from protocol_pin.extract import _use_record
from protocol_pin.rust import empty_for_skip

PARSER = Parser(Language(ts_rust.language()))
SKIP_DIRS = {"target", ".git", "node_modules", "vendor"}


class Usage:
    def __init__(self):
        self.sent_fields = set()
        self.sent_variants = set()
        self.request_roots = set()
        self.response_roots = set()

    def emits(self, path, variant, field):
        if field.skip == "never":
            return False
        if not field.skip:
            return True
        return (path, variant, field.name) in self.sent_fields


def collect_usage(repo, schema):
    """Scan Rust sources for constructors and HTTP request/response roots."""
    usage = Usage()
    repo = Path(repo)
    files = schema.files
    by_file = {Path(path).resolve(): module for module, path in files.items()}
    for path in repo.rglob("*.rs"):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        module = by_file.get(path.resolve())
        _scan_file(path, module, schema, usage)
    _follow_aliases(schema, usage.request_roots)
    _follow_aliases(schema, usage.response_roots)
    return usage


def _follow_aliases(schema, roots):
    extra = set()
    for path in list(roots):
        canonical = schema.canonical(path)
        if canonical in schema.types:
            extra.add(canonical)
    roots.update(extra)


def _names(schema, module):
    """Inside the protocol crate every wire type name is unique."""
    if module is None:
        return {}
    return {name: schema.canonical(path) for name, path in schema.by_name.items()}


def _client_names(schema, uses):
    found = {}
    for use in uses:
        for local, segments in use["names"]:
            if not segments or segments[0] != "cloud_backup_protocol":
                continue
            body = [part for part in segments[1:] if part != "*"]
            if local == "*":
                module = "::".join(body)
                for name, path in schema.exports.get(module, {}).items():
                    found[name] = schema.canonical(path)
                continue
            leaf = body[-1] if body else local
            path = schema.by_name.get(leaf)
            if path:
                found[local] = schema.canonical(path)
    return found


def _scan_file(path, module, schema, usage):
    source = path.read_bytes()
    tree = PARSER.parse(source)
    if tree.root_node.has_error and module is None:
        return
    uses = []
    for node in tree.root_node.children:
        if node.type == "use_declaration":
            uses.append(_use_record(node))
    names = _names(schema, module) if module is not None else _client_names(schema, uses)
    if not names and module is None:
        return
    _walk(tree.root_node, names, schema, usage, None)


def _pattern(node):
    parent = node.parent
    while parent is not None:
        if parent.type.endswith("_pattern") or parent.type == "match_pattern":
            return True
        parent = parent.parent
    return False


def _walk(node, names, schema, usage, params):
    if node.type == "function_item":
        params = _params(node, names, schema)
        _roots_in_function(node, params, names, schema, usage)
    if node.type == "struct_expression":
        _struct_expr(node, names, schema, usage)
    elif node.type == "call_expression":
        _call_expr(node, names, schema, usage)
    elif node.type == "scoped_identifier" and not _pattern(node):
        _scoped_variant(node, names, schema, usage)
    for child in node.children:
        _walk(child, names, schema, usage, params)


def _params(fn, names, schema):
    params = {}
    listed = fn.child_by_field_name("parameters")
    if listed is None:
        return params
    for child in listed.children:
        if child.type != "parameter":
            continue
        ident = child.child_by_field_name("pattern")
        ty = child.child_by_field_name("type")
        if ident is None or ty is None:
            continue
        path = _type_name(ty.text.decode(), names, schema)
        if path:
            params[ident.text.decode()] = path
    return params


def _type_name(text, names, schema):
    text = " ".join(text.replace("&", " ").split())
    text = text.split("<", 1)[0].strip()
    base = text.split("::")[-1]
    path = names.get(base)
    if path is None and text in names:
        path = names[text]
    if path is None:
        return None
    return schema.canonical(path)


def _roots_in_function(fn, params, names, schema, usage):
    text = fn.text.decode()
    posts = ".post(" in text or "canonical_bytes(" in text or "post_shared_append(" in text
    if posts:
        for path in params.values():
            usage.request_roots.add(path)
        ret = fn.child_by_field_name("return_type")
        if ret is not None:
            _add_response(ret.text.decode(), names, schema, usage)
    for node in _nodes(fn):
        if node.type != "let_declaration":
            continue
        ty = node.child_by_field_name("type")
        value = node.child_by_field_name("value")
        if ty is None or value is None:
            continue
        blob = value.text.decode()
        if ".post(" not in blob and "from_slice" not in blob:
            continue
        _add_response(ty.text.decode(), names, schema, usage)


def _add_response(text, names, schema, usage):
    raw = text.replace("Option<", " ").replace("Vec<", " ").replace("Result<", " ")
    for token in raw.replace("<", " ").replace(">", " ").replace(",", " ").split():
        path = _type_name(token, names, schema)
        if path and schema.get(path) is not None:
            usage.response_roots.add(path)


def _nodes(node):
    yield node
    for child in node.children:
        yield from _nodes(child)


def _struct_expr(node, names, schema, usage):
    if _pattern(node):
        return
    name = node.child_by_field_name("name")
    if name is None:
        return
    path, variant = _type_and_variant(name.text.decode(), names, schema)
    if path is None:
        return
    typedef = schema.get(path)
    if typedef is None:
        return
    if variant:
        usage.sent_variants.add((typedef.path, variant))
    body = node.child_by_field_name("body")
    if body is None:
        return
    fields = _variant_fields(typedef, variant)
    for child in body.children:
        if child.type == "shorthand_field_initializer":
            field_name = _field_name(child)
            expr = field_name or ""
        elif child.type == "field_initializer":
            field_name = _field_name(child)
            value = child.child_by_field_name("value")
            expr = field_name if value is None else value.text.decode()
        else:
            continue
        if not field_name:
            continue
        field = fields.get(field_name)
        if field is None or empty_for_skip(expr, field.skip):
            continue
        usage.sent_fields.add((typedef.path, variant, field_name))


def _field_name(node):
    ident = node.child_by_field_name("field")
    if ident is None:
        ident = node.child_by_field_name("name")
    if ident is not None:
        return ident.text.decode()
    for child in node.children:
        if child.type in {"field_identifier", "identifier"}:
            return child.text.decode()
    return None


def _call_expr(node, names, schema, usage):
    fn = node.child_by_field_name("function")
    if fn is None or fn.type != "scoped_identifier" or _pattern(node):
        return
    path, variant = _type_and_variant(fn.text.decode(), names, schema)
    if path and variant and schema.get(path) is not None:
        usage.sent_variants.add((schema.canonical(path), variant))


def _scoped_variant(node, names, schema, usage):
    if node.parent is not None and node.parent.type in {"call_expression", "struct_expression"}:
        return
    path, variant = _type_and_variant(node.text.decode(), names, schema)
    if path and variant and schema.get(path) is not None:
        usage.sent_variants.add((schema.canonical(path), variant))


def _type_and_variant(text, names, schema):
    parts = text.split("::")
    if len(parts) == 1:
        path = names.get(parts[0])
        return (schema.canonical(path) if path else None, None)
    owner = names.get(parts[-2])
    if owner is None:
        return None, None
    typedef = schema.get(owner)
    if typedef is None or typedef.kind != "enum":
        return schema.canonical(owner), None
    variant = parts[-1]
    if any(item.name == variant for item in typedef.variants):
        return typedef.path, variant
    return typedef.path, None


def _variant_fields(typedef, variant):
    if variant is None:
        return {field.name: field for field in typedef.fields}
    for item in typedef.variants:
        if item.name == variant:
            return {field.name: field for field in item.fields}
    return {}
