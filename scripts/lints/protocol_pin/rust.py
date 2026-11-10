"""Small Rust/serde helpers for the cloud protocol pin check."""

import re

PRIMITIVES = {
    "bool": "bool",
    "char": "string",
    "str": "string",
    "String": "string",
    "u8": "integer",
    "u16": "integer",
    "u32": "integer",
    "u64": "integer",
    "u128": "integer",
    "usize": "integer",
    "i8": "integer",
    "i16": "integer",
    "i32": "integer",
    "i64": "integer",
    "i128": "integer",
    "isize": "integer",
    "f32": "float",
    "f64": "float",
}

SKIP_AT_EMPTY = ("is_none", "is_empty", "is_default")


def norm_code(text):
    """Drop comments and collapse whitespace so formatting is not a wire change."""
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
    text = re.sub(r"//[^\n]*", " ", text)
    return " ".join(text.split())


def rename_ident(name, style):
    """Apply a serde rename_all style. Unknown styles keep the Rust name."""
    if not style or style == "PascalCase":
        return name
    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    snake = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", snake).lower()
    parts = [part for part in snake.split("_") if part]
    if style == "snake_case":
        return "_".join(parts)
    if style == "camelCase":
        return parts[0] + "".join(part.title() for part in parts[1:]) if parts else name
    if style == "lowercase":
        return snake.replace("_", "")
    if style == "UPPERCASE":
        return snake.replace("_", "").upper()
    if style == "SCREAMING_SNAKE_CASE":
        return "_".join(parts).upper()
    if style == "kebab-case":
        return "-".join(parts)
    return name


def _top_items(inner):
    items = []
    buf = []
    depth = 0
    for char in inner:
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        if char == "," and depth == 0:
            items.append("".join(buf).strip())
            buf = []
        else:
            buf.append(char)
    tail = "".join(buf).strip()
    if tail:
        items.append(tail)
    return items


def serde_flags(text):
    """Parse one #[serde(...)] attribute into a flag dict."""
    match = re.search(r"#\s*\[\s*serde\s*\((.*)\)\s*\]", text, flags=re.S)
    flags = {
        "deny": False,
        "transparent": False,
        "untagged": False,
        "other": False,
        "default": False,
        "skip": None,
        "flatten": False,
        "rename": None,
        "rename_all": None,
        "tag": None,
        "content": None,
    }
    if not match:
        return flags
    for item in _top_items(match.group(1)):
        if item == "deny_unknown_fields":
            flags["deny"] = True
        elif item == "transparent":
            flags["transparent"] = True
        elif item == "untagged":
            flags["untagged"] = True
        elif item == "other":
            flags["other"] = True
        elif item == "flatten":
            flags["flatten"] = True
        elif item == "skip" or item == "skip_serializing":
            flags["skip"] = "never"
        elif item == "default" or item.startswith("default "):
            flags["default"] = True
        elif "=" in item:
            key, value = item.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"')
            if key == "rename_all":
                flags["rename_all"] = value
            elif key == "rename":
                flags["rename"] = value
            elif key == "tag":
                flags["tag"] = value
            elif key == "content":
                flags["content"] = value
            elif key == "skip_serializing_if":
                flags["skip"] = value
            elif key == "default":
                flags["default"] = True
    return flags


def merge_serde(texts):
    """Combine every serde attribute that decorates one item."""
    merged = serde_flags("")
    for text in texts:
        if "serde" not in text:
            continue
        flags = serde_flags(text)
        for key, value in flags.items():
            if value:
                merged[key] = value
    return merged


def derive_serde(texts):
    """True when a derive list names Serialize or Deserialize."""
    blob = " ".join(texts)
    return "Serialize" in blob or "Deserialize" in blob


def split_args(text):
    """Split a generic argument list on top-level commas."""
    return [part.strip() for part in _top_items(text) if part.strip()]


def split_generic(text):
    """Return (base, args) for `Foo<A, B>` or (text, ()) otherwise."""
    if "<" not in text or not text.endswith(">"):
        return text, ()
    base, rest = text.split("<", 1)
    return base.strip(), tuple(split_args(rest[:-1]))


def strip_ref(text):
    """Remove leading references and lifetimes from a type string."""
    text = " ".join(text.split())
    while text.startswith("&"):
        text = text[1:].strip()
        text = re.sub(r"^'[A-Za-z0-9_]+\s+", "", text)
    return text


def empty_for_skip(expr, predicate):
    """True when a struct initializer cannot appear on the wire."""
    if not predicate or predicate == "never":
        return False
    text = " ".join(expr.split())
    tail = predicate.split("::")[-1]
    if tail not in SKIP_AT_EMPTY and not predicate.endswith(SKIP_AT_EMPTY):
        return False
    if tail == "is_none" or predicate.endswith("is_none"):
        return text == "None"
    if tail == "is_empty" or predicate.endswith("is_empty"):
        return text in {"Vec::new()", "vec![]", "String::new()", '""'}
    if tail == "is_default" or predicate.endswith("is_default"):
        return text in {"Default::default()", "None", "Vec::new()", "vec![]", '""', "0", "false"}
    return False
