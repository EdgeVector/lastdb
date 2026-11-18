"""Check /v1 app API calls against lastdb_node/app-api.json.

record_app_api installs one wrapper on send_unix_request. Every /v1 request
and every reply passes through it. A reply that does not match fails the
operation. A request that does not match fails only when the status is below
400, because the operation also sends bodies the server refuses.

The record-home job has no pip install, so this checker is the standard
library. It implements the keywords the OpenAPI file uses and rejects any
other keyword at load, so a constraint in the file cannot be silently ignored.
"""

import json
from pathlib import Path

SPEC = Path(__file__).resolve().parents[2] / "lastdb_node" / "app-api.json"
ALLOWED = {
    "$ref", "type", "enum", "const", "minimum", "maximum", "minItems", "maxItems",
    "items", "required", "properties", "additionalProperties", "oneOf",
    "description", "title", "default",
}
TYPES = {"null", "boolean", "integer", "number", "string", "array", "object"}
_SPEC = None


def install(module, node_module, *also):
    """Wrap every copy of send_unix_request that a /v1 call uses."""
    spec()
    wrapped = _wrap(module.send_unix_request)
    module.send_unix_request = wrapped
    node_module.send_unix_request = wrapped
    for other in also:
        other.send_unix_request = wrapped


def spec():
    global _SPEC
    if _SPEC is None:
        _SPEC = _load()
    return _SPEC


def check_exchange(document, method, path, body, status, raw):
    """Raise when a /v1 reply does not match, or a successful request does not."""
    operation = (document.get("paths") or {}).get(path, {}).get(str(method).lower())
    if not isinstance(operation, dict):
        _fail(path, status, f"no {method} operation in app-api.json")
    decoded, request_error = _decode_request(body)
    request = _request_schema(operation)
    if request_error is None:
        request_error = "no request schema" if request is None else mismatch(document, request, decoded, "$")
    reply, reply_error = _decode_reply(raw)
    if reply_error:
        _fail(path, status, reply_error)
    response = _response_schema(document, operation, status)
    if response is None:
        _fail(path, status, "no response schema")
    reply_error = mismatch(document, response, reply, "$")
    if reply_error:
        _fail(path, status, reply_error)
    if request_error and status < 400:
        _fail(path, status, "request " + request_error)


def mismatch(document, schema, value, path="$", stack=()):
    """Return the first mismatch, or None when value matches schema."""
    if not isinstance(schema, dict):
        return f"{path}: schema is not an object"
    if "$ref" in schema:
        ref = schema["$ref"]
        if ref in stack:
            return f"{path}: circular {ref}"
        return mismatch(document, _resolve(document, ref), value, path, stack + (ref,))
    if "oneOf" in schema:
        return _one_of(document, schema["oneOf"], value, path, stack)
    if "const" in schema and not _const_ok(value, schema["const"]):
        return f"{path}: {value!r} is not {schema['const']!r}"
    if "type" in schema and not _type_ok(schema["type"], value):
        return f"{path}: expected {_type_label(schema['type'])}, got {_kind(value)}"
    if "enum" in schema and not _enum_ok(value, schema["enum"]):
        return f"{path}: {value!r} is not one of {schema['enum']}"
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return None
    if isinstance(value, (int, float)):
        return _bounds(schema, value, path)
    if isinstance(value, list):
        return _array(document, schema, value, path, stack)
    if isinstance(value, dict):
        return _object(document, schema, value, path, stack)
    return f"{path}: got {_kind(value)}"


def _wrap(original):
    def send_unix_request(socket_path, method, path, body=None, headers=None, **kwargs):
        result = original(socket_path, method, path, body, headers, **kwargs)
        if isinstance(path, str) and path.startswith("/v1/"):
            check_exchange(spec(), method, path, body, result[0], result[1])
        return result
    return send_unix_request


def _load():
    try:
        document = json.loads(SPEC.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"app-api.json cannot be read: {error}") from error
    problems, done = [], set()
    for path, schema in _schema_nodes(document):
        _walk(document, schema, path, problems, done)
    _response_refs(document, problems)
    if problems:
        raise RuntimeError("app-api.json: " + problems[0])
    return document


def _schema_nodes(document):
    components = document.get("components") or {}
    for name, schema in (components.get("schemas") or {}).items():
        yield f"#/components/schemas/{name}", schema
    for name, response in (components.get("responses") or {}).items():
        schema = _media_schema(response)
        if schema is not None:
            yield f"#/components/responses/{name}", schema
    for path, item in (document.get("paths") or {}).items():
        operation = item.get("post") or {}
        request = _request_schema(operation)
        if request is not None:
            yield f"{path} request", request
        for code, response in (operation.get("responses") or {}).items():
            if "$ref" not in response:
                schema = _media_schema(response)
                if schema is not None:
                    yield f"{path} {code}", schema


def _walk(document, schema, path, problems, done):
    if not isinstance(schema, dict):
        problems.append(f"{path} is not a schema object")
        return
    if "$ref" in schema:
        _walk_ref(document, schema, path, problems, done)
        return
    for key in schema:
        if str(key).startswith("x-") or key in ALLOWED:
            continue
        problems.append(f"{path} uses unsupported keyword {key}")
    _walk_type(schema, path, problems)
    props = schema.get("properties")
    if isinstance(props, dict):
        for name, sub in props.items():
            _walk(document, sub, f"{path}/{name}", problems, done)
    elif "properties" in schema:
        problems.append(f"{path} properties is not an object")
    if "items" in schema:
        if isinstance(schema["items"], dict):
            _walk(document, schema["items"], path + "/items", problems, done)
        else:
            problems.append(f"{path} items must be one schema")
    if "additionalProperties" in schema:
        _walk_additional(document, schema["additionalProperties"], path, problems, done)
    if "oneOf" in schema:
        if not isinstance(schema["oneOf"], list):
            problems.append(f"{path} oneOf must be a list")
        else:
            for index, sub in enumerate(schema["oneOf"]):
                _walk(document, sub, f"{path}/oneOf/{index}", problems, done)


def _walk_ref(document, schema, path, problems, done):
    for key in schema:
        if key in ("$ref", "description", "title") or str(key).startswith("x-"):
            continue
        problems.append(f"{path} puts {key} beside $ref")
    ref = schema["$ref"]
    if ref in done:
        return
    done.add(ref)
    try:
        target = _resolve(document, ref)
    except RuntimeError as error:
        problems.append(str(error))
        return
    _walk(document, target, ref, problems, done)


def _walk_type(schema, path, problems):
    if "type" not in schema:
        return
    names = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
    if any(not isinstance(name, str) or name not in TYPES for name in names):
        problems.append(f"{path} has a type this checker does not know")


def _walk_additional(document, additional, path, problems, done):
    if isinstance(additional, dict):
        _walk(document, additional, path + "/additionalProperties", problems, done)
    elif not isinstance(additional, bool):
        problems.append(f"{path} additionalProperties must be a boolean or a schema")


def _response_refs(document, problems):
    known = (document.get("components") or {}).get("responses") or {}
    for path, item in (document.get("paths") or {}).items():
        for code, response in ((item.get("post") or {}).get("responses") or {}).items():
            if "$ref" in response and response["$ref"].rsplit("/", 1)[-1] not in known:
                problems.append(f"{path} {code} ref does not resolve: {response['$ref']}")


def _one_of(document, branches, value, path, stack):
    matched, first = 0, None
    for branch in branches:
        found = mismatch(document, branch, value, path, stack)
        if found is None:
            matched += 1
        elif first is None:
            first = found
    if matched == 1:
        return None
    if matched == 0:
        return f"{path}: matches no alternative ({first})"
    return f"{path}: matches more than one alternative"


def _object(document, schema, value, path, stack):
    props = schema.get("properties") or {}
    for name in schema.get("required") or []:
        if name not in value:
            return f"{path}.{name}: missing {name}"
    additional = schema.get("additionalProperties", None)
    for name, entry in value.items():
        child = f"{path}.{name}"
        if name in props:
            found = mismatch(document, props[name], entry, child, stack)
        elif additional is False:
            return f"{child}: unexpected property {name}"
        elif isinstance(additional, dict):
            found = mismatch(document, additional, entry, child, stack)
        else:
            continue
        if found:
            return found
    return None


def _array(document, schema, value, path, stack):
    if "minItems" in schema and len(value) < schema["minItems"]:
        return f"{path}: has {len(value)} items, minimum is {schema['minItems']}"
    if "maxItems" in schema and len(value) > schema["maxItems"]:
        return f"{path}: has {len(value)} items, maximum is {schema['maxItems']}"
    item = schema.get("items")
    if not isinstance(item, dict):
        return None
    for index, entry in enumerate(value):
        found = mismatch(document, item, entry, f"{path}[{index}]", stack)
        if found:
            return found
    return None


def _bounds(schema, value, path):
    if "minimum" in schema and value < schema["minimum"]:
        return f"{path}: {value} is below minimum {schema['minimum']}"
    if "maximum" in schema and value > schema["maximum"]:
        return f"{path}: {value} is above maximum {schema['maximum']}"
    return None


def _type_ok(expected, value):
    names = expected if isinstance(expected, list) else [expected]
    return any(_kind_is(name, value) for name in names)


def _kind_is(name, value):
    if name == "null":
        return value is None
    if name == "boolean":
        return isinstance(value, bool)
    if name == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if name == "string":
        return isinstance(value, str)
    if name == "array":
        return isinstance(value, list)
    if name == "object":
        return isinstance(value, dict)
    return False


def _enum_ok(value, options):
    return any(value == option and type(value) is type(option) for option in options)


def _const_ok(value, expected):
    return value == expected and type(value) is type(expected)


def _kind(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _type_label(expected):
    return " or ".join(expected) if isinstance(expected, list) else expected


def _decode_request(body):
    if body is None:
        return None, "request body is missing"
    if isinstance(body, (bytes, bytearray)):
        body = body.decode("utf-8", "replace")
    if isinstance(body, str):
        try:
            return json.loads(body), None
        except json.JSONDecodeError as error:
            return None, f"request body is not JSON ({error})"
    return body, None


def _decode_reply(raw):
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "replace")
    if not isinstance(raw, str):
        return raw, None
    try:
        return json.loads(raw), None
    except json.JSONDecodeError as error:
        return None, f"reply is not JSON ({error})"


def _request_schema(operation):
    return _media_schema(operation.get("requestBody") or {})


def _response_schema(document, operation, status):
    response = (operation.get("responses") or {}).get(str(status))
    if not isinstance(response, dict):
        return None
    if "$ref" in response:
        response = _resolve(document, response["$ref"])
    return _media_schema(response)


def _media_schema(node):
    content = (node.get("content") or {}).get("application/json") or {}
    return content.get("schema")


def _resolve(document, ref):
    if not isinstance(ref, str) or not ref.startswith("#/"):
        raise RuntimeError(f"app-api.json ref is not inside the document: {ref}")
    node = document
    for part in ref[2:].split("/"):
        part = part.replace("~1", "/").replace("~0", "~")
        if not isinstance(node, dict) or part not in node:
            raise RuntimeError(f"app-api.json ref does not resolve: {ref}")
        node = node[part]
    return node


def _fail(path, status, detail):
    raise RuntimeError(f"app API schema: {path} status {status}: {detail}")
