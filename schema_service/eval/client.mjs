// Thin HTTP client for the schema service v1 API.
// Every method returns { status, body } so callers can branch on HTTP status
// (201 Added / 200 AlreadyExists / 409 Conflict) as well as the JSON payload.

const DEFAULT_BASE = "http://127.0.0.1:9102";

export class SchemaServiceClient {
  constructor(base = DEFAULT_BASE) {
    this.base = base;
  }

  async #req(method, path, body) {
    const res = await fetch(this.base + path, {
      method,
      headers: {
        ...(body ? { "content-type": "application/json" } : {}),
        connection: "close",
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    const text = await res.text();
    let json;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = text;
    }
    return { status: res.status, body: json };
  }

  health() {
    return this.#req("GET", "/v1/health");
  }

  // POST /v1/schemas -> { schema, mutation_mappers, replaced_schema?, system }
  // status: 201 Added | 201 Expanded(replaced_schema set) | 200 AlreadyExists | 409 Conflict
  addSchema(schema, mutation_mappers = {}) {
    return this.#req("POST", "/v1/schemas", { schema, mutation_mappers });
  }

  // Sled-only wipe of the registry. Lambda rejects this.
  reset() {
    return this.#req("POST", "/v1/system/reset", { confirm: true });
  }

  // Full registry export (embeddings stripped server-side).
  snapshot() {
    return this.#req("GET", "/v1/snapshot");
  }

  // Lightweight: just the names of every registered schema (seeds + user).
  listSchemas() {
    return this.#req("GET", "/v1/schemas");
  }

  // Full definitions filtered by provenance. source=user excludes the 914
  // schema.org seeds + persona seeds, leaving only what this run pushed.
  availableUser() {
    return this.#req("GET", "/v1/schemas/available?source=user");
  }

  // NON-DESTRUCTIVE probe: what would each proposal merge into, without
  // mutating state. entries: [{ descriptive_name, fields }]
  batchCheckReuse(entries) {
    return this.#req("POST", "/v1/schemas/batch-check-reuse", { schemas: entries });
  }

  // Eval: cosine field-context scores for embedding-beam component cover.
  // left/right: [{ id, text }], topK optional (default 24).
  fieldMatchProbe(left, right, topK = 24) {
    return this.#req("POST", "/v1/debug/field-match-probe", {
      left,
      right,
      top_k: topK,
    });
  }
}
