// Read actual app records by supplied keys. This helper never creates data.
import { createHash } from "node:crypto";

const require = (ok, message) => { if (!ok) throw Error(message); };
const payload = (value) => value?.data ?? value;
const stable = (value) => JSON.stringify(ordered(value));
const digest = (value) => createHash("sha256").update(stable(value)).digest("hex");
// Reserve half of the held native 10,000-key budget for controls and metadata.
const PLANNED_KEY_BUDGET = 5000;
const MAX_BATCH_QUERIES = 64;
const MAX_PAGE_ROWS = 128;

function ordered(value) {
  if (Array.isArray(value)) return value.map(ordered);
  if (value && typeof value === "object")
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, ordered(value[key])]));
  return value;
}

function fieldProfile(cfg, runtime, definition) {
  const names = cfg.schemaFieldAliases?.[runtime] ?? {};
  const inverse = new Map(Object.entries(names).map(([logical, canonical]) => [canonical, logical]));
  require(inverse.size === Object.keys(names).length, "The snapshot field profile is not one-to-one");
  const logical = (name) => inverse.get(name) ?? name;
  require(Array.isArray(definition.fields), "The snapshot source lacks declared fields");
  const declared = definition.fields.map(logical).sort();
  require(new Set(declared).size === declared.length, "The snapshot field names collide");
  const normalize = (values) => {
    require(values && typeof values === "object" && !Array.isArray(values), "A snapshot record lacks its fields");
    const result = Object.create(null);
    for (const [name, value] of Object.entries(values)) {
      const field = logical(name);
      require(declared.includes(field) && !Object.hasOwn(result, field), "A snapshot field is outside its exact profile");
      result[field] = value;
    }
    return result;
  };
  return { declared, normalize, key: {
    hash_field: logical(definition.key.hash_field),
    range_field: definition.key.range_field ? logical(definition.key.range_field) : null,
  }};
}

function rowWitness(row, state) {
  require(typeof row.key?.hash === "string" && row.key.hash.length > 0, "A snapshot record lacks its primary hash");
  const ranged = state.key.range_field !== null;
  require(ranged ? typeof row.key.range === "string" : row.key.range == null || row.key.range === "",
    "A snapshot record changed its primary key shape");
  const key = { hash: row.key.hash, range: ranged ? row.key.range : null };
  const fields = state.profile.normalize(row.fields), metadata = state.profile.normalize(row.metadata);
  require(fields[state.key.hash_field] === key.hash && (!ranged || fields[state.key.range_field] === key.range),
    "The stored primary fields differ from the actual query key");
  const references = {};
  for (const [field, value] of Object.entries(fields)) {
    const info = metadata[field];
    require(info && typeof info === "object" && !Array.isArray(info),
      "A present snapshot field lacks its actual atom metadata");
    require(typeof info.atom_uuid === "string" && info.atom_uuid.length > 0, "A snapshot field lacks its current atom");
    // New owned molecules are expected. Every other public tip attribute must
    // remain exact, including an absent writer on an unsigned local tip.
    references[field] = Object.fromEntries(Object.entries(info).filter(([name]) => name !== "molecule_uuid"));
  }
  require(Object.keys(metadata).every((field) => Object.hasOwn(fields, field)), "Snapshot metadata has no field value");
  return { key, fields: Object.keys(fields).length, atoms: Object.keys(references).length,
    values_sha256: digest(fields), references_sha256: digest(references) };
}

function queryWindow(filter, definition, width) {
  require(Number.isSafeInteger(width) && width > 0, "The snapshot needs a declared field width");
  const costPerRow = 1 + 2 * width;
  const boundedRows = Math.min(MAX_PAGE_ROWS, Math.floor(PLANNED_KEY_BUDGET / costPerRow));
  require(boundedRows > 0, "One full snapshot record exceeds its planned native budget");
  let rows;
  if (typeof filter.HashKey === "string") {
    rows = definition.schema_type === "Hash" ? 1 : boundedRows;
  } else {
    rows = filter.HashRangeKeys.length;
    require(rows > 0 && rows <= MAX_PAGE_ROWS && rows * costPerRow <= PLANNED_KEY_BUDGET,
      "The explicit snapshot key batch exceeds its full-field native budget");
  }
  return { limit: Math.min(MAX_PAGE_ROWS, rows), plannedKeys: rows * costPerRow };
}

function queryBatches(entries) {
  const chunks = [];
  let chunk = [], plannedKeys = 0;
  for (const entry of entries) {
    require(entry.plannedKeys > 0 && entry.plannedKeys <= PLANNED_KEY_BUDGET,
      "A snapshot query exceeds its planned native budget");
    if (chunk.length === MAX_BATCH_QUERIES || plannedKeys + entry.plannedKeys > PLANNED_KEY_BUDGET) {
      chunks.push(chunk);
      chunk = [];
      plannedKeys = 0;
    }
    chunk.push(entry);
    plannedKeys += entry.plannedKeys;
  }
  if (chunk.length) chunks.push(chunk);
  return chunks;
}

async function readPages(node, entries, states) {
  let pending = entries;
  for (let page = 0; pending.length && page < 10000; page++) {
    const response = await node.rawCall("POST", "/api/queries/batch", { queries: pending.map((entry) => entry.query) });
    const body = payload(response.json);
    require(response.status === 200 && body?.count === pending.length && Array.isArray(body.results)
      && body.results.length === pending.length, "The actual snapshot query batch is incomplete");
    const next = [];
    for (let index = 0; index < pending.length; index++) {
      const entry = pending[index], item = body.results[index], reply = payload(item.response), state = states.get(entry.name);
      require(item.status === 200 && reply?.ok === true && reply.unresolved_rows === 0
        && Array.isArray(reply.results) && typeof reply.has_more === "boolean", "An actual snapshot page is unresolved or failed");
      for (const row of reply.results) {
        const witness = rowWitness(row, state), id = stable(witness.key);
        const filter = entry.query.filter;
        require(typeof filter.HashKey === "string" ? witness.key.hash === filter.HashKey
          : filter.HashRangeKeys.some(([hash, range]) => witness.key.hash === hash && witness.key.range === range),
        "An actual snapshot read returned another key");
        require(!state.records.has(id), "An actual snapshot key repeats");
        state.records.set(id, witness);
        require(state.records.size <= state.expected, "The snapshot exceeds its actual generated cohort");
      }
      if (reply.has_more) {
        const cursor = reply.next_cursor, id = stable(cursor);
        require(cursor != null && cursor !== "" && !entry.seen.has(id), "The actual snapshot cursor did not advance");
        entry.seen.add(id);
        next.push({ ...entry, query: { ...entry.query, cursor } });
      }
    }
    pending = next;
  }
  require(pending.length === 0, "The actual snapshot exceeded its page limit");
}

/** Scopes contain only actual app hashes or exact keys from the generated workload. */
export async function captureKeyedSnapshot(node, cfg, scopes, ownerAppId) {
  require(typeof ownerAppId === "string" && ownerAppId.length > 0
    && scopes.length > 0 && scopes.length <= 64, "The snapshot needs an owner and bounded app schemas");
  const states = new Map(), identities = {};
  const definitions = await Promise.all(scopes.map(async (scope) => {
    const runtime = cfg.schemaHashes[scope.name];
    require(typeof runtime === "string" && runtime, "A persisted snapshot binding is absent");
    const response = await node.rawCall("GET", "/api/schema/" + encodeURIComponent(runtime));
    const body = payload(response.json), definition = body?.schema ?? body;
    require(response.status === 200 && definition?.name === runtime && definition.owner_app_id === ownerAppId
      && ["Hash", "HashRange"].includes(definition.schema_type) && typeof definition.key?.hash_field === "string",
    "The actual snapshot definition differs from its app binding");
    require((definition.schema_type === "HashRange") === Boolean(definition.key.range_field),
      "The actual snapshot range layout is incomplete");
    require(cfg.storageModel !== "multi-key-v1" || definition.independent_fields === true,
      "A converted or fresh snapshot schema does not own its values");
    return { scope, runtime, definition };
  }));
  require(new Set(definitions.map(({ runtime }) => runtime)).size === scopes.length, "Two snapshot scopes use one runtime schema");
  const entries = [];
  for (const { scope, runtime, definition } of definitions) {
    require(!states.has(scope.name) && Number.isInteger(scope.expected) && scope.expected >= 0,
      "The snapshot scope or expected membership is invalid");
    const profile = fieldProfile(cfg, runtime, definition);
    states.set(scope.name, { runtime, profile, key: profile.key, expected: scope.expected, records: new Map() });
    identities[scope.name] = definition.identity_hash;
    for (const filter of scope.filters) {
      require(filter && typeof filter === "object" && Object.keys(filter).length === 1
        && (typeof filter.HashKey === "string" || Array.isArray(filter.HashRangeKeys)), "A snapshot read has no declared key");
      const window = queryWindow(filter, definition, profile.declared.length);
      entries.push({ name: scope.name, seen: new Set(), plannedKeys: window.plannedKeys, query: {
        schema_name: runtime, fields: [], filter, limit: window.limit, response_format: "full",
      }});
    }
  }
  const chunks = queryBatches(entries);
  for (let start = 0; start < chunks.length; start += 4)
    await Promise.all(chunks.slice(start, start + 4).map((chunk) => readPages(node, chunk, states)));
  const schemas = {};
  for (const [name, state] of states) {
    require(state.records.size === state.expected, "Actual snapshot membership differs for " + name);
    schemas[name] = { runtime: state.runtime, fields: state.profile.declared, key: state.key,
      records: [...state.records.values()].sort((left, right) => stable(left.key).localeCompare(stable(right.key))) };
  }
  return { version: 1, identities, schemas };
}

export function assertSnapshotsEqual(before, after, names = Object.keys(before.schemas)) {
  require(before.version === 1 && after.version === 1, "The actual snapshot format differs");
  for (const name of names)
    require(before.schemas[name] && after.schemas[name]
      && stable(before.schemas[name]) === stable(after.schemas[name]),
    "Actual values, keys, current atoms, or author metadata changed for " + name);
}
