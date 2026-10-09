// Emit a `lastdb app …` manifest (app_id, metadata, version, uses[], schemas[],
// optional source/run) from a first-party app's real `src/schemas.ts` module —
// the same `schemaFor()` definitions the app already declares on its own local
// Mini. Used by app-registry-first-party-dogfood.sh so the dogfood manifest is
// never a hand-written fixture; it is always derived from the shipped schema
// source.
//
// Env in (required):
//   APP_ID, DISPLAY_NAME, DESCRIPTION, HOMEPAGE_URL, SCHEMAS_MODULE, OUT_FILE
// Env in (optional):
//   PASS_TYPES     — csv of record types (empty = every RECORD_TYPES entry)
//   VERSION        — SemVer x.y.z (default 0.1.0; required by lastdb app publish)
//   SOURCE         — git-cloneable source pointer published into the registry
//   RUN_RUNTIME    — with RUN_ENTRYPOINT, emit a local `run` block for `lastdb app run`
//   RUN_ENTRYPOINT — relative path inside the source checkout
//   RUN_ARGS       — csv of default args prepended before CLI extras

const appId = must("APP_ID");
const displayName = must("DISPLAY_NAME");
const description = must("DESCRIPTION");
const homepageUrl = must("HOMEPAGE_URL");
const schemasModule = must("SCHEMAS_MODULE");
const outFile = must("OUT_FILE");
const version = (process.env.VERSION ?? "0.1.0").trim() || "0.1.0";
const source = (process.env.SOURCE ?? "").trim();
const runRuntime = (process.env.RUN_RUNTIME ?? "").trim();
const runEntrypoint = (process.env.RUN_ENTRYPOINT ?? "").trim();
const passTypes = (process.env.PASS_TYPES ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const runArgs = (process.env.RUN_ARGS ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

function must(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`generate-app-manifest: missing env ${name}`);
  return v;
}

if (!/^\d+\.\d+\.\d+$/.test(version)) {
  throw new Error(
    `generate-app-manifest: VERSION must be SemVer x.y.z (got ${JSON.stringify(version)})`,
  );
}
if ((runRuntime && !runEntrypoint) || (!runRuntime && runEntrypoint)) {
  throw new Error(
    "generate-app-manifest: RUN_RUNTIME and RUN_ENTRYPOINT must be set together",
  );
}

const mod = await import(schemasModule);
const { RECORD_TYPES, schemaFor } = mod as {
  RECORD_TYPES: readonly string[];
  schemaFor: (t: string) => { schema: unknown };
};

const types = passTypes.length > 0 ? passTypes : [...RECORD_TYPES];
const schemas: unknown[] = [];
for (const t of types) {
  if (!RECORD_TYPES.includes(t)) {
    throw new Error(
      `generate-app-manifest: ${appId} has no record type "${t}" (known: ${RECORD_TYPES.join(", ")})`,
    );
  }
  schemas.push(schemaFor(t).schema);
}

const manifest: Record<string, unknown> = {
  app_id: appId,
  metadata: {
    display_name: displayName,
    description,
    homepage_url: homepageUrl,
  },
  version,
  uses: [],
  schemas,
};

if (source) {
  manifest.source = source;
}

if (runRuntime && runEntrypoint) {
  const run: Record<string, unknown> = {
    runtime: runRuntime,
    entrypoint: runEntrypoint,
  };
  if (runArgs.length > 0) {
    run.args = runArgs;
  }
  manifest.run = run;
}

await Bun.write(outFile, JSON.stringify(manifest, null, 2));
const extras = [
  `version=${version}`,
  source ? "source" : null,
  runRuntime ? `run=${runRuntime}:${runEntrypoint}` : null,
]
  .filter(Boolean)
  .join(", ");
console.log(
  `wrote manifest for ${appId}: ${schemas.length} schemas (${types.join(", ")}); ${extras} -> ${outFile}`,
);
