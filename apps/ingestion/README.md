# Ingestion App Scaffold

Ingestion is a zero-UI LastDB sidecar app. It talks to the node through
`@lastdb/app-sdk`, owns `ingestion/*` state, and starts with one deterministic
file-summary slice instead of the old hidden UI ingestion surface.

## Commands

```bash
node apps/ingestion/bin/ingestion-app.mjs list --json
node apps/ingestion/bin/ingestion-app.mjs inspect --json
node apps/ingestion/bin/ingestion-app.mjs status --json
node apps/ingestion/bin/ingestion-app.mjs ingest-file --path apps/ingestion/fixtures/sample-note.txt --json
```

`ingest-file` connects with `INGESTION_SOCKET_PATH`/`FOLDDB_SOCKET_PATH` for a
dev-node UDS target, or `INGESTION_BASE_URL` for a production HTTP target. It
writes `ingestion/Run`, `ingestion/ProgressEvent`, and `ingestion/IngestedFile`
rows through SDK `mutate()` calls, then queries the result back through SDK
`query()` to prove the app-owned record is readable.

Production nodes use the SDK consent flow. If no stored capability exists, the
command prints the `folddb consent grant ingestion` action and waits for the
grant. Dev nodes use `folddb app trust ingestion --binary "$(command -v node)"`
before running the command. Tests and explicit dev harnesses may set
`INGESTION_CAPABILITY` to pass a preloaded SDK capability token.

## Local Validation

```bash
node apps/ingestion/test/scaffold.test.mjs
```

The focused test suite injects a fake SDK client into the same ingestion runner
so it can verify the exact app-scoped SDK writes without touching a real node.
The e2e contract test starts a LastDB-shaped server on a real Unix socket and
runs the CLI in a separate process with `INGESTION_CAPABILITY=ingestion`; the
server only accepts SDK data-path requests for the `ingestion/*` scope and
returns stable denial errors for the wrong app capability.
