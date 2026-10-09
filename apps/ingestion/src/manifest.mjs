import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const APP_ID = 'ingestion';
export const APP_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

export const STATE_SCHEMAS = [
  'ingestion/AppStatus',
  'ingestion/Run',
  'ingestion/ProgressEvent',
  'ingestion/AppError',
];

export const OUTPUT_SCHEMAS = ['ingestion/IngestedFile'];

export const COMMANDS = [
  {
    name: 'list',
    mutates: false,
    description: 'List declared zero-UI apps in the local scaffold registry.',
  },
  {
    name: 'inspect',
    mutates: false,
    description: 'Print the Ingestion manifest, schemas, and command contract.',
  },
  {
    name: 'status',
    mutates: false,
    description: 'Return a machine-readable readiness/status snapshot.',
  },
  {
    name: 'ingest-file',
    mutates: true,
    description: 'Summarize one file and persist app-scoped run/progress/result rows through @lastdb/app-sdk.',
  },
];

export const APP_MANIFEST = {
  app_id: APP_ID,
  display_name: 'Ingestion',
  version: '0.1.0',
  zero_ui: true,
  persistent_ui: false,
  description:
    'Ingests explicitly supplied files as a zero-UI LastDB sidecar, writing app-owned progress and result state through @lastdb/app-sdk.',
  owned_outputs: OUTPUT_SCHEMAS,
  state_schemas: STATE_SCHEMAS,
  commands: COMMANDS,
  lifecycle: {
    status: 'return readiness for node target, schemas, trust/capability, and last known run state',
    ingest_file:
      'read one explicitly supplied file, write Run/ProgressEvent/IngestedFile rows via the SDK, then query the result back',
  },
  boundaries: {
    core_owned: ['cloud sync', 'node storage engine', 'act/mutation execution', 'schema registration enforcement'],
    app_owned: ['Run', 'ProgressEvent', 'IngestedFile', 'AppStatus', 'AppError'],
    out_of_scope: ['persistent product UI', 'AI-heavy extraction', 'implicit broad scans', 'private LastDB internals'],
  },
};

export function schemaFiles() {
  const schemaDir = path.join(APP_ROOT, 'schemas');
  return fs
    .readdirSync(schemaDir)
    .filter((name) => name.endsWith('.schema.json'))
    .sort()
    .map((name) => path.join(schemaDir, name));
}

export function loadSchemas() {
  return schemaFiles().map((file) => JSON.parse(fs.readFileSync(file, 'utf8')));
}

export function listApps() {
  return [
    {
      app_id: APP_ID,
      display_name: APP_MANIFEST.display_name,
      path: 'apps/ingestion',
      zero_ui: true,
      persistent_ui: false,
      commands: COMMANDS.map((command) => command.name),
      status_schema: 'ingestion/AppStatus',
    },
  ];
}

export function inspectApp() {
  const schemas = loadSchemas().map((schema) => ({
    name: schema.name,
    schema_type: schema.schema_type,
    key: schema.key,
    fields: schema.fields,
  }));
  return {
    ...APP_MANIFEST,
    manifest_file: 'apps/ingestion/folddb.toml',
    schemas,
  };
}

export function firstNonEmpty(...values) {
  for (const value of values) {
    if (typeof value === 'string' && value.length > 0) {
      return value;
    }
  }
  return null;
}

export function nodeTarget(env) {
  return firstNonEmpty(
    env.INGESTION_NODE_TARGET,
    env.INGESTION_SOCKET_PATH,
    env.FOLDDB_SOCKET_PATH,
    env.FOLDDB_SOCK,
    env.INGESTION_BASE_URL,
  );
}

function nodeKind(target) {
  if (!target) return 'none';
  if (target.startsWith('unix:') || target.endsWith('.sock')) return 'uds';
  if (target.startsWith('http://') || target.startsWith('https://')) return 'http';
  return 'unknown';
}

export function buildStatus(env = process.env) {
  const target = nodeTarget(env);
  const errors = [];
  const nextActions = [];

  if (!target) {
    errors.push({
      kind: 'missing_node_target',
      message: 'Set INGESTION_SOCKET_PATH, FOLDDB_SOCKET_PATH, or INGESTION_BASE_URL before running ingestion.',
      retryable: true,
    });
    nextActions.push('set INGESTION_SOCKET_PATH, FOLDDB_SOCKET_PATH, or INGESTION_BASE_URL');
  }

  return {
    ok: errors.length === 0,
    app_id: APP_ID,
    zero_ui: true,
    command: 'status',
    node: {
      target,
      kind: nodeKind(target ?? ''),
      health: target ? 'unknown' : 'unknown',
    },
    lifecycle: errors.length === 0 ? 'ready' : 'blocked',
    schemas: {
      required: [...STATE_SCHEMAS, ...OUTPUT_SCHEMAS],
      loaded: [],
      missing: [...STATE_SCHEMAS, ...OUTPUT_SCHEMAS],
      migrations_pending: [],
    },
    trust: {
      mode: target ? 'capability-or-dev-trust' : 'none',
      state: target ? 'unknown' : 'missing',
      detail: target ? null : 'no node target configured',
    },
    last_run: null,
    progress: {
      status: 'not_started',
      scanned_files: 0,
      written_records: 0,
      message: 'No ingestion run has been started by this scaffold.',
    },
    errors,
    next_actions: nextActions,
  };
}
