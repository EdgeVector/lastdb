import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import { APP_ID, nodeTarget } from './manifest.mjs';

export const RUN_SCHEMA = 'ingestion/Run';
export const PROGRESS_SCHEMA = 'ingestion/ProgressEvent';
export const FILE_SCHEMA = 'ingestion/IngestedFile';

function isoNow(clock = Date) {
  return new clock().toISOString();
}

function normalizePreview(buffer) {
  return buffer.toString('utf8').replace(/\s+/g, ' ').trim().slice(0, 240);
}

export async function summarizeFile(filePath, { clock = Date } = {}) {
  const absolutePath = path.resolve(filePath);
  const st = await fs.stat(absolutePath);
  if (!st.isFile()) {
    const err = new Error(`ingestion source is not a file: ${absolutePath}`);
    err.code = 'ENOTFILE';
    throw err;
  }

  const bytes = await fs.readFile(absolutePath);
  const text = bytes.toString('utf8');
  const sha256 = crypto.createHash('sha256').update(bytes).digest('hex');
  const lineCount = text.length === 0 ? 0 : text.split(/\r\n|\r|\n/).length;
  const words = text.trim().length === 0 ? [] : text.trim().split(/\s+/);
  const fileId = `file-${sha256}`;
  const runId = `run-${sha256.slice(0, 16)}`;
  const now = isoNow(clock);

  return {
    run_id: runId,
    file_id: fileId,
    source_path: absolutePath,
    base_name: path.basename(absolutePath),
    sha256,
    byte_count: bytes.length,
    line_count: lineCount,
    word_count: words.length,
    text_preview: normalizePreview(bytes),
    ingested_at: now,
  };
}

export async function defaultConnect(env = process.env) {
  const { connect } = await import('@lastdb/app-sdk');
  const target = nodeTarget(env);
  const capability = env.INGESTION_CAPABILITY || undefined;
  if (target === null) {
    const err = new Error('Set INGESTION_SOCKET_PATH, FOLDDB_SOCKET_PATH, or INGESTION_BASE_URL before running ingestion.');
    err.code = 'EMISSINGTARGET';
    throw err;
  }
  if (target.startsWith('unix:') || target.endsWith('.sock')) {
    return connect({ socketPath: target.replace(/^unix:/, ''), appId: APP_ID, capability });
  }
  const defaultHeaders = env.INGESTION_USER_HASH ? { 'X-User-Hash': env.INGESTION_USER_HASH } : undefined;
  return connect({ baseUrl: target, appId: APP_ID, defaultHeaders, capability });
}

async function ensureConsent(client, { socketMode, stdout = process.stdout }) {
  if (client.hasCapability || socketMode) {
    return;
  }
  const { requestId } = await client.requestConsent('wildcard');
  stdout.write('\nThis app needs your consent. In another terminal, run:\n');
  stdout.write('  folddb consent grant ingestion\n\n');
  stdout.write('waiting for the grant...\n');
  await client.awaitConsent(requestId, { timeoutMs: 120_000 });
  stdout.write('consent granted; capability stored.\n');
}

async function mutate(client, schema, fields, keyRange) {
  return client.mutate(schema, {
    mutationType: 'create',
    fields,
    key: { hash: null, range: keyRange },
  });
}

export async function runFileIngestion(options) {
  const {
    filePath,
    env = process.env,
    clock = Date,
    connectClient = defaultConnect,
    stdout = process.stdout,
  } = options;
  if (!filePath) {
    const err = new Error('ingest-file requires --path <file>');
    err.code = 'EMISSINGPATH';
    throw err;
  }

  const summary = await summarizeFile(filePath, { clock });
  const client = await connectClient(env);
  const target = nodeTarget(env);
  const socketMode = Boolean(target && (target.startsWith('unix:') || target.endsWith('.sock')));
  await ensureConsent(client, { socketMode, stdout });

  const startedAt = summary.ingested_at;
  await mutate(
    client,
    RUN_SCHEMA,
    {
      run_id: summary.run_id,
      operation: 'ingest_file',
      status: 'running',
      source_kind: 'file',
      source_path: summary.source_path,
      started_at: startedAt,
      finished_at: null,
      scanned_files: 0,
      written_records: 1,
      result_id: null,
      error_id: null,
    },
    summary.run_id,
  );

  await mutate(
    client,
    PROGRESS_SCHEMA,
    {
      event_id: `${summary.run_id}:started`,
      run_id: summary.run_id,
      status: 'running',
      source_path: summary.source_path,
      scanned_files: 0,
      written_records: 1,
      message: 'started deterministic file ingestion',
      created_at: startedAt,
    },
    `${summary.run_id}:started`,
  );

  await mutate(client, FILE_SCHEMA, summary, summary.file_id);

  await mutate(
    client,
    PROGRESS_SCHEMA,
    {
      event_id: `${summary.run_id}:succeeded`,
      run_id: summary.run_id,
      status: 'succeeded',
      source_path: summary.source_path,
      scanned_files: 1,
      written_records: 4,
      message: 'wrote ingestion/IngestedFile and terminal run state',
      created_at: isoNow(clock),
    },
    `${summary.run_id}:succeeded`,
  );

  await mutate(
    client,
    RUN_SCHEMA,
    {
      run_id: summary.run_id,
      operation: 'ingest_file',
      status: 'succeeded',
      source_kind: 'file',
      source_path: summary.source_path,
      started_at: startedAt,
      finished_at: isoNow(clock),
      scanned_files: 1,
      written_records: 4,
      result_id: summary.file_id,
      error_id: null,
    },
    summary.run_id,
  );

  const readBack = await client.query(FILE_SCHEMA, {
    fields: [
      'file_id',
      'run_id',
      'source_path',
      'base_name',
      'sha256',
      'byte_count',
      'line_count',
      'word_count',
      'text_preview',
      'ingested_at',
    ],
  });
  const matched = readBack.rows.find((row) => row.fields.file_id === summary.file_id) ?? null;

  return {
    ok: matched !== null,
    app_id: APP_ID,
    command: 'ingest-file',
    target: client.target,
    schemas_written: [RUN_SCHEMA, PROGRESS_SCHEMA, FILE_SCHEMA],
    run_id: summary.run_id,
    result_id: summary.file_id,
    summary,
    read_back: matched
      ? {
          key: matched.key,
          fields: matched.fields,
          author_pub_key: matched.authorPubKey,
        }
      : null,
  };
}
