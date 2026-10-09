#!/usr/bin/env node
import {
  buildStatus,
  inspectApp,
  listApps,
} from '../src/manifest.mjs';
import { runFileIngestion } from '../src/fileIngestion.mjs';

const command = process.argv[2] ?? 'help';
const asJson = process.argv.includes('--json') || process.env.INGESTION_JSON === '1';

function optionValue(name) {
  const index = process.argv.indexOf(name);
  if (index === -1) return null;
  return process.argv[index + 1] ?? null;
}

function emit(value, exitCode = 0) {
  if (asJson || typeof value !== 'string') {
    process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
  } else {
    process.stdout.write(`${value}\n`);
  }
  process.exitCode = exitCode;
}

try {
  switch (command) {
    case 'list':
      emit({ ok: true, apps: listApps() });
      break;
    case 'inspect':
      emit({ ok: true, app: inspectApp() });
      break;
    case 'status': {
      const status = buildStatus(process.env);
      emit(status, status.ok ? 0 : 2);
      break;
    }
    case 'ingest-file': {
      const filePath = optionValue('--path') ?? optionValue('-p');
      const result = await runFileIngestion({ filePath });
      emit(result, result.ok ? 0 : 1);
      break;
    }
    case 'help':
    case '--help':
    case '-h':
      emit([
        'Usage: ingestion-app <command> [--json]',
        '',
        'Commands:',
        '  list                  List declared zero-UI apps in this scaffold.',
        '  inspect               Print the Ingestion manifest, schemas, and command contract.',
        '  status                Print a machine-readable readiness snapshot.',
        '  ingest-file --path F  Summarize one file and persist SDK-backed ingestion state.',
      ].join('\n'));
      break;
    default:
      emit({
        ok: false,
        error: {
          kind: 'unknown_command',
          command,
          message: `unknown ingestion command: ${command}`,
          retryable: true,
          next_actions: ['run `ingestion-app help`'],
        },
      }, 64);
  }
} catch (err) {
  emit({
    ok: false,
    error: {
      kind: err?.code ?? err?.reason ?? 'ingestion_failed',
      message: err?.message ?? String(err),
      retryable: true,
    },
  }, err?.code === 'EMISSINGPATH' ? 64 : 1);
}
