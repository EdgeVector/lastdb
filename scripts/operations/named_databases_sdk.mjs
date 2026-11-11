/**
 * Create one owned database through the app SDK and read it back by key.
 *
 * The socket and expected personal value come from the CI operation.
 * Unscoped calls stay on personal. select() binds one database id.
 */
import { connect } from '../../lastdb_app_sdk/dist/index.js';

const socketPath = process.argv[2];
const personalValue = process.argv[3];

function require(value, message) {
  if (!value) {
    throw new Error(message);
  }
}

const root = await connect({ appId: 'named-db-ci', socketPath });
const personal = await root.query('RecordById', { filter: { HashKey: 'one' } });
require(personal.rows.length === 1 && personal.rows[0].fields.value === personalValue,
  'unscoped SDK query missed the personal record');
const selectedPersonal = root.select('personal');
const again = await selectedPersonal.query('RecordById', { filter: { HashKey: 'one' } });
require(again.rows[0].fields.value === personalValue, 'SDK select personal missed the record');

const listed = await root.listDatabases();
require(listed.some((row) => row.kind === 'personal' && row.access === 'owner'),
  'SDK list dropped the personal database');
const created = await root.createDatabase({ name: 'sdk-ledger' });
require(created.kind === 'shareable' && created.access === 'owner' && created.state === 'ready',
  'SDK create returned a different database');
const after = await root.listDatabases();
require(after.some((row) => row.id === created.id && row.name === 'sdk-ledger'),
  'SDK list dropped the created database');

const ledger = root.select(created.id);
const declared = await ledger.declareSchema({
  namespace: 'named-db-ci',
  schema: {
    name: 'Note',
    descriptive_name: 'SDK named database note',
    schema_type: 'Hash',
    record_keys: { primary: { kind: 'hash' }, alternates: {} },
    fields: ['id', 'value'],
    field_types: { id: 'String', value: 'String' },
    field_descriptions: {
      id: 'SDK named database note id',
      value: 'SDK named database note value',
    },
    field_versions: { id: 1, value: 1 },
  },
});
const key = { hash: 'sdk-1', range: null };
await ledger.mutate(declared.schemaName, {
  mutationType: 'create',
  expected: { type: 'absent' },
  alternates: {},
  key,
  record: { id: 'sdk-1', value: 'from-sdk' },
});
const stored = await ledger.query(declared.schemaName, { filter: { HashKey: 'sdk-1' } });
require(stored.rows.length === 1 && stored.rows[0].fields.value === 'from-sdk',
  'SDK query missed the owned record');
const untouched = await root.query('RecordById', { filter: { HashKey: 'one' } });
require(untouched.rows[0].fields.value === personalValue,
  'SDK owned mutation changed the personal record');

process.stdout.write(JSON.stringify({
  id: created.id,
  name: created.name,
  schema: declared.schemaName,
  value: 'from-sdk',
}));
