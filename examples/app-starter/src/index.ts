/**
 * app-starter — a minimal app on lastdb using @lastdb/app-sdk.
 *
 * The full app lifecycle in ~40 lines:
 *   connect -> (first run) requestConsent + awaitConsent -> mutate -> query.
 *
 * Two ways to reach a node, picked by environment (see the README):
 *
 *   - PRODUCTION/shared node: set APP_STARTER_BASE_URL to its TCP HTTP
 *     surface, e.g. http://127.0.0.1:9001. The node serves the consent
 *     endpoints, so on first run this app asks for `app-starter/*` and waits
 *     for the owner to grant consent.
 *
 *   - LOCAL Mini daemon (`lastdbd`): the data surface is on the owner Unix
 *     socket (`<LASTDB_HOME>/data/folddb.sock`); set APP_STARTER_SOCKET_PATH
 *     to that socket. Socket runs are owner-local and skip the production
 *     consent flow.
 *
 * See e2e/roundtrip.mjs for a fully scripted run against an ephemeral node.
 */

import { connect } from '@lastdb/app-sdk';

const APP_ID = 'app-starter';
const SCHEMA = 'app-starter/Note';

async function main(): Promise<void> {
  const socketPath = process.env.APP_STARTER_SOCKET_PATH;
  const baseUrl = process.env.APP_STARTER_BASE_URL ?? 'http://127.0.0.1:9001';
  const defaultHeaders = process.env.APP_STARTER_USER_HASH
    ? { 'X-User-Hash': process.env.APP_STARTER_USER_HASH }
    : undefined;

  // Connect. Local/test nodes expose the app data path over a Unix socket, so
  // prefer an explicit socket when one is configured; otherwise use the
  // production TCP surface.
  const fold = socketPath
    ? await connect({ socketPath, appId: APP_ID, defaultHeaders })
    : await connect({ baseUrl, appId: APP_ID, defaultHeaders });
  console.log(`connected to ${fold.target} as app '${fold.appId}'`);

  // First run on a production node: ask the owner for `app-starter/*`, then
  // wait for them to grant it. Socket-local runs are owner-local and skip this,
  // so this is only reached on the TCP/production path.
  if (!fold.hasCapability && !socketPath) {
    const { requestId } = await fold.requestConsent('wildcard');
    console.log('\nThis app needs your consent. In another terminal, run:');
    console.log('  folddb consent grant app-starter\n');
    console.log('waiting for the grant...');
    await fold.awaitConsent(requestId, { timeoutMs: 120_000 });
    console.log('consent granted — capability stored.');
  }

  // Write a row. `key` addresses it; pass the same key to update/delete later.
  const note = { id: 'note-1', text: `hello from app-starter @ ${new Date().toISOString()}` };
  // Hash schema (key.hash_field = "id"): address the row by hash key.
  // range is unused for Hash schemas (pass null).
  await fold.mutate(SCHEMA, {
    mutationType: 'create',
    fields: note,
    key: { hash: note.id, range: null },
  });
  console.log(`\nmutate: accepted create for ${SCHEMA}`);

  // Read it back and print the round-tripped row (the full per-row envelope).
  const { rows, rowCount } = await fold.query(SCHEMA, { fields: ['id', 'text'] });
  console.log(`query: ${SCHEMA} returned ${rowCount} row(s):`);
  for (const row of rows) {
    console.log(`  key=${row.key} fields=${JSON.stringify(row.fields)} author=${row.authorPubKey}`);
  }

  console.log('\nround-trip complete: created a row and read it back through the SDK.');
}

main().catch((err) => {
  console.error(`\napp-starter failed: ${err?.stack ?? err}`);
  process.exitCode = 1;
});
