import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { connect, socialClient } from './package/dist/index.js';

const [action, socialPath, , dataDir, , lastdbPath] = process.argv.slice(2);
const social = socialClient({ dataDir, socialPath, lastdbPath });
const input = JSON.parse(readFileSync(0, 'utf8'));

try {
  let result;
  switch (action) {
    case 'create':
      result = await social.createDatabase(input.name);
      break;
    case 'create-shared':
      result = await social.createSharedDatabase(input.name);
      break;
    case 'setup':
      result = await social.setupDatabase(input);
      break;
    case 'circles':
      result = await social.listCircles();
      break;
    case 'request-sharing':
      result = await social.requestSharing(input);
      break;
    case 'sharing-status':
      result = await social.getSharingStatus(input.requestId);
      break;
    case 'invite':
      result = await social.inviteReader(input.database, input.recipient);
      break;
    case 'seal':
      result = await social.sealInvitation(input.database, input.nodeKey);
      break;
    case 'accept':
      result = await social.acceptInvitation(input.receipt, input.publisher, input.name);
      break;
    case 'revoke':
      result = await social.revokeAccess(input.database, input.nodeKey);
      break;
    case 'read': {
      const root = await connect({ appId: input.appId ?? 'social-dev-sdk', socketPath: join(dataDir, 'data', 'folddb.sock') });
      const { client } = await root.openDatabaseSession(input.database, 'read');
      result = await client.get(input.schema, input.key);
      break;
    }
    case 'list-databases': {
      const root = await connect({ appId: 'social-dev-sdk', socketPath: join(dataDir, 'data', 'folddb.sock') });
      result = await root.listDatabases();
      break;
    }
    case 'submission-status': {
      const root = await connect({ appId: 'social-dev-sdk', socketPath: join(dataDir, 'data', 'folddb.sock') });
      const { client } = await root.openDatabaseSession(input.database, 'read');
      result = await Promise.all(input.ids.map(id => client.getShareSubmissionStatus(id)));
      break;
    }
    default:
      throw new Error('Unknown SDK operation');
  }
  process.stdout.write(JSON.stringify(result));
} catch {
  // Keep invitation material and captured replies off the workflow output.
  process.exitCode = 1;
}
