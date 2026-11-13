import { readFileSync } from 'node:fs';

import { socialClient } from '../../lastdb_app_sdk/dist/index.js';

const [action, socialPath, dataDir, lastdbPath] = process.argv.slice(2);
const social = socialClient({ dataDir, socialPath, lastdbPath });
const input = JSON.parse(readFileSync(0, 'utf8'));

try {
  let result;
  switch (action) {
    case 'create':
      result = await social.createDatabase(input.name, input.spec);
      break;
    case 'invite':
      result = await social.inviteReader(input.databaseId, input.recipient);
      break;
    case 'accept':
      result = await social.acceptInvitation(input.receipt, input.publisher, input.name);
      break;
    case 'revoke':
      result = await social.revokeAccess(input.databaseId, input.nodeKey);
      break;
    default:
      throw new Error('Unknown SDK operation');
  }
  process.stdout.write(JSON.stringify(result));
} catch {
  // Keep invitation material and captured replies off the workflow output.
  process.exitCode = 1;
}
