import { execFile } from 'node:child_process';

import { parseListedDatabase, type ListedDatabase } from './databases.js';
import { LastDbError, UnexpectedResponseError } from './errors.js';
import { isObject } from './responseParsers.js';

/** Use the same LastDB home for Social and the content client. */
export interface SocialClientOptions {
  dataDir: string;
  /** Installed Social command or explicit executable path. Defaults to `social`. */
  socialPath?: string;
  /** LastDB executable to use when it is outside the caller's PATH. */
  lastdbPath?: string;
}

/** A sealed invitation for the caller to hand to the verified recipient. */
export interface SocialInvitation {
  databaseId: string;
  revision: string;
  publisher: string;
  invitation: string;
}

/** Share with people through Social. Use `connect` and `select` for content. */
export function socialClient({ dataDir, socialPath = 'social', lastdbPath }: SocialClientOptions) {
  const baseArgs = ['--data-dir', dataDir];
  if (lastdbPath !== undefined) baseArgs.push('--lastdb', lastdbPath);

  function run(action: string, args: string[], input?: SocialInvitation): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const child = execFile(socialPath, [...baseArgs, action, ...args], {
        timeout: 180_000,
        maxBuffer: 1024 * 1024,
      }, (error, stdout, stderr) => {
        if (error) {
          const detail = stderr.split('\n').find((line) => line.startsWith('social: '));
          reject(new LastDbError(detail ?? `Social ${action} failed (${error.code ?? 'interrupted'})`));
          return;
        }
        try {
          resolve(JSON.parse(stdout));
        } catch {
          reject(new UnexpectedResponseError(`Social ${action} returned invalid JSON`, 0, null));
        }
      });
      // A child can reject its arguments and close stdin before reading.
      child.stdin?.on('error', (error: NodeJS.ErrnoException) => {
        if (error.code !== 'EPIPE') reject(new LastDbError(`Could not send input to Social ${action}`));
      });
      child.stdin?.end(input === undefined ? undefined : JSON.stringify(input));
    });
  }

  return {
    /** Create a named database using Social's built-in sharing format. */
    async createDatabase(name: string): Promise<ListedDatabase> {
      return parseListedDatabase(await run('create-db', ['--', name]));
    },

    /** Omit contactPath when Social already remembers the verified recipient. */
    async inviteReader(database: string, recipient: {
      name: string;
      nodeKey: string;
      contactPath?: string;
    }): Promise<SocialInvitation> {
      const args = [`--database=${database}`, `--user=${recipient.name}`, `--node-key=${recipient.nodeKey}`];
      if (recipient.contactPath !== undefined) args.push(`--contact=${recipient.contactPath}`);
      return parseInvitation(await run('add', args));
    },

    /** Accept using the independently verified publisher key. */
    async acceptInvitation(
      invitation: SocialInvitation,
      publisherKey: string,
      name?: string,
    ): Promise<ListedDatabase> {
      const args = [`--publisher=${publisherKey}`];
      if (name !== undefined) args.push(`--name=${name}`);
      const database = parseListedDatabase(await run('accept', args, invitation));
      if (database.id !== invitation.databaseId) {
        throw new UnexpectedResponseError('Social accepted a different database', 0, null);
      }
      return database;
    },

    /** Revoke a person's grant and return the applied revision. */
    async revokeAccess(database: string, nodeKey: string): Promise<string> {
      const result = await run('remove', [`--database=${database}`, `--node-key=${nodeKey}`]);
      if (!isObject(result) || typeof result['revision'] !== 'string' || result['state'] !== 'revoked'
        || result['nodeKey'] !== nodeKey || typeof result['databaseId'] !== 'string') {
        throw new UnexpectedResponseError('Social returned an invalid revoke receipt', 0, null);
      }
      return result['revision'];
    },
  };
}

function parseInvitation(result: unknown): SocialInvitation {
  if (isObject(result)) {
    const { databaseId, revision, publisher, invitation } = result;
    if (typeof databaseId === 'string' && typeof revision === 'string'
      && typeof publisher === 'string' && typeof invitation === 'string' && invitation.length > 0) {
      return { databaseId, revision, publisher, invitation };
    }
  }
  throw new UnexpectedResponseError('Social returned an invalid invitation receipt', 0, null);
}
