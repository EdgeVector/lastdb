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

/** Provision by a new local name or verify an explicitly selected database. */
export type SocialSetupRequest = {
  setupName: string;
  requiredVersion?: number;
} & (
  { localName: string; databaseId?: never; finishExisting?: never }
  | { databaseId: string; localName?: never; finishExisting?: true }
);

export interface SocialDatabaseSetup {
  databaseId: string;
  name: string | null;
  setupName: string;
  version: number;
  /** Verified installed definition IDs keyed by canonical schema name. */
  definitions: Record<string, string>;
}

type SocialGrantScope =
    { writeSchemas: string[] | 'all'; preset?: never }
    | { preset: 'viewer' | 'commenter' | 'album-organizer'; writeSchemas?: never };

export type SocialSharingChange =
  | ({ operation: 'setCircleBinding'; databaseId: string; circleId: string } & SocialGrantScope)
  | { operation: 'removeCircleBinding'; databaseId: string; circleId: string }
  | ({ operation: 'setPersonGrant' | 'restorePerson'; databaseId: string; nodeKey: string } & SocialGrantScope)
  | { operation: 'revokePerson'; databaseId: string; nodeKey: string };

export interface SocialSharingRequest {
  requestId: string;
  /** Display label for the app that proposed the change. */
  appId: string;
  change: SocialSharingChange;
}

export interface SocialSharingStatus {
  requestId: string;
  databaseId: string;
  status: 'awaitingApproval' | 'pending' | 'confirmed' | 'needsReview' | 'declined';
  revision?: string;
  error?: string;
}

/** Share with people through Social. Use `connect` and `select` for content. */
export function socialClient({ dataDir, socialPath = 'social', lastdbPath }: SocialClientOptions) {
  const baseArgs = ['--data-dir', dataDir];
  if (lastdbPath !== undefined) baseArgs.push('--lastdb', lastdbPath);

  function run(action: string, args: string[], input?: unknown): Promise<unknown> {
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
    /** List the owner's reusable circle labels and stable IDs. */
    async listCircles(): Promise<{ id: string; name: string }[]> {
      const value = await run('list-circles', []);
      if (!Array.isArray(value) || !value.every((item: unknown): item is { id: string; name: string } =>
        isObject(item) && typeof item['id'] === 'string' && typeof item['name'] === 'string')) {
        throw new UnexpectedResponseError('Social returned an invalid circle list', 0, null);
      }
      return value.map(({ id, name }) => ({ id, name }));
    },

    /** Save a declaration for approval in Social. Retry with the same UUID and change. */
    async requestSharing(request: SocialSharingRequest): Promise<SocialSharingStatus> {
      const receipt = parseSharingStatus(await run('request-sharing', [], request), request.requestId);
      if (receipt.databaseId !== request.change.databaseId) {
        throw new UnexpectedResponseError('Social returned sharing status for a different database', 0, null);
      }
      return receipt;
    },

    /** Read the outcome using the UUID assigned when the app proposed the change. */
    async getSharingStatus(requestId: string): Promise<SocialSharingStatus> {
      return parseSharingStatus(await run('sharing-status', ['--', requestId]), requestId);
    },

    /** Create a named database using Social's original filtered sharing format. */
    async createDatabase(name: string): Promise<ListedDatabase> {
      return parseListedDatabase(await run('create-db', ['--', name]));
    },

    /** Create an empty named whole-database share for an app's own schemas. */
    async createSharedDatabase(name: string): Promise<ListedDatabase> {
      const database = parseListedDatabase(await run('create-db', ['--whole-database', '--', name]));
      if (database.sharingMode !== 'whole_database') {
        throw new UnexpectedResponseError('Social did not create a whole-database share', 0, null);
      }
      return database;
    },

    /** Open a versioned database setup using Social's catalog. */
    async setupDatabase(request: SocialSetupRequest): Promise<SocialDatabaseSetup> {
      const args = [`--setup=${request.setupName}`, `--version=${request.requiredVersion ?? 1}`];
      if (request.localName !== undefined) args.push(`--name=${request.localName}`);
      if (request.databaseId !== undefined) args.push(`--database=${request.databaseId}`);
      if (request.finishExisting) args.push('--finish-existing');
      return parseSetup(await run('setup-database', args), request);
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

    /** Seal an invitation for a verified contact with an existing whole-database grant. */
    async sealInvitation(databaseId: string, nodeKey: string): Promise<SocialInvitation> {
      const result = parseInvitation(await run(
        'seal-invitation', [`--database=${databaseId}`, `--node-key=${nodeKey}`],
      ));
      if (result.databaseId !== databaseId) {
        throw new UnexpectedResponseError('Social returned an invitation for a different database', 0, null);
      }
      return result;
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

function parseSharingStatus(value: unknown, expectedId: string): SocialSharingStatus {
  if (isObject(value)) {
    const { requestId, databaseId, status, revision, error } = value;
    if (requestId === expectedId && typeof databaseId === 'string'
      && (status === 'awaitingApproval' || status === 'pending' || status === 'needsReview'
        || status === 'confirmed' || status === 'declined')
      && (revision === undefined || typeof revision === 'string')
      && (error === undefined || typeof error === 'string')
      && (status !== 'confirmed' || typeof revision === 'string')) {
      return { requestId, databaseId, status, ...(revision === undefined ? {} : { revision }),
        ...(error === undefined ? {} : { error }) };
    }
  }
  throw new UnexpectedResponseError('Social returned invalid sharing status', 0, null);
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

function parseSetup(result: unknown, request: SocialSetupRequest): SocialDatabaseSetup {
  if (isObject(result)) {
    const { databaseId, name, setupName, version, definitions } = result;
    if (typeof databaseId === 'string' && databaseId.length > 0
      && (request.databaseId === undefined || databaseId === request.databaseId)
      && (name === null || typeof name === 'string')
      && typeof setupName === 'string' && setupName === request.setupName
      && typeof version === 'number' && version === (request.requiredVersion ?? 1)
      && isObject(definitions)) {
      const entries = Object.entries(definitions);
      if (entries.length > 0 && entries.every(([, id]) => typeof id === 'string' && /^[0-9a-f]{64}$/.test(id))) {
        return { databaseId, name, setupName, version, definitions: definitions as Record<string, string> };
      }
    }
  }
  throw new UnexpectedResponseError('Social returned an invalid database setup', 0, null);
}
