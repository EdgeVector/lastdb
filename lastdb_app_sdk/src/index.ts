/**
 * @lastdb/app-sdk — the runtime SDK for LastDB apps.
 *
 * Wraps a node's Unix-socket `/api/*` data plane.
 */

export { connect, LastDbClient } from './client.js';
export {
  parseAutoIdentityResponse,
  parseNodeVersion,
  parseSchemaListResponse,
  parseListResponse,
  parseQueryResponse,
  resolveLoadedSchema,
} from './responseParsers.js';
export type { LastDbClientOptions } from './client.js';
export type {
  CreateDatabaseRequest,
  DatabaseSpec,
  ListedDatabase,
} from './databases.js';
export { socialClient } from './social.js';
export type { SocialClientOptions, SocialInvitation } from './social.js';

export {
  LASTDB_API_ROUTES,
  LASTDB_SDK_API_VERSION,
} from './apiRoutes.js';

export { ownerClient } from './ownerClient.js';
export type { OwnerClientOptions } from './ownerClient.js';

export {
  udsTransport,
  REQUEST_ID_HEADER,
} from './transport.js';
export type { Transport, RawResponse } from './transport.js';

export type {
  ConnectOptions,
  AutoIdentityResult,
  KeyValue,
  QueryCursor,
  ListOptions,
  ListRecordKey,
  ListResult,
  LoadedSchema,
  NodeVersion,
  QueryAllOptions,
  QueryBatchItem,
  QueryBatchOutcome,
  QueryFilter,
  QueryPage,
  QueryResult,
  QueryRow,
  RecordMetadata,
  FileMarker,
  SchemaDescriptor,
  CasExpectation,
  MutationConvergence,
  MutationDurability,
  MutationOp,
  MutationOptions,
  MutationResult,
  ShareSubmissionResult,
  DeclareSchemaRequest,
  DeclareSchemaResult,
  Row,
  RowFields,
  JsonValue,
} from './types.js';

export {
  LastDbError,
  AuthenticationRequiredError,
  TransportError,
  UnexpectedResponseError,
  ShareSubmissionUncertainError,
  PermissionDeniedError,
  QueryPaginationError,
  CasConflictError,
  RequestRejectedError,
  NodeTooOldError,
  nodeTooOldMessage,
  classifyPermissionReason,
} from './errors.js';
export type {
  AuthenticationRequiredReason,
  PermissionCategory,
  CasConflictDetail,
  NodeTooOldDetail,
  NodeTooOldReason,
  TransportErrorKind,
} from './errors.js';
