/**
 * @lastdb/app-sdk — the runtime SDK for LastDB apps.
 *
 * Wraps a node's Unix-socket `/api/*` data plane.
 */

export { connect, LastDbClient } from './client.js';
export type { LastDbClientOptions } from './client.js';

export {
  parseAutoIdentityResponse,
  parseFileMarker,
  parseGetSchemaResponse,
  parseListResponse,
  parseNodeVersion,
  parseQueryResponse,
  parseSchemaListResponse,
  resolveLoadedSchema,
} from './responseParsers.js';
export { parseMutationBatchResult } from './mutationOps.js';

export type {
  CreateDatabaseRequest,
  DatabaseSpec,
  ListedDatabase,
} from './databases.js';

export { socialClient } from './social.js';
export type { SocialClientOptions, SocialDatabaseSetup, SocialInvitation, SocialSetupRequest } from './social.js';

export {
  DATABASE_SESSION_HEADER,
  databaseScope,
  FILE_BLOB_METADATA_LENGTH_HEADER,
  FILE_SHA256_HEADER,
  FILE_SIZE_HEADER,
  isValidDatabaseSelector,
  LASTDB_API_ROUTES,
  LASTDB_MIN_SUPPORTED_API_VERSION,
  LASTDB_SDK_API_VERSION,
  schemaRoute,
  scopedApiPath,
  validateDatabaseSelector,
} from './apiRoutes.js';

export { ownerClient } from './ownerClient.js';
export type { OwnerClientOptions } from './ownerClient.js';

export {
  REQUEST_ID_HEADER,
  udsTransport,
  withDefaultHeaders,
} from './transport.js';
export type {
  BinaryResponse,
  RawResponse,
  Transport,
  TransportOptions,
  TransportSendOptions,
} from './transport.js';

export type {
  AutoIdentityResult,
  BlobThumbnailInput,
  CasExpectation,
  ConnectOptions,
  CreateMutationOp,
  DatabaseSession,
  DatabaseSessionIntent,
  DeclareSchemaRequest,
  DeclareSchemaResult,
  DeleteBlobOptions,
  DeleteMutationOp,
  FetchBlobOptions,
  FetchedBlob,
  FileMarker,
  FilePlacement,
  JsonValue,
  KeyValue,
  ListOptions,
  ListRecordKey,
  ListResult,
  LoadedSchema,
  MutationBatchItem,
  MutationBatchOperations,
  MutationBatchOptions,
  MutationBatchResult,
  MutationConvergence,
  MutationDurability,
  MutationOp,
  MutationOptions,
  MutationResult,
  NodeVersion,
  PutBlobBytesRequest,
  PutBlobLocalPathRequest,
  PutBlobMutationOp,
  PutBlobRequest,
  PutBlobResult,
  QueryAllOptions,
  QueryBatchItem,
  QueryBatchOutcome,
  QueryCursor,
  QueryFilter,
  QueryPage,
  QueryResult,
  QueryRow,
  RecordKeyInput,
  RecordMetadata,
  ReplaceMutationOp,
  Row,
  RowFields,
  SchemaDescriptor,
  SelectDatabaseOptions,
  ShareSubmissionResult,
} from './types.js';

export {
  AuthenticationRequiredError,
  CasConflictError,
  classifyPermissionReason,
  LastDbError,
  NodeTooOldError,
  nodeTooOldMessage,
  PermissionDeniedError,
  QueryPaginationError,
  RequestRejectedError,
  ShareSubmissionUncertainError,
  TransportError,
  UnexpectedResponseError,
} from './errors.js';
export type {
  AuthenticationRequiredReason,
  CasConflictDetail,
  NodeTooOldDetail,
  NodeTooOldReason,
  PermissionCategory,
  TransportErrorKind,
} from './errors.js';
