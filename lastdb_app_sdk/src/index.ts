/**
 * @lastdb/app-sdk — the runtime SDK for LastDB apps.
 *
 * Wraps a node's Unix-socket data plane: the `/api/*` methods and the app API
 * (`read`, `write`, `changes`).
 */

export { connect, LastDbClient, ownerClient } from './client.js';
export type { LastDbClientOptions, OwnerClientOptions } from './client.js';

export { AppApiError } from './appApi.js';
export type {
  AppApiChange,
  AppApiChangesOptions,
  AppApiChangesResult,
  AppApiCreate,
  AppApiDelete,
  AppApiFile,
  AppApiFileLink,
  AppApiMutation,
  AppApiRange,
  AppApiRead,
  AppApiReadError,
  AppApiReadResult,
  AppApiRecord,
  AppApiReplace,
  AppApiWriteResult,
} from './appApi.js';

export { parseQueryResponse } from './responseParsers.js';

export { LASTDB_API_ROUTES } from './databases.js';
export type {
  CreateDatabaseRequest,
  DatabaseSpec,
  ListedDatabase,
  SharedDatabaseAccess,
  ShareWriteScope,
} from './databases.js';

export {
  LASTDB_MIN_SUPPORTED_API_VERSION,
  LASTDB_SDK_API_VERSION,
} from './apiVersion.generated.js';

export {
  REQUEST_ID_HEADER,
  udsTransport,
} from './transport.js';
export type {
  BinaryResponse,
  RawResponse,
  Transport,
  TransportOptions,
  TransportSendOptions,
} from './transport.js';

export type {
  BlobThumbnailInput,
  DeleteBlobOptions,
  FetchBlobOptions,
  FetchedBlob,
  PutBlobBytesRequest,
  PutBlobLocalPathRequest,
  PutBlobMutationOp,
  PutBlobRequest,
  PutBlobResult,
} from './blobOps.js';

export type {
  AutoIdentityResult,
  DeclareSchemaRequest,
  DeclareSchemaResult,
  LoadedSchema,
  SchemaDescriptor,
} from './schemaOps.js';

export type {
  CasExpectation,
  ConnectOptions,
  CreateMutationOp,
  DatabaseSession,
  DatabaseSessionIntent,
  DeleteMutationOp,
  FileMarker,
  FilePlacement,
  JsonValue,
  KeyValue,
  ListOptions,
  ListRecordKey,
  ListResult,
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
  SelectDatabaseOptions,
  ShareSubmissionResult,
  ShareSubmissionStatus,
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
