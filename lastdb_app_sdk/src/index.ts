/**
 * @lastdb/app-sdk — the runtime SDK for LastDB apps.
 *
 * Wraps a node's Unix-socket `/api/*` data plane.
 */

export { connect, LastDbClient, ownerClient } from './client.js';
export type { LastDbClientOptions, OwnerClientOptions } from './client.js';

export { parseQueryResponse } from './responseParsers.js';

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
export { LASTDB_API_ROUTES } from './clientHelpers.js';

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
