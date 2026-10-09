/**
 * @lastdb/app-sdk — the runtime SDK for LastDB apps.
 *
 * Wraps a node's production `/api/*` surface: connect → request consent →
 * await grant → query/mutate with the granted capability. See the README for
 * a 10-line quickstart.
 */

export {
  connect,
  LastDbClient,
  parseAutoIdentityResponse,
  parseNodeVersion,
  parseSchemaListResponse,
  parseListResponse,
  parseQueryResponse,
  resolveLoadedSchema,
  parseSearchResponse,
  parseChangesResponse,
} from './client.js';
export type { AwaitConsentOptions, LastDbClientOptions } from './client.js';

export {
  LASTDB_API_ROUTES,
  LASTDB_SDK_API_VERSION,
  LASTDB_UDS_SHARED_ROUTES,
} from './apiRoutes.js';

export {
  LASTDB_DB_ENV,
  LASTDB_DB_HEADER,
  PERSONAL_DB_LOCATOR,
  DbLocatorError,
  orgDb,
  parseDbLocator,
  personalDb,
  resolveDbLocator,
} from './dbHandle.js';
export type {
  DbHandle,
  DbHashHandle,
  OrgDbHandle,
  PersonalDbHandle,
} from './dbHandle.js';

export { ownerClient } from './ownerClient.js';
export type { OwnerClientOptions } from './ownerClient.js';

export {
  capabilityStoreKey,
  defaultCapabilityStore,
  DEFAULT_KEYCHAIN_SERVICE,
  FileCapabilityStore,
  KeychainUnavailableError,
  KeychainWithFileFallbackStore,
  loadCapabilityOrThrow,
  MacKeychainStore,
} from './capabilityStore.js';
export type {
  CapabilityStore,
  LoadOptions,
  SecurityRunner,
  SecurityRunResult,
} from './capabilityStore.js';

export { canonicalize, canonicalizeBytes, JcsError } from './jcs.js';

export {
  CAPABILITY_GRANT_PURPOSE,
  decodeCapabilityBlob,
  sha256Hex,
  tokenIntegrityValid,
  verifyCapabilityBlob,
} from './capabilityToken.js';
export type {
  CapabilityBlobProblem,
  CapabilityBlobVerification,
  CapabilityToken,
  SignatureEnvelope,
} from './capabilityToken.js';

export {
  discoverTransport,
  httpTransport,
  udsTransport,
  REQUEST_ID_HEADER,
} from './transport.js';
export type { Transport, RawResponse } from './transport.js';

export type {
  ConnectOptions,
  ConsentScope,
  AutoIdentityResult,
  KeyValue,
  ListOptions,
  ListRecordKey,
  ListResult,
  LoadedSchema,
  NodeVersion,
  RequestConsentResult,
  QueryAllOptions,
  QueryBatchItem,
  QueryBatchOutcome,
  QueryFieldPredicate,
  QueryFilter,
  QueryPage,
  QueryResult,
  QueryRow,
  SchemaDescriptor,
  CasExpectation,
  MutationCloudCaptureReceipt,
  MutationCloudCaptureState,
  MutationCloudPublication,
  MutationCloudPublicationReceipt,
  MutationCloudPublicationState,
  MutationCloudPublicationTarget,
  MutationDurability,
  MutationOp,
  MutationResult,
  ChangesOptions,
  ChangeEvent,
  ChangesResult,
  SearchOptions,
  SearchHit,
  SearchResult,
  Row,
  RowFields,
  SchemaAdapter,
  SchemaMapping,
  ResolveResult,
  SchemaResolver,
  SchemaResolverResult,
  JsonValue,
} from './types.js';

export {
  FoldDbError,
  AuthenticationRequiredError,
  TransportError,
  UnexpectedResponseError,
  UnknownAppError,
  AppInSandboxError,
  InvalidScopeError,
  ConsentDeniedError,
  CapabilityRevokedError,
  ConsentExpiredError,
  ConsentRequestNotFoundError,
  ConsentTimeoutError,
  PermissionDeniedError,
  QueryPaginationError,
  FullScanNotAllowedError,
  CapabilityDeniedError,
  CapabilityVerificationError,
  CasConflictError,
  RequestRejectedError,
  NodeTooOldError,
  nodeTooOldMessage,
  CapabilityNotFoundError,
  classifyPermissionReason,
  CAPABILITY_DENIAL_REASONS,
  isCapabilityDenialReason,
  capabilityDenialReaction,
} from './errors.js';
export type {
  AuthenticationRequiredReason,
  PermissionCategory,
  CapabilityDenialReason,
  CapabilityDenialDetail,
  CapabilityDenialReaction,
  CasConflictDetail,
  NodeTooOldDetail,
  NodeTooOldReason,
  TransportErrorKind,
} from './errors.js';
