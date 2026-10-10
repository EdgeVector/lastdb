pub(super) const RESTORE_FAILURE_SCHEMA: &str = "lastdb.restore.error.v1";
pub(super) const RESTORE_FAILURE_MAX_JSON_BYTES: usize = 1024;
#[path = "restore_failure/diagnosis.rs"]
mod diagnosis;
use diagnosis::RestoreReplayDiagnosis;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum RestoreFailureStage {
    Preflight,
    SourceScope,
    CloudCredentials,
    IdentityKey,
    PrepareDestination,
    SourceLayout,
    OpenDestination,
    RuntimeInit,
    RestoreS0LatestPointer,
    RestoreS0ManifestDownload,
    RestoreS0ManifestDecode,
    RestoreS0ManifestValidation,
    RestoreS0LatestPointerValidation,
    RestoreS0SourceScopeValidation,
    RestoreS0DestinationValidation,
    RestoreS0ChunkDownload,
    RestoreS0ChunkInstall,
    RestoreS0DestinationIntegrity,
    RestoreS0DestinationCommit,
    OpenRestoreDatabase,
    RestoreTail,
    FlushDestination,
    CompletionMarker,
    RenderReport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum RestoreFailureCode {
    OperationFailed,
    InvalidSourceScope,
    InvalidIdentityKey,
    FrameAeadOptInRequired,
    LayoutMismatch,
    IoError,
    CryptoError,
    WrongKey,
    SerializationError,
    NetworkError,
    AuthError,
    Forbidden,
    QuotaExceeded,
    ObjectError,
    CorruptLogEntry,
    ReplayApplyFailed,
    UnsupportedEnvelope,
    /// Cloud `backup/latest` names a backup object format this build does not
    /// read (`SyncError::UnsupportedBackupFormat`). Serializes as
    /// `unsupported_backup_format`.
    UnsupportedBackupFormat,
    SequenceGap,
    SyncError,
}

#[derive(Debug)]
pub(super) struct RestoreFailure {
    pub(super) stage: RestoreFailureStage,
    pub(super) code: RestoreFailureCode,
    pub(super) detail: String,
    replay: Option<RestoreReplayDiagnosis>,
}

impl RestoreFailure {
    pub(super) fn new(
        stage: RestoreFailureStage,
        code: RestoreFailureCode,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            stage,
            code,
            detail: detail.into(),
            replay: None,
        }
    }

    pub(super) fn from_sync(
        stage: RestoreFailureStage,
        context: &str,
        error: &fold_db::sync::SyncError,
    ) -> Self {
        let code = restore_sync_error_code(error);
        let mut failure = Self::new(stage, code, format!("{context}: {error}"));
        failure.replay = RestoreReplayDiagnosis::from_sync(error);
        failure
    }

    pub(super) fn from_s0(
        context: &str,
        failure: &fold_db::sync::engine::S0RestoreFailure,
    ) -> Self {
        use fold_db::sync::engine::S0RestoreBoundary as Boundary;
        use RestoreFailureStage as Stage;

        // Match closed variants, never a server string or formatted error.
        let stage = match failure.boundary {
            Boundary::LatestPointer => Stage::RestoreS0LatestPointer,
            Boundary::ManifestDownload => Stage::RestoreS0ManifestDownload,
            Boundary::ManifestDecode => Stage::RestoreS0ManifestDecode,
            Boundary::ManifestValidation => Stage::RestoreS0ManifestValidation,
            Boundary::LatestPointerValidation => Stage::RestoreS0LatestPointerValidation,
            Boundary::SourceScopeValidation => Stage::RestoreS0SourceScopeValidation,
            Boundary::DestinationValidation => Stage::RestoreS0DestinationValidation,
            Boundary::ChunkDownload => Stage::RestoreS0ChunkDownload,
            Boundary::ChunkInstall => Stage::RestoreS0ChunkInstall,
            Boundary::DestinationIntegrity => Stage::RestoreS0DestinationIntegrity,
            Boundary::DestinationCommit => Stage::RestoreS0DestinationCommit,
        };
        Self::from_sync(stage, context, &failure.source)
    }
}

#[derive(serde::Serialize)]
pub(super) struct RestoreFailureEnvelope {
    pub(super) schema: &'static str,
    pub(super) ok: bool,
    pub(super) stage: RestoreFailureStage,
    pub(super) code: RestoreFailureCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    replay: Option<RestoreReplayDiagnosis>,
}

pub(super) fn restore_failure_pair_allowed(
    stage: RestoreFailureStage,
    code: RestoreFailureCode,
) -> bool {
    use RestoreFailureCode as Code;
    use RestoreFailureStage as Stage;

    match stage {
        Stage::SourceScope => matches!(code, Code::InvalidSourceScope),
        Stage::IdentityKey => matches!(
            code,
            Code::IoError | Code::InvalidIdentityKey | Code::CryptoError
        ),
        Stage::SourceLayout => matches!(
            code,
            Code::OperationFailed | Code::FrameAeadOptInRequired | Code::LayoutMismatch
        ),
        Stage::RestoreS0LatestPointer
        | Stage::RestoreS0ManifestDownload
        | Stage::RestoreS0ManifestDecode
        | Stage::RestoreS0ManifestValidation
        | Stage::RestoreS0LatestPointerValidation
        | Stage::RestoreS0SourceScopeValidation
        | Stage::RestoreS0DestinationValidation
        | Stage::RestoreS0ChunkDownload
        | Stage::RestoreS0ChunkInstall
        | Stage::RestoreS0DestinationIntegrity
        | Stage::RestoreS0DestinationCommit
        | Stage::RestoreTail => matches!(
            code,
            Code::OperationFailed
                | Code::IoError
                | Code::CryptoError
                | Code::WrongKey
                | Code::SerializationError
                | Code::NetworkError
                | Code::AuthError
                | Code::Forbidden
                | Code::QuotaExceeded
                | Code::ObjectError
                | Code::CorruptLogEntry
                | Code::ReplayApplyFailed
                | Code::UnsupportedEnvelope
                | Code::UnsupportedBackupFormat
                | Code::SequenceGap
                | Code::SyncError
        ),
        Stage::Preflight
        | Stage::CloudCredentials
        | Stage::OpenDestination
        | Stage::RuntimeInit
        | Stage::OpenRestoreDatabase
        | Stage::FlushDestination => matches!(code, Code::OperationFailed),
        Stage::PrepareDestination | Stage::CompletionMarker => matches!(code, Code::IoError),
        Stage::RenderReport => matches!(code, Code::SerializationError),
    }
}

pub(super) fn restore_sync_error_code(error: &fold_db::sync::SyncError) -> RestoreFailureCode {
    use fold_db::sync::SyncError;

    match error {
        SyncError::Crypto(_) => RestoreFailureCode::CryptoError,
        SyncError::Serialization(_) => RestoreFailureCode::SerializationError,
        SyncError::Io(_) => RestoreFailureCode::IoError,
        SyncError::Network(_) => RestoreFailureCode::NetworkError,
        SyncError::Auth(_) => RestoreFailureCode::AuthError,
        SyncError::Banned(_) => RestoreFailureCode::Forbidden,
        SyncError::QuotaExceeded(_) => RestoreFailureCode::QuotaExceeded,
        SyncError::S3(_) => RestoreFailureCode::ObjectError,
        SyncError::CorruptEntry { .. } => RestoreFailureCode::CorruptLogEntry,
        SyncError::ReplayApplyFailed { .. } => RestoreFailureCode::ReplayApplyFailed,
        SyncError::UnsupportedEnvelope { .. } => RestoreFailureCode::UnsupportedEnvelope,
        SyncError::UnsupportedBackupFormat { .. } => RestoreFailureCode::UnsupportedBackupFormat,
        SyncError::SequenceGap { .. } => RestoreFailureCode::SequenceGap,
        SyncError::WrongKey => RestoreFailureCode::WrongKey,
        SyncError::Storage(_) => RestoreFailureCode::OperationFailed,
        _ => RestoreFailureCode::SyncError,
    }
}

pub(super) fn render_restore_failure_json(failure: &RestoreFailure) -> String {
    let (stage, code) = if restore_failure_pair_allowed(failure.stage, failure.code) {
        (failure.stage, failure.code)
    } else {
        (
            RestoreFailureStage::RenderReport,
            RestoreFailureCode::SerializationError,
        )
    };
    let envelope = RestoreFailureEnvelope {
        schema: RESTORE_FAILURE_SCHEMA,
        ok: false,
        stage,
        code,
        replay: (stage == RestoreFailureStage::RestoreTail
            && code == RestoreFailureCode::ReplayApplyFailed)
            .then_some(failure.replay)
            .flatten(),
    };
    let json = serde_json::to_string(&envelope).unwrap_or_else(|_| {
        concat!(
            "{\"schema\":\"lastdb.restore.error.v1\",\"ok\":false,",
            "\"stage\":\"render_report\",\"code\":\"serialization_error\"}"
        )
        .to_string()
    });
    debug_assert!(json.len() <= RESTORE_FAILURE_MAX_JSON_BYTES);
    json
}
