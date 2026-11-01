//! Safe proof diagnostics retain no dynamic error text, path, key or record.
use super::StorageError;
use std::fmt;
use std::io::ErrorKind;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeProofStage {
    NativeReader,
    Admission,
    Validation,
    Restore,
    Profile,
    PhysicalRoot,
    Scratch,
    Account,
    Schemas,
    Blobs,
    Directories,
    NativeCas,
    Tips,
    FinalBinding,
}

impl fmt::Display for NativeProofStage {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::NativeReader => "native_reader",
            Self::Admission => "admission",
            Self::Validation => "validation",
            Self::Restore => "restore",
            Self::Profile => "profile",
            Self::PhysicalRoot => "physical_root",
            Self::Scratch => "scratch",
            Self::Account => "account",
            Self::Schemas => "schemas",
            Self::Blobs => "blobs",
            Self::Directories => "directories",
            Self::NativeCas => "native_cas",
            Self::Tips => "tips",
            Self::FinalBinding => "final_binding",
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeProofCause {
    Static(&'static str),
    Io {
        kind: ErrorKind,
        os_code: Option<i32>,
    },
    ReadResource,
    WriteResource,
    StorageFull,
    Authentication,
    NativeFormat,
    Configuration,
    InvalidOperation,
    FileSourceUnavailable,
    UnsupportedFileScope,
    UncertifiedFile,
    Encryption,
    Backend,
    Other,
}

impl fmt::Display for NativeProofCause {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static(code) => out.write_str(code),
            // ErrorKind is a data-free standard enum; the numeric OS code is safe.
            Self::Io { kind, os_code } => write!(out, "io kind={kind:?} os_code={os_code:?}"),
            other => out.write_str(match other {
                Self::ReadResource => "read_resource_limit",
                Self::WriteResource => "write_resource_limit",
                Self::StorageFull => "storage_full",
                Self::Authentication => "authentication",
                Self::NativeFormat => "native_format",
                Self::Configuration => "configuration",
                Self::InvalidOperation => "invalid_operation",
                Self::FileSourceUnavailable => "file_source_unavailable",
                Self::UnsupportedFileScope => "unsupported_file_scope",
                Self::UncertifiedFile => "uncertified_file",
                Self::Encryption => "encryption",
                Self::Backend => "backend",
                _ => "other",
            }),
        }
    }
}

impl NativeProofCause {
    /// Preserve the existing backup pause classification, not the dynamic text.
    pub fn invalid_operation(detail: &str) -> Self {
        match detail {
            "native authorized file consumer copy is unavailable" => Self::FileSourceUnavailable,
            "native file source owner scope needs a read-only grant" => Self::UnsupportedFileScope,
            "legacy or incomplete file dependency is uncertified"
            | "metadata-only file dependency is uncertified"
            | "live file metadata lacks certified record pointer coverage"
            | "native CAS per-file encryption suite is uncertified" => Self::UncertifiedFile,
            _ => Self::InvalidOperation,
        }
    }
}

impl StorageError {
    /// Preserve the nearest concrete proof stage and any typed static leaf.
    pub fn native_proof_context(
        &self,
        stage: NativeProofStage,
    ) -> (NativeProofStage, NativeProofCause) {
        let cause = match self {
            Self::NativeProof {
                stage: prior,
                cause,
            } => {
                let stage = if matches!(
                    prior,
                    NativeProofStage::Validation | NativeProofStage::NativeReader
                ) {
                    stage
                } else {
                    *prior
                };
                return (stage, *cause);
            }
            Self::IoError(io) => NativeProofCause::Io {
                kind: io.kind(),
                os_code: io.raw_os_error(),
            },
            Self::ReadResourceLimit { .. } => NativeProofCause::ReadResource,
            Self::WriteResourceLimit { .. } | Self::MutationLimit { .. } => {
                NativeProofCause::WriteResource
            }
            Self::StorageFull { .. } => NativeProofCause::StorageFull,
            Self::BackendError(_) => NativeProofCause::Backend,
            Self::ConfigurationError(_) => NativeProofCause::Configuration,
            Self::InvalidOperation(detail) => NativeProofCause::invalid_operation(detail),
            Self::EncryptionError(_) => NativeProofCause::Encryption,
            _ => NativeProofCause::Other,
        };
        (stage, cause)
    }

    pub fn at_native_proof(self, stage: NativeProofStage) -> Self {
        let (stage, cause) = self.native_proof_context(stage);
        drop(self);
        Self::NativeProof { stage, cause }
    }
}
