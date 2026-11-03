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
    Account,
    Schemas,
    Directories,
    NativeCas,
    Records,
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
            Self::Account => "account",
            Self::Schemas => "schemas",
            Self::Directories => "directories",
            Self::NativeCas => "native_cas",
            Self::Records => "records",
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
                Self::Encryption => "encryption",
                Self::Backend => "backend",
                _ => "other",
            }),
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
            Self::InvalidOperation(_) => NativeProofCause::InvalidOperation,
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
