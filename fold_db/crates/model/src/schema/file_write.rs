//! Sealed capability tokens for record file writes.

use crate::record_value::FileMarker;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Default)]
pub struct FileWriteIssuer {
    identity: Arc<()>,
}

struct Issued<T> {
    issuer: Arc<()>,
    marker: FileMarker,
    data: T,
}

impl<T: PartialEq> PartialEq for Issued<T> {
    fn eq(&self, o: &Self) -> bool {
        Arc::ptr_eq(&self.issuer, &o.issuer) && self.marker == o.marker && self.data == o.data
    }
}

#[derive(Clone, PartialEq)]
pub struct SealedRecordFile {
    inner: Arc<Issued<Vec<u8>>>,
}

impl SealedRecordFile {
    #[must_use]
    pub fn marker(&self) -> &FileMarker {
        &self.inner.marker
    }
}

#[derive(Clone, PartialEq)]
pub struct LocalFileReference {
    inner: Arc<Issued<PathBuf>>,
}

impl LocalFileReference {
    #[must_use]
    pub fn marker(&self) -> &FileMarker {
        &self.inner.marker
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.data
    }
}

impl FileWriteIssuer {
    #[must_use]
    pub fn issue(&self, marker: FileMarker, data: Vec<u8>) -> FileWrite {
        FileWrite::Put(SealedRecordFile {
            inner: Arc::new(Issued {
                issuer: Arc::clone(&self.identity),
                marker,
                data,
            }),
        })
    }

    #[must_use]
    pub fn issue_local(&self, marker: FileMarker, data: PathBuf) -> FileWrite {
        FileWrite::LocalPath(LocalFileReference {
            inner: Arc::new(Issued {
                issuer: Arc::clone(&self.identity),
                marker,
                data,
            }),
        })
    }

    #[must_use]
    pub fn admit(&self, file: SealedRecordFile) -> Option<Vec<u8>> {
        Arc::ptr_eq(&self.identity, &file.inner.issuer)
            .then(|| Arc::try_unwrap(file.inner).map_or_else(|p| p.data.clone(), |p| p.data))
    }

    #[must_use]
    pub fn admit_local<'a>(&self, file: &'a LocalFileReference) -> Option<&'a Path> {
        Arc::ptr_eq(&self.identity, &file.inner.issuer).then_some(file.path())
    }
}

#[derive(Clone, PartialEq)]
pub enum FileWrite {
    Put(SealedRecordFile),
    LocalPath(LocalFileReference),
    Clear,
}

impl FileWrite {
    #[must_use]
    pub fn row_len(&self) -> usize {
        self.sealed_row().map_or(0, <[u8]>::len)
    }

    #[must_use]
    pub fn sealed_row(&self) -> Option<&[u8]> {
        match self {
            Self::Put(file) => Some(&file.inner.data),
            Self::LocalPath(_) | Self::Clear => None,
        }
    }

    #[must_use]
    pub fn local_reference(&self) -> Option<&LocalFileReference> {
        match self {
            Self::LocalPath(file) => Some(file),
            Self::Put(_) | Self::Clear => None,
        }
    }
}

impl fmt::Debug for FileWrite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Put(file) => f
                .debug_struct("Put")
                .field("marker", file.marker())
                .field("row_bytes", &self.row_len())
                .finish(),
            Self::LocalPath(file) => f
                .debug_struct("LocalPath")
                .field("marker", file.marker())
                .finish_non_exhaustive(),
            Self::Clear => f.write_str("Clear"),
        }
    }
}
