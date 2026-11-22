//! Where the bytes of a record's file come from when a history is applied.

use crate::value::FileRef;

/// The plaintext of the file that a history record names.
///
/// The history carries a reference to the file and not its bytes. A node that
/// applies the history asks its source for the bytes and puts the file into the
/// store, so the record's file is readable without the relay. The source has
/// the bytes already: the caller fetched them before applying.
pub trait FileSource: Send + Sync {
    /// The verified plaintext of `file`.
    fn open(&self, file: &FileRef) -> Result<Vec<u8>, String>;
}

/// The source of a database that carries no files. A record that names one is
/// refused.
pub struct NoFiles;

impl FileSource for NoFiles {
    fn open(&self, _file: &FileRef) -> Result<Vec<u8>, String> {
        Err("this database carries no files".into())
    }
}
