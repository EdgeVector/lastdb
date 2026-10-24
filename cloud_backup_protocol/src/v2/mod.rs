//! Generic byte backup transport. Pure wire validation; no I/O or admission.
//! Native formats, recovery keys and client completeness stay with the client.
pub mod access;
mod common;
pub mod context;
pub mod discovery;
pub mod hash;
pub mod inventory;
pub mod jobs;
pub mod limits;
pub mod publication;
pub mod transfer;
pub mod usage;
mod version;

pub use common::{ClientDescriptor, Job, Object, Stream};
pub use jobs::Begin;
pub use publication::{AcceptedBackup, Commit, Head};
pub use version::Version;
