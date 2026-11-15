//! Generic byte backup transport. Pure wire validation; no I/O or admission.
//! Native formats, recovery keys and client completeness stay with the client.
pub mod blobs;
mod common;
pub mod context;
pub mod hash;
pub mod jobs;
pub mod limits;
pub mod share_access;
pub mod transfer;
mod version;

pub use common::{Destination, Job, Object, Stream};
pub use jobs::Begin;
pub use version::Version;
