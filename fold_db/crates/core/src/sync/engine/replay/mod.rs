//! Convergent replay: per-key / header / order merge paths.
//!
//! Legacy whole-molecule `ref:` blobs are retired (delete-train): replay drops
//! them and never migrates or dual-reads product ref blobs.

mod apply;
mod decode;
mod entry;
mod fence;
mod merge;
mod order;
mod records;
