//! # LastDB model
//!
//! The data types of the LastDB core: atoms, molecules, schemas, keys,
//! mutations, and the pure rules over them (canonical byte framing, key
//! codecs, logical layout rules, JSON order).
//!
//! The model is data plus pure functions. It reads no clock, makes no random
//! id, opens no file or socket, keeps no process-wide or task-local state, and
//! holds no signing key. A caller passes each time, id, and signer-derived
//! value as an argument. `fold_db` (core) owns the behavior: it reads the
//! clock, signs, installs the key comparison context, and stores data.
//! `scripts/ci/model-purity-lint.sh` enforces these rules in CI.

pub mod atom;
pub mod canonical;
pub mod constants;
pub mod hex;
pub mod json_order;
pub mod kind_partition;
pub mod schema;
