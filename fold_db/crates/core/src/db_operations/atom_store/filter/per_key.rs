//! Per-key point lookups and prefix/range scans.

use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;

use super::super::AtomStore;
use crate::records::molecule_record::PerKeyRecord;

mod scan;
