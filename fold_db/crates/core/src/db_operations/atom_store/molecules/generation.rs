//! Barrierless immutable molecule generations.

use crate::atom::molecule_key_codec;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;

use super::super::{AtomStore, MoleculeGenerationPointer};

mod slots;
