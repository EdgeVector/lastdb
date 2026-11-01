//! Atom reads and mutation-event history.

use crate::atom::{atom_key_codec, molecule_key_codec};
use crate::atom::{Atom, MutationEvent};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde::de::DeserializeOwned;
use std::collections::HashMap;

use super::AtomStore;

mod history;
mod read;
mod scan;
mod scope;
