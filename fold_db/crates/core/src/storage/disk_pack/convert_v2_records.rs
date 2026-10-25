//! The record transform of the v1 to v2 disk format conversion.
//!
//! A v1 home of node format 1 has key format 4 or 5: each record outside the
//! catalog, the CAS files, and the node home records has an outer envelope,
//! and the home has no content key proof. A v1 home of node format 2 has key
//! format 6: only the CAS files have the envelope. The v2 home has key format
//! 6, and its atom rows are plain: the block seal of each run is their
//! encryption at rest.
//!
//! Each stored record becomes:
//!
//! - a CAS file, a node home record, or a catalog record: the stored bytes;
//! - the key format marker: format 6, with the delete replay fence when the
//!   source has it;
//! - an atom body: the plain row of the opened atom;
//! - any other record: the stored bytes, without the outer envelope of a
//!   format 4 or 5 source.
//!
//! A format 4 or 5 source gets a new content key proof. The metadata
//! namespace record is added when the source has none.

use super::convert_v2::invalid;
use super::file_dependencies::{content_key, open_atom_with};
use super::physical::CAS_COLLECTION;
use super::*;
use crate::atom::atom_key_codec;
use crate::crypto::{at_rest::open_at_rest_bounded, content_key_proof as proof, is_sealed_at_rest};
use crate::db_operations::atom_store::{
    decode_plain_atom, encode_plain_atom, key_format, AtomStore,
};
use crate::storage::{encrypting_namespaced_store::CATALOG_NAMESPACES, laststore::native_routing};
use laststore::disk_pack::home::HomeSnapshot;
use std::collections::HashMap;
use zeroize::Zeroizing;

/// The node marker of a v1 home with key format 4 or 5.
pub(super) const FORMAT_1_NODE_BYTES: &[u8] = b"format=1\nhome=disk-pack\n";
/// The creation intents of the two v1 node formats.
pub(super) const INITIALIZING: [&str; 2] = [
    "laststore-current-keys-v2-initializing",
    "laststore-current-keys-v3-initializing",
];

const NODE_HOME: &str = "__native_home";
const METADATA_NAMESPACE: &str = "namespaces\0metadata";
const VALUE_MAX: u64 = laststore::disk_run::home::MAX_VALUE_BYTES as u64;

/// The key format of a v1 source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SourceFormat {
    /// Key format 4 or 5.
    Enveloped { fenced: bool },
    /// Key format 6, with a checked content key proof.
    Current { fenced: bool },
}

impl SourceFormat {
    /// Read the key format marker of `current`, and the key proof of a
    /// format 6 source. A wrong node key fails here, before any write.
    pub(super) fn detect(
        source: &DiskHome,
        writer: &HomeSnapshot,
        node_marker: &[u8],
        key: &[u8; 32],
    ) -> StorageResult<Self> {
        let mut read = source
            .read_from_writer(writer.current().clone(), budget(), &writer.read_lease())
            .map_err(map)?;
        let marker = AtomStore::CURRENT_KEY_FORMAT_KEY.as_bytes();
        let stored = read
            .get_bounded(
                native_routing::collection_for_key(marker),
                &native_routing::encode(marker),
                8192,
            )
            .map_err(map)?
            .ok_or_else(|| invalid("disk format conversion: the key format marker is missing"))?;
        if key_format::is_supported_key_format(&stored) {
            if node_marker != initialization::NODE_BYTES {
                return Err(invalid(
                    "disk format conversion: key format 6 in a node format 1 home",
                ));
            }
            let held = read
                .get_bounded(proof::NAMESPACE, &native_routing::encode(proof::KEY), 8192)
                .map_err(map)?
                .ok_or_else(|| {
                    invalid("disk format conversion: the content key proof is missing")
                })?;
            proof::verify(key, &held)?;
            let fenced = stored == key_format::FENCED_KEY_FORMAT;
            return Ok(Self::Current { fenced });
        }
        if node_marker != FORMAT_1_NODE_BYTES || !is_sealed_at_rest(&stored) {
            return Err(invalid(
                "disk format conversion: the key format marker does not match the node format",
            ));
        }
        let format = open_at_rest_bounded(key, &stored, 4096).map_err(|_| {
            invalid("disk format conversion: the node key does not open the key format marker")
        })?;
        match format.as_slice() {
            b"4" | b"5" => Ok(Self::Enveloped { fenced: false }),
            b"5;delete-replay-cutoff=v1" => Ok(Self::Enveloped { fenced: true }),
            _ => Err(invalid("disk format conversion: unsupported key format")),
        }
    }

    fn fenced(self) -> bool {
        match self {
            Self::Enveloped { fenced } | Self::Current { fenced } => fenced,
        }
    }
}

/// The v2 form of the records of one v1 generation.
pub(super) struct Transform<'a> {
    format: SourceFormat,
    key: &'a [u8; 32],
    source: &'a DiskHome,
    writer: &'a HomeSnapshot,
    /// The content key of each molecule key bundle opened so far.
    bundles: HashMap<String, Zeroizing<[u8; 32]>>,
    saw_metadata: bool,
    saw_cutoff: bool,
}

impl<'a> Transform<'a> {
    pub(super) fn new(
        format: SourceFormat,
        key: &'a [u8; 32],
        source: &'a DiskHome,
        writer: &'a HomeSnapshot,
    ) -> Self {
        Self {
            format,
            key,
            source,
            writer,
            bundles: HashMap::new(),
            saw_metadata: false,
            saw_cutoff: false,
        }
    }

    /// True for a source record that the conversion does not copy: the key
    /// proof of a format 4 or 5 source, which gets a new proof.
    pub(super) fn replaced(&self, collection: &str, key: &str) -> bool {
        matches!(self.format, SourceFormat::Enveloped { .. }) && is_key(collection, key, proof::KEY)
    }

    /// The v2 value of one stored v1 record.
    pub(super) fn value(
        &mut self,
        collection: &str,
        key: &str,
        stored: Vec<u8>,
    ) -> StorageResult<Vec<u8>> {
        if collection == NODE_HOME && key == METADATA_NAMESPACE {
            self.saw_metadata = true;
        }
        if collection == CAS_COLLECTION
            || collection == NODE_HOME
            || CATALOG_NAMESPACES.contains(&collection)
        {
            return Ok(stored);
        }
        if is_marker(collection, key) {
            return Ok(self.marker().to_vec());
        }
        if collection == "atoms" && atom_key_codec::body_uuid_of(key).is_some() {
            return self.plain_atom(&stored);
        }
        if is_marker_like(collection, key, key_format::DELETE_REPLAY_CUTOFF_KEY) {
            self.saw_cutoff = true;
        }
        match self.format {
            SourceFormat::Enveloped { .. } if is_sealed_at_rest(&stored) => {
                open_at_rest_bounded(self.key, &stored, VALUE_MAX).map_err(|_| {
                    invalid(&format!(
                        "disk format conversion: a record envelope in collection {collection} \
                         does not open"
                    ))
                })
            }
            _ => Ok(stored),
        }
    }

    /// The records to add after the copy. The source must hold the delete
    /// replay floor exactly when its marker has the fence.
    pub(super) fn extras(&self) -> StorageResult<Vec<HomeMutation>> {
        if self.format.fenced() != self.saw_cutoff {
            return Err(invalid(
                "disk format conversion: the delete replay floor and the format marker disagree",
            ));
        }
        let mut extras = Vec::new();
        if matches!(self.format, SourceFormat::Enveloped { .. }) {
            extras.push(HomeMutation {
                collection: proof::NAMESPACE.into(),
                key: native_routing::encode(proof::KEY),
                body: Some(proof::create(self.key)?),
            });
        }
        if !self.saw_metadata {
            extras.push(HomeMutation {
                collection: NODE_HOME.into(),
                key: METADATA_NAMESPACE.into(),
                body: Some(vec![1]),
            });
        }
        Ok(extras)
    }

    /// Check one added record after the reopen. A new key proof has a new
    /// nonce, so it is opened, not compared.
    pub(super) fn check_extra(&self, extra: &HomeMutation, found: &[u8]) -> StorageResult<()> {
        if extra.collection == proof::NAMESPACE {
            return proof::verify(self.key, found);
        }
        if extra.body.as_deref() != Some(found) {
            return Err(invalid(
                "disk format conversion: an added record differs after reopen",
            ));
        }
        Ok(())
    }

    /// Check a stored v2 atom row with the decoder of the node.
    pub(super) fn check_atom(collection: &str, key: &str, found: &[u8]) -> StorageResult<()> {
        if collection != "atoms" || atom_key_codec::body_uuid_of(key).is_none() {
            return Ok(());
        }
        let atom = decode_plain_atom(found).map_err(|error| {
            invalid(&format!(
                "disk format conversion: a plain atom row does not decode: {error}"
            ))
        })?;
        if encode_plain_atom(&atom).ok().as_deref() != Some(found) {
            return Err(invalid(
                "disk format conversion: a plain atom row does not decode to the same row",
            ));
        }
        Ok(())
    }

    fn marker(&self) -> &'static [u8] {
        if self.format.fenced() {
            key_format::FENCED_KEY_FORMAT
        } else {
            key_format::KEY_FORMAT
        }
    }

    /// The plain row of a stored atom: the outer envelope and the content
    /// seal removed, and no molecule key bundle name.
    fn plain_atom(&mut self, stored: &[u8]) -> StorageResult<Vec<u8>> {
        let (key, source, writer) = (self.key, self.source, self.writer);
        let bundles = &mut self.bundles;
        let atom = open_atom_with(key, stored, |molecule| {
            if let Some(found) = bundles.get(molecule) {
                return Ok(found.clone());
            }
            let opened = content_key(key, molecule, writer.current(), |budget| {
                source
                    .read_from_writer(writer.current().clone(), budget, &writer.read_lease())
                    .map_err(map)
            })?;
            bundles.insert(molecule.to_string(), opened.clone());
            Ok(opened)
        })?;
        encode_plain_atom(&atom).map_err(|error| {
            invalid(&format!(
                "disk format conversion: a plain atom row: {error}"
            ))
        })
    }
}

/// The key format marker of the main store.
fn is_marker(collection: &str, key: &str) -> bool {
    is_marker_like(collection, key, AtomStore::CURRENT_KEY_FORMAT_KEY)
}

/// True for the main store key `name` at its routed collection.
fn is_marker_like(collection: &str, key: &str, name: &str) -> bool {
    collection == native_routing::collection_for_key(name.as_bytes())
        && key == native_routing::encode(name.as_bytes())
}

fn is_key(collection: &str, key: &str, name: &[u8]) -> bool {
    collection == proof::NAMESPACE && key == native_routing::encode(name)
}

fn budget() -> ReadBudget {
    ReadBudget {
        pages: 4096,
        blocks: 16384,
        bytes: 8 * 1024 * 1024,
        keys: 2,
    }
}
