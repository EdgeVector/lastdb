//! Offline v2 backup de["mut-b", "mut-a"].into_iter().map(str::to_string)criptor types: descriptor pages, the encrypted
//! manifest commitment, the v2 retirement receipt, the carry receipt header,
//! one canonical byte encoding, and Ed25519 sign/verify over that encoding.
//!
//! Scope of this module (cloud GC package P2, PR-C):
//!
//! - Nothing here reads or writes the wire. No uploader, restore path, or
//!   storage-service arm consumes these types yet. The v1 manifest format
//!   ([`super::backup_manifest::BackupManifest`], `MANIFEST_VERSION` 1) is
//!   unchanged and its v1 chain gate stays as is.
//! - The four contract items of the cloud-owned GC plan are not yet approved.
//!   These types are the proposed production shape of the P0 model
//!   (`exemem_service/cloud_gc_protocol`) and can change before a producer
//!   lands.
//! - The frontier stays an opaque encrypted field inside the manifest. The
//!   page carries no typed frontier map until the frontier dimension
//!   (per writer, or per writer and schema) is settled (landing map 7.5).
//! - [`CarryReceipt`] is a header only. Page reuse semantics across roots are
//!   NOT defined by this module (landing map 7.6).
//!
//! # Vocabulary
//!
//! A digest of stored bytes is always `stored_bytes_sha256`. Frame-AEAD chunks
//! and sorted units store ciphertext, but plain-packaging segments and declared
//! cleartext catalogs upload verbatim, so the digest is not a ciphertext digest
//! and is never named one here.
//!
//! # Canonical encoding
//!
//! Signatures cover [`canonical_bytes`], never the serde form. The encoding is
//! a length-prefixed, field-tagged binary layout written by an explicit
//! per-type encoder:
//!
//! ```text
//! record  := MAGIC(4) kind(u8) version(u32 BE) field*
//! field   := tag(u16 BE) length(u32 BE) payload
//! u64     := 8 bytes BE
//! string  := UTF-8 bytes
//! list    := count(u32 BE) (length(u32 BE) element)*
//! ```
//!
//! Field tags are fixed constants per type and the encoder emits them in
//! ascending tag order. The encoder never consults serde, so the bytes do not
//! depend on struct field order, `#[serde(skip_serializing_if)]`, `null`
//! versus absent keys, or JSON whitespace. Sets (`BTreeSet`) encode in their
//! sorted order; ordered lists (`Vec`) encode in the order given, because that
//! order is part of what is signed (page entry order, lineage order). The
//! `signature` field is never part of the canonical bytes. Canonical JSON with
//! sorted keys was the alternative; it was rejected because a serde attribute
//! change on any field would silently change every signed digest.

use super::backup_manifest::{BackupManifest, DESCRIPTOR_VERSION};
use crate::hex::{hex_decode, hex_lower, sha256_hex};
use crate::security::keys::{Ed25519KeyPair, Ed25519PublicKey};
use crate::storage::error::{StorageError, StorageResult};
use ed25519_dalek::Signature;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// First four bytes of every canonical record.
const CANONICAL_MAGIC: &[u8; 4] = b"LDBK";
/// Record kinds, so a signature over one type never verifies as another.
const KIND_DESCRIPTOR_PAGE: u8 = 1;
const KIND_RETIREMENT_RECEIPT: u8 = 2;
const KIND_CARRY_RECEIPT: u8 = 3;
const KIND_MANIFEST_COMMITMENT: u8 = 4;
const KIND_DESCRIPTOR_ENTRY: u8 = 5;
const KIND_RETIRED_INSTANCE: u8 = 6;

const SHA256_HEX_LEN: usize = 64;
const SIGNATURE_LEN: usize = 64;

/// Physical object classes of the P0 model, as stored in a descriptor entry.
/// Every v2 object names one of these. The P0 `Other(u64)` escape hatch has no
/// production counterpart: an unknown class fails closed at the reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DescriptorObjectClass {
    BackupChunk,
    Manifest,
    MutationLog,
    OwnedFileVersion,
    DeclaredCache,
    StagingUpload,
    DescriptorPage,
    ReceiptPage,
}

impl DescriptorObjectClass {
    fn canonical_code(self) -> u64 {
        match self {
            Self::BackupChunk => 1,
            Self::Manifest => 2,
            Self::MutationLog => 3,
            Self::OwnedFileVersion => 4,
            Self::DeclaredCache => 5,
            Self::StagingUpload => 6,
            Self::DescriptorPage => 7,
            Self::ReceiptPage => 8,
        }
    }
}

/// Commitment to one encrypted manifest object: the digest of its stored
/// bytes plus their length. The page carries this so a cloud reader can bind
/// the reference set to exactly one manifest object without the data key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EncryptedManifestCommitment {
    /// SHA-256 over the stored manifest object bytes, lowercase hex.
    pub stored_bytes_sha256: String,
    /// Length of those stored bytes.
    pub byte_length: u64,
}

impl EncryptedManifestCommitment {
    /// Commit to the exact bytes that will be stored.
    pub fn of_stored_bytes(stored_bytes: &[u8]) -> Self {
        Self {
            stored_bytes_sha256: sha256_hex(stored_bytes),
            byte_length: stored_bytes.len() as u64,
        }
    }

    /// Check stored bytes against this commitment.
    pub fn verify_stored_bytes(&self, stored_bytes: &[u8]) -> StorageResult<()> {
        let actual = Self::of_stored_bytes(stored_bytes);
        if actual != *self {
            return Err(StorageError::BackendError(format!(
                "backup descriptor manifest commitment mismatch: expected {}/{} bytes, stored {}/{} bytes",
                self.stored_bytes_sha256,
                self.byte_length,
                actual.stored_bytes_sha256,
                actual.byte_length
            )));
        }
        Ok(())
    }
}

/// One instance-addressed object reference on a descriptor page. Entries carry
/// no logical address and no payload: this is the opaque within-scope
/// reference metadata plus object size of contract item 1.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DescriptorEntry {
    pub class: DescriptorObjectClass,
    /// Server-minted physical instance id. Two objects with equal stored
    /// digests are two entries with two instance ids.
    pub instance_id: String,
    pub bytes: u64,
}

/// One immutable page of a root's reference set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescriptorPage {
    /// Always [`DESCRIPTOR_VERSION`] (2).
    pub version: u32,
    /// Storage scope (`db_hash`) the page belongs to.
    pub scope: String,
    pub store_uuid: String,
    /// Authority epoch the root was prepared under. Not physical identity.
    pub authority_epoch: u64,
    pub root_id: String,
    pub page_index: u32,
    pub page_count: u32,
    pub manifest_commitment: EncryptedManifestCommitment,
    pub entries: Vec<DescriptorEntry>,
    /// Ed25519 signature over [`canonical_bytes`] of this page, lowercase hex.
    /// `None` until signed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

/// Header fields shared by every page of one root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescriptorPageHeader {
    pub scope: String,
    pub store_uuid: String,
    pub authority_epoch: u64,
    pub root_id: String,
}

/// One retired physical instance named by a v2 retirement receipt.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RetiredInstance {
    pub class: DescriptorObjectClass,
    pub instance_id: String,
}

/// Explicit record of what a v2 receipt does NOT carry. The P0 semantic model
/// binds selected atoms per Delete; production has no durable set for them
/// yet (P7 owns it). The receipt says so instead of staying silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectedAtomsRecord {
    Unrecorded,
}

impl SelectedAtomsRecord {
    fn canonical_code(self) -> u64 {
        match self {
            Self::Unrecorded => 0,
        }
    }
}

/// Production shape of the P0 `RetirementEvidence`.
///
/// The cloud verifies the signature, the predecessor and replacement roots,
/// and that every retired instance was held by the predecessor and by no live
/// root. It cannot verify semantic completeness (P0 README, "Proof boundary").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetirementReceiptV2 {
    /// Always [`DESCRIPTOR_VERSION`] (2).
    pub version: u32,
    pub scope: String,
    pub store_uuid: String,
    pub authority_epoch: u64,
    /// Native Delete identities: `MutationEnvelope.mutation_uuid` values.
    pub delete_ids: BTreeSet<String>,
    /// Root that held the retired instances.
    pub predecessor_root: String,
    /// Root that replaces it and references none of the retired instances.
    pub replacement_root: String,
    /// Exact chain, newest predecessor first, through the selected genesis.
    pub lineage: Vec<String>,
    pub retired_instances: BTreeSet<RetiredInstance>,
    /// Instances the replacement root introduces for the rewritten image.
    pub replacement_instances: BTreeSet<String>,
    pub selected_atoms: SelectedAtomsRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

/// Header of a carry: a new root whose image equals its predecessor's.
///
/// This PR defines the header only. Which pages a carry may reuse, and how the
/// cloud binds reused pages to the new root, are NOT defined here (landing map
/// 7.6). Until that is settled a carry root installs its own pages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CarryReceipt {
    /// Always [`DESCRIPTOR_VERSION`] (2).
    pub version: u32,
    pub scope: String,
    pub store_uuid: String,
    pub authority_epoch: u64,
    /// The new root.
    pub root_id: String,
    /// The root whose image the new root carries unchanged.
    pub carry_from: String,
    /// Commitment to the carried encrypted manifest image.
    pub image_commitment: EncryptedManifestCommitment,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

/// One root's reference set assembled from its pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescriptorRootView {
    pub scope: String,
    pub store_uuid: String,
    pub authority_epoch: u64,
    pub root_id: String,
    pub page_count: u32,
    pub manifest_commitment: EncryptedManifestCommitment,
    /// Keyed by instance id; unique across every page of the root.
    pub entries: BTreeMap<String, DescriptorEntry>,
}

impl DescriptorRootView {
    pub fn instance_ids(&self) -> BTreeSet<&str> {
        self.entries.keys().map(String::as_str).collect()
    }
}

// ---------------------------------------------------------------------------
// Canonical encoder
// ---------------------------------------------------------------------------

/// Byte writer for the canonical layout described in the module docs.
#[derive(Default)]
struct CanonicalEncoder {
    out: Vec<u8>,
    last_tag: Option<u16>,
}

impl CanonicalEncoder {
    fn record(kind: u8, version: u32) -> Self {
        let mut out = Vec::with_capacity(64);
        out.extend_from_slice(CANONICAL_MAGIC);
        out.push(kind);
        out.extend_from_slice(&version.to_be_bytes());
        Self {
            out,
            last_tag: None,
        }
    }

    fn field(&mut self, tag: u16, payload: &[u8]) {
        debug_assert!(
            self.last_tag.is_none_or(|last| last < tag),
            "canonical field tags must ascend"
        );
        self.last_tag = Some(tag);
        self.out.extend_from_slice(&tag.to_be_bytes());
        self.out
            .extend_from_slice(&(payload.len() as u32).to_be_bytes());
        self.out.extend_from_slice(payload);
    }

    fn u64(&mut self, tag: u16, value: u64) {
        self.field(tag, &value.to_be_bytes());
    }

    fn str(&mut self, tag: u16, value: &str) {
        self.field(tag, value.as_bytes());
    }

    fn list<'a, I>(&mut self, tag: u16, items: I)
    where
        I: IntoIterator<Item = Vec<u8>>,
        I::IntoIter: ExactSizeIterator + 'a,
    {
        let items = items.into_iter();
        let mut payload = Vec::new();
        payload.extend_from_slice(&(items.len() as u32).to_be_bytes());
        for item in items {
            payload.extend_from_slice(&(item.len() as u32).to_be_bytes());
            payload.extend_from_slice(&item);
        }
        self.field(tag, &payload);
    }

    fn finish(self) -> Vec<u8> {
        self.out
    }
}

/// A type with a canonical byte form. The signature field, when the type has
/// one, is never part of it.
pub trait CanonicalRecord {
    fn encode_canonical(&self) -> Vec<u8>;
}

/// Canonical bytes of a record: the message that is signed and verified.
pub fn canonical_bytes<T: CanonicalRecord>(record: &T) -> Vec<u8> {
    record.encode_canonical()
}

impl CanonicalRecord for EncryptedManifestCommitment {
    fn encode_canonical(&self) -> Vec<u8> {
        let mut enc = CanonicalEncoder::record(KIND_MANIFEST_COMMITMENT, DESCRIPTOR_VERSION);
        enc.str(1, &self.stored_bytes_sha256);
        enc.u64(2, self.byte_length);
        enc.finish()
    }
}

impl CanonicalRecord for DescriptorEntry {
    fn encode_canonical(&self) -> Vec<u8> {
        let mut enc = CanonicalEncoder::record(KIND_DESCRIPTOR_ENTRY, DESCRIPTOR_VERSION);
        enc.u64(1, self.class.canonical_code());
        enc.str(2, &self.instance_id);
        enc.u64(3, self.bytes);
        enc.finish()
    }
}

impl CanonicalRecord for RetiredInstance {
    fn encode_canonical(&self) -> Vec<u8> {
        let mut enc = CanonicalEncoder::record(KIND_RETIRED_INSTANCE, DESCRIPTOR_VERSION);
        enc.u64(1, self.class.canonical_code());
        enc.str(2, &self.instance_id);
        enc.finish()
    }
}

impl CanonicalRecord for DescriptorPage {
    fn encode_canonical(&self) -> Vec<u8> {
        let mut enc = CanonicalEncoder::record(KIND_DESCRIPTOR_PAGE, self.version);
        enc.str(1, &self.scope);
        enc.str(2, &self.store_uuid);
        enc.u64(3, self.authority_epoch);
        enc.str(4, &self.root_id);
        enc.u64(5, u64::from(self.page_index));
        enc.u64(6, u64::from(self.page_count));
        enc.field(7, &self.manifest_commitment.encode_canonical());
        enc.list(
            8,
            self.entries.iter().map(CanonicalRecord::encode_canonical),
        );
        enc.finish()
    }
}

impl CanonicalRecord for RetirementReceiptV2 {
    fn encode_canonical(&self) -> Vec<u8> {
        let mut enc = CanonicalEncoder::record(KIND_RETIREMENT_RECEIPT, self.version);
        enc.str(1, &self.scope);
        enc.str(2, &self.store_uuid);
        enc.u64(3, self.authority_epoch);
        enc.list(4, self.delete_ids.iter().map(|id| id.as_bytes().to_vec()));
        enc.str(5, &self.predecessor_root);
        enc.str(6, &self.replacement_root);
        enc.list(7, self.lineage.iter().map(|id| id.as_bytes().to_vec()));
        enc.list(
            8,
            self.retired_instances
                .iter()
                .map(CanonicalRecord::encode_canonical),
        );
        enc.list(
            9,
            self.replacement_instances
                .iter()
                .map(|id| id.as_bytes().to_vec()),
        );
        enc.u64(10, self.selected_atoms.canonical_code());
        enc.finish()
    }
}

impl CanonicalRecord for CarryReceipt {
    fn encode_canonical(&self) -> Vec<u8> {
        let mut enc = CanonicalEncoder::record(KIND_CARRY_RECEIPT, self.version);
        enc.str(1, &self.scope);
        enc.str(2, &self.store_uuid);
        enc.u64(3, self.authority_epoch);
        enc.str(4, &self.root_id);
        enc.str(5, &self.carry_from);
        enc.field(6, &self.image_commitment.encode_canonical());
        enc.finish()
    }
}

// ---------------------------------------------------------------------------
// Signatures
// ---------------------------------------------------------------------------

fn sign_canonical<T: CanonicalRecord>(record: &T, key: &Ed25519KeyPair) -> String {
    let signature = key.sign(&canonical_bytes(record));
    hex_lower(signature.to_bytes())
}

fn verify_canonical<T: CanonicalRecord>(
    what: &str,
    record: &T,
    signature: Option<&str>,
    key: &Ed25519PublicKey,
) -> StorageResult<()> {
    let signature_hex = signature
        .ok_or_else(|| StorageError::BackendError(format!("backup {what} is unsigned")))?;
    let raw = hex_decode(signature_hex)
        .filter(|bytes| bytes.len() == SIGNATURE_LEN)
        .ok_or_else(|| {
            StorageError::BackendError(format!(
                "backup {what} signature is not {SIGNATURE_LEN} hex-encoded bytes"
            ))
        })?;
    let signature = Signature::from_slice(&raw).map_err(|e| {
        StorageError::BackendError(format!("backup {what} signature malformed: {e}"))
    })?;
    if !key.verify(&canonical_bytes(record), &signature) {
        return Err(StorageError::BackendError(format!(
            "backup {what} signature invalid"
        )));
    }
    Ok(())
}

/// Sign a page in place over its canonical bytes.
pub fn sign_descriptor_page(page: &mut DescriptorPage, key: &Ed25519KeyPair) {
    page.signature = Some(sign_canonical(page, key));
}

/// Sign a retirement receipt in place over its canonical bytes.
pub fn sign_retirement_receipt_v2(receipt: &mut RetirementReceiptV2, key: &Ed25519KeyPair) {
    receipt.signature = Some(sign_canonical(receipt, key));
}

/// Sign a carry receipt in place over its canonical bytes.
pub fn sign_carry_receipt(receipt: &mut CarryReceipt, key: &Ed25519KeyPair) {
    receipt.signature = Some(sign_canonical(receipt, key));
}

/// Verify one page: version, index bounds, the expected manifest commitment,
/// and the signature over the canonical bytes. Each failure names its cause.
pub fn verify_descriptor_page(
    page: &DescriptorPage,
    expected_commitment: &EncryptedManifestCommitment,
    key: &Ed25519PublicKey,
) -> StorageResult<()> {
    check_descriptor_version("descriptor page", page.version)?;
    if page.page_count == 0 || page.page_index >= page.page_count {
        return Err(StorageError::BackendError(format!(
            "backup descriptor page index {} out of range for page count {}",
            page.page_index, page.page_count
        )));
    }
    if page.manifest_commitment != *expected_commitment {
        return Err(StorageError::BackendError(format!(
            "backup descriptor page manifest commitment mismatch: page commits to {}/{} bytes, expected {}/{} bytes",
            page.manifest_commitment.stored_bytes_sha256,
            page.manifest_commitment.byte_length,
            expected_commitment.stored_bytes_sha256,
            expected_commitment.byte_length
        )));
    }
    verify_canonical("descriptor page", page, page.signature.as_deref(), key)
}

/// Verify a retirement receipt: version and signature over the canonical
/// bytes. Structural chain checks live in
/// `backup_manifest::classify_descriptor_chain_step`.
pub fn verify_retirement_receipt_v2(
    receipt: &RetirementReceiptV2,
    key: &Ed25519PublicKey,
) -> StorageResult<()> {
    check_descriptor_version("retirement receipt", receipt.version)?;
    verify_canonical(
        "retirement receipt",
        receipt,
        receipt.signature.as_deref(),
        key,
    )
}

/// Verify a carry receipt header: version and signature over the canonical
/// bytes.
pub fn verify_carry_receipt(receipt: &CarryReceipt, key: &Ed25519PublicKey) -> StorageResult<()> {
    check_descriptor_version("carry receipt", receipt.version)?;
    verify_canonical("carry receipt", receipt, receipt.signature.as_deref(), key)
}

// ---------------------------------------------------------------------------
// v2 readers (version 2 only)
// ---------------------------------------------------------------------------

fn check_descriptor_version(what: &str, version: u32) -> StorageResult<()> {
    if version != DESCRIPTOR_VERSION {
        return Err(StorageError::BackendError(format!(
            "unsupported backup {what} version {version}; expected {DESCRIPTOR_VERSION}"
        )));
    }
    Ok(())
}

/// Decode a descriptor page from its stored JSON bytes. Accepts version 2
/// only; a v1 manifest body is refused by name.
pub fn parse_descriptor_page(bytes: &[u8]) -> StorageResult<DescriptorPage> {
    let page: DescriptorPage = serde_json::from_slice(bytes).map_err(|e| {
        StorageError::BackendError(format!("backup descriptor page decode failed: {e}"))
    })?;
    check_descriptor_version("descriptor page", page.version)?;
    Ok(page)
}

/// Decode a v2 retirement receipt from its stored JSON bytes. Accepts version
/// 2 only.
pub fn parse_retirement_receipt_v2(bytes: &[u8]) -> StorageResult<RetirementReceiptV2> {
    let receipt: RetirementReceiptV2 = serde_json::from_slice(bytes).map_err(|e| {
        StorageError::BackendError(format!("backup retirement receipt decode failed: {e}"))
    })?;
    check_descriptor_version("retirement receipt", receipt.version)?;
    Ok(receipt)
}

// ---------------------------------------------------------------------------
// Page construction and assembly
// ---------------------------------------------------------------------------

/// Turn a manifest's chunk references into instance-addressed entries. Every
/// reference must carry a server-minted `instance`; a v1 reference with no
/// instance is refused by chunk identity. Instance ids must be unique.
pub fn descriptor_entries_from_manifest(
    manifest: &BackupManifest,
) -> StorageResult<Vec<DescriptorEntry>> {
    let mut seen = BTreeSet::new();
    let mut entries =
        Vec::with_capacity(manifest.mutable_chunks.len() + manifest.atom_chunks.len());
    for chunk in manifest
        .mutable_chunks
        .iter()
        .chain(manifest.atom_chunks.iter())
    {
        let instance_id = chunk.instance.clone().ok_or_else(|| {
            StorageError::BackendError(format!(
                "backup descriptor requires an instance id for chunk {}/{}/{:?}/{} (stored_bytes_sha256 {})",
                chunk.collection, chunk.shard, chunk.group_id, chunk.chunk_uuid, chunk.sha256
            ))
        })?;
        if !seen.insert(instance_id.clone()) {
            return Err(StorageError::BackendError(format!(
                "backup descriptor instance id {instance_id} referenced twice"
            )));
        }
        entries.push(DescriptorEntry {
            class: DescriptorObjectClass::BackupChunk,
            instance_id,
            bytes: chunk.bytes,
        });
    }
    Ok(entries)
}

/// Split entries into unsigned pages of at most `max_entries_per_page`. An
/// empty reference set still produces one empty page so `page_count` is never
/// zero.
pub fn build_descriptor_pages(
    header: &DescriptorPageHeader,
    manifest_commitment: &EncryptedManifestCommitment,
    entries: &[DescriptorEntry],
    max_entries_per_page: usize,
) -> StorageResult<Vec<DescriptorPage>> {
    if max_entries_per_page == 0 {
        return Err(StorageError::BackendError(
            "backup descriptor page size must be at least one entry".to_string(),
        ));
    }
    let chunks: Vec<Vec<DescriptorEntry>> = if entries.is_empty() {
        vec![Vec::new()]
    } else {
        entries
            .chunks(max_entries_per_page)
            .map(<[DescriptorEntry]>::to_vec)
            .collect()
    };
    let page_count = u32::try_from(chunks.len()).map_err(|_| {
        StorageError::BackendError("backup descriptor page count exceeds u32".to_string())
    })?;
    Ok(chunks
        .into_iter()
        .enumerate()
        .map(|(index, entries)| DescriptorPage {
            version: DESCRIPTOR_VERSION,
            scope: header.scope.clone(),
            store_uuid: header.store_uuid.clone(),
            authority_epoch: header.authority_epoch,
            root_id: header.root_id.clone(),
            page_index: index as u32,
            page_count,
            manifest_commitment: manifest_commitment.clone(),
            entries,
            signature: None,
        })
        .collect())
}

/// Assemble one root from its pages, in any input order. Structural checks
/// only: every page is version 2, shares one header and one commitment,
/// `page_count` equals the number of pages, indices are exactly
/// `0..page_count`, and instance ids are unique across the root (the P0
/// `IncompleteDescriptor` rule). Signatures are checked per page by
/// [`verify_descriptor_page`]; this function does not need the key.
pub fn assemble_descriptor_root(pages: &[DescriptorPage]) -> StorageResult<DescriptorRootView> {
    let first = pages.first().ok_or_else(|| {
        StorageError::BackendError("backup descriptor root has no pages".to_string())
    })?;
    let mut sorted: Vec<&DescriptorPage> = pages.iter().collect();
    sorted.sort_by_key(|page| page.page_index);
    if usize::try_from(first.page_count).ok() != Some(sorted.len()) {
        return Err(StorageError::BackendError(format!(
            "backup descriptor root {} declares {} pages but {} were supplied",
            first.root_id,
            first.page_count,
            sorted.len()
        )));
    }
    let mut entries = BTreeMap::new();
    for (expected_index, page) in sorted.iter().enumerate() {
        check_descriptor_version("descriptor page", page.version)?;
        if page.scope != first.scope
            || page.store_uuid != first.store_uuid
            || page.authority_epoch != first.authority_epoch
            || page.root_id != first.root_id
            || page.page_count != first.page_count
        {
            return Err(StorageError::BackendError(format!(
                "backup descriptor page {} header disagrees with page {}",
                page.page_index, first.page_index
            )));
        }
        if page.manifest_commitment != first.manifest_commitment {
            return Err(StorageError::BackendError(format!(
                "backup descriptor page {} manifest commitment disagrees with page {}",
                page.page_index, first.page_index
            )));
        }
        if usize::try_from(page.page_index).ok() != Some(expected_index) {
            return Err(StorageError::BackendError(format!(
                "backup descriptor root {} is missing page {expected_index}",
                first.root_id
            )));
        }
        for entry in &page.entries {
            if entries
                .insert(entry.instance_id.clone(), entry.clone())
                .is_some()
            {
                return Err(StorageError::BackendError(format!(
                    "backup descriptor instance id {} appears on more than one entry",
                    entry.instance_id
                )));
            }
        }
    }
    Ok(DescriptorRootView {
        scope: first.scope.clone(),
        store_uuid: first.store_uuid.clone(),
        authority_epoch: first.authority_epoch,
        root_id: first.root_id.clone(),
        page_count: first.page_count,
        manifest_commitment: first.manifest_commitment.clone(),
        entries,
    })
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// True when `text` is a lowercase 64-character SHA-256 hex digest.
pub fn is_stored_bytes_sha256_hex(text: &str) -> bool {
    text.len() == SHA256_HEX_LEN && crate::hex::is_lower_hex_sha256(text)
}
