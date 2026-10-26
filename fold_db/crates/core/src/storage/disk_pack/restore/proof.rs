//! The same bounded account proof for restored enrollment and publication.

use super::{map, refusal, StorageResult};
use crate::crypto::content_key_proof as proof;
use crate::db_operations::atom_store::{key_format, AtomStore};
use crate::storage::disk_pack::native_routing;
use laststore::disk_pack::home::HomeRead;

pub(super) fn verify(reader: &mut HomeRead, account_key: &[u8; 32]) -> StorageResult<()> {
    let key = AtomStore::CURRENT_KEY_FORMAT_KEY.as_bytes();
    let stored = reader
        .get_bounded(
            native_routing::collection_for_key(key),
            &native_routing::encode(key),
            8192,
        )
        .map_err(map)?
        .ok_or_else(|| refusal("native logical format marker is missing"))?;
    if !key_format::is_supported_key_format(&stored) {
        return Err(refusal(
            "unsupported restored logical format; convert it offline",
        ));
    }
    let stored = reader
        .get_bounded(proof::NAMESPACE, &native_routing::encode(proof::KEY), 8192)
        .map_err(map)?
        .ok_or_else(|| refusal("account content-key proof is missing"))?;
    proof::verify(account_key, &stored)
        .map_err(|_| refusal("account content-key proof cannot be opened"))
}
