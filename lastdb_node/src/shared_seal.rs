//! Seal the opaque content of entries in a shared database.

use mutation_log::{AuthorCtx, DataBody, DatabaseId, SchemaBody, SealOpener, SEALED_NAMESPACE};
use zeroize::Zeroizing;

use crate::personal_seal::Sealer;

/// The fixed-width prefix prevents an encrypted record key or schema name from
/// appearing in the relay-visible entry shape.
fn pack(namespace: &str, bytes: &[u8]) -> Result<Vec<u8>, String> {
    let count = u16::try_from(namespace.len()).map_err(|_| "shared namespace is too long")?;
    let mut out = Vec::with_capacity(usize::from(count) + bytes.len() + 2);
    out.extend_from_slice(&count.to_be_bytes());
    out.extend_from_slice(namespace.as_bytes());
    out.extend_from_slice(bytes);
    Ok(out)
}

fn unpack(bytes: &[u8]) -> Option<(String, Vec<u8>)> {
    let count = usize::from(u16::from_be_bytes(bytes.get(..2)?.try_into().ok()?));
    let name = std::str::from_utf8(bytes.get(2..2 + count)?)
        .ok()?
        .to_owned();
    Some((name, bytes.get(2 + count..)?.to_vec()))
}

pub(crate) fn seal_data(
    sealer: &Sealer,
    ctx: &AuthorCtx,
    body: DataBody,
) -> Result<DataBody, String> {
    let plain = pack(&body.namespace, &body.payload)?;
    Ok(DataBody {
        namespace: SEALED_NAMESPACE.to_owned(),
        payload: sealer.seal(ctx.node_key, ctx.device_id, ctx.next_writer_seq(), &plain)?,
        ..body
    })
}

pub(crate) fn seal_schema(
    sealer: &Sealer,
    ctx: &AuthorCtx,
    body: &SchemaBody,
) -> Result<SchemaBody, String> {
    let plain = pack(&body.namespace, &body.definition)?;
    Ok(SchemaBody {
        namespace: SEALED_NAMESPACE.to_owned(),
        definition: sealer.seal(ctx.node_key, ctx.device_id, ctx.next_writer_seq(), &plain)?,
    })
}

pub(crate) fn open_entry(
    sealer: &Sealer,
    entry: &mutation_log::SignedEntry,
) -> Option<mutation_log::SignedEntry> {
    match entry {
        mutation_log::SignedEntry::Data { header, body } if body.namespace == SEALED_NAMESPACE => {
            let (namespace, payload) = unpack(&sealer.open(header, &body.payload)?)?;
            Some(mutation_log::SignedEntry::Data {
                header: header.clone(),
                body: DataBody {
                    namespace,
                    payload,
                    ..body.clone()
                },
            })
        }
        mutation_log::SignedEntry::Schema { header, body }
            if body.namespace == SEALED_NAMESPACE =>
        {
            let (namespace, definition) = unpack(&sealer.open(header, &body.definition)?)?;
            Some(mutation_log::SignedEntry::Schema {
                header: header.clone(),
                body: SchemaBody {
                    namespace,
                    definition,
                },
            })
        }
        _ => Some(entry.clone()),
    }
}

pub(crate) fn for_key(database_id: &str, key: Zeroizing<[u8; 32]>) -> Result<Sealer, String> {
    Ok(Sealer::for_read_key(
        DatabaseId::from_hex(database_id).map_err(|_| "database id is not hex32")?,
        key,
    ))
}

/// A shared database opens with its read key, independently of the local signer.
#[derive(Debug)]
pub(crate) struct Opener(pub(crate) Sealer);

impl mutation_log::EntryOpener for Opener {
    fn open_entry(&self, entry: &mutation_log::SignedEntry) -> Option<mutation_log::SignedEntry> {
        open_entry(&self.0, entry)
    }
}
