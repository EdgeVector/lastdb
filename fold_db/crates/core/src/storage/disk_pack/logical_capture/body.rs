//! Certify exact stored atom bodies through one bounded native write view.

use super::*;
use std::io::{self, Write};

impl PackRuntime {
    /// Capture maintenance reads the staged bytes, not the prospective serving model.
    /// One caller collects every key; internal copies share its native work budget.
    pub(crate) async fn logical_capture_atoms(
        self: &Arc<Self>,
        keys: Vec<String>,
    ) -> StorageResult<Vec<crate::atom::Atom>> {
        let pending = transaction::PendingBatch::current(self)
            .ok_or_else(|| invalid("atom certification needs the data's native transaction"))?;
        if !self.logical_capture_enabled() || keys.len() > 1024 {
            return Err(invalid(
                "logical capture atom certification is outside its limits",
            ));
        }
        let context = read::ReadContext::current(self).await?;
        run_blocking(move || {
            context.validate_owner(Some(&pending))?;
            let mut call = crate::read_receipts::lock(&context.call);
            let result = certify_bodies(&context, &pending, &mut call, &keys);
            context.remember(&result);
            result
        })
        .await
    }
}

fn certify_bodies(
    context: &read::ReadContext,
    pending: &Arc<transaction::PendingBatch>,
    call: &mut physical::PhysicalRead,
    keys: &[String],
) -> StorageResult<Vec<crate::atom::Atom>> {
    let mut remaining = MAX_EVENT_BYTES;
    let mut atoms = Vec::with_capacity(keys.len());
    for key in keys {
        let collection = native_routing::collection_for_key(key.as_bytes());
        if collection != "atoms" || !key.ends_with("\0b") {
            return Err(invalid(
                "logical capture requires exact personal atom body keys",
            ));
        }
        let encoded = native_routing::encode(key.as_bytes());
        let atom = if let Some(record) = context.overlay_record(Some(pending), collection, &encoded)
        {
            let bytes = record
                .body
                .as_deref()
                .ok_or_else(|| invalid("a captured tip has a deleted atom body"))?;
            if bytes.len() > remaining {
                return Err(body_limit());
            }
            context.account_memory_record(call, key.len(), bytes.len())?;
            remaining -= bytes.len();
            decode_atom(bytes)?
        } else {
            let mut output = LimitedBody {
                bytes: Vec::new(),
                limit: remaining,
                exceeded: false,
            };
            let copied =
                call.home(collection)
                    .copy_body_to(collection, encoded.as_bytes(), &mut output);
            if output.exceeded {
                return Err(body_limit());
            }
            if copied.map_err(map)?.is_none() {
                return Err(invalid("a captured tip has no stored atom body"));
            }
            context.account_memory_record(call, key.len(), output.bytes.len())?;
            remaining -= output.bytes.len();
            decode_atom(&output.bytes)?
        };
        atoms.push(atom);
    }
    Ok(atoms)
}

fn decode_atom(bytes: &[u8]) -> StorageResult<crate::atom::Atom> {
    crate::db_operations::atom_store::decode_plain_atom(bytes)
        .map_err(|_| invalid("logical capture stored atom is not a valid plain V2 body"))
}

fn body_limit() -> StorageError {
    StorageError::ReadResourceLimit {
        detail: "logical capture actual atom bodies exceed one MiB; complete write refused".into(),
    }
}

struct LimitedBody {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl Write for LimitedBody {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("logical capture body allowance exceeded"));
        }
        self.bytes
            .try_reserve_exact(bytes.len())
            .map_err(|_| io::Error::other("logical capture body allocation refused"))?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
