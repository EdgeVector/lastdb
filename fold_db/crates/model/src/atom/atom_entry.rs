//! The current tip of one molecule key, and its stored JSON form.

use super::{lww_order_key, Provenance};

fn is_zero_u8(v: &u8) -> bool {
    *v == 0
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

fn is_empty_str(v: &&str) -> bool {
    v.is_empty()
}

/// Current atom and author identity at one molecule key.
///
/// Current constructors leave the optional crypto fields empty. A tip has no
/// archived-value query or history chain. Separate `history\0` mutation events
/// retain conflict and replay outcomes with their own atom references.
///
/// The signer fields and the reserved link are almost always empty, so they
/// live in one boxed part that is `None` when all of them are empty. The warm
/// set holds one entry per read field, and five empty strings and an empty
/// provenance cost about 128 bytes in each (measured 2026-10-20). The stored
/// JSON is unchanged: [`AtomEntryWire`] keeps the flat field list.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
#[serde(from = "AtomEntryWire")]
pub struct AtomEntry {
    pub atom_uuid: String,
    pub written_at: u64, // nanos since epoch
    /// Durable per-device logical author counter. Zero when no counter is supplied.
    pub logical_counter: u64,
    /// Mutation identity used after the author clock as a stable tie-break.
    pub mutation_uuid: String,
    /// Device identity for the LWW tie-break, usually the writer's public key.
    pub device_id: String,
    /// Sync device that wrote this tip (the sync `writer_id` of its log
    /// stream). Not part of the LWW order: `device_id` above is the owner key,
    /// which own devices share. Empty means "in doubt": an old tip, a write
    /// without a sync device id, or an import whose writer is unknown.
    pub origin_device: String,
    /// Signer fields and the reserved link. `None` when all are empty.
    extra: Option<Box<AtomEntryExtra>>,
}

/// The rarely set part of an [`AtomEntry`].
#[derive(Debug, Clone, PartialEq, Default)]
struct AtomEntryExtra {
    writer_pubkey: String,
    signature: String,
    signature_version: u8,
    provenance: Option<Provenance>,
    prev_tip_id: String,
}

impl AtomEntryExtra {
    fn is_empty(&self) -> bool {
        self.writer_pubkey.is_empty()
            && self.signature.is_empty()
            && self.signature_version == 0
            && self.provenance.is_none()
            && self.prev_tip_id.is_empty()
    }
}

/// The stored form of an [`AtomEntry`]: one flat object.
#[derive(serde::Deserialize)]
struct AtomEntryWire {
    atom_uuid: String,
    #[serde(default)]
    written_at: u64,
    #[serde(default)]
    logical_counter: u64,
    #[serde(default)]
    mutation_uuid: String,
    #[serde(default)]
    writer_pubkey: String,
    #[serde(default)]
    signature: String,
    #[serde(default)]
    signature_version: u8,
    #[serde(default)]
    provenance: Option<Provenance>,
    #[serde(default)]
    device_id: String,
    #[serde(default)]
    origin_device: String,
    #[serde(default)]
    prev_tip_id: String,
}

/// Borrowed stored form, so serialization does not clone the entry.
#[derive(serde::Serialize)]
struct AtomEntryWireRef<'a> {
    atom_uuid: &'a str,
    written_at: u64,
    #[serde(skip_serializing_if = "is_zero_u64")]
    logical_counter: u64,
    #[serde(skip_serializing_if = "is_empty_str")]
    mutation_uuid: &'a str,
    #[serde(skip_serializing_if = "is_empty_str")]
    writer_pubkey: &'a str,
    #[serde(skip_serializing_if = "is_empty_str")]
    signature: &'a str,
    #[serde(skip_serializing_if = "is_zero_u8")]
    signature_version: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    provenance: Option<&'a Provenance>,
    #[serde(skip_serializing_if = "is_empty_str")]
    device_id: &'a str,
    #[serde(skip_serializing_if = "is_empty_str")]
    origin_device: &'a str,
    #[serde(skip_serializing_if = "is_empty_str")]
    prev_tip_id: &'a str,
}

impl From<AtomEntryWire> for AtomEntry {
    fn from(wire: AtomEntryWire) -> Self {
        let mut entry = Self {
            atom_uuid: wire.atom_uuid,
            written_at: wire.written_at,
            logical_counter: wire.logical_counter,
            mutation_uuid: wire.mutation_uuid,
            device_id: wire.device_id,
            origin_device: wire.origin_device,
            extra: None,
        };
        entry.set_extra(AtomEntryExtra {
            writer_pubkey: wire.writer_pubkey,
            signature: wire.signature,
            signature_version: wire.signature_version,
            provenance: wire.provenance,
            prev_tip_id: wire.prev_tip_id,
        });
        entry
    }
}

impl serde::Serialize for AtomEntry {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        AtomEntryWireRef {
            atom_uuid: &self.atom_uuid,
            written_at: self.written_at,
            logical_counter: self.logical_counter,
            mutation_uuid: &self.mutation_uuid,
            writer_pubkey: self.writer_pubkey(),
            signature: self.signature(),
            signature_version: self.signature_version(),
            provenance: self.provenance(),
            device_id: &self.device_id,
            origin_device: &self.origin_device,
            prev_tip_id: self.prev_tip_id(),
        }
        .serialize(serializer)
    }
}

impl AtomEntry {
    /// Construct a current tip with the reserved link field empty.
    #[must_use]
    pub fn thin(atom_uuid: String, written_at: u64, device_id: String) -> Self {
        Self::thin_with_prev(atom_uuid, written_at, device_id, String::new())
    }

    /// Construct a current tip with the supplied reserved link field.
    #[must_use]
    pub fn thin_with_prev(
        atom_uuid: String,
        written_at: u64,
        device_id: String,
        prev_tip_id: String,
    ) -> Self {
        Self::thin_with_author(
            atom_uuid,
            written_at,
            0,
            device_id,
            String::new(),
            prev_tip_id,
        )
    }

    /// Construct a current tip with a signed mutation author identity.
    #[must_use]
    pub fn thin_with_author(
        atom_uuid: String,
        written_at: u64,
        logical_counter: u64,
        device_id: String,
        mutation_uuid: String,
        prev_tip_id: String,
    ) -> Self {
        let mut entry = Self {
            atom_uuid,
            written_at,
            logical_counter,
            mutation_uuid,
            device_id,
            origin_device: String::new(),
            extra: None,
        };
        entry.set_prev_tip_id(prev_tip_id);
        entry
    }

    /// Optional base64-encoded public key. Current constructors leave it empty.
    #[must_use]
    pub fn writer_pubkey(&self) -> &str {
        self.extra.as_ref().map_or("", |extra| &extra.writer_pubkey)
    }

    /// Optional base64-encoded signature. Current constructors leave it empty.
    #[must_use]
    pub fn signature(&self) -> &str {
        self.extra.as_ref().map_or("", |extra| &extra.signature)
    }

    /// Optional signature scheme version. Current constructors use zero.
    #[must_use]
    pub fn signature_version(&self) -> u8 {
        self.extra
            .as_ref()
            .map_or(0, |extra| extra.signature_version)
    }

    /// Optional writer provenance. Current constructors leave it absent.
    #[must_use]
    pub fn provenance(&self) -> Option<&Provenance> {
        self.extra
            .as_ref()
            .and_then(|extra| extra.provenance.as_ref())
    }

    /// Retained serialized field. Current writes leave it empty; no archived
    /// tip reader or historical query follows this value.
    #[must_use]
    pub fn prev_tip_id(&self) -> &str {
        self.extra.as_ref().map_or("", |extra| &extra.prev_tip_id)
    }

    /// Set the reserved link field.
    pub fn set_prev_tip_id(&mut self, prev_tip_id: String) {
        let mut extra = self.take_extra();
        extra.prev_tip_id = prev_tip_id;
        self.set_extra(extra);
    }

    /// Heap bytes of the boxed part, for the warm set gauge: its strings, its
    /// provenance strings, and the box itself.
    #[must_use]
    pub fn extra_heap_bytes(&self) -> usize {
        self.extra.as_ref().map_or(0, |extra| {
            let provenance = extra.provenance.as_ref().map_or(0, Provenance::heap_bytes);
            std::mem::size_of::<AtomEntryExtra>()
                + extra.writer_pubkey.capacity()
                + extra.signature.capacity()
                + extra.prev_tip_id.capacity()
                + provenance
        })
    }

    fn take_extra(&mut self) -> AtomEntryExtra {
        self.extra.take().map(|extra| *extra).unwrap_or_default()
    }

    fn set_extra(&mut self, extra: AtomEntryExtra) {
        self.extra = (!extra.is_empty()).then(|| Box::new(extra));
    }

    /// Device key used for LWW: explicit `device_id`, otherwise `writer_pubkey`.
    #[must_use]
    pub fn lww_device(&self) -> &str {
        if self.device_id.is_empty() {
            self.writer_pubkey()
        } else {
            &self.device_id
        }
    }

    /// LWW tuple for this tip: origin time, author counter, device,
    /// mutation identity, then atom identity.
    #[must_use]
    pub fn lww_key(&self) -> (u64, u64, &str, &str, &str) {
        lww_order_key(
            self.written_at,
            self.logical_counter,
            self.lww_device(),
            self.mutation_uuid.as_str(),
            self.atom_uuid.as_str(),
        )
    }
}
