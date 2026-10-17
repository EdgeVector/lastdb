use super::*;
use crate::schema::types::field::FieldValue;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReadVersion {
    pub generation: u64,
    pub accepted: usize,
    pub revision: u64,
}

/// Only one field's content and provenance belong to this logical path.
#[derive(Serialize, Deserialize)]
pub(super) struct PathBody {
    pub content: serde_json::Value,
    pub provenance: Option<(ReadVersion, FieldValue)>,
    #[serde(default)]
    pub witness: Option<u64>,
    #[serde(default)]
    pub wire: Option<super::warm::WireMetadata>,
    /// Query descriptors name other already-used paths; they contain no values.
    #[serde(default)]
    pub projections: BTreeMap<String, Vec<String>>,
}

impl PathBody {
    /// Measure the fresh content-only body before its content is cloned.
    pub(super) fn content_encoded_len(value: &serde_json::Value) -> StorageResult<usize> {
        encoded_len(&BorrowedPathBody {
            content: value,
            provenance: None,
            witness: None,
            wire: None,
            projections: EmptyProjections,
        })
    }

    /// FieldValue always emits `value`. Exchanging that value with content's
    /// null preserves the exact encoded size without cloning either value.
    pub(super) fn from_field_encoded_len(
        version: ReadVersion,
        value: &FieldValue,
        witness: Option<u64>,
    ) -> StorageResult<usize> {
        encoded_len(&BorrowedPathBody {
            content: &serde_json::Value::Null,
            provenance: Some((version, value)),
            witness,
            wire: None,
            projections: EmptyProjections,
        })
    }

    pub(super) fn content(value: &serde_json::Value) -> Self {
        Self {
            content: value.clone(),
            provenance: None,
            witness: None,
            wire: None,
            projections: BTreeMap::new(),
        }
    }

    pub(super) fn from_field(
        version: ReadVersion,
        value: &FieldValue,
        witness: Option<u64>,
    ) -> Self {
        let mut provenance = value.clone();
        provenance.value = serde_json::Value::Null;
        Self {
            content: value.value.clone(),
            provenance: Some((version, provenance)),
            witness,
            wire: None,
            projections: BTreeMap::new(),
        }
    }

    pub(super) fn valid(&self, context: &super::super::read::ReadContext) -> bool {
        self.provenance
            .as_ref()
            .is_some_and(|(version, _)| context.provenance_valid(*version, self.witness))
    }

    pub(super) fn field(self, context: &super::super::read::ReadContext) -> Option<FieldValue> {
        let (cached_version, mut value) = self.provenance?;
        if !context.provenance_valid(cached_version, self.witness)
            || !context.inherit_witness(self.witness)
        {
            return None;
        }
        if !context.refresh_molecule_version(self.witness, &mut value) {
            return None;
        }
        value.value = self.content;
        Some(value)
    }
}

#[derive(Serialize)]
struct BorrowedPathBody<'a> {
    content: &'a serde_json::Value,
    provenance: Option<(ReadVersion, &'a FieldValue)>,
    witness: Option<u64>,
    wire: Option<&'a super::warm::WireMetadata>,
    projections: EmptyProjections,
}

struct EmptyProjections;

impl Serialize for EmptyProjections {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::ser::SerializeMap::end(serializer.serialize_map(Some(0))?)
    }
}

fn encoded_len(value: &impl Serialize) -> StorageResult<usize> {
    let mut length = EncodedLength::default();
    serde_json::to_writer(&mut length, value).map_err(|error| serialization(&error))?;
    Ok(length.0)
}

#[derive(Default)]
struct EncodedLength(usize);

impl std::io::Write for EncodedLength {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("native path encoded length overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
