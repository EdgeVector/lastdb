//! Keyed reads and link expansions a saved query is allowed to name.

use serde::Deserialize;

use super::PublishError;

const MAX_READS: usize = 32;
const MAX_LINKS: usize = 8;
const MAX_KEY_BYTES: usize = 1024;

/// One saved query. Reads run first. Each link point-gets the schema named
/// by a string field on a row those reads returned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Definition {
    pub reads: Vec<Read>,
    pub links: Vec<Link>,
}

/// One keyed read. `Point` is a hash schema. `All` is every range under one
/// hash. `Exact` is one hash and one range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Read {
    pub namespace: String,
    pub schema: String,
    pub hash: String,
    pub range: RangeSelect,
}

/// How much of one hash the read asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RangeSelect {
    /// The one record of a hash schema. The JSON range is absent.
    Point,
    /// Every range under the hash. The JSON range is `*`.
    All,
    /// One range. The JSON range is that value.
    Exact(String),
}

/// A point get whose hash is a string field of an already selected row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Link {
    pub from_namespace: String,
    pub from_schema: String,
    pub field: String,
    pub namespace: String,
    pub schema: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDefinition {
    reads: Vec<RawRead>,
    #[serde(default)]
    links: Vec<RawLink>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRead {
    namespace: String,
    schema: String,
    hash: String,
    range: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLink {
    from_namespace: String,
    from_schema: String,
    field: String,
    namespace: String,
    schema: String,
}

impl Definition {
    /// Parse a query file. Unknown fields are refused. The text must fit in
    /// the saved-query record.
    pub fn parse(bytes: &[u8]) -> Result<Self, PublishError> {
        if bytes.len() > super::MAX_STORED_TEXT {
            return Err(PublishError::new("saved query definition is too large"));
        }
        let raw: RawDefinition = serde_json::from_slice(bytes)
            .map_err(|_| PublishError::new("saved query JSON is invalid"))?;
        raw.into_definition()
    }

    /// Namespace and schema pairs, reads first, then link targets. Duplicates
    /// are omitted after the first.
    pub fn schema_refs(&self) -> Vec<(&str, &str)> {
        let mut out = Vec::new();
        for read in &self.reads {
            push_unique(&mut out, &read.namespace, &read.schema);
        }
        for link in &self.links {
            push_unique(&mut out, &link.namespace, &link.schema);
        }
        out
    }
}

impl RawDefinition {
    fn into_definition(self) -> Result<Definition, PublishError> {
        if self.reads.is_empty() {
            return Err(PublishError::new("saved query has no reads"));
        }
        if self.reads.len() > MAX_READS || self.links.len() > MAX_LINKS {
            return Err(PublishError::new("saved query has too many reads or links"));
        }
        let reads = self
            .reads
            .into_iter()
            .map(RawRead::into_read)
            .collect::<Result<Vec<_>, _>>()?;
        refuse_duplicate_reads(&reads)?;
        let links = self
            .links
            .into_iter()
            .map(|link| link.into_link(&reads))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Definition { reads, links })
    }
}

impl RawRead {
    fn into_read(self) -> Result<Read, PublishError> {
        require_label(&self.namespace)?;
        require_label(&self.schema)?;
        require_record_key(&self.hash)?;
        let range = match self.range {
            None => RangeSelect::Point,
            Some(value) if value == "*" => RangeSelect::All,
            Some(value) => {
                require_record_key(&value)?;
                RangeSelect::Exact(value)
            }
        };
        Ok(Read {
            namespace: self.namespace,
            schema: self.schema,
            hash: self.hash,
            range,
        })
    }
}

impl RawLink {
    fn into_link(self, reads: &[Read]) -> Result<Link, PublishError> {
        require_label(&self.from_namespace)?;
        require_label(&self.from_schema)?;
        require_label(&self.field)?;
        require_label(&self.namespace)?;
        require_label(&self.schema)?;
        let named = reads
            .iter()
            .any(|read| read.namespace == self.from_namespace && read.schema == self.from_schema);
        if !named {
            return Err(PublishError::new(
                "saved query link does not start from a read",
            ));
        }
        Ok(Link {
            from_namespace: self.from_namespace,
            from_schema: self.from_schema,
            field: self.field,
            namespace: self.namespace,
            schema: self.schema,
        })
    }
}

fn refuse_duplicate_reads(reads: &[Read]) -> Result<(), PublishError> {
    let mut seen = Vec::new();
    for read in reads {
        let mark = read_mark(read);
        if seen.contains(&mark) {
            return Err(PublishError::new("saved query repeats a read"));
        }
        seen.push(mark);
    }
    Ok(())
}

fn read_mark(read: &Read) -> (String, String, String, String) {
    let range = match &read.range {
        RangeSelect::Point => "point".to_owned(),
        RangeSelect::All => "all".to_owned(),
        RangeSelect::Exact(value) => format!("exact:{value}"),
    };
    (
        read.namespace.clone(),
        read.schema.clone(),
        read.hash.clone(),
        range,
    )
}

fn push_unique<'a>(out: &mut Vec<(&'a str, &'a str)>, namespace: &'a str, schema: &'a str) {
    if !out.iter().any(|item| *item == (namespace, schema)) {
        out.push((namespace, schema));
    }
}

pub fn label_ok(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn require_label(value: &str) -> Result<(), PublishError> {
    if label_ok(value) {
        Ok(())
    } else {
        Err(PublishError::new("saved query name is invalid"))
    }
}

fn require_record_key(value: &str) -> Result<(), PublishError> {
    if value.is_empty() || value.len() > MAX_KEY_BYTES || value.bytes().any(|byte| byte == 0) {
        Err(PublishError::new("saved query record key is invalid"))
    } else {
        Ok(())
    }
}
