//! Streaming upgrade retains every account before the compact index replaces format1.

use super::*;
use control_files::MAX_CONTROL_BYTES;
use serde::de::{DeserializeSeed, Error as _, MapAccess, Visitor};
use std::cell::Cell;
use std::fmt;
use std::fs::File;
use std::io::{BufReader, Read};
use std::rc::Rc;

#[derive(Clone)]
struct ReadBudget(Rc<Cell<u64>>, Rc<Cell<u64>>);

impl ReadBudget {
    fn new() -> Self {
        Self(
            Rc::new(Cell::new(0)),
            Rc::new(Cell::new(MAX_CONTROL_BYTES as u64)),
        )
    }
    fn next_value(&self) {
        self.1
            .set(self.0.get().saturating_add(MAX_CONTROL_BYTES as u64));
    }
}

struct BudgetReader<R> {
    reader: R,
    budget: ReadBudget,
}

impl<R: Read> Read for BudgetReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.budget.1.get().saturating_sub(self.budget.0.get());
        if remaining == 0 {
            return Err(std::io::Error::other(
                "peer account metadata budget exceeded",
            ));
        }
        let maximum = buffer.len().min(remaining as usize);
        let read = self.reader.read(&mut buffer[..maximum])?;
        self.budget.0.set(self.budget.0.get() + read as u64);
        Ok(read)
    }
}

struct Context<'a> {
    directory: &'a Path,
    source_id: &'a str,
    budget: ReadBudget,
    legacy_entries: Cell<usize>,
}

pub(super) fn open(directory: &Path, source_id: &str) -> SyncResult<PeerState> {
    let path = directory.join("state.json");
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PeerState {
                format_version: 2,
                ..Default::default()
            })
        }
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() {
        return Err(held("the peer state index is not a regular file"));
    }
    let budget = ReadBudget::new();
    let reader = BudgetReader {
        reader: BufReader::new(File::open(&path)?),
        budget: budget.clone(),
    };
    let mut decoder = serde_json::Deserializer::from_reader(reader);
    let context = Context {
        directory,
        source_id,
        budget,
        legacy_entries: Cell::new(0),
    };
    let (version, state) = StateSeed(&context).deserialize(&mut decoder)?;
    decoder.end()?;
    if context.budget.0.get() != metadata.len() {
        return Err(held("the peer state index size changed"));
    }
    if version == 2 && context.legacy_entries.get() != 0 {
        return Err(held(
            "the compact peer index contains an embedded legacy control",
        ));
    }
    if version == 1 {
        write(directory, &state)?;
    }
    Ok(state)
}

pub(super) fn write(directory: &Path, state: &PeerState) -> SyncResult<()> {
    let encoded = crate::sync::cloud_purge::inventory::bounded_json(state, MAX_CONTROL_BYTES)?;
    write_atomic(&directory.join("state.json"), &encoded)
}

struct StateSeed<'a>(&'a Context<'a>);
impl<'de> DeserializeSeed<'de> for StateSeed<'_> {
    type Value = (u32, PeerState);
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        decoder.deserialize_map(StateVisitor(self.0))
    }
}

struct StateVisitor<'a>(&'a Context<'a>);
impl<'de> Visitor<'de> for StateVisitor<'_> {
    type Value = (u32, PeerState);
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a peer state index")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut version = None;
        let mut accounts = None;
        while let Some(field) = map.next_key::<String>()? {
            match field.as_str() {
                "format_version" if version.is_none() => version = Some(map.next_value::<u32>()?),
                "accounts" if accounts.is_none() => {
                    accounts = Some(map.next_value_seed(AccountsSeed(self.0))?);
                }
                _ => return Err(M::Error::custom("unknown or duplicate peer state field")),
            }
        }
        let version = version.ok_or_else(|| M::Error::custom("peer state version absent"))?;
        if !matches!(version, 1 | 2) {
            return Err(M::Error::custom("unsupported peer state version"));
        }
        let accounts = accounts.ok_or_else(|| M::Error::custom("peer state accounts absent"))?;
        Ok((
            version,
            PeerState {
                format_version: 2,
                accounts,
            },
        ))
    }
}

struct AccountsSeed<'a>(&'a Context<'a>);
impl<'de> DeserializeSeed<'de> for AccountsSeed<'_> {
    type Value = BTreeMap<String, PeerAccount>;
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        decoder.deserialize_map(AccountsVisitor(self.0))
    }
}

struct AccountsVisitor<'a>(&'a Context<'a>);
impl<'de> Visitor<'de> for AccountsVisitor<'_> {
    type Value = BTreeMap<String, PeerAccount>;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded peer accounts")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut accounts = BTreeMap::new();
        let mut index_bytes = 1024;
        while let Some(key) = map.next_key::<String>()? {
            if key.len() > 1024 {
                return Err(M::Error::custom("peer account key exceeds its budget"));
            }
            self.0.budget.next_value();
            let raw = map.next_value::<AccountRecord>()?;
            let account = raw.into_account(self.0).map_err(M::Error::custom)?;
            if key != account_key(&account.control.account).map_err(M::Error::custom)? {
                return Err(M::Error::custom("peer account key and control differ"));
            }
            index_bytes += serde_json::to_vec(&(&key, &account))
                .map_err(M::Error::custom)?
                .len();
            if index_bytes > MAX_CONTROL_BYTES || accounts.insert(key, account).is_some() {
                return Err(M::Error::custom(
                    "peer state index exceeds its budget or repeats an account",
                ));
            }
            self.0.budget.next_value();
        }
        Ok(accounts)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountRecord {
    control: RecordControl,
    #[serde(default)]
    adoption: Option<AdoptionReceipt>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RecordControl {
    Reference(ControlRef),
    Legacy(Box<CloudPurgeControl>),
}

impl AccountRecord {
    fn into_account(self, context: &Context<'_>) -> SyncResult<PeerAccount> {
        let control = match self.control {
            RecordControl::Reference(reference) => {
                reference.validate()?;
                reference
            }
            RecordControl::Legacy(control) => {
                context.legacy_entries.set(context.legacy_entries.get() + 1);
                control_files::store(context.directory, &control, context.source_id)?
            }
        };
        if self.adoption.as_ref().is_some_and(|receipt| {
            receipt.account != control.account
                || receipt.source_id != context.source_id
                || receipt.generation > control.generation
                || !crate::hex::is_lower_hex_sha256(&receipt.installed_manifest_sha256)
                || !crate::hex::is_lower_hex_sha256(&receipt.retention_sha256)
                || !crate::hex::is_lower_hex_sha256(&receipt.switch_receipt_sha256)
        }) {
            return Err(held("the peer adoption receipt identity is invalid"));
        }
        Ok(PeerAccount {
            control,
            adoption: self.adoption,
        })
    }
}
