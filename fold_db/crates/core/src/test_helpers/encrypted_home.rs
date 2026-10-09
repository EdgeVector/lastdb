//! A `FoldDB` whose molecule keys are **really encoded** — the configuration
//! every node booted with a recovery phrase runs, and the one this crate's
//! tests almost never build.
//!
//! ## Why this exists as a shared fixture
//!
//! Nearly every test in the crate builds its store with `FoldDB::new(path)`: no
//! identity, both key encodings `Plain`, so the API↔storage transform is the
//! identity function. That is precisely the one configuration in which an
//! API-form/storage-form confusion **cannot** appear.
//!
//! It has hidden the same defect three times. A caller that holds a
//! storage-form molecule and hands it to a full rewrite that assumes API form
//! produces `blind(blind(hash))` / `ope(ope(range))` — rows that are still on
//! disk, still list, and are unreachable by any keyed read. `purge_records_bulk`
//! and `repair_dangling_tips` were both fixed in fold #1209; the fixture that
//! caught them was local to the purge suite, so the next path to need it would
//! have copied it or gone without. It lives here now.
//!
//! ## The oracle rule, learned the hard way
//!
//! An unfiltered list is **not** an oracle for this defect class. It resolves
//! field values out of atom bodies and reports whatever slot it scanned, so a
//! row stranded at a key nothing will ever derive lists perfectly intact. Assert
//! [`EncryptedHome::point_read_payload`] — supplied the plaintext key, it
//! re-derives the storage segment and therefore cannot be satisfied by a row
//! sitting at the wrong key — or inspect
//! [`EncryptedHome::stored_slot_keys`] directly.
//!
//! ## Keep the control
//!
//! Every suite built on this should also run its assertions against [`PLAIN`].
//! A fixture that silently stopped encrypting would otherwise make the
//! encrypted case pass for the wrong reason.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{json, Value};

use crate::atom::{HashKeyEncoding, MoleculeKeyCodec, RangeKeyEncoding};
use crate::db_operations::DbOperations;
use crate::fold_db_core::fold_db::FoldDbInit;
use crate::fold_db_core::FoldDB;
use crate::schema::types::field::HashRangeFilter;
use crate::schema::types::operations::{MutationType, Query};
use crate::schema::types::{KeyValue, Mutation};
use crate::schema::{SchemaError, SchemaState};
use crate::test_helpers::TestSchemaBuilder;

/// Fixed key material. Only that `storage_* != api_*` matters.
pub const BLIND_KEY: [u8; 32] = [0x5a; 32];
pub const OPE_KEY: [u8; 32] = [0xa5; 32];

/// The product default for a node booted with a recovery phrase.
pub const ENCRYPTED: (HashKeyEncoding, RangeKeyEncoding) =
    (HashKeyEncoding::BlindV1, RangeKeyEncoding::OpeV1);
/// The control. `Plain`/`Plain` is what `FoldDB::new` gives every other test.
pub const PLAIN: (HashKeyEncoding, RangeKeyEncoding) =
    (HashKeyEncoding::Plain, RangeKeyEncoding::Plain);

/// A live node plus the `owner`/`stamp`/`payload` HashRange schema the helpers
/// below read and write.
///
/// HashRange on purpose: it exercises **both** encodings at once (blinded hash
/// segment, order-preserving range segment), where a Hash-only schema would
/// leave the range transform as the identity and halve the coverage.
pub struct EncryptedHome {
    pub db: FoldDB,
    pub schema: String,
    _dir: tempfile::TempDir,
}

/// A node whose molecule keys are encoded with `encodings`, with no schema
/// loaded. Prefer [`EncryptedHome::seeded`] unless the test needs to control
/// the schema itself.
pub async fn node(schema: &str, encodings: (HashKeyEncoding, RangeKeyEncoding)) -> EncryptedHome {
    let (hash_encoding, range_encoding) = encodings;
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(
        crate::storage::LastStoreNamespacedStore::open(dir.path()).expect("open laststore"),
    ) as Arc<dyn crate::storage::traits::NamespacedStore>;

    let codec = MoleculeKeyCodec::with_encodings(
        hash_encoding,
        range_encoding,
        Some(BLIND_KEY),
        Some(OPE_KEY),
    );
    let db_ops =
        DbOperations::from_namespaced_store_with_atom_content_and_hash_key(store, None, codec)
            .await
            .expect("db ops");

    let signer = Arc::new(crate::security::Ed25519KeyPair::generate().expect("signer"));
    let db = FoldDB::initialize_from_init(FoldDbInit {
        db_ops: Arc::new(db_ops),
        db_path: dir.path().to_string_lossy().into_owned(),
        signer,
        search_outbox_inbox: None,
        #[cfg(feature = "cloud-sync")]
        mutation_log_capture: None,
        #[cfg(feature = "cloud-sync")]
        packing_slots: None,
    })
    .await
    .expect("FoldDB");

    EncryptedHome {
        db,
        schema: schema.to_string(),
        _dir: dir,
    }
}

impl EncryptedHome {
    /// A node with the `owner`/`stamp`/`payload` HashRange schema approved and
    /// `rows` (as `(owner, stamp)`) written. Each row's `payload` is
    /// `payload-{stamp}`.
    pub async fn seeded(
        schema: &str,
        encodings: (HashKeyEncoding, RangeKeyEncoding),
        rows: &[(&str, &str)],
    ) -> Self {
        let mut f = node(schema, encodings).await;
        f.seed_schema(schema, rows).await;
        f
    }

    /// Approve one more `owner`/`stamp`/`payload` HashRange schema on this
    /// node, write `rows` to it, and point every helper at it.
    ///
    /// For suites that need two schemas in one store: seed the second, then
    /// set `schema` back to switch the helpers between them.
    pub async fn seed_schema(&mut self, schema: &str, rows: &[(&str, &str)]) {
        self.schema = schema.to_string();
        let json_str = TestSchemaBuilder::new(schema)
            .fields(&["owner", "stamp", "payload"])
            .hash_key("owner")
            .range_key("stamp")
            .build_json();
        self.db
            .load_schema_from_json(&json_str)
            .await
            .expect("load schema");
        self.db
            .schema_manager()
            .set_schema_state(schema, SchemaState::Available)
            .await
            .expect("approve schema");

        for (owner, stamp) in rows {
            self.write(
                owner,
                stamp,
                &format!("payload-{stamp}"),
                MutationType::Create,
            )
            .await;
        }
        // Docker overlayfs on the Mini runner can hide unflushed LastStore
        // puts from the next query; local APFS does not. Flush so seed is
        // visible before the caller asserts (`surviving_stamps` empty on CI).
        self.db.flush().await.expect("flush seeded rows");
    }

    async fn write(&self, owner: &str, stamp: &str, payload: &str, kind: MutationType) {
        let mut fields: HashMap<String, Value> = HashMap::new();
        fields.insert("owner".to_string(), json!(owner));
        fields.insert("stamp".to_string(), json!(stamp));
        fields.insert("payload".to_string(), json!(payload));
        self.db
            .mutation_manager()
            .write_mutations_batch_async(
                vec![Mutation::new(
                    self.schema.clone(),
                    fields,
                    KeyValue::new(Some(owner.to_string()), Some(stamp.to_string())),
                    "pk".to_string(),
                    kind,
                )],
                None,
            )
            .await
            .expect("write row");
        // Resident mode acks before the deferred durable persist. The fixture
        // promises a durable seed: record-molecule compaction zips DURABLE
        // field tips, and on an undrained seed it found an empty zip and
        // skipped the key (Mini lane flake,
        // compact_blindv1_hashrangekey_stamps_envelope_from_api_and_storage_hash).
        assert!(
            self.db
                .wait_for_background_tasks(crate::constants::MUTATION_BACKGROUND_TASK_TIMEOUT)
                .await,
            "fixture write must drain its deferred persist"
        );
        self.db.flush().await.expect("flush written row");
    }

    /// Overwrite a row, so its previous value becomes a superseded atom
    /// reachable only through the slot's `tv:` chain.
    pub async fn rewrite(&self, owner: &str, stamp: &str, payload: &str) {
        self.write(owner, stamp, payload, MutationType::Update)
            .await;
    }

    pub async fn purge(&self, owner: &str, stamp: &str) -> Result<(), SchemaError> {
        self.purge_batch(&[(owner, stamp)]).await
    }

    /// Purge several rows in **one** batch — `purge_records_bulk`, which is a
    /// different code path from N single purges and the one that has to reclaim.
    pub async fn purge_batch(&self, rows: &[(&str, &str)]) -> Result<(), SchemaError> {
        self.db
            .mutation_manager()
            .write_mutations_batch_async(
                rows.iter()
                    .map(|(owner, stamp)| {
                        Mutation::new(
                            self.schema.clone(),
                            HashMap::new(),
                            KeyValue::new(Some((*owner).to_string()), Some((*stamp).to_string())),
                            "pk".to_string(),
                            MutationType::Purge,
                        )
                    })
                    .collect(),
                None,
            )
            .await
            .map(|_| ())
    }

    /// Every surviving row's `stamp`, sorted — read through an unfiltered list.
    ///
    /// Necessary but **never sufficient**: see the module docs. A stranded row
    /// still appears here.
    ///
    /// Note which molecule this reads. Each field is its OWN molecule, so this
    /// answers for `stamp` alone. A whole-record operation (purge) empties every
    /// field's slot and shows up here; damage confined to one field's molecule
    /// (a dangling `payload` tip) does not — use [`Self::surviving_payloads`].
    pub async fn surviving_stamps(&self) -> Vec<String> {
        self.list_field("stamp").await
    }

    /// Every surviving row's `payload`, sorted. The list counterpart to
    /// [`Self::point_read_payload`], over the same molecule.
    pub async fn surviving_payloads(&self) -> Vec<String> {
        self.list_field("payload").await
    }

    async fn list_field(&self, field: &str) -> Vec<String> {
        let res = self
            .db
            .query_executor()
            .query(Query::new_with_filter(
                self.schema.clone(),
                vec![field.to_string()],
                None,
            ))
            .await
            .expect("list query");
        let mut out: Vec<String> = res
            .get(field)
            .map(|values| {
                values
                    .values()
                    .filter_map(|fv| fv.value.as_str().map(ToString::to_string))
                    .collect()
            })
            .unwrap_or_default();
        out.sort();
        out
    }

    /// `payload` fetched by an exact `HashRangeKey` point read — **the** oracle.
    ///
    /// The caller supplies the plaintext key and the read re-derives the storage
    /// segment, so this cannot be satisfied by a row sitting at a key nothing
    /// derives.
    pub async fn point_read_payload(&self, owner: &str, stamp: &str) -> Option<String> {
        self.point_read(owner, stamp)
            .await
            .and_then(|fv| fv.value.as_str().map(ToString::to_string))
    }

    /// The whole `FieldValue` behind [`Self::point_read_payload`] — carries
    /// `atom_uuid`, which is how a test names the atom body it wants to damage.
    ///
    /// Result maps may be keyed in API form, storage form, or mixed
    /// (BlindV1 hash + decoded OPE range). Pick the row for *this* key only —
    /// `HashMap::values().next()` would accept a sibling that leaked into the
    /// same field map (CI flake: `storage_slot_purge_is_exact` saw
    /// `Some("shared-payload")` from the retained twin).
    pub async fn point_read(
        &self,
        owner: &str,
        stamp: &str,
    ) -> Option<crate::schema::types::field::FieldValue> {
        let res = self
            .db
            .query_executor()
            .query(Query::new_with_filter(
                self.schema.clone(),
                vec!["payload".to_string()],
                Some(HashRangeFilter::HashRangeKey {
                    hash: owner.to_string(),
                    range: stamp.to_string(),
                }),
            ))
            .await
            .expect("point read");
        let payload = res.get("payload")?;
        let api = KeyValue::new(Some(owner.to_string()), Some(stamp.to_string()));
        if let Some(fv) = payload.get(&api) {
            return Some(fv.clone());
        }
        let mol = self.molecule_uuid("payload");
        let atoms = self.db.db_ops().atoms();
        let storage_hash = atoms.storage_hash(&mol, owner).ok()?;
        let storage_range = atoms.storage_range(&mol, stamp).ok()?;
        for kv in [
            KeyValue::new(Some(storage_hash.clone()), Some(stamp.to_string())),
            KeyValue::new(Some(storage_hash), Some(storage_range.clone())),
            KeyValue::new(Some(owner.to_string()), Some(storage_range)),
        ] {
            if let Some(fv) = payload.get(&kv) {
                return Some(fv.clone());
            }
        }
        None
    }

    /// Read a row through a **share prefix** rather than the primary namespace.
    ///
    /// Same derivation as [`Self::point_read_payload`] — the storage segment is
    /// computed from the plaintext key — so it is an oracle for the share
    /// fan-out in exactly the same way, and for the same reason.
    pub async fn point_read_payload_at_prefix(
        &self,
        prefix: &str,
        owner: &str,
        stamp: &str,
    ) -> Option<String> {
        let atoms = self.db.db_ops().atoms();
        let mol_uuid = self.molecule_uuid("payload");
        let molecule = atoms
            .load_molecule_per_key(&mol_uuid, Some(prefix))
            .await
            .expect("load share-prefix molecule")?;
        // `load_molecule_per_key` yields STORAGE-form slots, so look the row up
        // by the storage segments a reader derives from the plaintext key —
        // which is the whole question a double-encoded rewrite gets wrong.
        let storage_hash = atoms.storage_hash(&mol_uuid, owner).expect("storage hash");
        let storage_range = atoms
            .storage_range(&mol_uuid, stamp)
            .expect("storage range");
        let uuid = molecule
            .get_atom_uuid(&storage_hash, &storage_range)?
            .clone();
        let atom = atoms
            .get_atom_by_uuid(&uuid, None)
            .await
            .expect("get shared atom")
            .expect("shared atom body");
        atom.content().as_str().map(ToString::to_string)
    }

    /// The molecule uuid backing `field`.
    pub fn molecule_uuid(&self, field: &str) -> String {
        self.db
            .schema_manager()
            .get_schema_metadata(&self.schema)
            .expect("schema metadata")
            .expect("schema present")
            .runtime_fields
            .get(field)
            .expect("field present")
            .common()
            .molecule_uuid()
            .expect("molecule uuid")
            .clone()
    }

    /// Every `mk:` storage key currently present for `field`'s molecule under
    /// `prefix` (`None` = the primary namespace).
    ///
    /// The direct oracle: a rewrite that re-encoded its input produces keys that
    /// appear in no earlier snapshot, which says *what* went wrong rather than
    /// merely that a read missed.
    pub async fn stored_slot_keys(&self, field: &str, prefix: Option<&str>) -> Vec<String> {
        let mol_uuid = self.molecule_uuid(field);
        let scan_prefix = crate::schema::types::field::build_storage_key(
            prefix,
            &crate::atom::molecule_key_codec::molecule_record_prefix(&mol_uuid),
        );
        let scanned: Vec<(String, Value)> = self
            .db
            .db_ops()
            .atoms()
            .raw()
            .scan_items_with_prefix(&scan_prefix)
            .await
            .expect("scan mk: rows");
        let mut keys: Vec<String> = scanned.into_iter().map(|(k, _)| k).collect();
        keys.sort();
        keys
    }

    pub async fn atom_count(&self) -> usize {
        self.db
            .db_ops()
            .atoms()
            .list_atoms_by_schema(&self.schema, None)
            .await
            .expect("list atoms")
            .len()
    }

    /// Make the tip for `(owner, stamp)` **dangle**: delete the atom body it
    /// points at, by every route a reader would try, and its locator.
    ///
    /// This is the damage `repair_dangling_tips` exists to clean up, and the
    /// live primary's standing `Read integrity: DEGRADED` condition. Returns the
    /// orphaned atom uuid.
    ///
    /// Deletes by scanning the `atom:` namespace for the uuid rather than
    /// rebuilding the body key, because the body key shape depends on the home's
    /// `AtomKeyEncoding` (flat vs partition-prefixed) and a test that guessed
    /// wrong would silently leave the body readable — and then assert nothing.
    pub async fn dangle_tip(&self, owner: &str, stamp: &str) -> String {
        let uuid = self
            .point_read(owner, stamp)
            .await
            .expect("row must exist before it can be damaged")
            .atom_uuid;
        assert!(!uuid.is_empty(), "row has no atom uuid");

        let raw = self.db.db_ops().atoms().raw();
        let rows: Vec<(String, Value)> = raw
            .scan_items_with_prefix("atom:")
            .await
            .expect("scan atom bodies");
        let mut deleted = 0usize;
        for (key, _) in rows {
            if key.contains(&uuid) {
                raw.delete_item(&key).await.expect("delete atom body");
                deleted += 1;
            }
        }
        assert!(
            deleted > 0,
            "no atom body found for {uuid} — the fixture damaged nothing, so any \
             assertion after this would pass vacuously"
        );

        let locator = crate::atom::atom_locator_codec::locator_key(&uuid);
        if raw.exists_item(&locator).await.expect("locator exists") {
            raw.delete_item(&locator).await.expect("delete locator");
        }
        // This fixture models an atom body that is gone from the whole node,
        // not merely from T1. In resident-write mode the seed also installed
        // the atom in T0, where a point read would otherwise keep serving it
        // and the repair precondition would be false.
        self.db.db_ops().resident().purge_atom(&uuid);
        // Pair the seed flush: overlayfs / a flushed LastStore otherwise
        // keeps serving the pre-delete body and this assert fails.
        self.db.flush().await.expect("flush dangling-tip deletes");

        assert_eq!(
            self.point_read_payload(owner, stamp).await,
            None,
            "the row still reads back after its body was deleted — the fixture \
             did not actually create a dangling tip"
        );
        uuid
    }
}
