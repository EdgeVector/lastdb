use super::*;

/// One exact root path, stored outside its target object row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttributionPath {
    pub version: u8,
    pub epoch_id: String,
    pub object_kind: AttributionObjectKind,
    pub object_id: String,
    pub root_kind: AttributionRootKind,
    pub root_id: String,
    pub edge_path_digest: String,
}

impl AttributionPath {
    #[must_use]
    pub fn new(
        epoch_id: impl Into<String>,
        object_kind: AttributionObjectKind,
        object_id: impl Into<String>,
        root_kind: AttributionRootKind,
        root_id: impl Into<String>,
        edge_path_digest: impl Into<String>,
    ) -> Self {
        Self {
            version: ATTRIBUTION_VERSION,
            epoch_id: epoch_id.into(),
            object_kind,
            object_id: object_id.into(),
            root_kind,
            root_id: root_id.into(),
            edge_path_digest: edge_path_digest.into(),
        }
    }

    pub fn validate(&self) -> Result<(), SchemaError> {
        if self.version != ATTRIBUTION_VERSION {
            return Err(SchemaError::InvalidData(format!(
                "unsupported attribution path version {}",
                self.version
            )));
        }
        if self.epoch_id.trim().is_empty()
            || self.object_id.trim().is_empty()
            || self.root_id.trim().is_empty()
            || self.edge_path_digest.trim().is_empty()
        {
            return Err(SchemaError::InvalidData(
                "attribution path requires epoch, object, root, and digest".to_string(),
            ));
        }
        Ok(())
    }

    fn storage_key(&self) -> String {
        format!(
            "{ATTRIBUTION_PATH_PREFIX}{}\0{}\0{}\0{}\0{}",
            hash(ATTRIBUTION_EPOCH_DOMAIN, &self.epoch_id),
            self.object_kind.key(),
            hash(ATTRIBUTION_OBJECT_DOMAIN, &self.object_id),
            self.root_kind.key(),
            hash(ATTRIBUTION_ROOT_DOMAIN, &self.root_id),
        )
    }
}

impl AttributionLedger {
    pub(crate) async fn new(store: Arc<dyn KvStore>) -> Result<Self, StorageError> {
        let store = Arc::new(TypedKvStore::new(store));
        let event_tip = store
            .get_item::<u64>(ATTRIBUTION_EVENT_TIP_KEY)
            .await?
            .unwrap_or(0);
        Ok(Self {
            store,
            event_tip: Arc::new(Mutex::new(event_tip)),
        })
    }

    pub(crate) async fn flush(&self) -> Result<(), SchemaError> {
        self.store.inner().flush().await.map_err(Into::into)
    }

    /// Persist one class row. The caller writes root paths first, then this row.
    /// A crash can therefore leave extra path evidence but never classify an
    /// object as attributed without its paths.
    pub async fn put_attribution_record(
        &self,
        record: &AttributionRecord,
    ) -> Result<(), SchemaError> {
        record.validate()?;
        self.store
            .put_item(&record.storage_key(), record)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("put attribution record: {error}")))
    }

    /// Persist one exact root path before its object's classification row.
    pub async fn put_attribution_path(&self, path: &AttributionPath) -> Result<(), SchemaError> {
        path.validate()?;
        self.store
            .put_item(&path.storage_key(), path)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("put attribution path: {error}")))
    }

    /// Count every durable classification row by class, plus the total path
    /// row count, across every epoch this ledger holds.
    ///
    /// This is the read surface behind `GET /api/db/inventory`'s attribution
    /// summary: an owner diagnostic over the ledger's own (bounded) namespace,
    /// not a walk of product data. It reports whatever the classification
    /// producers (the schema/system/retention root walk, and the inline
    /// per-write path) have already written; it does not run them itself.
    pub async fn summarize(&self) -> Result<AttributionSummary, SchemaError> {
        let records = self
            .store
            .scan_items_with_prefix::<AttributionRecord>(ATTRIBUTION_RECORD_PREFIX)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("scan attribution records: {error}"))
            })?;
        let mut objects = AttributionClassCounts::default();
        for (_, record) in records {
            match record.classification {
                AttributionClass::SchemaAttributed => objects.schema_attributed += 1,
                AttributionClass::RetentionAttributed => objects.retention_attributed += 1,
                AttributionClass::SystemAttributed => objects.system_attributed += 1,
                AttributionClass::DerivedAttributed => objects.derived_attributed += 1,
                AttributionClass::UnattributedResidue => objects.unattributed_residue += 1,
                AttributionClass::Unknown => objects.unknown += 1,
            }
        }
        let path_rows = self
            .store
            .scan_items_with_prefix::<AttributionPath>(ATTRIBUTION_PATH_PREFIX)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("scan attribution paths: {error}")))?
            .len() as u64;
        Ok(AttributionSummary { objects, path_rows })
    }
}
