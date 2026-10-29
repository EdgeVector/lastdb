use super::*;

/// Immutable association for one enrolled source hash partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateGuard {
    pub version: u8,
    pub source_schema_name: String,
    pub source_partition_hash: String,
    pub target_schema_name: String,
    pub target_key: String,
    pub member_schema_name: String,
    pub member_partition_hash: String,
    pub metric_fingerprint: String,
    pub metric_fields: Vec<String>,
    /// Highest source mutation that declared this association.
    pub enrollment_winner: AggregateWinner,
    /// Exact source row that carried `enrollment_winner`.
    pub enrollment_source_key: String,
    /// Exact canonical member row that proves the enrollment commit landed.
    pub enrollment_member_key: String,
    /// False while the guard is a crash-safe pre-commit reservation.
    pub committed: bool,
    /// Deterministic tiebreak and corruption check for the association fields.
    pub association_fingerprint: String,
}

impl AggregateGuard {
    #[must_use]
    pub fn new(
        source_schema_name: String,
        source_key: &super::super::key_value::KeyValue,
        aggregate: &AggregateSet,
        enrollment_winner: AggregateWinner,
    ) -> Self {
        let mut guard = Self {
            version: AGGREGATE_GUARD_VERSION,
            source_schema_name,
            source_partition_hash: source_key
                .hash
                .clone()
                .expect("aggregate source key requires hash"),
            target_schema_name: aggregate.target_schema_name.clone(),
            target_key: aggregate.target_key_value.to_storage_key(),
            member_schema_name: aggregate.member_schema_name.clone(),
            member_partition_hash: aggregate
                .member_key_value
                .hash
                .clone()
                .expect("AggregateSet::validate requires member hash"),
            metric_fingerprint: metric_fingerprint(aggregate.contribution.keys()),
            metric_fields: aggregate.contribution.keys().cloned().collect(),
            enrollment_winner,
            enrollment_source_key: source_key.to_storage_key(),
            enrollment_member_key: aggregate.member_key_value.to_storage_key(),
            committed: false,
            association_fingerprint: String::new(),
        };
        guard.association_fingerprint = guard.compute_association_fingerprint();
        guard
    }

    #[must_use]
    fn compute_association_fingerprint(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"lastdb-aggregate-association-v1\0");
        for value in [
            self.source_schema_name.as_str(),
            self.source_partition_hash.as_str(),
            self.target_schema_name.as_str(),
            self.target_key.as_str(),
            self.member_schema_name.as_str(),
            self.member_partition_hash.as_str(),
            self.metric_fingerprint.as_str(),
        ] {
            hasher.update(value.as_bytes());
            hasher.update([0]);
        }
        format!("{:x}", hasher.finalize())
    }

    #[must_use]
    pub fn same_association(&self, other: &Self) -> bool {
        self.association_fingerprint == other.association_fingerprint
            && self.source_schema_name == other.source_schema_name
            && self.source_partition_hash == other.source_partition_hash
            && self.target_schema_name == other.target_schema_name
            && self.target_key == other.target_key
            && self.member_schema_name == other.member_schema_name
            && self.member_partition_hash == other.member_partition_hash
            && self.metric_fingerprint == other.metric_fingerprint
            && self.metric_fields == other.metric_fields
    }

    /// Deterministic association declaration order for replay conflicts.
    #[must_use]
    pub fn cmp_enrollment(&self, other: &Self) -> std::cmp::Ordering {
        self.enrollment_winner
            .cmp_source(&other.enrollment_winner)
            .then_with(|| {
                self.association_fingerprint
                    .cmp(&other.association_fingerprint)
            })
    }

    #[must_use]
    pub fn committed(mut self) -> Self {
        self.committed = true;
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version != AGGREGATE_GUARD_VERSION {
            return Err(format!(
                "unsupported aggregate guard version {}",
                self.version
            ));
        }
        if self.metric_fields.is_empty()
            || self.metric_fingerprint != metric_fingerprint(self.metric_fields.iter())
        {
            return Err("aggregate guard metric fingerprint does not match its fields".into());
        }
        if self.association_fingerprint != self.compute_association_fingerprint() {
            return Err("aggregate guard association fingerprint is invalid".into());
        }
        let source_key =
            super::super::key_value::KeyValue::from_storage_key(&self.enrollment_source_key)
                .map_err(|error| error.to_string())?;
        let member_key =
            super::super::key_value::KeyValue::from_storage_key(&self.enrollment_member_key)
                .map_err(|error| error.to_string())?;
        if source_key.hash.as_deref() != Some(self.source_partition_hash.as_str())
            || member_key.hash.as_deref() != Some(self.member_partition_hash.as_str())
            || member_key.range.is_none()
        {
            return Err("aggregate guard enrollment keys are invalid".into());
        }
        Ok(())
    }
}
