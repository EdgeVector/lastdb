//! Exact forward/reverse inspection for the approved one-time age repair case.

use super::*;
use crate::schema::types::KeyValue;

impl SchemaStore {
    /// Check both age directions and the obsolete arrival-time forward rows.
    pub async fn ci_check_ages(
        &self,
        name: &str,
        clocks: &[(KeyValue, u64)],
        repaired: bool,
    ) -> Result<(), SchemaError> {
        let anchor = self.retention_anchor(name).await?;
        let latest: Vec<_> = clocks
            .iter()
            .map(|(key, _)| retention_age_latest_key(&anchor, key))
            .collect();
        let good: Vec<_> = clocks
            .iter()
            .map(|(key, age)| retention_age_key(&anchor, *age, key))
            .collect();
        let bad: Vec<_> = clocks
            .iter()
            .map(|(key, age)| retention_age_key(&anchor, age + 100_000, key))
            .collect();
        let rows = self
            .schema_states_store
            .get_items::<SchemaRetentionAgeEntry>(&[latest, good, bad].concat())
            .await?;
        let count = clocks.len();
        for (i, (key, age)) in clocks.iter().enumerate() {
            let expected = SchemaRetentionAgeEntry {
                key: key.clone(),
                written_at: *age,
            };
            let wrong = SchemaRetentionAgeEntry {
                key: key.clone(),
                written_at: age + 100_000,
            };
            let valid = if repaired {
                rows[i].as_ref() == Some(&expected)
                    && rows[count + i].as_ref() == Some(&expected)
                    && rows[2 * count + i].is_none()
            } else {
                rows[i].as_ref() == Some(&wrong)
                    && rows[count + i].is_none()
                    && rows[2 * count + i].as_ref() == Some(&wrong)
            };
            if !valid {
                return Err(SchemaError::InvalidData(
                    "CI age forward/reverse mismatch".into(),
                ));
            }
        }
        Ok(())
    }
}
