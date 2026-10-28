//! Direct field readers retain the molecule's type when no schema scope exists.

use super::*;

impl KeyComparisonContext {
    pub(crate) fn current_profile() -> Option<MoleculeRangeProfile> {
        COMPARISON.try_with(|active| active.profile).ok().flatten()
    }

    pub(crate) async fn run_for_molecule<T>(
        profile: Option<MoleculeRangeProfile>,
        future: impl Future<Output = Result<T, SchemaError>>,
    ) -> Result<T, SchemaError> {
        if let Ok(active) = COMPARISON.try_with(Arc::clone) {
            if active.profile != profile {
                return Err(SchemaError::InvalidData(
                    "the field range profile differs from the captured schema".into(),
                ));
            }
            let result = future.await?;
            active.require_valid_comparison()?;
            return Ok(result);
        }
        let Some(profile) = profile else {
            return future.await;
        };
        let range = RangeKeyComparison::typed(profile.comparison_type())
            .map_err(SchemaError::InvalidData)?;
        Arc::new(Self {
            identity: format!("field-comparison:{}", profile.format_id()),
            range,
            failure: OnceLock::new(),
            profile: Some(profile),
        })
        .run(future)
        .await
    }
}
