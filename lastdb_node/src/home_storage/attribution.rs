use super::*;

/// One schema binding used to build inclusive app reachability.
///
/// This is an in-memory reconcile input. The persisted report stores only the
/// resulting app rows and equations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppLogicalBinding {
    pub schema_binding: String,
    /// `None` is a system schema, not an unknown app.
    pub app_id: Option<String>,
    pub molecules: Vec<AppLogicalMolecule>,
}

/// One logical molecule counter referenced by a schema binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppLogicalMolecule {
    pub molecule_id: String,
    /// Missing means the write-path counter cannot prove this unit yet.
    pub logical_bytes: Option<u64>,
}

/// One app's inclusive logical reachability row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeStorageAppAttributionRow {
    pub app_id: String,
    /// Logical units reached by this app and no other app.
    pub exclusive_logical_bytes: u64,
    /// Logical units reached by this app and at least one other app.
    pub shared_logical_bytes: u64,
    /// `exclusive_logical_bytes + shared_logical_bytes`.
    pub inclusive_logical_bytes: u64,
    /// The part of this row that overlaps another app. This equals the row's
    /// `shared_logical_bytes`; the report-level overlap removes the first
    /// unique copy only once.
    pub overlap_logical_bytes: u64,
    /// Inclusive divided by exclusive, in basis points. `None` means the app
    /// reaches shared data but has no exclusive denominator.
    pub amplification_basis_points: Option<u64>,
    pub molecule_count: u64,
    pub shared_molecule_count: u64,
    pub schema_bindings: Vec<String>,
}

/// Inclusive app ledger persisted beside the unique physical home ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeStorageInclusiveAppAttribution {
    pub metric: String,
    pub measured_at: DateTime<Utc>,
    pub complete: bool,
    /// Every valid logical molecule reached by at least one app, once.
    pub unique_logical_bytes: u64,
    /// Valid logical molecules reached only by system schemas.
    pub system_unique_logical_bytes: u64,
    /// `unique_logical_bytes + system_unique_logical_bytes`.
    pub total_unique_logical_bytes: u64,
    /// Sum of all app rows. Shared molecules repeat once per reaching app.
    pub inclusive_app_logical_bytes: u64,
    /// `inclusive_app_logical_bytes - unique_logical_bytes`.
    pub overlap_logical_bytes: u64,
    /// Inclusive app bytes divided by unique app bytes, in basis points.
    pub amplification_basis_points: Option<u64>,
    pub pending_protein_folds: u64,
    pub apps: Vec<HomeStorageAppAttributionRow>,
    #[serde(default)]
    pub unresolved_scopes: Vec<String>,
}

impl Default for HomeStorageInclusiveAppAttribution {
    fn default() -> Self {
        Self {
            metric: HOME_STORAGE_APP_ATTRIBUTION_METRIC.to_string(),
            measured_at: Utc::now(),
            complete: false,
            unique_logical_bytes: 0,
            system_unique_logical_bytes: 0,
            total_unique_logical_bytes: 0,
            inclusive_app_logical_bytes: 0,
            overlap_logical_bytes: 0,
            amplification_basis_points: None,
            pending_protein_folds: 0,
            apps: Vec::new(),
            unresolved_scopes: vec!["attribution_not_measured".to_string()],
        }
    }
}

#[derive(Default)]
pub(super) struct LogicalUnit {
    pub(super) logical_bytes: Option<u64>,
    pub(super) apps: BTreeSet<String>,
    pub(super) system_reachable: bool,
}

#[derive(Default)]
pub(super) struct AppLogicalBucket {
    pub(super) exclusive_logical_bytes: u64,
    pub(super) shared_logical_bytes: u64,
    pub(super) molecule_count: u64,
    pub(super) shared_molecule_count: u64,
    pub(super) schema_bindings: BTreeSet<String>,
}

/// Build the inclusive logical ledger without reading any atom or tip plane.
/// A molecule contributes once to the unique ledger and once to every app
/// that reaches it through a declared schema binding.
#[must_use]
pub fn build_inclusive_app_attribution(
    bindings: &[AppLogicalBinding],
    counters_complete: bool,
    pending_protein_folds: u64,
    mut unresolved_scopes: Vec<String>,
    measured_at: DateTime<Utc>,
) -> HomeStorageInclusiveAppAttribution {
    let supplied_scopes = std::mem::take(&mut unresolved_scopes);
    for scope in supplied_scopes {
        push_scope(&mut unresolved_scopes, &scope);
    }
    let mut app_buckets: BTreeMap<String, AppLogicalBucket> = BTreeMap::new();

    if !counters_complete {
        push_scope(&mut unresolved_scopes, "molecule_counters_incomplete");
    }
    if pending_protein_folds > 0 {
        push_scope(&mut unresolved_scopes, "pending_protein_folds");
    }

    let units = collect_logical_units(bindings, &mut app_buckets, &mut unresolved_scopes);
    let (total_unique_logical_bytes, unique_logical_bytes, system_unique_logical_bytes) =
        assign_units_to_apps(&units, &mut app_buckets);
    let apps = attribution_rows(app_buckets);
    let inclusive_app_logical_bytes = apps.iter().fold(0_u64, |sum, row| {
        sum.saturating_add(row.inclusive_logical_bytes)
    });
    let overlap_logical_bytes = inclusive_app_logical_bytes.saturating_sub(unique_logical_bytes);

    HomeStorageInclusiveAppAttribution {
        metric: HOME_STORAGE_APP_ATTRIBUTION_METRIC.to_string(),
        measured_at,
        complete: unresolved_scopes.is_empty(),
        unique_logical_bytes,
        system_unique_logical_bytes,
        total_unique_logical_bytes,
        inclusive_app_logical_bytes,
        overlap_logical_bytes,
        amplification_basis_points: ratio_basis_points(
            inclusive_app_logical_bytes,
            unique_logical_bytes,
        ),
        pending_protein_folds,
        apps,
        unresolved_scopes,
    }
    .validated_for_read()
}

fn collect_logical_units(
    bindings: &[AppLogicalBinding],
    app_buckets: &mut BTreeMap<String, AppLogicalBucket>,
    unresolved_scopes: &mut Vec<String>,
) -> BTreeMap<String, LogicalUnit> {
    let mut units: BTreeMap<String, LogicalUnit> = BTreeMap::new();
    for binding in bindings {
        let app_id = binding
            .app_id
            .as_deref()
            .map(str::trim)
            .filter(|app| !app.is_empty());
        if let Some(app_id) = app_id {
            app_buckets
                .entry(app_id.to_string())
                .or_default()
                .schema_bindings
                .insert(binding.schema_binding.clone());
        }

        let mut seen = BTreeSet::new();
        for molecule in &binding.molecules {
            if !seen.insert(molecule.molecule_id.as_str()) {
                continue;
            }
            let Some(logical_bytes) = molecule.logical_bytes else {
                push_scope(
                    unresolved_scopes,
                    &format!("missing_molecule_counter:{}", molecule.molecule_id),
                );
                continue;
            };
            let unit = units.entry(molecule.molecule_id.clone()).or_default();
            match unit.logical_bytes {
                Some(existing) if existing != logical_bytes => {
                    push_scope(
                        unresolved_scopes,
                        &format!("molecule_counter_conflict:{}", molecule.molecule_id),
                    );
                    unit.logical_bytes = Some(existing.max(logical_bytes));
                }
                None => unit.logical_bytes = Some(logical_bytes),
                Some(_) => {}
            }
            if let Some(app_id) = app_id {
                unit.apps.insert(app_id.to_string());
            } else {
                unit.system_reachable = true;
            }
        }
    }
    units
}

/// Returns `(total_unique, app_unique, system_unique)` logical bytes.
fn assign_units_to_apps(
    units: &BTreeMap<String, LogicalUnit>,
    app_buckets: &mut BTreeMap<String, AppLogicalBucket>,
) -> (u64, u64, u64) {
    let mut total_unique_logical_bytes = 0_u64;
    let mut unique_logical_bytes = 0_u64;
    let mut system_unique_logical_bytes = 0_u64;
    for unit in units.values() {
        let logical_bytes = unit.logical_bytes.unwrap_or(0);
        total_unique_logical_bytes = total_unique_logical_bytes.saturating_add(logical_bytes);
        if unit.apps.is_empty() {
            if unit.system_reachable {
                system_unique_logical_bytes =
                    system_unique_logical_bytes.saturating_add(logical_bytes);
            }
            continue;
        }
        unique_logical_bytes = unique_logical_bytes.saturating_add(logical_bytes);
        let shared = unit.apps.len() > 1;
        for app_id in &unit.apps {
            let bucket = app_buckets.entry(app_id.clone()).or_default();
            bucket.molecule_count = bucket.molecule_count.saturating_add(1);
            if shared {
                bucket.shared_logical_bytes =
                    bucket.shared_logical_bytes.saturating_add(logical_bytes);
                bucket.shared_molecule_count = bucket.shared_molecule_count.saturating_add(1);
            } else {
                bucket.exclusive_logical_bytes =
                    bucket.exclusive_logical_bytes.saturating_add(logical_bytes);
            }
        }
    }
    (
        total_unique_logical_bytes,
        unique_logical_bytes,
        system_unique_logical_bytes,
    )
}

fn attribution_rows(
    app_buckets: BTreeMap<String, AppLogicalBucket>,
) -> Vec<HomeStorageAppAttributionRow> {
    let mut apps: Vec<HomeStorageAppAttributionRow> = app_buckets
        .into_iter()
        .map(|(app_id, bucket)| {
            let inclusive_logical_bytes = bucket
                .exclusive_logical_bytes
                .saturating_add(bucket.shared_logical_bytes);
            HomeStorageAppAttributionRow {
                app_id,
                exclusive_logical_bytes: bucket.exclusive_logical_bytes,
                shared_logical_bytes: bucket.shared_logical_bytes,
                inclusive_logical_bytes,
                overlap_logical_bytes: bucket.shared_logical_bytes,
                amplification_basis_points: ratio_basis_points(
                    inclusive_logical_bytes,
                    bucket.exclusive_logical_bytes,
                ),
                molecule_count: bucket.molecule_count,
                shared_molecule_count: bucket.shared_molecule_count,
                schema_bindings: bucket.schema_bindings.into_iter().collect(),
            }
        })
        .collect();
    apps.sort_by(|left, right| {
        right
            .inclusive_logical_bytes
            .cmp(&left.inclusive_logical_bytes)
            .then_with(|| left.app_id.cmp(&right.app_id))
    });
    apps
}

pub(super) fn ratio_basis_points(numerator: u64, denominator: u64) -> Option<u64> {
    if denominator == 0 {
        return None;
    }
    Some(
        numerator
            .saturating_mul(ONE_X_BASIS_POINTS)
            .checked_div(denominator)
            .unwrap_or(u64::MAX),
    )
}

impl HomeStorageInclusiveAppAttribution {
    #[must_use]
    pub fn validated_for_read(mut self) -> Self {
        self.metric = HOME_STORAGE_APP_ATTRIBUTION_METRIC.to_string();
        let persisted_scopes = std::mem::take(&mut self.unresolved_scopes);
        for scope in persisted_scopes {
            push_scope(&mut self.unresolved_scopes, &scope);
        }
        let mut app_ids = BTreeSet::new();
        let mut inclusive = 0_u64;
        for row in &mut self.apps {
            if !app_ids.insert(row.app_id.clone()) {
                push_scope(&mut self.unresolved_scopes, "duplicate_app_id");
            }
            let expected_inclusive = row
                .exclusive_logical_bytes
                .saturating_add(row.shared_logical_bytes);
            if row.inclusive_logical_bytes != expected_inclusive
                || row.overlap_logical_bytes != row.shared_logical_bytes
            {
                push_scope(&mut self.unresolved_scopes, "app_row_arithmetic_mismatch");
            }
            row.inclusive_logical_bytes = expected_inclusive;
            row.overlap_logical_bytes = row.shared_logical_bytes;
            row.amplification_basis_points =
                ratio_basis_points(row.inclusive_logical_bytes, row.exclusive_logical_bytes);
            row.schema_bindings.sort_unstable();
            row.schema_bindings.dedup();
            inclusive = inclusive.saturating_add(row.inclusive_logical_bytes);
        }
        let expected_total_unique = self
            .unique_logical_bytes
            .saturating_add(self.system_unique_logical_bytes);
        let expected_overlap = inclusive.saturating_sub(self.unique_logical_bytes);
        if self.total_unique_logical_bytes != expected_total_unique
            || self.inclusive_app_logical_bytes != inclusive
            || self.overlap_logical_bytes != expected_overlap
        {
            push_scope(&mut self.unresolved_scopes, "attribution_total_mismatch");
        }
        self.total_unique_logical_bytes = expected_total_unique;
        self.inclusive_app_logical_bytes = inclusive;
        self.overlap_logical_bytes = expected_overlap;
        self.amplification_basis_points = ratio_basis_points(inclusive, self.unique_logical_bytes);
        self.apps.sort_by(|left, right| {
            right
                .inclusive_logical_bytes
                .cmp(&left.inclusive_logical_bytes)
                .then_with(|| left.app_id.cmp(&right.app_id))
        });
        self.unresolved_scopes.sort_unstable();
        self.unresolved_scopes.dedup();
        self.complete &= self.unresolved_scopes.is_empty();
        self
    }
}
