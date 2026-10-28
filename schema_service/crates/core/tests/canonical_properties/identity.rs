use std::collections::{BTreeMap, BTreeSet};

use schema_types::{
    compute_declared_field_identity, compute_field_hash, compute_identity_hash_parts,
    DeclarativeSchemaType, FieldValueType, IdentityRecompute, KeyConfig, Schema,
    IDENTITY_HASH_ALGO_VERSION,
};

fn schema(fields: &[&str]) -> Schema {
    Schema::new(
        "ParcelMeasurements".into(),
        DeclarativeSchemaType::Single,
        None,
        Some(fields.iter().map(|field| (*field).into()).collect()),
        None,
        None,
    )
}

fn field_types() -> Vec<FieldValueType> {
    vec![
        FieldValueType::String,
        FieldValueType::Integer,
        FieldValueType::Float,
        FieldValueType::Number,
        FieldValueType::Boolean,
        FieldValueType::Date,
        FieldValueType::DateTime,
        FieldValueType::Array(Box::new(FieldValueType::String)),
        FieldValueType::Map(Box::new(FieldValueType::String)),
        FieldValueType::OneOf(vec![FieldValueType::String, FieldValueType::Null]),
    ]
}

#[test]
fn canonical_identity_separates_generated_definition_components() {
    let mut identities = BTreeSet::new();
    for subject in 0..16 {
        let name = format!("parcel_{subject}_measurement");
        for description in ["Mass in kilograms", "Mass in pounds", "Volume in litres"] {
            for field_type in field_types() {
                for version in 1..=3 {
                    let identity = compute_field_hash(&name, description, &field_type, version);
                    assert!(
                        identities.insert(identity),
                        "distinct definitions share an identity"
                    );
                }
            }
        }
    }
    assert_eq!(identities.len(), 16 * 3 * field_types().len() * 3);
}

#[test]
fn canonical_identity_trims_edges_but_preserves_case_and_internal_text() {
    for field_type in field_types() {
        let base = compute_field_hash("parcel_mass", "Parcel mass in kilograms", &field_type, 1);
        let trimmed = compute_field_hash(
            " \tparcel_mass\n",
            "\nParcel mass in kilograms\t ",
            &field_type,
            1,
        );
        assert_eq!(base, trimmed);
        for (name, description) in [
            ("Parcel_mass", "Parcel mass in kilograms"),
            ("parcel_mass", "parcel mass in kilograms"),
            ("parcel_mass", "Parcel  mass in kilograms"),
        ] {
            assert_ne!(base, compute_field_hash(name, description, &field_type, 1));
        }
    }
}

#[test]
fn compound_type_identity_is_stable_across_object_insertion_order() {
    let mut first = BTreeMap::new();
    first.insert("parcel_mass".into(), FieldValueType::Number);
    first.insert("parcel_unit".into(), FieldValueType::String);
    let mut second = BTreeMap::new();
    second.insert("parcel_unit".into(), FieldValueType::String);
    second.insert("parcel_mass".into(), FieldValueType::Number);
    let identity = |fields| {
        compute_field_hash(
            "parcel_measurement",
            "Parcel measurement",
            &FieldValueType::Object(fields),
            1,
        )
    };
    assert_eq!(identity(first), identity(second));
}

#[test]
fn declared_identity_requires_the_same_handle_name_type_and_version() {
    let mut identities = BTreeSet::new();
    for declaration in 0..16 {
        let declaration_id = format!("fd_property_{declaration}");
        for name in ["parcel_mass", "parcel_weight"] {
            for field_type in field_types() {
                for version in 1..=3 {
                    let identity = compute_declared_field_identity(
                        &declaration_id,
                        name,
                        &field_type,
                        version,
                    );
                    assert!(identities.insert(identity));
                }
            }
        }
    }
    assert_eq!(identities.len(), 16 * 2 * field_types().len() * 3);
}

#[test]
fn declaration_and_canonical_identities_use_separate_domains() {
    for field_type in field_types() {
        let declared = compute_declared_field_identity("fd_parcel", "parcel_mass", &field_type, 1);
        let trimmed =
            compute_declared_field_identity(" fd_parcel\n", "\tparcel_mass ", &field_type, 1);
        let canonical = compute_field_hash("parcel_mass", "fd_parcel", &field_type, 1);
        assert_eq!(declared, trimmed);
        assert_ne!(declared, canonical);
    }
}

#[test]
fn declaration_prose_and_permission_changes_preserve_coherence_identity() {
    use schema_service_core::{DeclareFieldInput, DeclareFieldRequest, DeclaredFieldRegistry};

    let mut registry = DeclaredFieldRegistry::new();
    let mut request = DeclareFieldRequest {
        owner_app_id: "parcel_app".into(),
        handle: "parcel/measurement".into(),
        fields: vec![DeclareFieldInput {
            name: "parcel_mass".into(),
            field_type: FieldValueType::Number,
            description: "Parcel mass in kilograms".into(),
            version: 1,
        }],
        readers: Vec::new(),
    };
    let first = registry.declare(&request, || "fd_parcel".into()).unwrap();
    request.fields[0].description = "Corrected parcel mass description".into();
    request.readers = vec!["shipment_app".into()];
    let revised = registry
        .declare(&request, || {
            panic!("an existing handle must not mint a new id")
        })
        .unwrap();
    assert_eq!(first.declaration_id, revised.declaration_id);
    assert_eq!(first.fields[0].identity, revised.fields[0].identity);
    assert_ne!(first.fields[0].description, revised.fields[0].description);
    assert!(revised.permits("shipment_app"));
    assert!(!first.permits("shipment_app"));
}

#[test]
fn schema_identity_ignores_field_order_and_duplicate_occurrences() {
    for length in 1..=8 {
        let names: Vec<String> = (0..length)
            .map(|index| format!("parcel_property_{index}"))
            .collect();
        let expected = compute_identity_hash_parts(
            Some("parcel_app"),
            None,
            "ParcelMeasurements",
            names.iter().map(String::as_str),
            None,
        );
        for rotation in 0..length {
            let mut permuted = names.clone();
            permuted.rotate_left(rotation);
            permuted.reverse();
            permuted.extend(names.clone());
            assert_eq!(
                expected,
                compute_identity_hash_parts(
                    Some("parcel_app"),
                    None,
                    "ParcelMeasurements",
                    permuted.iter().map(String::as_str),
                    None,
                )
            );
        }
    }
}

#[test]
fn schema_identity_separates_app_readable_name_and_lookup_key() {
    let mut identities = BTreeSet::new();
    for owner in [None, Some("parcel_app"), Some("shipment_app")] {
        for name in ["ParcelMeasurements", "ShipmentMeasurements"] {
            for key in [
                None,
                Some(KeyConfig::new(Some("parcel_id".into()), None)),
                Some(KeyConfig::new(Some("shipment_id".into()), None)),
                Some(KeyConfig::new(
                    Some("parcel_id".into()),
                    Some("parcel_mass".into()),
                )),
            ] {
                let identity = compute_identity_hash_parts(
                    owner,
                    Some(name),
                    "ignored_fallback",
                    ["parcel_id", "parcel_mass", "shipment_id"],
                    key.as_ref(),
                );
                assert!(identities.insert(identity));
            }
        }
    }
    assert_eq!(identities.len(), 3 * 2 * 4);
}

#[test]
fn empty_owner_description_and_key_preserve_legacy_schema_identity() {
    let fields = ["parcel_id", "parcel_mass"];
    let base = compute_identity_hash_parts(None, None, "ParcelMeasurements", fields, None);
    let empty = KeyConfig::new(Some(" \t".into()), Some("\n".into()));
    assert_eq!(
        base,
        compute_identity_hash_parts(
            Some(""),
            Some(""),
            "ParcelMeasurements",
            fields,
            Some(&empty)
        )
    );
    let plain_key = KeyConfig::new(Some("parcel_id".into()), None);
    let padded_key = KeyConfig::new(Some(" parcel_id\n".into()), Some(" ".into()));
    assert_eq!(
        compute_identity_hash_parts(None, None, "ParcelMeasurements", fields, Some(&plain_key)),
        compute_identity_hash_parts(None, None, "ParcelMeasurements", fields, Some(&padded_key))
    );
}

#[test]
fn schema_catalog_stamps_do_not_reidentify_an_existing_shape() {
    let mut base = schema(&["parcel_mass"]);
    base.compute_identity_hash();
    let mut changed = base.clone();
    changed
        .field_types
        .insert("parcel_mass".into(), FieldValueType::Number);
    changed.field_versions.insert("parcel_mass".into(), 3);
    changed
        .field_descriptions
        .insert("parcel_mass".into(), "Parcel mass in kilograms".into());
    changed
        .field_declarations
        .insert("parcel_mass".into(), "fd_parcel".into());
    changed
        .field_hashes
        .insert("parcel_mass".into(), "updated_catalog_stamp".into());
    changed.compute_identity_hash();
    assert_eq!(base.identity_hash, changed.identity_hash);
    assert_ne!(base.field_hashes, changed.field_hashes);
}

#[test]
fn a_newer_identity_algorithm_survives_an_older_binary() {
    for future_version in (IDENTITY_HASH_ALGO_VERSION + 1)..=(IDENTITY_HASH_ALGO_VERSION + 16) {
        let mut definition = schema(&["parcel_mass"]);
        definition.identity_hash = Some(format!("future_identity_{future_version}"));
        definition.identity_hash_algo_version = Some(future_version);
        let previous_hash = definition.identity_hash.clone();
        assert_eq!(
            definition.recompute_identity_hash_unless_newer(),
            IdentityRecompute::RefusedDowngrade {
                stored: future_version,
                binary: IDENTITY_HASH_ALGO_VERSION
            }
        );
        assert_eq!(definition.identity_hash, previous_hash);
        assert_eq!(definition.identity_hash_algo_version, Some(future_version));
    }
}

#[test]
fn distinct_wire_field_sets_require_distinct_schema_identities() {
    // Refuse malformed raw names before identity-based lookup. Distinct
    // accepted field sets must retain distinct identities without a format
    // migration for existing names.
    for malformed in [
        "parcel_mass,parcel_unit",
        "parcel_mass:parcel_unit",
        "",
        " parcel_mass",
        "parcel_mass\n",
        "parcel\0mass",
        "parcel\u{0085}mass",
        "parcel\u{2003}mass",
    ] {
        let definition = schema(&[malformed]);
        assert!(
            definition.validate_definition().is_err(),
            "admitted {malformed:?}"
        );
        let wire = serde_json::to_value(&definition).unwrap();
        assert!(serde_json::from_value::<Schema>(wire).is_err());
    }
    let mut combined = schema(&["parcel_mass_parcel_unit"]);
    let mut separate = schema(&["parcel_mass", "parcel_unit"]);
    assert!(combined.validate_definition().is_ok());
    assert!(separate.validate_definition().is_ok());
    assert!(
        schema_service_core::field_name_policy::validate_new_field_name("parcel_mass,parcel_unit")
            .is_err()
    );
    combined.compute_identity_hash();
    separate.compute_identity_hash();
    assert_ne!(
        combined.identity_hash, separate.identity_hash,
        "two admitted wire field sets share a schema identity"
    );
    for valid in ["parcel_mass", "parcel.mass", "parcel-mass", "parcel_重量"] {
        assert!(schema(&[valid]).validate_definition().is_ok());
    }
}
