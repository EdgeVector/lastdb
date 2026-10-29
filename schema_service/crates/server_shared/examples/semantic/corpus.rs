use schema_service_core::types::CanonicalField;
use schema_types::FieldValueType;

#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Expected {
    Accepted,
    ReuseRequired,
    DefinitionConflict,
    InvalidMeaning,
    GenericContainer,
}

#[derive(Clone, Copy)]
pub enum Kind {
    String,
    Integer,
    Number,
}

#[derive(Clone, Copy)]
pub struct Field {
    pub name: &'static str,
    pub description: &'static str,
    pub kind: Kind,
}

impl Field {
    pub fn definition(self) -> CanonicalField {
        CanonicalField {
            description: self.description.into(),
            field_type: match self.kind {
                Kind::String => FieldValueType::String,
                Kind::Integer => FieldValueType::Integer,
                Kind::Number => FieldValueType::Number,
            },
            version: 1,
            name_policy_version: 1,
            classification: None,
            interest_category: None,
        }
    }
}

pub struct Case {
    pub id: &'static str,
    pub category: &'static str,
    pub prior: Option<Field>,
    pub proposed: Field,
    pub expected: Expected,
}

const PARCEL: Field = Field {
    name: "parcel_delivery_status",
    description: "Parcel delivery status reported by the carrier",
    kind: Kind::String,
};

fn pair(
    id: &'static str,
    category: &'static str,
    prior: Field,
    proposed: Field,
    expected: Expected,
) -> Case {
    Case {
        id,
        category,
        prior: Some(prior),
        proposed,
        expected,
    }
}

fn field(name: &'static str, description: &'static str, kind: Kind) -> Field {
    Field {
        name,
        description,
        kind,
    }
}

/// This curated corpus expresses meaning, not a tuned cosine threshold.
/// New cases must state the expected admission outcome before a model run.
pub fn cases() -> Vec<Case> {
    let mut cases = conflicts();
    cases.extend(equivalents());
    cases.extend(distinct_units());
    cases.extend(distinct_roles());
    cases.extend(standalone());
    cases
}

fn conflicts() -> Vec<Case> {
    vec![
        pair(
            "identical_definition",
            "exact",
            PARCEL,
            PARCEL,
            Expected::Accepted,
        ),
        pair(
            "same_name_other_meaning",
            "conflict",
            PARCEL,
            field(
                PARCEL.name,
                "Parcel delivery status of customs clearance",
                Kind::String,
            ),
            Expected::DefinitionConflict,
        ),
        pair(
            "same_name_other_type",
            "conflict",
            PARCEL,
            field(PARCEL.name, PARCEL.description, Kind::Integer),
            Expected::DefinitionConflict,
        ),
    ]
}

fn equivalents() -> Vec<Case> {
    vec![
        pair(
            "normalized_duplicate",
            "equivalent",
            PARCEL,
            field(
                "parcel_delivery_state",
                "PARCEL delivery status reported by the carrier!",
                Kind::String,
            ),
            Expected::ReuseRequired,
        ),
        pair(
            "article_paraphrase",
            "equivalent",
            PARCEL,
            field(
                "parcel_delivery_state",
                "The parcel delivery status reported by the carrier",
                Kind::String,
            ),
            Expected::ReuseRequired,
        ),
        pair(
            "telephone_paraphrase",
            "equivalent",
            field(
                "person_phone_number",
                "Phone number assigned to a person for voice calls",
                Kind::String,
            ),
            field(
                "person_telephone_number",
                "Telephone number assigned to a person for voice calls",
                Kind::String,
            ),
            Expected::ReuseRequired,
        ),
        pair(
            "duration_paraphrase",
            "equivalent",
            field(
                "request_elapsed_seconds",
                "Elapsed request duration measured in seconds",
                Kind::Number,
            ),
            field(
                "request_duration_seconds",
                "Request elapsed duration measured in seconds",
                Kind::Number,
            ),
            Expected::ReuseRequired,
        ),
    ]
}

fn distinct_units() -> Vec<Case> {
    vec![
        pair(
            "bytes_and_bits",
            "distinct",
            field(
                "file_size_bytes",
                "File size measured in bytes",
                Kind::Integer,
            ),
            field(
                "file_size_bits",
                "File size measured in bits",
                Kind::Integer,
            ),
            Expected::Accepted,
        ),
        pair(
            "seconds_and_milliseconds",
            "distinct",
            field(
                "request_duration_seconds",
                "Request duration measured in seconds",
                Kind::Number,
            ),
            field(
                "request_duration_milliseconds",
                "Request duration measured in milliseconds",
                Kind::Number,
            ),
            Expected::Accepted,
        ),
        pair(
            "kilograms_and_pounds",
            "distinct",
            field(
                "parcel_weight_kilograms",
                "Parcel weight measured in kilograms",
                Kind::Number,
            ),
            field(
                "parcel_weight_pounds",
                "Parcel weight measured in pounds",
                Kind::Number,
            ),
            Expected::Accepted,
        ),
    ]
}

fn distinct_roles() -> Vec<Case> {
    vec![
        pair(
            "source_and_target_roles",
            "distinct",
            field(
                "parcel_source_identifier",
                "Parcel source identifier assigned by the carrier",
                Kind::String,
            ),
            field(
                "parcel_target_identifier",
                "Parcel target identifier assigned by the carrier",
                Kind::String,
            ),
            Expected::Accepted,
        ),
        pair(
            "delivery_and_customs_meanings",
            "distinct",
            PARCEL,
            field(
                "parcel_customs_status",
                "Parcel customs inspection decision reported by the border authority",
                Kind::String,
            ),
            Expected::Accepted,
        ),
    ]
}

fn standalone() -> Vec<Case> {
    vec![
        Case {
            id: "generic_message_container",
            category: "generic",
            prior: None,
            proposed: field(
                "message_text_content",
                "the textual body of a message",
                Kind::String,
            ),
            expected: Expected::GenericContainer,
        },
        Case {
            id: "generic_container_paraphrase",
            category: "generic",
            prior: None,
            proposed: field(
                "message_body_content",
                "the main text body of a message",
                Kind::String,
            ),
            expected: Expected::GenericContainer,
        },
        Case {
            id: "name_description_disagreement",
            category: "meaning",
            prior: None,
            proposed: field(
                "parcel_tracking_number",
                "The time a telescope exposure starts",
                Kind::String,
            ),
            expected: Expected::InvalidMeaning,
        },
    ]
}
