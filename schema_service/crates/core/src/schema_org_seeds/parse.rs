#[cfg(feature = "schema-org-seed-data")]
use super::*;

// ─────────────────────────────────────────────────────────────────────
//  JSON-LD parsing
//
//  Used by the two offline generators, not by the cold-start seed path.
// ─────────────────────────────────────────────────────────────────────

/// Parsed form of the Schema.org JSON-LD dump, indexed for lookup.
#[cfg(feature = "schema-org-seed-data")]
pub struct ParsedDump {
    pub classes: Vec<SchemaOrgClass>,
    pub properties: Vec<SchemaOrgProperty>,
    /// For each class label, the list of property labels (original
    /// camelCase, not snake_case) whose `domainIncludes` references the
    /// class.
    pub properties_by_domain: HashMap<String, Vec<String>>,
    /// Property lookup by snake_case name for description resolution
    /// during schema construction.
    pub property_by_snake_name: HashMap<String, SchemaOrgProperty>,
}

#[cfg(feature = "schema-org-seed-data")]
#[derive(Debug, Clone)]
pub struct SchemaOrgClass {
    pub label: String,
    pub comment: Option<String>,
}

#[cfg(feature = "schema-org-seed-data")]
#[derive(Debug, Clone)]
pub struct SchemaOrgProperty {
    pub label: String,
    pub comment: Option<String>,
    pub domain_includes: Vec<String>,
    pub range_includes: Vec<String>,
}

#[cfg(feature = "schema-org-seed-data")]
#[derive(Deserialize)]
struct RawDump {
    #[serde(rename = "@graph")]
    graph: Vec<RawNode>,
}

#[cfg(feature = "schema-org-seed-data")]
#[derive(Deserialize)]
struct RawNode {
    #[serde(rename = "@id")]
    id: Option<String>,
    #[serde(rename = "@type")]
    #[serde(default)]
    node_type: OneOrMany<String>,
    #[serde(rename = "rdfs:label")]
    label: Option<LabelField>,
    #[serde(rename = "rdfs:comment")]
    comment: Option<LabelField>,
    #[serde(rename = "schema:domainIncludes")]
    #[serde(default)]
    domain_includes: OneOrMany<RefNode>,
    #[serde(rename = "schema:rangeIncludes")]
    #[serde(default)]
    range_includes: OneOrMany<RefNode>,
}

/// Schema.org JSON-LD uses both single values and arrays for the same
/// fields depending on whether a property applies to one or many types.
/// Collapse both shapes to a Vec.
#[cfg(feature = "schema-org-seed-data")]
#[derive(Deserialize, Default)]
#[serde(untagged)]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
    #[default]
    None,
}

#[cfg(feature = "schema-org-seed-data")]
impl<T> OneOrMany<T> {
    fn into_vec(self) -> Vec<T> {
        match self {
            Self::One(v) => vec![v],
            Self::Many(v) => v,
            Self::None => Vec::new(),
        }
    }
}

#[cfg(feature = "schema-org-seed-data")]
#[derive(Deserialize)]
struct RefNode {
    #[serde(rename = "@id")]
    id: String,
}

/// `rdfs:label` is usually a bare string but occasionally an object with
/// `@value` / `@language`. Accept both.
#[cfg(feature = "schema-org-seed-data")]
#[derive(Deserialize)]
#[serde(untagged)]
enum LabelField {
    Plain(String),
    Tagged {
        #[serde(rename = "@value")]
        value: String,
    },
}

#[cfg(feature = "schema-org-seed-data")]
impl LabelField {
    fn into_string(self) -> String {
        match self {
            Self::Plain(s) => s,
            Self::Tagged { value } => value,
        }
    }
}

/// Parse the embedded Schema.org JSON-LD dump.
///
/// Public so tests can exercise the parse path independently of the seed
/// path.
#[cfg(feature = "schema-org-seed-data")]
pub fn parse_dump() -> FoldDbResult<ParsedDump> {
    let raw: RawDump = serde_json::from_str(SCHEMA_ORG_JSONLD).map_err(|e| {
        FoldDbError::Config(format!("Failed to parse embedded Schema.org JSON-LD: {e}"))
    })?;

    let mut classes: Vec<SchemaOrgClass> = Vec::new();
    let mut properties: Vec<SchemaOrgProperty> = Vec::new();

    for node in raw.graph {
        let types: Vec<String> = node.node_type.into_vec();
        let is_class = types.iter().any(|t| t == "rdfs:Class");
        let is_property = types.iter().any(|t| t == "rdf:Property");

        // Skip enum values, datatypes, and documents — we only care about
        // "rdfs:Class" and "rdf:Property" nodes.
        if !is_class && !is_property {
            continue;
        }

        let Some(id) = node.id else {
            continue;
        };
        // Only keep nodes defined in the Schema.org namespace. External
        // namespaces (foaf, owl, rdfs, xsd, ...) appear as
        // `rdfs:subClassOf` references and shouldn't become seed schemas
        // themselves.
        let label = match node.label {
            Some(l) => l.into_string(),
            None => match id.strip_prefix("schema:") {
                Some(stripped) => stripped.to_string(),
                None => continue,
            },
        };
        if !id.starts_with("schema:") {
            continue;
        }

        let comment_text = node.comment.map(LabelField::into_string);

        if is_class {
            classes.push(SchemaOrgClass {
                label,
                comment: comment_text,
            });
        } else {
            let domain = node
                .domain_includes
                .into_vec()
                .into_iter()
                .filter_map(|r| r.id.strip_prefix("schema:").map(String::from))
                .collect();
            let range = node
                .range_includes
                .into_vec()
                .into_iter()
                .filter_map(|r| r.id.strip_prefix("schema:").map(String::from))
                .collect();
            properties.push(SchemaOrgProperty {
                label,
                comment: comment_text,
                domain_includes: domain,
                range_includes: range,
            });
        }
    }

    let mut properties_by_domain: HashMap<String, Vec<String>> = HashMap::new();
    for prop in &properties {
        for domain in &prop.domain_includes {
            properties_by_domain
                .entry(domain.clone())
                .or_default()
                .push(prop.label.clone());
        }
    }

    // Dedup each domain's property list — Schema.org occasionally lists the
    // same property under a class via multiple paths.
    for props in properties_by_domain.values_mut() {
        let seen: HashSet<String> = props.iter().cloned().collect();
        *props = seen.into_iter().collect();
        props.sort();
    }

    let property_by_snake_name: HashMap<String, SchemaOrgProperty> = properties
        .iter()
        .map(|p| (property_name_to_snake_case(&p.label), p.clone()))
        .collect();

    Ok(ParsedDump {
        classes,
        properties,
        properties_by_domain,
        property_by_snake_name,
    })
}

// ─────────────────────────────────────────────────────────────────────
//  Helpers
// ─────────────────────────────────────────────────────────────────────

/// Convert a Schema.org property name to the canonical-field snake_case
/// form used elsewhere in the schema service.
///
/// Examples:
///   `givenName` → `given_name`
///   `birthDate` → `birth_date`
///   `telephone` → `telephone`
///   `url` → `url`
///   `ISBN` → `isbn`
pub fn property_name_to_snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    let chars: Vec<char> = name.chars().collect();
    for (i, c) in chars.iter().enumerate() {
        if c.is_ascii_uppercase() {
            let prev_is_lower = i > 0 && chars[i - 1].is_ascii_lowercase();
            let next_is_lower = i + 1 < chars.len() && chars[i + 1].is_ascii_lowercase();
            let prev_is_upper = i > 0 && chars[i - 1].is_ascii_uppercase();
            // Insert underscore at camelCase boundary (prev lowercase) or
            // at the end of an acronym run (prev uppercase, next lowercase,
            // e.g. `URLPath` → `url_path`).
            if i > 0 && (prev_is_lower || (prev_is_upper && next_is_lower)) {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(*c);
        }
    }
    out
}

/// Map Schema.org `rangeIncludes` values to a `FieldValueType`. Schema.org
/// properties can have multiple ranges (`Person | Organization`); we
/// collapse to the most general scalar type since fold_db's structural
/// `OneOf` unions don't round-trip cleanly. `String` is the safe default
/// for unknown or object-typed ranges.
#[cfg(feature = "schema-org-seed-data")]
pub(super) fn infer_field_type_from_ranges(ranges: &[String]) -> FieldValueType {
    let has = |t: &str| ranges.iter().any(|r| r == t);

    if has("Integer") {
        FieldValueType::Integer
    } else if has("Number") || has("Float") {
        FieldValueType::Float
    } else if has("Boolean") {
        FieldValueType::Boolean
    } else {
        // Text, Date, DateTime, URL, Time, Duration, and any reference to
        // another Schema.org class all collapse to String. Losing
        // structural union information is an accepted trade-off in this
        // pass — see projects/schema-org-seeding in gbrain.
        FieldValueType::String
    }
}

// ─────────────────────────────────────────────────────────────────────
//  Tests
// ─────────────────────────────────────────────────────────────────────
