//! Deterministic matching for advice. Field identity and validation use the
//! original strings, including the canonical definition's wording and units.

pub(super) struct FieldName {
    words: Vec<String>,
    pub(super) normalized: String,
}

pub(super) fn field_name(name: &str) -> FieldName {
    let mut separated = String::new();
    let mut chars = name.chars().peekable();
    let mut previous = None::<char>;
    while let Some(current) = chars.next() {
        let word_start = current.is_uppercase()
            && previous.is_some_and(|previous| {
                previous.is_lowercase()
                    || (previous.is_uppercase() && chars.peek().is_some_and(|c| c.is_lowercase()))
            });
        if word_start {
            separated.push('_');
        }
        separated.push(current);
        previous = Some(current);
    }
    let mut words: Vec<_> = crate::words(&separated)
        .iter()
        .map(|word| unit_spelling(word).to_owned())
        .collect();
    words.dedup();
    if words.len() == 2 && words[0] == "id" {
        words.swap(0, 1);
    }
    let joined = words.join("_");
    let normalized = match joined.as_str() {
        "mime_type" | "file_mime_type" => "file_media_type",
        "person_surname" | "person_last_name" => "person_family_name",
        "birth_date" | "person_birthdate" | "person_birth_date" => "date_of_birth",
        "contact_email" => "contact_email_address",
        "person_identifier" | "directory_person_id" | "directory_person_identifier" => "person_id",
        "contact_invite_code" | "contact_invitation_code" | "contact_invitation_token" => {
            "contact_code"
        }
        "profile_display_name" | "profile_display_label" => "display_name",
        _ => &joined,
    }
    .to_owned();
    FieldName { words, normalized }
}

fn description_words(text: &str) -> Vec<String> {
    let words = crate::words(text);
    let mut normalized = Vec::new();
    let mut previous = None::<&str>;
    for (index, word) in words.iter().map(String::as_str).enumerate() {
        if word == "assigned"
            && words.get(index + 1).is_some_and(|word| word == "to")
            && matches!(previous, Some("id" | "identifier"))
        {
            continue;
        }
        if !matches!(word, "a" | "an" | "the" | "and" | "for" | "to" | "of") {
            let word = match word {
                "identifier" => "id",
                _ => unit_spelling(word),
            };
            normalized.push(word.to_owned());
        }
        previous = Some(word);
    }
    for index in 1..normalized.len() {
        if normalized[index - 1] == "stable" && normalized[index] == "opaque" {
            normalized.swap(index - 1, index);
        }
    }
    normalized
}

pub(super) fn descriptions_agree(left: &str, right: &str) -> bool {
    if crate::same_description(left, right) {
        return true;
    }
    let left = description_words(left);
    left.len() >= 2 && left == description_words(right)
}

pub(super) fn descriptions_compatible(left: Option<&str>, right: Option<&str>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) if !left.trim().is_empty() && !right.trim().is_empty() => {
            descriptions_agree(left, right)
        }
        _ => true,
    }
}

const QUALIFIERS: &[&[&str]] = &[
    &["created", "updated", "deleted", "sent", "received", "taken"],
    &["start", "end"],
    &["source", "target", "destination"],
    &["minimum", "maximum"],
    &["first", "last"],
    &["public", "private"],
    &["latitude", "longitude"],
    &["preparation", "cooking", "cooling"],
    &[
        "seconds",
        "milliseconds",
        "microseconds",
        "minutes",
        "hours",
        "days",
    ],
    &["bytes", "bits"],
    &["degrees", "radians"],
    &[
        "pixels",
        "inches",
        "feet",
        "centimeters",
        "millimeters",
        "meters",
        "kilometers",
    ],
];

fn unit_spelling(word: &str) -> &str {
    match word {
        "degree" => "degrees",
        "radian" => "radians",
        "ms" | "millisecond" => "milliseconds",
        "second" => "seconds",
        "minute" => "minutes",
        "hour" => "hours",
        "byte" => "bytes",
        "bit" => "bits",
        "px" | "pixel" => "pixels",
        "inch" => "inches",
        "cm" | "centimeter" | "centimetre" | "centimetres" => "centimeters",
        "mm" | "millimeter" | "millimetre" | "millimetres" => "millimeters",
        "km" | "kilometer" | "kilometre" | "kilometres" => "kilometers",
        _ => word,
    }
}

fn conflicting_qualifier(left: &[String], right: &[String]) -> bool {
    QUALIFIERS.iter().any(|group| {
        let in_group = |word: &String| group.contains(&word.as_str());
        left.iter().any(in_group)
            && right.iter().any(in_group)
            && !left
                .iter()
                .filter(|word| in_group(word))
                .any(|word| right.contains(word))
    })
}

pub(super) fn canonical_candidate(
    name: &FieldName,
    description: Option<&str>,
    candidate: &str,
    canonical_description: &str,
) -> Option<u8> {
    let candidate = field_name(candidate);
    if conflicting_qualifier(&name.words, &candidate.words) {
        return None;
    }
    let description = description.filter(|text| !text.trim().is_empty());
    if !descriptions_compatible(description, Some(canonical_description)) {
        return None;
    }
    if name.normalized == candidate.normalized {
        // An exact normalized name or a curated alias is stronger evidence.
        Some(2)
    } else {
        // A different spelling can match when the supplied description corroborates it.
        description.map(|_| 1)
    }
}
