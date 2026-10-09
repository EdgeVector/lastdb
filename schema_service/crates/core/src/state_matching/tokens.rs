use std::collections::HashSet;

pub(super) fn lexical_field_corresponds(left: &str, right: &str) -> bool {
    let left_tokens = field_tokens(left);
    let right_tokens = field_tokens(right);
    if left_tokens.is_empty() || right_tokens.is_empty() {
        return false;
    }
    if left_tokens == right_tokens {
        return true;
    }
    let shared: HashSet<&String> = left_tokens.intersection(&right_tokens).collect();
    if shared.is_empty() || !shared.iter().any(|token| !is_generic_field_token(token)) {
        return false;
    }
    left_tokens.is_subset(&right_tokens)
        || right_tokens.is_subset(&left_tokens)
        || (shared.len() as f32 / left_tokens.union(&right_tokens).count() as f32) >= 0.5
}

pub(super) fn field_tokens(raw: &str) -> HashSet<String> {
    raw.split(|c: char| !c.is_ascii_alphanumeric())
        .filter_map(|part| {
            let token = normalize_match_token(part);
            (!token.is_empty()).then_some(token)
        })
        .collect()
}

pub(super) fn lexical_name_similarity(left: &str, right: &str) -> f32 {
    let left_tokens = name_tokens(left);
    let right_tokens = name_tokens(right);
    if left_tokens.is_empty() || right_tokens.is_empty() {
        return 0.0;
    }
    let shared = left_tokens.intersection(&right_tokens).count();
    if shared == 0 {
        return 0.0;
    }
    let union = left_tokens.union(&right_tokens).count();
    shared as f32 / union as f32
}

pub(super) fn name_tokens(raw: &str) -> HashSet<String> {
    raw.split(|c: char| !c.is_ascii_alphanumeric())
        .filter_map(|part| {
            let token = normalize_match_token(part);
            (!token.is_empty() && !is_generic_name_token(&token)).then_some(token)
        })
        .collect()
}

pub(super) fn normalize_match_token(raw: &str) -> String {
    let token = raw.trim().to_ascii_lowercase();
    let token = match token.as_str() {
        "captured" | "capture" => "taken",
        "pictures" | "picture" | "images" | "image" => "photo",
        "emails" => "email",
        "phones" => "phone",
        "addresses" => "address",
        "customers" => "customer",
        "contacts" => "contact",
        "transactions" => "transaction",
        "records" => "record",
        "items" => "item",
        _ => token.as_str(),
    };
    token.to_string()
}

pub(super) fn is_generic_name_token(token: &str) -> bool {
    matches!(
        token,
        "record"
            | "records"
            | "list"
            | "lists"
            | "archive"
            | "archives"
            | "collection"
            | "collections"
            | "entry"
            | "entries"
            | "item"
            | "items"
    )
}

pub(super) fn is_generic_field_token(token: &str) -> bool {
    matches!(
        token,
        "id" | "identifier"
            | "value"
            | "data"
            | "info"
            | "record"
            | "records"
            | "item"
            | "items"
            | "field"
            | "date"
            | "time"
            | "at"
            | "number"
    )
}
