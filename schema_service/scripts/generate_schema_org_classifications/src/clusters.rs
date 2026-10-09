//! Cluster definitions for hand-curated Schema.org classification.
//!
//! Each cluster matches a pattern in the Schema.org property name
//! (snake_case form) and assigns a `(sensitivity_level, data_domain,
//! interest_category)`. Order matters — the first cluster to match a
//! property name wins. Put the most specific / most-sensitive clusters
//! first.
//!
//! Unmatched properties fall back to the fail-closed baseline
//! (`sensitivity=4, domain="general", interest_category=None`) in the
//! generator's output, so they are still explicit in the JSON overlay —
//! the runtime loader trusts the overlay completely and never makes a
//! classification decision of its own.
//!
//! ## Classification scale (fold_db DataClassification)
//!
//! - 0 = Public — fine to share (URLs, titles, tag labels)
//! - 1 = Internal — rarely sensitive metadata (timestamps, hashes)
//! - 2 = Confidential — business/topic content (biographies, body text)
//! - 3 = Restricted — identifying/PII (email, phone, names, DOB)
//! - 4 = Highly Restricted — most sensitive (medical, financial,
//!   credentials, ethnicity, sexual/political)
//!
//! ## Data domains (must match DataClassification allowed domains)
//!
//! `general`, `identity`, `financial`, `medical`, `location`,
//! `communication`, `content`, `temporal`, `commerce`, `media`,
//! `social`, `document`.

/// How a cluster matches a snake_case property name.
#[derive(Debug, Clone)]
pub enum MatchRule {
    /// Exact field-name match (post-snake_case).
    Exact(&'static [&'static str]),
    /// Name starts with any of these (post-snake_case).
    Prefix(&'static [&'static str]),
    /// Name contains any of these (post-snake_case).
    Contains(&'static [&'static str]),
    /// Name ends with any of these (post-snake_case).
    Suffix(&'static [&'static str]),
}

/// One classification cluster.
#[derive(Debug, Clone)]
pub struct Cluster {
    pub name: &'static str,
    pub rule: MatchRule,
    pub sensitivity: u8,
    pub data_domain: &'static str,
    pub interest_category: Option<&'static str>,
}

impl Cluster {
    pub fn matches(&self, snake_name: &str) -> bool {
        match &self.rule {
            MatchRule::Exact(names) => names.contains(&snake_name),
            MatchRule::Prefix(prefixes) => prefixes.iter().any(|&p| snake_name.starts_with(p)),
            MatchRule::Contains(needles) => needles.iter().any(|&n| snake_name.contains(n)),
            MatchRule::Suffix(suffixes) => suffixes.iter().any(|&s| snake_name.ends_with(s)),
        }
    }
}

/// All clusters in priority order. First match wins.
///
/// Organizing principle: most-specific → least-specific, most-sensitive
/// → least-sensitive. A property like `patientEmail` should hit the
/// medical cluster (sensitivity=4) before the email cluster
/// (sensitivity=3), so medical clusters are listed first.
pub const CLUSTERS: &[Cluster] = &[
    // ──────────────────────────────────────────────────────────────
    // Explicit high-risk field names first (exact-name overrides)
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "credentials",
        rule: MatchRule::Exact(&[
            "password",
            "passphrase",
            "secret",
            "api_key",
            "access_token",
            "refresh_token",
            "session_token",
            "token",
            "private_key",
            "signature",
            "auth_code",
            "otp",
            "mfa_code",
        ]),
        sensitivity: 4,
        data_domain: "identity",
        interest_category: None,
    },
    Cluster {
        name: "government_id",
        rule: MatchRule::Exact(&[
            "tax_id",
            "vat_id",
            "national_id",
            "passport_number",
            "ssn",
            "drivers_license",
            "isic_v4",
            "naics",
            "duns",
            "lei_code",
            "global_location_number",
            "iso6523_code",
        ]),
        sensitivity: 4,
        data_domain: "identity",
        interest_category: None,
    },
    Cluster {
        name: "sensitive_demographics",
        rule: MatchRule::Exact(&[
            "nationality",
            "ethnicity",
            "race",
            "religion",
            "political_affiliation",
            "sexual_orientation",
            "gender_identity",
            "caste",
        ]),
        sensitivity: 4,
        data_domain: "identity",
        interest_category: None,
    },
    // ──────────────────────────────────────────────────────────────
    // Medical / health (sensitivity=4, domain=medical)
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "medical_prefix",
        rule: MatchRule::Prefix(&[
            "medical_",
            "health_",
            "drug_",
            "clinical_",
            "diagnostic_",
            "physician_",
            "hospital_",
            "disease_",
            "therapy_",
            "therapeutic_",
            "dosage_",
            "symptom_",
        ]),
        sensitivity: 4,
        data_domain: "medical",
        interest_category: Some("Health"),
    },
    Cluster {
        name: "medical_contains",
        rule: MatchRule::Contains(&[
            "health_condition",
            "medical_code",
            "medical_audience",
            "diagnosis",
            "therapy",
            "_disease",
            "_procedure",
            "adverse_outcome",
            "risk_factor",
            "interaction_with_drug",
            "mechanism_of_action",
            "drug_unit",
            "contraindication",
            "_infection",
        ]),
        sensitivity: 4,
        data_domain: "medical",
        interest_category: Some("Health"),
    },
    Cluster {
        name: "body_measurement",
        rule: MatchRule::Exact(&[
            "body_location",
            "body_measurement",
            "body_type",
            "height_cm",
            "weight_kg",
            "blood_type",
            "heart_rate_bpm",
            "blood_pressure",
            "cholesterol",
            "glucose",
            "body_weight",
            "body_height",
        ]),
        sensitivity: 4,
        data_domain: "medical",
        interest_category: Some("Health"),
    },
    Cluster {
        name: "birth_death",
        rule: MatchRule::Prefix(&["birth_", "death_", "deceased_"]),
        sensitivity: 4,
        data_domain: "identity",
        interest_category: None,
    },
    // ──────────────────────────────────────────────────────────────
    // Financial / payment / commerce (sensitivity=3, domain=financial)
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "payment",
        rule: MatchRule::Prefix(&[
            "payment_",
            "price_",
            "cost_",
            "tax_",
            "invoice_",
            "billing_",
            "fee_",
            "charge_",
            "credit_",
            "debit_",
            "loan_",
            "interest_rate",
            "earnings",
            "budget_",
            "finance_",
            "financial_",
        ]),
        sensitivity: 3,
        data_domain: "financial",
        interest_category: None,
    },
    Cluster {
        name: "payment_contains",
        rule: MatchRule::Contains(&[
            "_payment",
            "_price",
            "_cost",
            "_fee",
            "_charge",
            "_amount",
            "total_payment_due",
            "payment_method",
            "payment_accepted",
            "accepted_payment_method",
            "price_range",
            "_currency",
        ]),
        sensitivity: 3,
        data_domain: "financial",
        interest_category: None,
    },
    Cluster {
        name: "account",
        rule: MatchRule::Exact(&[
            "account_id",
            "account_number",
            "bank_account_type",
            "iban",
            "swift_code",
            "routing_number",
        ]),
        sensitivity: 4,
        data_domain: "financial",
        interest_category: None,
    },
    // ──────────────────────────────────────────────────────────────
    // Communication endpoints (sensitivity=3, domain=communication)
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "email",
        rule: MatchRule::Contains(&["email"]),
        sensitivity: 3,
        data_domain: "communication",
        interest_category: None,
    },
    Cluster {
        name: "telephone",
        rule: MatchRule::Contains(&["telephone", "_phone", "fax_number"]),
        sensitivity: 3,
        data_domain: "communication",
        interest_category: None,
    },
    Cluster {
        name: "messaging",
        rule: MatchRule::Contains(&["messaging_protocol", "contact_point", "contact_type"]),
        sensitivity: 3,
        data_domain: "communication",
        interest_category: None,
    },
    // ──────────────────────────────────────────────────────────────
    // Location / address (sensitivity=3, domain=location)
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "address_prefix",
        rule: MatchRule::Prefix(&["address_", "postal_", "street_", "geo_"]),
        sensitivity: 3,
        data_domain: "location",
        interest_category: None,
    },
    Cluster {
        name: "address_contains",
        rule: MatchRule::Contains(&[
            "_address",
            "_location",
            "_postal",
            "_locality",
            "_region",
            "_country",
            "_coordinate",
            "_latitude",
            "_longitude",
            "geo_shape",
            "geo_radius",
            "elevation",
            "postal_code",
            "address_region",
            "address_locality",
            "address_country",
            "service_area",
            "area_served",
        ]),
        sensitivity: 3,
        data_domain: "location",
        interest_category: None,
    },
    Cluster {
        name: "venue_location",
        rule: MatchRule::Exact(&[
            "location",
            "place",
            "venue",
            "event_location",
            "pickup_location",
            "location_created",
            "origin_address",
            "destination_address",
        ]),
        sensitivity: 2,
        data_domain: "location",
        interest_category: None,
    },
    // ──────────────────────────────────────────────────────────────
    // Identity / person attributes (sensitivity=3, domain=identity)
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "person_names",
        rule: MatchRule::Exact(&[
            "given_name",
            "family_name",
            "additional_name",
            "honorific_prefix",
            "honorific_suffix",
            "alternate_name",
            "legal_name",
            "maiden_name",
            "nick_name",
            "full_name",
            "first_name",
            "last_name",
            "name_suffix",
            "pronouns",
        ]),
        sensitivity: 3,
        data_domain: "identity",
        interest_category: None,
    },
    Cluster {
        name: "person_relations",
        rule: MatchRule::Exact(&[
            "children",
            "parent",
            "parents",
            "sibling",
            "siblings",
            "spouse",
            "relatedTo",
            "follows",
            "follower",
            "knows",
            "colleague",
            "colleagues",
            "contact_point_option",
        ]),
        sensitivity: 3,
        data_domain: "social",
        interest_category: None,
    },
    // ──────────────────────────────────────────────────────────────
    // Commerce (sensitivity=2, domain=commerce) — less sensitive than financial
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "commerce_prefix",
        rule: MatchRule::Prefix(&[
            "product_",
            "sku",
            "gtin",
            "mpn",
            "isbn",
            "issn",
            "offer_",
            "inventory_",
            "order_",
            "ship_",
        ]),
        sensitivity: 2,
        data_domain: "commerce",
        interest_category: None,
    },
    Cluster {
        name: "commerce_contains",
        rule: MatchRule::Contains(&[
            "_offer",
            "_inventory",
            "_warranty",
            "_sku",
            "_gtin",
            "_mpn",
            "delivery_method",
            "delivery_time",
            "ship_to",
            "shipping_",
            "pickup_time",
            "merchant_",
        ]),
        sensitivity: 2,
        data_domain: "commerce",
        interest_category: None,
    },
    // ──────────────────────────────────────────────────────────────
    // Media / photography / audio / video (sensitivity=1, domain=media)
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "photography",
        rule: MatchRule::Contains(&[
            "exif_",
            "camera_",
            "iso_speed",
            "aperture",
            "shutter_speed",
            "focal_length",
            "photo_",
            "image_dimension",
        ]),
        sensitivity: 1,
        data_domain: "media",
        interest_category: Some("Photography"),
    },
    Cluster {
        name: "audio_video",
        rule: MatchRule::Contains(&[
            "video_quality",
            "audio_quality",
            "bit_rate",
            "frame_rate",
            "audio_format",
            "video_format",
            "transcript",
            "caption",
            "subtitle_language",
        ]),
        sensitivity: 1,
        data_domain: "media",
        interest_category: None,
    },
    // ──────────────────────────────────────────────────────────────
    // Time / temporal (sensitivity=0, domain=temporal)
    //
    // Most event / content timing is public. Birth/death dates are
    // handled by the "birth_death" cluster above; anything else matching
    // these patterns is safe to default to Public.
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "temporal_events",
        rule: MatchRule::Suffix(&["_date", "_time", "_at", "_duration", "_hours", "_schedule"]),
        sensitivity: 0,
        data_domain: "temporal",
        interest_category: None,
    },
    Cluster {
        name: "temporal_prefix",
        rule: MatchRule::Prefix(&[
            "start_",
            "end_",
            "cook_time",
            "prep_time",
            "duration",
            "opening_hours",
            "valid_from",
            "valid_through",
            "valid_until",
            "expires",
            "scheduled_",
        ]),
        sensitivity: 0,
        data_domain: "temporal",
        interest_category: None,
    },
    // ──────────────────────────────────────────────────────────────
    // URLs & identifiers (sensitivity=0, domain=content)
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "urls",
        rule: MatchRule::Exact(&[
            "url",
            "sameAs",
            "same_as",
            "main_entity_of_page",
            "previous",
            "next",
            "identifier",
        ]),
        sensitivity: 0,
        data_domain: "content",
        interest_category: None,
    },
    Cluster {
        name: "urls_suffix",
        rule: MatchRule::Suffix(&["_url", "_uri", "_href"]),
        sensitivity: 0,
        data_domain: "content",
        interest_category: None,
    },
    // ──────────────────────────────────────────────────────────────
    // Content metadata (sensitivity=0-1, domain=content)
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "content_labels",
        rule: MatchRule::Exact(&[
            "name",
            "alternate_name",
            "disambiguating_description",
            "description",
            "headline",
            "keywords",
            "genre",
            "category",
            "in_language",
            "tag_name",
            "abstract",
            "about",
            "audience_type",
            "color",
            "material",
        ]),
        sensitivity: 0,
        data_domain: "content",
        interest_category: None,
    },
    Cluster {
        name: "content_body",
        rule: MatchRule::Exact(&[
            "text",
            "article_body",
            "article_section",
            "body",
            "quotation",
            "caption",
            "transcript",
            "comment_text",
            "review_body",
            "recipe_instructions",
            "recipe_ingredient",
            "message_attachment",
        ]),
        sensitivity: 2,
        data_domain: "content",
        interest_category: None,
    },
    // ──────────────────────────────────────────────────────────────
    // Ratings / stats (sensitivity=0, domain=content)
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "ratings_counts",
        rule: MatchRule::Contains(&[
            "rating",
            "review_count",
            "review_aspect",
            "review_body",
            "_count",
            "playback_",
            "watch_count",
        ]),
        sensitivity: 0,
        data_domain: "content",
        interest_category: None,
    },
    // ──────────────────────────────────────────────────────────────
    // Physical measurements (ambiguous — sensitivity=2 to be safe)
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "measurements",
        rule: MatchRule::Exact(&[
            "width", "height", "depth", "weight", "length", "size", "volume", "mass", "area",
            "diameter", "radius",
        ]),
        sensitivity: 2,
        data_domain: "content",
        interest_category: None,
    },
    // ──────────────────────────────────────────────────────────────
    // Fitness / activity (sensitivity=2, domain=general, Fitness)
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "fitness",
        rule: MatchRule::Exact(&[
            "activity_type",
            "exercise_type",
            "exercise_course",
            "exercise_plan",
            "sport_type",
            "distance_km",
            "calories_burned",
            "steps_count",
            "pace",
            "workout",
        ]),
        sensitivity: 2,
        data_domain: "general",
        interest_category: Some("Fitness"),
    },
    // ──────────────────────────────────────────────────────────────
    // Cooking (sensitivity=0, domain=content, interest=Cooking)
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "cooking",
        rule: MatchRule::Contains(&[
            "recipe_",
            "ingredients",
            "cooking_method",
            "cook_time",
            "prep_time",
            "menu_",
            "suitable_for_diet",
        ]),
        sensitivity: 0,
        data_domain: "content",
        interest_category: Some("Cooking"),
    },
    // ──────────────────────────────────────────────────────────────
    // Events (sensitivity=0, domain=content, interest=Events)
    // ──────────────────────────────────────────────────────────────
    Cluster {
        name: "events",
        rule: MatchRule::Contains(&[
            "event_",
            "schedule_",
            "attendee",
            "organizer",
            "performer",
            "rsvp",
            "door_time",
            "previous_start_date",
        ]),
        sensitivity: 1,
        data_domain: "content",
        interest_category: Some("Events"),
    },
];
