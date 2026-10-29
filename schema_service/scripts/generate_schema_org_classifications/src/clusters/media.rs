[
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
    }
]
