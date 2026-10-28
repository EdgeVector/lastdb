# Canonical seed review

This manifest reviews all 151 former seed definitions.
It approves the source vocabulary in `builtin_canonical_fields.json`.
Removed concepts require a precise, separately approved definition before reuse.
No removed name serves as an accepted alias. App adapters can keep external names.

Date and integer changes require explicit value conversion outside this definition reset.
Money and container fields stay absent until their value contracts are explicit.

| Former field | Action | Approved name | Type | Reason |
| --- | --- | --- | --- | --- |
| user_id | Keep | user_id | String | The subject and property are explicit. |
| user_name | Replace | user_display_name | String | Use the specific subject and property. |
| first_name | Replace | person_given_name | String | Use the specific subject and property. |
| last_name | Replace | person_family_name | String | Use the specific subject and property. |
| full_name | Replace | person_full_name | String | Use the specific subject and property. |
| display_name | Remove | — | — | The subject can be a person or any entity. Select the actual subject. |
| handle | Replace | account_handle | String | Use the specific subject and property. |
| username | Replace | account_login_name | String | Use the specific subject and property. |
| user_email | Keep | user_email | String | The subject and property are explicit. |
| email_address | Replace | contact_email_address | String | Use the specific subject and property. |
| phone_number | Replace | contact_phone_number | String | Use the specific subject and property. |
| date_of_birth | Replace | date_of_birth | Date | The subject and property are explicit. Use the semantic type; old values require explicit conversion. |
| gender | Replace | person_gender_identity | String | Use the specific subject and property. |
| pronouns | Replace | person_pronouns | String | Use the specific subject and property. |
| avatar_url | Replace | user_avatar_url | String | Use the specific subject and property. |
| bio | Replace | person_biography | String | Use the specific subject and property. |
| identity_hash | Remove | — | — | The entity, digest algorithm and encoding are unspecified. |
| public_key | Remove | — | — | The key purpose, algorithm and encoding are unspecified. |
| id | Remove | — | — | The identifier subject is unspecified. |
| name | Remove | — | — | The named subject is unspecified. |
| title | Remove | — | — | Select a document, event or other specific title. |
| description | Remove | — | — | The described subject is unspecified. |
| summary | Remove | — | — | The summarized subject is unspecified. |
| content | Remove | — | — | The content subject and format are unspecified. |
| tags | Remove | — | — | The seed says String, but app contracts use Array(String). Select a subject-specific label list. |
| category | Remove | — | — | The classification subject is unspecified. |
| language | Replace | content_language_code | String | Use the specific subject and property. |
| slug | Replace | content_url_slug | String | Use the specific subject and property. |
| url | Remove | — | — | The referenced resource is unspecified. |
| source_url | Replace | content_source_url | String | Use the specific subject and property. |
| created_at | Remove | — | — | Select the subject creation event. |
| updated_at | Remove | — | — | Select the subject update event. |
| deleted_at | Remove | — | — | Select the subject deletion event. |
| published_at | Remove | — | — | Select the subject publication event. |
| timestamp | Remove | — | — | Select the observed event. |
| start_time | Remove | — | — | Select the event that starts. |
| end_time | Remove | — | — | Select the event that ends. |
| duration_seconds | Remove | — | — | Select the measured process or event. |
| expires_at | Remove | — | — | Select the subject expiration event. |
| street_address | Keep | street_address | String | The subject and property are explicit. |
| city | Replace | address_city | String | Use the specific subject and property. |
| state | Remove | — | — | The lifecycle or machine subject is unspecified. |
| address_state | Keep | address_state | String | The subject and property are explicit. |
| postal_code | Replace | address_postal_code | String | Use the specific subject and property. |
| country | Replace | address_country | String | Use the specific subject and property. |
| gps_latitude | Replace | geographic_latitude_degrees | Number | Use the specific subject and property. |
| gps_longitude | Replace | geographic_longitude_degrees | Number | Use the specific subject and property. |
| altitude_meters | Replace | geographic_altitude_meters | Number | Use the specific subject and property. |
| place_name | Keep | place_name | String | The subject and property are explicit. |
| venue_name | Keep | venue_name | String | The subject and property are explicit. |
| timezone | Replace | iana_timezone_identifier | String | Use the specific subject and property. |
| photo_url | Keep | photo_url | String | The subject and property are explicit. |
| photo_caption | Keep | photo_caption | String | The subject and property are explicit. |
| photo_taken_at | Replace | photo_taken_at | DateTime | The subject and property are explicit. Use the semantic type; old values require explicit conversion. |
| image_width | Replace | image_width_pixels | Integer | Use the specific subject and property. Use the semantic type; old values require explicit conversion. |
| image_height | Replace | image_height_pixels | Integer | Use the specific subject and property. Use the semantic type; old values require explicit conversion. |
| video_url | Keep | video_url | String | The subject and property are explicit. |
| video_duration_seconds | Keep | video_duration_seconds | Number | The subject and property are explicit. |
| thumbnail_url | Replace | image_thumbnail_url | String | Use the specific subject and property. |
| audio_url | Keep | audio_url | String | The subject and property are explicit. |
| file_size_bytes | Replace | file_size_bytes | Integer | The subject and property are explicit. Use the semantic type; old values require explicit conversion. |
| file_type | Replace | file_media_type | String | Use the specific subject and property. |
| file_hash | Remove | — | — | The digest algorithm and encoding are unspecified. |
| camera_make | Keep | camera_make | String | The subject and property are explicit. |
| camera_model | Keep | camera_model | String | The subject and property are explicit. |
| iso_speed | Replace | camera_iso_sensitivity | Number | Use the specific subject and property. |
| aperture | Replace | camera_aperture_ratio | Number | Use the specific subject and property. |
| shutter_speed | Remove | — | — | The current text contract does not define a unit or representation. |
| message_id | Keep | message_id | String | The subject and property are explicit. |
| subject | Remove | — | — | The record or event subject is unspecified. |
| email_subject | Keep | email_subject | String | The subject and property are explicit. |
| sender | Replace | message_sender | String | Use the specific subject and property. |
| recipient | Replace | message_recipient | String | Use the specific subject and property. |
| cc | Remove | — | — | The recipient representation and text versus list contract need a decision. |
| bcc | Remove | — | — | The recipient representation and text versus list contract need a decision. |
| reply_to | Replace | message_reply_to_id | String | Use the specific subject and property. |
| thread_id | Keep | thread_id | String | The subject and property are explicit. |
| sent_at | Replace | message_sent_at | DateTime | Use the specific subject and property. Use the semantic type; old values require explicit conversion. |
| amount | Remove | — | — | The currency, unit and decimal precision are unspecified. |
| currency | Replace | currency_iso_code | String | Use the specific subject and property. |
| transaction_id | Keep | transaction_id | String | The subject and property are explicit. |
| payment_method | Keep | payment_method | String | The subject and property are explicit. |
| merchant_name | Keep | merchant_name | String | The subject and property are explicit. |
| vendor_id | Keep | vendor_id | String | The subject and property are explicit. |
| account_number | Replace | financial_account_number | String | Use the specific subject and property. |
| invoice_number | Keep | invoice_number | String | The subject and property are explicit. |
| due_date | Remove | — | — | Split payment due dates from task due dates in their approved app definitions. |
| tax_amount | Remove | — | — | The currency, unit and decimal precision are unspecified. |
| product_id | Keep | product_id | String | The subject and property are explicit. |
| product_name | Keep | product_name | String | The subject and property are explicit. |
| sku | Replace | product_stock_keeping_unit | String | Use the specific subject and property. |
| price | Remove | — | — | The currency, unit and decimal precision are unspecified. |
| quantity | Remove | — | — | The counted object is unspecified. |
| order_id | Keep | order_id | String | The subject and property are explicit. |
| discount_amount | Remove | — | — | The currency, unit and decimal precision are unspecified. |
| diagnosis | Replace | medical_diagnosis | String | Use the specific subject and property. |
| medication | Replace | medication_name | String | Use the specific subject and property. |
| dosage | Replace | medication_dosage_text | String | Use the specific subject and property. |
| provider_name | Replace | healthcare_provider_name | String | Use the specific subject and property. |
| appointment_date | Replace | healthcare_appointment_at | DateTime | Use the specific subject and property. Use the semantic type; old values require explicit conversion. |
| symptom | Replace | medical_symptom | String | Use the specific subject and property. |
| allergy | Replace | person_allergy | String | Use the specific subject and property. |
| blood_type | Replace | person_blood_type | String | Use the specific subject and property. |
| height_cm | Replace | person_height_cm | Number | Use the specific subject and property. |
| weight_kg | Replace | person_weight_kg | Number | Use the specific subject and property. |
| document_title | Keep | document_title | String | The subject and property are explicit. |
| document_author | Keep | document_author | String | The subject and property are explicit. |
| publisher | Replace | document_publisher | String | Use the specific subject and property. |
| publication_date | Replace | document_publication_date | Date | Use the specific subject and property. Use the semantic type; old values require explicit conversion. |
| isbn | Replace | book_isbn | String | Use the specific subject and property. |
| doi | Replace | document_doi | String | Use the specific subject and property. |
| page_count | Replace | document_page_count | Integer | Use the specific subject and property. Use the semantic type; old values require explicit conversion. |
| chapter | Replace | document_chapter_heading | String | Use the specific subject and property. |
| event_name | Keep | event_name | String | The subject and property are explicit. |
| event_type | Keep | event_type | String | The subject and property are explicit. |
| organizer | Replace | event_organizer | String | Use the specific subject and property. |
| attendees | Remove | — | — | The person reference and text versus list contract need a decision. |
| rsvp_status | Replace | event_invitation_response | String | Use the specific subject and property. |
| location | Remove | — | — | The event versus activity subject and reference contract are unspecified. |
| post_id | Replace | social_post_id | String | Use the specific subject and property. |
| post_text | Replace | social_post_text | String | Use the specific subject and property. |
| like_count | Replace | social_post_like_count | Integer | Use the specific subject and property. Use the semantic type; old values require explicit conversion. |
| comment_text | Replace | social_comment_text | String | Use the specific subject and property. |
| follower_count | Replace | account_follower_count | Integer | Use the specific subject and property. Use the semantic type; old values require explicit conversion. |
| following_count | Replace | account_following_count | Integer | Use the specific subject and property. Use the semantic type; old values require explicit conversion. |
| share_count | Replace | social_post_share_count | Integer | Use the specific subject and property. Use the semantic type; old values require explicit conversion. |
| domain_name | Keep | domain_name | String | The subject and property are explicit. |
| ip_address | Keep | ip_address | String | The subject and property are explicit. |
| user_agent | Replace | http_user_agent | String | Use the specific subject and property. |
| referrer | Replace | http_referrer_url | String | Use the specific subject and property. |
| recipe_name | Keep | recipe_name | String | The subject and property are explicit. |
| ingredients | Remove | — | — | The text versus structured list contract needs a decision. |
| instructions | Replace | recipe_preparation_instructions | String | Use the specific subject and property. |
| servings | Replace | recipe_serving_count | Number | Use the specific subject and property. |
| prep_time_minutes | Replace | recipe_preparation_minutes | Number | Use the specific subject and property. |
| cook_time_minutes | Replace | recipe_cooking_minutes | Number | Use the specific subject and property. |
| activity_type | Replace | exercise_activity_type | String | Use the specific subject and property. |
| distance_km | Replace | exercise_distance_km | Number | Use the specific subject and property. |
| calories_burned | Replace | exercise_calories_burned | Number | Use the specific subject and property. |
| heart_rate_bpm | Replace | person_heart_rate_bpm | Number | Use the specific subject and property. |
| steps_count | Replace | exercise_step_count | Integer | Use the specific subject and property. Use the semantic type; old values require explicit conversion. |
| note_text | Keep | note_text | String | The subject and property are explicit. |
| journal_entry | Replace | journal_entry_text | String | Use the specific subject and property. |
| mood | Replace | person_reported_mood | String | Use the specific subject and property. |
| fingerprint_id | Replace | fingerprint_observation_id | String | Use the specific subject and property. |
| fingerprint_kind | Replace | fingerprint_observation_kind | String | Use the specific subject and property. |
| fingerprint_value | Remove | — | — | The observation kind controls an unspecified value encoding. |
| confidence | Remove | — | — | The score subject and calibration are unspecified. |
| persona_id | Keep | persona_id | String | The subject and property are explicit. |
| relationship | Replace | persona_owner_relationship | String | Use the specific subject and property. |
| trust_tier | Replace | access_trust_tier | Integer | Use the specific subject and property. Use the semantic type; old values require explicit conversion. |
