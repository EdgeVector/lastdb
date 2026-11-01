# Compound alternate keys

A schema owns its values. Canonical field identities can match across schemas while their record values remain separate.
Schema Service keeps different key layouts as separate definitions.
Registration does not rewrite existing values.

An alternate key uses a declared hash field or an explicit `hash_literal`. It uses one range field or ordered `range_parts`.
Each alternate key retains the exact primary field without formatting. The primary schema still uses a Hash key.
Separate HashRange schemas can store relationship records, without alternate keys.

```json
{
  "by_board": {
    "hash_field": "board",
    "range_separator": "#",
    "range_parts": [
      {"field": "column"},
      {"field": "position", "format": {"padded_text": {"width": 8}}},
      {"field": "slug"}
    ]
  },
  "recent": {
    "hash_literal": "all",
    "range_separator": "#",
    "range_parts": [
      {"field": "updated_at", "format": "reverse_time_millis"},
      {"field": "slug"}
    ]
  }
}
```

The board range preserves `column#position#slug`. Padded text preserves the existing minimum zero padding.
Padded text preserves lexical positions and counts UTF-16 units, as the app does. Text values cannot contain the separator.
The separator is one ASCII punctuation byte. This prevents ambiguous boundaries between range parts.
The reverse time format uses RFC 3339 input, millisecond precision, and a fixed 20-digit descending UTC order.
Equal timestamps use the following slug part. The time order covers signed Unix millisecond values.

Queries select `key_name` and a hash, exact key, prefix, or range. A cursor remains bound to the declaration and query.
The response `key` remains the primary mutation key. The `matched_key` contains the alternate coordinates.
Writes maintain all declared keys in the same native transaction as the field values.

Fresh homes use format 7. This program opens only format 7.
An older home moves by a one-time copy into a fresh home.
No command upgrades an older home in place.

A plain owned Hash can use the external hash supplied by a request when it declares exactly one field and no named alternate keys.
Adding alternate keys still requires a declared primary field. No key field is inferred from a payload.

For existing data, follow [the multi-key schema guide](multi-key-schemas.md#existing-data).
