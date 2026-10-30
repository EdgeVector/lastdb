# Compound alternate keys

A schema owns its values. Canonical field identities can match across schemas without shared values or proteins.

An alternate key uses a declared hash field or an explicit `hash_literal`. It uses one range field or ordered `range_parts`.
Each alternate key includes the primary field. The primary schema still uses a Hash key.
Independent HashRange schemas can store separate relationship records, without alternate keys.

```json
{
  "by_board": {
    "hash_field": "board",
    "range_separator": "#",
    "range_parts": [
      {"field": "column"},
      {"field": "position", "format": {"decimal": {"width": 8}}},
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

The board range preserves `column#position#slug`. Decimal width specifies minimum zero padding.
Decimal values use nonnegative integers within u64. Text values cannot contain the separator.
The reverse time format uses RFC 3339 input, millisecond precision, and a fixed 20-digit descending UTC order.
Equal timestamps use the following slug part. The time order covers signed Unix millisecond values.

Queries select `key_name` and a hash, exact key, prefix, or range. A cursor remains bound to the declaration and query.
The response `key` remains the primary mutation key. The `matched_key` contains the alternate coordinates.
Writes maintain all declared keys in the same native transaction as the field values.

Fresh homes use format 6. Existing format 4 and 5 homes remain readable.
Stop the daemon and run `lastdb db enable-alternate-keys` before new compound declarations or independent HashRange declarations.
The command changes the format marker and leaves encrypted data files unchanged. Older writers refuse the new marker.
Snapshot profiles 2, 3, and 4 retain format 4, 5, and 6 respectively.
Existing simple alternate declarations remain valid in format 5, with unchanged declaration revisions.

This change does not convert existing app data or change the primary on a machine.
Each app must use an explicit conversion before it replaces legacy lookup records.
