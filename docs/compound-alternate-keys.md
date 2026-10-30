# Compound alternate keys

A schema owns its values. Canonical field identities can match across schemas without shared values or proteins.
Schema Service keeps different key layouts as separate definitions and does not create value aliases between them.
Existing published definitions retain their metadata until an explicit compatible conversion. Registration does not rewrite existing values.

An alternate key uses a declared hash field or an explicit `hash_literal`. It uses one range field or ordered `range_parts`.
Each alternate key retains the exact primary field without formatting. The primary schema still uses a Hash key.
Independent HashRange schemas can store separate relationship records, without alternate keys.

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

Fresh homes use format 6. Existing format 4 and 5 homes remain readable.
Stop the daemon and run `lastdb db enable-alternate-keys` before new compound declarations or independent HashRange declarations.
The command changes the format marker and leaves encrypted data files unchanged. Older writers refuse the new marker.
Snapshot profiles 2, 3, and 4 retain format 4, 5, and 6 respectively.
Existing simple alternate declarations remain valid in format 5, with unchanged declaration revisions.

Existing apps use the explicit owner conversion actions at `POST /api/db/alternate-keys`.
The app supplies exact source names, definition identities, named keys, membership rules, and legacy schemas to retire.
The begin action requires `maintenance:true`. It freezes all installed schema states in one native transaction.
All requests stay blocked during the copy. Metadata reads, flushes, and conversion requests remain available.

Pages visit every frozen source key and copy selected current tips, atom references, and author clocks into private molecules.
A field-existence rule or an app directory selects members. A directory rule also checks an exact count guard.
Coverage must match every frozen field count and every expected selected count.
Every source field/atom pair must remain represented by a selected member.
Unknown unique values cause refusal before publication.

After verification, the commit action publishes all target schemas in one native transaction.
Cleanup retires the exact reviewed legacy schemas through the normal binding and molecule lifecycle.
Retirement requires `app_schema_alias_retirement_v1` and a fully mapped alias of one kept source.
Before the freeze, the transaction checks every alias field, actual molecule, field type, and primary key role.
An ordinary old layout must become a separate owned historical record type. Its values cannot disappear during this conversion.
The durable job records this retirement audit. Older active jobs with retirements cannot publish, delete, or finish without that audit.
An owner can still abort an older job before publication. An older published job requires review before the upgrade.
The app writes its config to disk before the finish action restores schema states.
A restart preserves the maintenance state and the page cursor.
Before publication, the abort action removes unpublished tips and restores the source state.
After publication, the owner resumes forward.

Legacy cross-key metadata can name values that already occupy separate molecules at this schema's own key coordinates.
The `app_schema_private_mapper_conversion_v1` capability permits that exact case.
Each mapped field must retain a stored private molecule UUID, distinct from its referenced field's actual UUID.
Both fields must retain the same app owner, canonical field identity, and type. Their complete key coordinates must differ.
Every referenced schema must be explicitly kept in the same conversion. Missing or shared molecule authority causes refusal.
The target copies the existing current tips at the source's own keys and removes every mapper.
The field coverage, membership, atom, author, and count checks still apply. No value comes from another schema by inference.

This bounded converter refuses other mapped source authorities, kept history, molecule generations, database catalog entries, and registered logical peers.
It does not change the primary on a machine.

An explicit field transition also requires the `app_schema_field_mapping_v1` capability.
Each kept schema supplies `target_definition_identity` and `field_map` together.
The map names every source field and one distinct target field for each source field.
Both source and target identities must be exact published identities.
The host fetches the target definition from Schema Service before the maintenance freeze.
The request cannot supply the target definition itself.
The target uses published field metadata only. Legacy catalog value aliases never become local record authorities.
This rule also applies to a normal owned declaration. Existing local mappers require the private-source checks above.

The transition preserves the app owner, record type, runtime schema address, field types, primary key roles, and child schema references.
Data classifications cannot weaken. The `secret` and `no_index` labels cannot disappear.
Membership rules still use source field names. Named alternate keys use target field names.
The copy preserves atom references, author clocks, and stored values.
An ordinary independent source can use this explicit transition too.

The durable conversion scope and every status reply retain the target identity and complete map.
The app must preserve this scope across a restart and save the new definition identity before the finish action.
Requests without these optional properties keep the original conversion contract.
