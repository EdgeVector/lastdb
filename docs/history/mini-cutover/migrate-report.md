PASS

# Mini cutover Phase 3 migrator report

- Source: offline CoW of `~/lastdb-cloudtest`
- Main keys: 6049 total, 6049 mapped, 0 unmapped
- Coverage: 100%
- Idempotent re-run: ok (clear-then-write per collection)

## Main → collections

| collection | keys |
| --- | ---: |
| `atoms` | 1076 |
| `field_tip_headers` | 106 |
| `field_tips` | 1351 |
| `legacy_schema_secondary_index` | 1076 |
| `mutation_history` | 1900 |
| `sync_conflicts` | 540 |

## Notes

- restored 2 non-main namespaces 1:1
- split main: mapped=6049 unmapped=0 total=6049

