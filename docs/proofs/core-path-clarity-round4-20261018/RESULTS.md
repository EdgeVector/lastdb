# Native path clarity review

The review found dormant history code, inactive factory inputs, an empty private event list, and duplicate query preparation.
The change removes those alternatives. Current reads and writes retain one core model and one native disk engine.

## Changes

| Finding | Change |
|---|---|
| A private history switch always stays false. Its queue and disk output remain empty. | Remove the switch, history queue, history output, and startup history scheduler. |
| Writes pass an always-empty event list to a consumer that discards it. | Remove the private list, tuple members, job field, and unused parameters. |
| Factory names mention a removed pool and accept inactive cloud inputs. | Use `FoldDbOptions` for the actual local encryption and Search inputs. |
| Startup passes an unused path and optional prebuilt-engine locks that are always absent. | Remove those inputs. The coordinator owns its normal local maintenance locks directly. |
| Query conditions format and copy full records before they inspect field values. | Borrow the selected values directly. Keep the formatter at the response boundary. |
| The second query step has a separate serial field reader. | Reuse the existing bounded reader and value resolver for both steps. |
| Two checks calculate the same primary physical source independently. | Share that calculation and retain its selection order. |

The Rust factory API changes to `create_fold_db_with_options` and `FoldDbOptions`.
All repository callers use the new API. External embedded callers must update their Rust calls; no compatibility wrapper remains.

## Intentional paths

Current reverse-reference cleanup and conflict records remain active.
Physical molecule generations still support complete current-key rewrites. They are not stored atom history.
Explicit physical history maintenance remains separate from normal logical reads and writes.

The private disk queue, held read owners, memory reservations, and both completeness checks remain necessary.
Unknown data, confirmed absence, a known tip, and a loaded value remain distinct states.
This change adds no value store, queue, or map.
The [source review](source-review.txt) records the caller traces and retained controls.

## Validation

The product source is `e91e9b299cc37f36c0600251349b306ed4181ef5`.
The normal release SHA-256 is `4b35587b13611906d130b00d581965e0266299f684d6b9ab2b56e391a59e6d8b`.
The source check verifies all 1,877 product inputs before and after the release build.
Later documentation and proof commits do not change those product inputs.

Full workspace Clippy, separate embedding-package Clippy, format, file/function limits, new-allow, registry, and write-route checks pass.
No new lint exemption forms part of the change. The old file-size exemption is removed from the smaller edge helper.
The first compiler pass exposes one missing explicit import after the parent import disappears.
The next pass succeeds with the explicit import. Its original diagnostic remains available.
The removed serving symbols have no remaining Rust references in the checked product crates.
These source checks do not establish a live-primary result.

The ordinary API calls use fresh synthetic data and normal native startup.
All 63 condition cases pass with field-reader concurrency 0, 1, and 8.
They cover Hash and exact/bounded HashRange reads, missing fields, explicit null, order, and zero or positive limits.
A fresh completion passes 31 further query items, including shared-key changes and normal reopen.
Source and peer share the same current atom identities. The known old key stays absent; unrelated records stay unchanged.
The pre-reopen and post-reopen atom identities match exactly.
The [root predicate review](root-predicate-review.json) independently checks the saved requests and responses for the 63 condition cases.
The [root completion review](root-completion-review.json) independently checks the 31 completion items and their atom identities.

## Corrected procedure assumption

The initial procedure stops after 77 checked query items and remains `complete=false`.
Its extra identity check incorrectly requires an unchanged key value to retain its atom identity after a write through another schema.
The current values and shared identities already match at that point.

Preparation supplies omitted key fields from the known lookup key.
Atom identity includes the source schema and value. It does not include the mutation clock.
Independent offline calculations match both observed identities exactly.
The reverse write therefore selects a new, correctly shared atom for those key fields.

No product source change follows this result.
The fresh completion uses the same wire requests and requires the exact identity for the reverse writer's schema and content.
It also retains the strict same-schema move, unrelated-record, shared-atom, and reopen identity checks.
The 63 completed condition items do not repeat. The original procedure and its raw receipts remain unchanged.

## Cleanup

Both owned temporary roots are removed through the supported exact-path cleanup command.
All three node sessions stop normally. No owned process or operation lock remains.
The independent final check confirms that all 24 sealed receipt files remain unchanged.
The cleanup commands and checks remain in `cleanup-verification.json` and `root-final-verification.json`.

## Boundaries

No benchmark comparison runs in this round. The preceding round's timings do not measure this source.
The primary and archive remain untouched. No existing home, migration, cloud activation, or install forms part of the work.
No tests, coverage commands, or new executable fixture suite run.
