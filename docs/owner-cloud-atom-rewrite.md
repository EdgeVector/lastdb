# Owner repair of deleted atom copies in cloud backup

A local atom compact retires the chunks that it rewrites. Older cloud chunks can still contain copies of those same bodies.
This command removes selected main-namespace bodies from exact cloud chunks. It preserves all other encoded records and their order.

CAUTION: This command implements an explicit owner erase. The plan must name bodies from authorized deletes.
Local absence alone does not establish erase intent. The command requires both an authorized plan and live owner proof of absence.

1. Retain the original durable Delete receipts. Do not repeat a Delete to obtain another receipt.
2. Complete normal atom reclamation and local compaction. Confirm each selected body returns HTTP 404 from the owner atom route.
3. Complete a normal cloud snapshot. Identify the exact prior cloud chunks that still contain the selected bodies.
4. Set cloud Off with `lastdb cloud off`. Keep cloud Off until the repair and its local manifest mirror both succeed.
5. Create an owner-only plan file. Use mode `0600` and an expiry within two hours.
6. Run the dry-run command. Review the chunk count, record count, and proposed manifest digest.
7. Add `--execute` to publish the repair.
8. Set cloud On, then complete a normal snapshot. Confirm that the retired chunk digests remain absent from the new manifest.
9. Restore from cloud into a fresh ephemeral home. Verify record absence, raw body absence, and unaffected control records.

```sh
lastdb cloud rewrite-deleted-atoms --plan /private/tmp/owner-rewrite.json --json
lastdb cloud rewrite-deleted-atoms --plan /private/tmp/owner-rewrite.json --execute --json
```

The plan uses these JSON fields:

| Field | Required value |
|---|---|
| `version` | `1` |
| `source_store_uuid` | Exact primary store identity |
| `expected_manifest_sha256` | Exact cloud tip manifest digest |
| `chunk_shas` | One to 64 distinct selected atom chunk digests |
| `atom_ids` | One to 64 distinct selected main atom body digests |
| `expires_at_unix_secs` | A future Unix timestamp within two hours |
| `user_authorized` | `true` for the explicit owner erase |

The command accepts only plain, digest-verified atom chunks. Each chunk is limited to 64 MiB; the total is limited to 128 MiB.
It preserves namespaced bodies, even when their digest matches a selected main body. It refuses unsupported or damaged record formats.
Each replacement receives a new deterministic chunk identity. An exact `purged_atom_chunk_retirement_receipt` names only the old chunk digests.
The existing manifest-chain validator remains unchanged. Every replacement upload and manifest upload precedes the normal cloud tip CAS.
Cloud Off waits for the current publication turn and invalidates the old cloud cleanup proof. Cloud On requires a new successful snapshot before cleanup resumes.

The owner journal contains the prepared manifest and a hash of the plan. It contains no atom identity list or body bytes.
The command keeps cloud Off on success or failure. A retry with the same unexpired plan checks the journal and exact cloud tip.
If CAS already succeeded, the retry repairs the local mirror. If the old tip remains, it rederives the exact successor before another CAS.
Any other tip refuses the retry. Keep the evidence and resolve that conflict before another plan.

Remove the private plan after the proof and closeout. Do not copy its atom identities into a permanent delete ledger.
This command does not delete cloud objects. It changes only the exact selected references in the live manifest.
