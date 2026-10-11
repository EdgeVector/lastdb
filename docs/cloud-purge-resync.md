# Cloud purge and resync

CAUTION: This operation deletes the account's cloud content. The selected device supplies the replacement snapshot.

Use this operation when the local data must replace the cloud data. A personal account always permits the operation. An org permits its current owner or admin to authorize it.

The operation keeps account identity, devices, encryption keys, access grants, settings, and billing metadata. It deletes owned cloud files, old file versions, backup roots, share roots, message content, and usage credit records. It keeps content that another account owns.

The cloud service writes a permanent control file outside the content that it deletes. The control file identifies the account, operation, and generation. Every cloud write needs the current generation. A stale device cannot publish its old queue.

## Start the operation

The owner command uses the local node socket. The command returns a durable job ID before the snapshot work starts.

```sh
lastdb cloud purge-resync begin
lastdb cloud purge-resync begin --org <org-id>
```

Use `--request-id <uuid>` to repeat a request after a lost response. Keep the same UUID for that request.

The selected device retains an exact independent source before the cloud hold. It fetches missing file bytes before the hold. It pauses local mutations until the replacement reaches Ready and the local device acknowledges adoption.

An org source includes only that org's schemas, molecules, atoms, history, and referenced files. It keeps personal data and other org data unchanged.

The operation verifies existing local file bytes and keeps their encryption. It uses the configured memory guard before each file read. It does not add a fixed file size cap. Source admission finishes before the cloud hold. A refusal keeps the local source.

The service preserves external file references through independent consumer copies before deletion. Then it deletes all owned cloud content. Only after deletion completes does the selected device upload the replacement snapshot.

## Read or resume a job

```sh
lastdb cloud purge-resync status --job <job-id> --json
lastdb cloud purge-resync resume --job <job-id>
```

The job keeps the exact source after a disconnect or node restart. Resume uses that source and the same operation. A large upload renews its links and resumes from completed parts.

A failure after deletion keeps the cloud hold. It also keeps the source. It cannot resume ordinary cloud sync with old data.

## Cancel before deletion

```sh
lastdb cloud purge-resync cancel --job <job-id>
```

The service permits cancellation only before deletion. Cancel does not start a cloud hold.

If the service did not accept the hold, it records cancellation for the exact operation. A delayed start request then fails. The local device releases its write pause only after that durable receipt.

If the service accepted the hold, it retains the new control generation and proves that cloud content stays unchanged. Each device retains its local data before it acknowledges continuation. A device that missed an earlier Ready snapshot first adopts that snapshot.

## Restore without the source disk

Use a recovered identity home with the account identity and cloud configuration. That home must contain no LastStore data. Use a fresh destination.

```sh
lastdb --data-dir <recovered-identity-home> restore --remote-latest \
  --env dev --into <fresh-home> --json
```

For a completed purge, the command selects the exact Ready snapshot from the permanent cloud control. It verifies the snapshot, encryption, layout, and current cloud data generation. It removes the selected device's inherited queues before it restores the later writes.

Use these flags if the account has more than one eligible root, or if the restore must use an exact snapshot:

```sh
lastdb --data-dir <recovered-identity-home> restore --remote-latest \
  --env dev --db-hash <root-id> --manifest-sha256 <manifest-sha256> \
  --into <fresh-home> --json
```

The destination stays Cloud Off. It writes a local completion receipt only after the data flush and the final cloud control check. A held cloud generation refuses this restore.

If the first hold is cancelled, no earlier Ready snapshot exists. The same command uses the verified legacy descriptor and later writes. Each read binds the exact cancelled control and unchanged cloud data generation. A changed control refuses completion. Cloud On still requires the durable continuation receipt and cloud ack.

## Cloud On after adoption

```sh
lastdb --data-dir <account-home> cloud on
```

Cloud On requires the exact local adoption receipt and the cloud ack for the current generation. A device that needs a local replacement completes its clean restart first. The node installs its adoption check before normal cloud workers start.

A legacy account without a permanent control uses the existing primary-resume plan. Cloud On does not bypass that plan after a paused boot.

## Other devices

Other devices hold cloud sync when they observe the control file. They retain their old local data and queues outside cloud sync. They automatically adopt the exact replacement through a clean stopped-node switch. They preserve unrelated orgs and inbound subscriptions. Old queues cannot publish under the new generation.

## Service readiness

The service refuses the purge until the operator records the provider cutoff and exact account scope evidence. The service also preserves external references. All service functions and rollback versions must enforce the generation protocol. The service retains the normal deployment and safe upgrade gates.

A build result does not prove a cloud purge. Use the real command and deployed service on an independent owned account. Keep the primary account intact during product proof.

```sh
lastdb --data-dir <owned-account-home> cloud purge-resync status \
  --job <job-id> --proof-receipt <receipt-path> --json
```

The receipt requires Ready and the completed local generation switch. It binds the exact account, operation, generation, and replacement snapshot.
