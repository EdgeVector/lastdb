# Resume a cloud restore after S0

Run the same `lastdb restore --into <destination>` command after a mutation-tail failure.
The CLI reuses the installed S0 snapshot. It downloads only the mutation tail.

After S0 integrity validation and commit, the CLI writes `restore-s0-checkpoint.json` in the destination home.
The file binds the source database, manifest digest, counter, cut, and restored epoch.
It records the account public key and every verified installed chunk path, size, and digest.
It has an internal checksum and an atomic durable commit.

Before resume, the CLI validates the checkpoint against the source and local high-water marker.
It verifies each installed chunk prefix and local storage integrity.
A missing, truncated, or changed chunk prevents resume. Tail appends are allowed.
A source or destination account key change prevents resume before any destination write. Replay uses the writer frontier inside the installed snapshot.
A change to cloud latest does not change the installed snapshot identity.

Destinations from older CLI versions lack this checkpoint. Resume refuses them.
Use a fresh destination for those versions. Do not copy or fabricate a checkpoint.
An interruption before the checkpoint commit also requires a fresh destination.

This checkpoint covers S0 completion. It does not resume a partial chunk download.
Cloud restore keeps the remote write interlock active during a resumed attempt.
