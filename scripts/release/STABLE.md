# Automatic stable Homebrew releases

`stable-release.yml` runs daily at 07:45 UTC and can be started manually.
Release-flow changes also run it on main. PR updates build both native packages
and exercise the operational path briefly; PR previews cannot publish or claim
the one-hour qualification.

The publishing run freezes one main commit, the next numeric Homebrew patch
version, and the published compatible Brain, LastSecrets, and Search commits.
Missing listings use app main only to prepare bootstrap evidence. It requires
that database source's normal CI, benchmark completion, and security checks.
Both native packages must pass durable write, keyed/range read, reopen, and
crash recovery operations. The Linux package installs the three apps from the
signed registry, verifies their exact receipts, then runs their real commands
and completes at least 3600 uninterrupted seconds of health and durable
operations on a fresh generated database. Health P95 must stay at or below
two seconds. The package receives its stable numeric version before this gate;
promotion does not rebuild or relabel a nightly executable.

Search is enabled only for this dedicated app compatibility home, so its real
change delivery, indexing, and query can be proved. The disk/delete benchmarks
keep Search off, the 10 MiB corpus, and the 1.5× total-size cap.

CI exports unsigned app declarations with `lastdb app publish --prepare PATH`.
The command enforces the same clean-commit, typed-manifest, complete owned-schema,
and running-node API checks as publication. It reads no publisher key and makes
no registry write. An app owner publishes a declaration only after reviewing
the CI result. Publisher keys stay in LastSecrets, outside GitHub. Stable
promotion requires an authenticated stable-channel listing for every proved
app commit at that exact API; missing listings block publication, rather than
being treated as app successes. A changed API needs new app declarations.
If a listing is missing, CI can prepare bootstrap evidence. A publishing run
then stops before starting the hour. After the owner publishes the declarations,
retry qualification; all three real registry installs must pass first.

CI verifies retained artifact digests, expiration, package hashes, and GitHub
provenance before using the cross-repository `GH_PAT` credential. It uploads
immutable Homebrew prerelease assets, opens a formula PR, and waits for that
repository's normal merge gates. Only a merged formula PR makes the release
stable. A retry may reuse identical existing assets; it cannot replace them.
The formulas declare the OpenSSL 3 runtime dependency used by the packages.
Release notes state the proved home format and API and the need for explicit
conversion of older homes.
An already published source is skipped. Active qualifications are serialized
and are never cancelled by another main update.

The separate API 23 backfill job uses an attested historical package and the
known Brain/LastSecrets commits on another generated CI home. It repairs
missing listings without operating on the primary. It is app evidence, not
the one-hour qualification of the new stable package.

The legacy tag workflow accepts only canary tags; a numeric stable tag cannot
publish through that route. The old local candidate and soak routines remain paused. The workflow does
not use a developer home, upgrade a primary, or change its backup settings.
Summaries and retained CI artifacts are the validation record.
