# A normal backup without its local predecessor

A copied home can lose its local backup manifest but keep its durable store identity.
Cloud can still hold a packed backup for that same database.
A normal snapshot must keep that predecessor. It cannot publish an empty history.

The normal publisher recovers the predecessor only when its local predecessor is absent.
It uses the authenticated database scope and the existing durable store UUID and epoch.
It reads cloud latest, downloads the full manifest chain, and checks every digest and chain step.
It checks cloud latest again before it writes the verified tip to the local manifest mirror.
The next normal cut uses that exact predecessor.

The mirror records a backup that already reached cloud CAS.
Recovery does not record a new local backup commit or grant cloud deletion authority.
The normal cut still checks the local bytes, mutation frontier, history, packs, and retention receipts.
A missing or corrupt ancestor, wrong source, changed latest, or local mirror error stops the snapshot.
The backup-only and primary-resume paths stay unchanged.

Each manifest download keeps the existing 16 MiB cap.
The full chain has no separate count or total-byte cap.
The manual snapshot keeps its normal request deadline.
The continuous publisher has no aggregate chain deadline. This change adds no timeout.
The publication turn excludes this process's publisher and its publication GC.
It does not exclude another device. Recovery does not add an atomic remote parent-digest condition.
