"""Constants and field lists for the admin Kanban deliver."""

from __future__ import annotations



# BoardCards payload fields (fkanban src/schemas.ts). Snapshot legs may only
# project this set. `milestone` is omitted from the default snapshot projection
# because a missing atom silently drops the whole row.
BOARD_CARDS_FIELDS = (
    "board",
    "sk",
    "slug",
    "title",
    "column",
    "position",
    "assignee",
    "tags",
    "deps",
    "surfaces",
    "created_at",
    "created_by",
    "updated_at",
    "db",
    "repo",
    "base",
    "kind",
    "block_status",
    "block_reason",
    "north_star",
    "milestone",
    "pr_url",
    "branch",
    "layout",
)
BOARD_CARDS_FIELD_SET = set(BOARD_CARDS_FIELDS)

# Slim snapshot / factory projection. `slug` leads so a missing trailing atom
# does not drop the row. No `body`. No `milestone` (gating).
DEFAULT_FIELDS = [
    "slug",
    "title",
    "column",
    "position",
    "tags",
    "updated_at",
    "board",
    "assignee",
    "repo",
    "kind",
    "north_star",
    "pr_url",
]

# Extra fields for the admin Kanban Factory theater (assignee → Active Hands).
# Keep bodies out — sealed Exemem messages are ~64KB capped.
FACTORY_FIELDS = DEFAULT_FIELDS + [
    "block_status",
    "branch",
]

DEFAULT_SCHEMA_NAME = "fkanban/Card"
DEFAULT_BOARD_CARDS_SCHEMA_NAME = "fkanban/BoardCards"
DEFAULT_BOARD = "default"
DEFAULT_SINCE_FIELD = "updated_at"
DEFAULT_ORDER_BY = "updated_at"
# Full-board default (Mini gzip-before-JWE, 2026-07-17). Exemem still caps
# sealed blobs at ~64KB; gzip of slim Card fields holds a whole board.
DEFAULT_MAX_RECORDS = 2000
DEFAULT_MAX_RECORDS_PER_COLUMN = 2000
DEFAULT_COLUMNS = ["backlog", "todo", "doing", "done"]
# "all" / empty / "none" → no time window (deliver entire columns).
DEFAULT_SINCE = "all"
# Empirically: ~60 molecule-signed cards seal under Exemem's 87382 base64
# cap after gzip-before-JWE (2026-07-17 full-board probe). Stay under that
# by chunking stage+approve into multiple mailbox messages; admin merges.
DEFAULT_SEAL_SAFE_RECORDS = 55

# Factory profile: full board + theater fields. Same high caps as default after
# Mini compress-before-encrypt; no last-day filter.
FACTORY_SINCE = "all"
FACTORY_MAX_BY_COLUMN = {
    "doing": 2000,
    "todo": 2000,
    "backlog": 2000,
    "done": 2000,
}
