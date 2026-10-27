"""A primary-shaped seed for the disk format v2 node proof.

The schemas follow the brain app on the primary (workspace design
`docs/design-lastdb-disk-size-20261024.md`, section 2.4): list pages, list
locators, token sets, edge sets, a type directory, and content records. Each
content put also writes its index records, as the brain does, so most atoms
are small and the list pages change often. Bodies are markdown text of up to
64 KiB. The model keeps the exact fields that each key must read back, and
the keys that a delete removed. One seed number always gives the same data.
"""

import copy
import hashlib
import json
import math
import random
import time

# Mutations in one request. A key occurs at most once in a request.
BATCH = 64
PAGE_ENTRIES = 20
TOKEN_SLUGS = 40
BODY_MEDIAN = 420
BODY_MAX = 64 * 1024
KINDS = ("papercut", "decision", "reference", "design", "sop", "concept",
         "project", "spike", "task", "preference", "agent", "closeout")
RELATIONS = ("links", "supersedes", "blocks", "cites", "fixes")
STATUSES = ("open", "fixed", "verified", "wontfix", "active", "archived")
UNICODE_WORDS = ("décision", "naïve", "über", "façade", "smörgåsbord", "日本語", "データ",
                 "één", "ça", "emoji🙂", "€uro", "Ωmega", "straße", "café", "jalapeño")

# Schema name: (hash field, range field, fields).
SCHEMAS = {
    "ProofRecord": ("kind", "slug", ("kind", "slug", "title", "body", "tags", "status",
                                     "created_at", "updated_at", "source", "digest",
                                     "revision")),
    "ProofListPage": ("list", "page", ("list", "page", "entries", "count", "next_page")),
    "ProofListLocator": ("list", "slug", ("list", "slug", "page", "position")),
    "ProofTokenSet": ("token", "bucket", ("token", "bucket", "slugs", "weight")),
    "ProofEdgeSet": ("source", "edge", ("source", "edge", "target", "relation", "weight",
                                        "created_at")),
    "ProofTypeDirectory": ("kind", "slug", ("kind", "slug", "title", "updated_at")),
}


def schema_request(name):
    hash_field, range_field, fields = SCHEMAS[name]
    return {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": name, "descriptive_name": name,
        "schema_type": "HashRange",
        "key": {"hash_field": hash_field, "range_field": range_field},
        "fields": list(fields),
        "field_types": {hash_field: "String", range_field: "String"},
        "field_descriptions": {field: f"disk v2 proof {field}" for field in fields}}}


def batch_request(batch):
    return {"mutations": [
        {"type": "mutation", "schema": schema, "mutation_type": operation,
         "durability": "durable", "key_value": {"hash": key[0], "range": key[1]},
         "fields_and_values": fields}
        for operation, schema, key, fields in batch]}


def query_request(schema, keys):
    return {"schema_name": schema, "fields": list(SCHEMAS[schema][2]),
            "filter": {"HashRangeKeys": [list(key) for key in keys]}, "limit": len(keys)}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def page_name(number):
    return f"{number:06d}"


def vocabulary(rng, size=3000):
    onsets = ("", "b", "br", "c", "ch", "cl", "d", "dr", "f", "fl", "g", "gr", "h", "j", "k",
              "l", "m", "n", "p", "pl", "pr", "qu", "r", "s", "sh", "sl", "st", "t", "th",
              "tr", "v", "w", "z")
    vowels = ("a", "e", "i", "o", "u", "ai", "ea", "ee", "io", "ou", "oa", "y")
    codas = ("", "", "n", "r", "s", "t", "l", "m", "nd", "st", "ck", "ng", "x")
    words = set(UNICODE_WORDS)
    while len(words) < size:
        syllables = rng.choice((1, 1, 2, 2, 2, 3, 3, 4))
        word = "".join(rng.choice(onsets) + rng.choice(vowels) + rng.choice(codas)
                       for _ in range(syllables))
        if 2 <= len(word) <= 14:
            words.add(word)
    # Sorted: a set of strings has no fixed order between runs.
    ordered = sorted(words)
    rng.shuffle(ordered)
    return ordered


class Seed:
    """The generator and the model of one home."""

    def __init__(self, number):
        self.rng = random.Random(number)
        self.words = vocabulary(self.rng)
        self.records = {}     # (schema, hash, range) -> fields
        self.deleted = set()  # (schema, hash, range) that a delete removed
        self.live = []        # (kind, slug) of each live content record
        self.dead = []        # (kind, slug) of each deleted content record
        self.slugs = set()
        self.pages = {}       # kind -> (current page number, its entries)
        self.page_of = {}     # slug -> (kind, page number)
        self.entries = {}     # (kind, page number) -> entries
        self.tokens = {}      # token -> (bucket number, slugs)
        self.clock = 1_729_000_000.0
        self.pending = []
        self.counts = {"mutations": 0, "creates": 0, "updates": 0, "deletes": 0,
                       "field_values": 0, "value_bytes": 0}

    def put(self, operation, schema, key, fields=None):
        record = (schema,) + key
        fields = copy.deepcopy(fields or {})
        if operation == "delete":
            del self.records[record]
            self.deleted.add(record)
        elif operation == "create":
            self.records[record] = copy.deepcopy(fields)
            self.deleted.discard(record)
        else:
            self.records[record].update(copy.deepcopy(fields))
        self.pending.append((operation, schema, key, fields))
        self.counts["mutations"] += 1
        self.counts[operation + "s"] += 1
        self.counts["field_values"] += len(fields)
        self.counts["value_bytes"] += sum(len(canonical(value)) for value in fields.values())

    def phase(self, base=0, creates=0, updates=0, deletes=0, recreates=0):
        """The request batches of one phase: `base` creates first, then the
        other actions in a random order."""
        for _ in range(base):
            self.create()
        actions = (["create"] * creates + ["update"] * updates + ["delete"] * deletes
                   + ["recreate"] * recreates)
        self.rng.shuffle(actions)
        for action in actions:
            if action == "recreate" and self.dead:
                self.recreate()
            elif action == "update" and self.live:
                self.update()
            elif action == "delete" and len(self.live) > 1:
                self.delete()
            else:
                self.create()
        return self.batches()

    def batches(self):
        out, batch, keys = [], [], set()
        for operation in self.pending:
            record = (operation[1],) + operation[2]
            if record in keys or len(batch) == BATCH:
                out.append(batch)
                batch, keys = [], set()
            batch.append(operation)
            keys.add(record)
        if batch:
            out.append(batch)
        self.pending = []
        return out

    def expected(self):
        """Each key to read: its fields, or None for a key that a delete removed."""
        keys = dict(self.records)
        for record in self.deleted:
            keys[record] = None
        return keys

    def create(self, kind=None, slug=None):
        kind = kind or self.rng.choice(KINDS)
        slug = slug or self.slug(kind)
        title = self.sentence(3, 9)
        body = self.body(title)
        stamp, status = self.stamp(), self.rng.choice(STATUSES)
        self.put("create", "ProofRecord", (kind, slug), {
            "kind": kind, "slug": slug, "title": title, "body": body,
            "tags": [self.word() for _ in range(self.rng.randrange(7))], "status": status,
            "created_at": stamp, "updated_at": stamp, "source": self.uuid(),
            "digest": hashlib.sha256(body.encode()).hexdigest(), "revision": 1})
        self.put("create", "ProofTypeDirectory", (kind, slug),
                 {"kind": kind, "slug": slug, "title": title, "updated_at": stamp})
        page, position = self.add_entry(kind, slug, title, stamp, status)
        listing = f"list:{kind}"
        self.put("create", "ProofListLocator", (listing, slug),
                 {"list": listing, "slug": slug, "page": page, "position": position})
        for token in self.rng.sample(title.split(), 2):
            self.add_token(token.lower(), slug)
        if self.live and self.rng.random() < 0.4:
            self.add_edge(slug)
        self.live.append((kind, slug))

    def update(self):
        kind, slug = self.rng.choice(self.live)
        record = self.records[("ProofRecord", kind, slug)]
        stamp, roll = self.stamp(), self.rng.random()
        change = {"updated_at": stamp, "revision": record["revision"] + 1}
        if roll < 0.6:
            body = self.body(record["title"])
            change.update(body=body, digest=hashlib.sha256(body.encode()).hexdigest())
        if roll > 0.3:
            change["status"] = self.rng.choice(STATUSES)
        if roll > 0.85:
            change["title"] = self.sentence(3, 9)
        self.put("update", "ProofRecord", (kind, slug), change)
        title, status = record["title"], record["status"]
        self.put("update", "ProofTypeDirectory", (kind, slug),
                 {"title": title, "updated_at": stamp})
        page_kind, number = self.page_of[slug]
        entries = self.entries[(page_kind, number)]
        for entry in entries:
            if entry["slug"] == slug:
                entry.update(title=title, updated_at=stamp, status=status)
        self.put("update", "ProofListPage", (f"list:{kind}", page_name(number)),
                 {"entries": entries})

    def delete(self):
        kind, slug = self.live.pop(self.rng.randrange(len(self.live)))
        listing = f"list:{kind}"
        self.put("delete", "ProofRecord", (kind, slug))
        self.put("delete", "ProofTypeDirectory", (kind, slug))
        self.put("delete", "ProofListLocator", (listing, slug))
        page_kind, number = self.page_of.pop(slug)
        entries = self.entries[(page_kind, number)]
        entries[:] = [entry for entry in entries if entry["slug"] != slug]
        self.put("update", "ProofListPage", (listing, page_name(number)),
                 {"entries": entries, "count": len(entries)})
        self.dead.append((kind, slug))

    def recreate(self):
        kind, slug = self.dead.pop(self.rng.randrange(len(self.dead)))
        self.create(kind, slug)

    def add_entry(self, kind, slug, title, stamp, status):
        listing = f"list:{kind}"
        number, entries = self.pages.get(kind, (None, None))
        entry = {"slug": slug, "title": title, "updated_at": stamp, "status": status}
        if entries is None or len(entries) >= PAGE_ENTRIES:
            if entries is not None:
                self.put("update", "ProofListPage", (listing, page_name(number)),
                         {"next_page": page_name(number + 1)})
            number = 0 if number is None else number + 1
            entries = [entry]
            self.put("create", "ProofListPage", (listing, page_name(number)), {
                "list": listing, "page": page_name(number), "entries": entries, "count": 1,
                "next_page": None})
        else:
            entries.append(entry)
            self.put("update", "ProofListPage", (listing, page_name(number)),
                     {"entries": entries, "count": len(entries)})
        self.pages[kind] = (number, entries)
        self.entries[(kind, number)] = entries
        self.page_of[slug] = (kind, number)
        return page_name(number), len(entries) - 1

    def add_token(self, token, slug):
        bucket, slugs = self.tokens.get(token, (0, None))
        weight = round(self.rng.random(), 4)
        if slugs is None or len(slugs) >= TOKEN_SLUGS:
            bucket = 0 if slugs is None else bucket + 1
            slugs = [slug]
            self.put("create", "ProofTokenSet", (token, f"{bucket:02d}"),
                     {"token": token, "bucket": f"{bucket:02d}", "slugs": slugs,
                      "weight": weight})
        else:
            slugs.append(slug)
            self.put("update", "ProofTokenSet", (token, f"{bucket:02d}"),
                     {"slugs": slugs, "weight": weight})
        self.tokens[token] = (bucket, slugs)

    def add_edge(self, slug):
        target = self.rng.choice(self.live)[1]
        relation = self.rng.choice(RELATIONS)
        key = (slug, f"{relation}#{target}")
        if ("ProofEdgeSet",) + key in self.records:
            return
        self.put("create", "ProofEdgeSet", key, {
            "source": slug, "edge": key[1], "target": target, "relation": relation,
            "weight": self.rng.randrange(-5, 1 << 40), "created_at": self.stamp()})

    def word(self):
        # Low indexes come often, as in real text.
        return self.words[int(len(self.words) * self.rng.random() ** 2.2)]

    def sentence(self, low, high):
        text = " ".join(self.word() for _ in range(self.rng.randint(low, high)))
        return text[:1].upper() + text[1:]

    def slug(self, kind):
        while True:
            slug = f"{kind}-{self.word()}-{self.word()}-{self.stamp()[:10].replace('-', '')}"
            if slug not in self.slugs:
                self.slugs.add(slug)
                return slug

    def body(self, title):
        size = int(math.exp(self.rng.gauss(math.log(BODY_MEDIAN), 1.1)))
        size = min(BODY_MAX, max(40, size))
        lines = [f"# {title}", ""]
        length = len(lines[0])
        while length < size:
            roll = self.rng.random()
            if roll < 0.55:
                line = " ".join(self.sentence(6, 18) + "." for _ in range(self.rng.randint(1, 4)))
            elif roll < 0.8:
                line = f"- {self.sentence(3, 10)} `{self.word()}_{self.word()}`"
            elif roll < 0.9:
                target = self.rng.choice(self.live)[1] if self.live else self.word()
                line = f"See [{target}]({target}) on {self.stamp()[:10]}."
            elif roll < 0.95:
                line = f"## {self.sentence(2, 5)}"
            else:
                line = f"Digest `{self.hex(16)}` from run {self.rng.randrange(10**9, 10**11)}."
            lines.append(line)
            length += len(line) + 1
        return "\n".join(lines)[:BODY_MAX]

    def stamp(self):
        """A UTC timestamp with 0, 3, 6, or 9 fraction digits."""
        self.clock += self.rng.expovariate(1 / 90)
        text = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(int(self.clock)))
        digits = self.rng.choice((0, 3, 6, 9))
        if digits:
            text += "." + str(self.rng.randrange(10 ** digits)).zfill(digits)
        return text + "Z"

    def uuid(self):
        digits = f"{self.rng.getrandbits(128):032x}"
        return f"{digits[:8]}-{digits[8:12]}-{digits[12:16]}-{digits[16:20]}-{digits[20:]}"

    def hex(self, size):
        return f"{self.rng.getrandbits(8 * size):0{2 * size}x}"
