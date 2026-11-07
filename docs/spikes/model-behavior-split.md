# Spike: split the LastDB models from their behavior

Date: 2026-10-30. Base: `main` at `579101853`.

## The question

Tom asked: can we split the models from how they behave? The core mixes
models, behavior, and concerns. The core is a knot of dependencies. Try a
few cuts, then pick the cut that works best.

## The short answer

Yes. Most of the models are already plain data. The knot is held by a small
number of ties. The problem is where the code lives, not what the code is.

## What the measurement shows

The tools are in `scripts/split-lab/`. Each tool reads the source. No tool
builds or runs the code. A source check is a source check: it does not prove
the runtime behavior.

### Models and behavior in `fold_db` core

`model_behavior.py` classifies each type, impl block, and file. A model is
data plus pure rules: no async, no lock, no atomic, no clock, no id
generator, no environment read, no file or network access, no global state,
and no handle to a service object. Behavior is all other code.

| Measure | Value |
|---|---|
| Files / lines | 796 / 104,002 |
| Types | 846 |
| Model types (plain data) | 717 (85%) |
| Service types (hold a lock, a task, a channel, or another service) | 129 |
| Files with no behavior | 219 files, 22,338 lines (21%) |
| Model items (types and pure functions) that sit in behavior files | 974 |
| Model types with a hidden effect (clock, id, environment, task-local) | 50 |
| Ambient task-local contexts (`tokio::task_local!`) | 10 |

The behavior sits on six service objects. Each object gets methods from
many files:

| Service object | Impl blocks | Files |
|---|---|---|
| `AtomStore` | 126 | 121 |
| `MutationManager` | 61 | 60 |
| `PackRuntime` | 40 | 40 |
| `FoldDB` | 24 | 22 |
| `SchemaStore` | 17 | 17 |
| `HashRangeQueryProcessor` | 17 | 17 |

Examples of a model type with a hidden effect:

- `Atom` and `MoleculeHashRange` read the clock when they are made.
- `Mutation::new` makes a random id.
- `KeyValue` order reads a task-local context (`schema/types/key_comparison.rs`).
- A schema field holds a private key that signs (`signer: Arc<Ed25519KeyPair>`).

### The knot

`knot_map.py` groups the file references by module and finds the cycles.
Then it finds the layer order with the fewest upward references.

| Level | Largest knot | Share of lines | Ties (upward references) |
|---|---|---|---|
| Core, top modules | 11 of 30 modules | 96% | 70 of 1,903 |
| Core, second level: storage and operations | 22 modules | 50% | 34 of 280 |
| Core, second level: database and schema core | 5 modules | 19% | not ranked |
| Core, method calls inside the service objects | 24 concept groups | 26,144 lines | 32 of 146 calls |
| Node (`lastdb_node`), top modules | 7 of 38 modules | 34% | 14 edges |

`method_knot.py` exists because a path reference does not show a method
call. A call such as `store.commit_tip_changes(..)` names no file. Inside
`AtomStore` and `PackRuntime`, 24 concept groups call each other through
`self`.

The best top-level layer order is:

`crypto < atom < resident < schema < memory_budget < storage < error <
db_operations < alternate_keys < sharing < fold_db_core`

Most upward references are models in the wrong module. Examples:

- `PerKeyRecord` and `MoleculeGeneration*` sit in `db_operations`, but `storage` uses them.
- `FILE_BLOB_CIPHER_SUITE` sits in `sharing`, but `atom` and `storage` use it.
- Storage error kinds sit in `storage`, but schema errors use them.

A few are real behavior calls that go up:

- `storage::disk_pack` calls `AtomStore`.
- `schema::core` calls `DbOperations` and the disk read code.
- `sharing::query_slice` calls `FoldDB`.

### The bottom of the core is almost free now

`closure.py` measured the pure bottom of the core: `atom`, `schema::types`,
`canonical`, `hex`, `json_order`, `constants`, `kind_partition`, and
`schema::logical_rules`. The set has 71 files and 8,037 lines. It names only
8 files outside the set, with 23 references:

- storage error kinds (9)
- `kind_partition` and `logical_rules` (6; they can move in)
- the private key type and its sign helpers (5)
- the file blob cipher name (1)
- the clock (1)
- the end-to-end key type (1)

## The cuts that we tried

| Trial | Idea | How the cut stays cut |
|---|---|---|
| T1 model crate | Move the pure models into their own crate | The compiler |
| T2 layer check | Give every module a layer; move or invert each upward reference | A lint in CI |
| T3 ports | Store concepts use narrow traits, not `AtomStore` | The compiler (a concept cannot name `AtomStore`) |
| T4 plan and execute | A write decision is a pure function that returns a plan; a shell reads and commits | A lint in CI |

Two earlier trials give more data:

- PR #842 gave the molecule drain its own owner. Its functions still take `&AtomStore`.
- PR #843 moved the warm set ownership from the store runtime to the database.

## Results

All four trials start from `main` at `519ed65fd`. Each trial is a draft PR.
CI is the compiler. `ci-required` and `benchmark-complete` pass on all four.
The speed changes are in the noise. I measured each branch again with the same
tools, so the numbers below are on equal terms.

| Measure | `main` | T1 model crate | T2 layer check | T3 ports | T4 plan and execute |
|---|---|---|---|---|---|
| PR | | #874 | #875 | #873 | #871 |
| Files changed | | 170 | 131 | 78 | 34 |
| Lines added / removed | | +1,464 / −913 | +1,520 / −636 | +2,532 / −637 | +956 / −640 |
| Core, top-level knot | 96% | 96% | 94% | 96% | 96% |
| Core, largest second-level knot | 50% | 54% (1) | 24% | 51% | 50% |
| Node, top-level knot | 34% | 34% | none | 34% | 34% |
| Node, largest second-level knot | 24% | 24% | 13% | 24% | 24% |
| Method-call knot: groups, lines | 24, 26,144 | 24, 26,145 | 13, 21,570 | 17, 16,687 (2) | 24, 26,144 |
| `impl AtomStore` blocks | 125 | 125 | 125 | 79 | 125 |
| Files that name `AtomStore` | 150 | 150 | 145 | 105 | 150 |
| Lines with no behavior | 22,338 | 23,768 (3) | 23,168 | 22,552 | 22,925 |
| Model types with a hidden effect | 50 | 46, and 0 in the crate | 49 | 57 (4) | 50 |
| What keeps the cut | | the compiler and a lint | a CI lint | the compiler | a CI lint |
| Score from the trial agent | | 6 | 6 | 6 | 5 |

Notes:

1. The share goes up because 7,330 model lines left the core crate.
2. Measured with the trial's receiver-aware tool. `method_knot.py` drops a
   method name that a concept and a wrapper both define.
3. 16,438 lines in the core and 7,330 lines in the model crate.
4. The tool counts the 8 new concept structs as model types. The 50 old
   types do not change.

### What each trial did

- **T1 model crate.** The atom, schema type, and key code is now a crate with
  no behavior: 70 files, 7,330 lines. The clock read, the id generator,
  the signature functions, and the task-local key order moved back to the core as small
  traits. The old paths still work. The main core knot did not change.
- **T2 layer check.** `scripts/ci/layer-check.py` reads a layer file for each
  crate and fails on an upward reference. The core has 11 layers, from
  `primitives` and `model` at the bottom to `surface` at the top. The node has
  7 layers. The trial moved shared records down into a new `records` module,
  moved the molecule key store into `storage`, and put the node jobs that use
  `Host` into a new `jobs` module. One call is inverted: the disk schema key
  preload takes a 5-method trait, not `DbOperations`. The node knot is gone.
  The largest core knot at the second level went from 50% to 24%.
- **T3 ports.** 8 of 10 store concepts use 23 small traits, not `AtomStore`.
  The compiler keeps them apart. The two largest concepts, the counter and the
  molecule write path, are not converted. Each one needs about 10 more traits
  on the hottest path. The trial added 69 wrappers and 1,170 lines of
  connection code, so outside callers did not change.
- **T4 plan and execute.** About 1,000 lines of write decisions became pure
  functions in a `plan` module. About half of these lines are old rule files
  that moved. No tie was cut: a write decision and its reads alternate, and
  some steps read the clock or make a random id.

### Three kinds of ties

The trials show that the knot has three kinds of ties. Each trial cuts a
different kind.

1. **Module ties.** A module uses a module above it. T2 removes all of them in
   both crates, and its lint blocks a new one.
2. **Hidden effects in models.** A model reads the clock, makes an id, or reads
   a task-local value. T1 removes them from the bottom 7,330 lines, and the
   compiler blocks a new one.
3. **Method ties through the service objects.** `impl AtomStore` is in 120
   files, so any concept can call any other concept through `self`. A layer
   file cannot see these calls. Only T3 cuts them, and the cost is high.

T4 cuts none of the three.

## Recommendation

Take the layer check (T2, #875) as the main cut.

- It cuts the most knot for each changed line: the node knot is gone, and the
  largest core knot at the second level goes from 50% to 24%.
- It covers all of the core and all of the node, not one slice.
- CI blocks a new upward reference. The rule and the reason are in one file
  that a reader can see.
- The risk is low. Most of the change moves code. One call uses a new trait.
  No code outside lastdb uses the node paths that changed.

Then use the other trials as steps under it:

1. Put the model crate (T1, #874) under the layer check as its `model` layer.
   Rebase it onto #875 first, because both trials move some of the same items.
2. Cut the method ties with ports (T3, #873) one concept at a time. Change the
   callers, not add wrappers, so that the code does not grow. Start with the
   counter and the molecule write path, because they hold the ties that stay.
3. Close T4 (#871). Its pure decisions do not cut a tie.
4. Keep the earlier trials #842 and #843 as drafts until step 2.

### What this spike does not prove

The tools read the source. They do not prove the runtime behavior. CI proves
that each branch compiles and passes clippy and the lints. The benchmarks are
advisory. Before a merge, check the open PRs for names that a trial moved,
because `main` does not require a PR to be current.
