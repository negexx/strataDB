# How Strata Works — an in-depth guide

> A conceptual explanation of what Strata is, how every part of it works, what exists today, and what
> it is going to become. Written in plain prose — no code, no file references — so it stands on its
> own. For the terse module map see `architecture.md`; for the reasoning behind specific tradeoffs see
> the decision records; for the future scope see `scope-addendum-v2.md`.

---

## 1. What Strata is, in one breath

Strata is an embedded, single-node database engine that lets many AI agents read and write the same
store at the same time, with real transactional guarantees, over both structured columns and vector
embeddings held in one unified format. "Embedded" means it runs inside your process like a library,
not as a separate server. "Single-node" means it lives on one machine's disk, not a distributed
cluster. "Real transactional guarantees" means the hard promises a serious database makes — no lost
updates, no half-visible writes, no reader ever seeing a torn state — and Strata extends those
promises to cover the vector index, which almost no vector store does.

The one-sentence thesis has two halves. The first, present since the beginning: *correct under
concurrent multi-agent writes, with no silent buffering.* The second, added once branching became a
requirement: *the storage engine an agent can fork.*

---

## 2. Why it exists — the gap it fills

Two mature worlds sit on either side of Strata, and neither covers the middle.

On one side, relational databases have spent fifty years perfecting transactions, isolation, and
crash recovery — but they treat vector search as an afterthought bolted on through an extension, and
that extension's index typically lives outside the transaction boundary. On the other side, dedicated
vector stores have excellent approximate-nearest-neighbour search — but they treat consistency as a
nice-to-have, buffer writes for throughput, and rebuild their index whenever the underlying data
branches.

An agent workload lands squarely in the gap. An agent writes a fact, invalidates a contradicting one,
updates the vector index so the new fact is findable, and updates any derived structure — and if those
steps are not one atomic unit, the agent's memory silently corrupts: it "remembers something it
shouldn't have" or "forgot what you just told it." Worse, agents fork reality constantly — they try a
line of reasoning in a sandbox, and abandon it far more often than they keep it. Measurements from
production systems show agents branching and rolling back an order of magnitude more than humans do.
No engine today lets you fork a *vector index* cheaply, because nobody put the vector index inside the
transaction engine where forking becomes a manifest copy instead of an index rebuild.

Strata's whole reason to exist is to close that gap: put the vector index inside the transactional
core, make every multi-structure write atomic, and store everything on a layout that can be forked.

---

## 3. The mental model — a stack of layers, each with one job

The cleanest way to hold Strata in your head is as a vertical stack. Every read and every write from
the top enters at the top and passes down through every layer; nothing bypasses the transaction layer
to touch storage directly. From top to bottom:

1. **The client surface.** How you talk to Strata: a query and filter interface, a data-loading
   interface for feeding training loops, a command-line tool for inspecting a store, and language
   bindings so non-native code can drive it.

2. **The execution layer.** Three sibling engines that do the actual work of a read: the query
   executor that scans, filters, and aggregates columns; the vector index that answers
   nearest-neighbour questions; and a direct random-access reader for pulling specific rows.

3. **The transaction and conflict-resolution layer.** The flagship. Every write is proposed here,
   checked for conflicts here, and only from here published. This is the layer that makes the whole
   system's promises true, and it is the reason the project chose the language it did.

4. **The manifest and version layer.** The single source of truth for "what is the current state of
   the dataset." It is a versioned list describing which files make up the store right now. A commit,
   at bottom, is nothing more than atomically changing which version of this list is the current one.

5. **The columnar storage layer.** The bytes on disk: append-only files, each holding a batch of rows
   in a column-oriented format, written once and never modified afterward.

The discipline that makes everything else possible is that layer three is not optional. There is no
back door from the execution layer to the storage layer. If you want to change data, you go through
the transaction layer, and the transaction layer decides whether your change is allowed, makes it
durable, and only then makes it visible.

---

## 4. How storage works — append-only, immutable, and why that matters

At the very bottom, data lives in files that are written exactly once and never changed. When you add
rows, Strata writes a brand-new file containing them; it never opens an old file to edit it in place.
Each file stores its rows column-by-column rather than row-by-row, which means a query that only cares
about two columns out of twenty reads only those two columns' worth of bytes, and values within a
column — being all the same type and often repetitive — compress far better than mixed row data would.

Two properties fall out of "write once, never modify," and both are load-bearing:

- **Crash safety becomes almost free.** A file that is never edited cannot be left half-edited by a
  power loss. Either the whole file is there and complete, or it isn't there yet — there is no torn
  middle state to reason about.

- **Old versions stay valid for free.** Because a file is never overwritten, a reader who started
  looking at the store a moment ago still sees exactly the files that existed then, even as new files
  are being written alongside. Point-in-time consistency is the natural state, not something bolted on.

The cost of never modifying files is that deletions and updates can't happen in place. Strata handles
this with *tombstones* — small markers that say "row such-and-such is no longer live" — and defers the
actual reclamation of the bytes to a later background process. This is a deliberate trade: cheap,
safe writes now, in exchange for a housekeeping job later.

---

## 5. How versioning and durability work — the atomic swap

If the files themselves never change, then "the current state of the database" cannot be a property of
any one file. It has to be a separate thing that says *which* files, together, make up the store right
now. That separate thing is the manifest: a small versioned document listing every live data file,
the statistics needed to prune them during queries, and the bookkeeping the transaction layer needs.

Publishing a change is a carefully ordered sequence, and the order is the whole point:

1. First, write the new data files and make them durable — force them all the way to the physical disk
   before proceeding. At this moment the new data exists on disk but is invisible: no manifest points
   at it, so no reader can find it.

2. Then, write a new version of the manifest — one number higher than the last — that includes the new
   files, and make *it* durable too.

3. Finally, publish that new manifest version as the current one with a single atomic operation. The
   filesystem guarantees this step happens completely or not at all; there is no in-between where half
   the switch has occurred.

Until that final atomic step, nothing about the write is visible to anyone. This is the meaning of the
project's iron rule: *no write is acknowledged until it is durable, conflict-checked, and visible —
and nothing is ever buffered for later.* Strata never tells a caller "your write succeeded" while the
data still sits in memory waiting to be flushed. The acknowledgement and the durability are the same
moment. That rule costs throughput — every commit waits on a physical disk sync — but it is a
correctness guarantee, not a tunable, because an agent that is told its memory was saved and then
loses it on a crash is worse than useless.

To find the current state when a process starts, Strata looks at the set of manifest versions and
takes the highest-numbered one. Recovery, then, is simply reading that manifest and trusting the files
it lists — which are, by construction, exactly the ones that were durably committed.

---

## 6. How transactions and concurrency work — optimism, then a check

Many agents write at once, and they must not corrupt each other. Strata coordinates them with
*optimistic* concurrency control, which is the right choice when genuine conflicts are rare — as they
are when different agents mostly touch different data.

The shape of a transaction is: read, work, then check-and-publish.

- **Read.** When a transaction begins, it notes the manifest version that is current at that instant.
  That version is its private, frozen view of the world; everything it reads comes from that version,
  even if other transactions publish newer versions while it runs.

- **Work.** It prepares its changes — new rows, deletions, index updates — entirely off to the side,
  touching nothing shared. Crucially, the expensive part of this work, writing and syncing the new
  data files, happens *before* any coordination, so agents doing unrelated work never wait on each
  other for it.

- **Check and publish.** Only at the final moment does the transaction take a turn in a single
  serialized line. It asks one question: *since the version I read, did any committed transaction
  change something I also changed?* If the answer is no, it is clean — it publishes its new manifest
  version and steps out of line. If the answer is yes, it has conflicted, and instead of guessing or
  silently overwriting, it fails with a typed error that names exactly which rows were contested, so
  the caller can decide what to do.

The conflict check needs to know what recent commits touched. Strata keeps a bounded, rolling record
of the most recent commits and the set of rows each one changed. A committing transaction compares its
own change-set against every commit that landed between its read point and now. If a transaction ran
so long that the relevant history has already scrolled out of that bounded record, Strata cannot prove
the transaction is clean, so it conservatively treats it as a conflict rather than risk a false
"clean" — a safe-by-default choice.

The isolation level this provides is *snapshot isolation*: every transaction sees a consistent
snapshot and two transactions that touch different rows never spuriously conflict. Strata deliberately
does not provide the stronger guarantee of full serializability. Serializability on a mutable vector
index is a research-grade problem, and snapshot isolation covers the real use cases; the stronger
guarantee is an explicit, documented cut, not an oversight.

Only the final check-and-publish step is serialized. Everything expensive — preparing data, writing
files, syncing them — happens in parallel outside the line. This is why the design can coordinate many
concurrent writers without their throughput collapsing to single-file.

---

## 7. How reads stay consistent — snapshots that never block

A reader in Strata gets a *snapshot*: an immutable, point-in-time view of the whole store. Taking a
snapshot is nearly free — it captures which manifest version is current, a marker for how far the
row-numbering had advanced at that moment, and the set of deletions in effect — and then the reader
holds that view for as long as it likes. Because data files are never modified and old manifest
versions remain valid, a snapshot keeps showing exactly the state it captured, even as writers publish
newer versions right alongside it.

Determining whether a given row is visible to a snapshot is a quick two-part test: the row's identity
must fall at or below the snapshot's advancement marker (meaning it existed as of that snapshot), and
it must not appear in that snapshot's set of deletions. There is a third, subtler part that closes a
race between a writer that has reserved a row's identity but not yet finished publishing it and a
concurrent reader — the reader must not see such an in-flight row — and getting that exactly right,
under every possible thread interleaving, is precisely the kind of thing the project's correctness
tooling exists to verify.

The headline property is that readers never block writers and writers never block readers. A query in
flight is completely unaffected by commits happening at the same time; it finishes against the world
as it was when it started.

---

## 8. How the vector index fits inside all of this — the central bet

Here is the architectural decision that everything else pays off from: the vector index is not a
separate system updated after the fact. It shares the exact same transaction boundary as the row data.
When a transaction writes a row and updates the index, both land together or neither does.

The mechanism that makes this possible is to represent index changes not as edits to a live graph but
as an *append-only record of what changed* — a running log saying "this vector was added," "this one
was removed." Because that record is append-only and travels with the data files, it can be made
durable and published in the very same atomic step as the row data. A crash or a conflict in the
middle of a transaction leaves neither the row nor the index change behind. That single guarantee —
row and index committing as one unit — is the thing memory systems built on ordinary vector stores
cannot get, and it is why they exhibit the two dominant failure modes of the category.

The index itself is a *hierarchical navigable small-world graph*, the standard high-quality structure
for approximate nearest-neighbour search. Picture a stack of layers: the top layer is a sparse graph
with a few long-range links, the bottom layer is a dense graph connecting every vector to its nearby
neighbours, and the layers in between interpolate. A search drops in at the top, greedily walks toward
the query through the sparse long-range links to get into the right neighbourhood fast, then descends
and does a careful local exploration at the bottom to collect the true nearest results. It is
approximate — it can occasionally miss a true neighbour — but the accuracy is tunable, and the payoff
is that it answers in microseconds what an exhaustive scan would take milliseconds to answer.

Filtering interacts with this in a genuinely hard way. If a query says "find the nearest vectors,
*but only among rows matching this predicate*," you cannot simply search and then filter the results,
because the nearest overall might all be filtered out, leaving you with too few answers. Strata
instead resolves which rows match the predicate first, then constrains the graph traversal so it only
counts matching rows as it explores — the filter is applied *during* the search, not after it, so the
result set is filled from genuinely-matching neighbours however deep in the graph they sit. This is
one of the parts of the system where the honest engineering cost is largest, and where the future
direction (segments plus per-segment metadata) does the most good.

There is one important consequence of the current design worth naming plainly. Today the index is a
single graph held in memory, and it is reconstructed from the append-only change record every time a
process opens the store. For a large store that reconstruction is the single most expensive operation
in the whole system — it can dominate startup entirely. That cost is a direct consequence of the
"one monolithic graph" shape, and eliminating it is one of the strongest reasons for the future
layout described later.

---

## 9. How queries execute — batches, pruning, and hashing

Reads that aren't vector searches go through a vectorized query executor: it works on batches of many
rows at a time rather than row-by-row, which lets the underlying column operations run at memory
speed instead of paying per-row overhead.

Two ideas make filtered scans fast. First, *pruning*: every data file carries small summary
statistics — the minimum and maximum value of each column it contains. Before reading a file at all,
a query with a range predicate checks those summaries and skips any file that provably cannot contain
a matching row. A well-partitioned query touches a fraction of the files. Second, *pushdown*: the
filter is applied as close to the raw data as possible, so rows that don't match are discarded before
any further work is spent on them.

Aggregations like grouping and summing use hash aggregation: rows are bucketed by their grouping key
through a hash table, and each bucket accumulates its running totals. The whole thing is arranged to
touch each value once and to avoid allocating per-row scratch, so it scales to tens of millions of
rows comfortably.

---

## 10. How correctness is actually proven — the reason for Rust

A concurrent storage engine that is merely "tested" is not trustworthy, because the bugs live in rare
thread interleavings that ordinary tests almost never hit. Strata attacks this on two fronts.

First, *exhaustive interleaving testing* of the concurrency primitives. For the small, sharp pieces of
code where threads coordinate through shared memory — the atomic publish of a new version, the
lock-free growth of the index's internal tables, the compare-and-set loops — a specialized tool
enumerates *every* possible ordering of the threads' memory operations and checks that the invariants
hold in all of them. This is not sampling; it is a proof over the model. The borrow checker already
rules out data races in safe code, but it does not prove that the concurrency *logic* is correct — and
this is the tool that does. That capability is the original reason the project is written in the
language it is.

Second, a *chaos harness* in the style of the well-known distributed-systems testing methodology.
Rather than trusting that recovery works, it spawns real processes, has them do real concurrent work,
and kills them abruptly at instrumented checkpoints — precisely at the delicate moments around making
a write durable — then restarts and verifies that the store came back to a consistent, uncorrupted
state showing exactly the last durably-committed version. The kill points are seed-reproducible, so a
failure can be replayed deterministically. Thousands of randomized runs with zero invariant violations
is the bar.

It is worth being candid about what "the tests pass" means here. This machinery is exactly what has,
in recent work, surfaced serious correctness bugs that ordinary tests missed — a failed commit whose
index changes lingered and became findable, and a race that could render a store permanently
unopenable. Finding those is the machinery working as intended. But it also means the flagship
correctness guarantee is being established the hard way, through continued adversarial testing, rather
than being a box that was checked once and closed.

---

## 11. What exists today

Working through the layers described above, this much is built and passing its tests:

- The **transaction model and file format** are specified and reviewed.
- The **single-writer vertical slice** works end to end: create a store, insert, scan, filter, do an
  exact nearest-neighbour search, and recover the last committed version after an abrupt kill.
- The **columnar core** — real encodings, batch-at-a-time scan, filter, projection, and grouped
  aggregation over tens of millions of rows — is in place.
- The **query refinements** — predicate pushdown and file pruning via column statistics — work, and
  can prove which files a filtered query skips.
- The **vector index** — the navigable small-world graph, its search, and filtered search over a real
  public embedding dataset — is built and its accuracy is measured.
- **Snapshot isolation** — immutable point-in-time reads that never block writers — holds.
- The **concurrent multi-agent write engine**, the flagship — optimistic conflict detection, typed
  conflict errors, atomic row-plus-index commits, and no-buffering durability — is built and passing,
  and is the current frontier of active correctness hardening.
- The **correctness harness** — the exhaustive interleaving tests and the process-killing chaos
  suite — exists and runs green.

What is *not* yet built: a time-travel read interface for querying the store as of an older version; a
background compaction process to reclaim the space held by tombstoned rows and to shrink the
ever-growing index; a data-loading path optimized for feeding training loops; an object-storage
backend for running against cloud storage instead of local disk; and full language bindings beyond a
stub. And, most consequentially, the entire future layout and the branching capability described next.

There are also a few smaller absences that the future direction specifically needs and that do not
exist today: the filter language expresses only a single simple condition, with no way to combine
conditions with "and"/"or"; there is no built-in notion of a per-row timestamp; and deletions are only
ever soft — the underlying bytes and index nodes are never physically removed, only marked, and grow
without bound until a compaction process that has not been built.

---

## 12. Where it is going — the future architecture

Everything up to here describes a working engine. This section describes what it is deliberately
becoming, and — importantly — *how* each piece works, because the "how" is what makes the ambitious
end state credible rather than aspirational.

### 12.1 The pivot: from one big graph to many small immutable ones

The single most important future change is the shape of the vector index's storage. Today it is one
large mutable graph. It will become a collection of immutable *segments* — self-contained little index
files — plus a manifest that lists them, exactly mirroring how row data is already stored.

The mechanism: a write no longer rewires a shared graph; it produces a brand-new small segment
containing just the new vectors and appends it to the list. A search asks *each* segment for its own
nearest neighbours and merges the answers into a global result. In the background, a compaction
process periodically merges many small segments into fewer large ones, keeping the number of segments
a search must consult under control.

This one change cascades into four benefits, each of which is a current weakness:

- **Recovery stops being a rebuild.** Opening the store becomes reading a list of segment files, not
  reconstructing a graph from scratch — turning the system's most expensive operation into a cheap one.
- **Snapshot isolation gets simpler and sturdier.** Immutable segments compose naturally with the
  same versioned-manifest, atomic-swap machinery the row data already uses, instead of the more
  delicate shared-mutable-graph coordination the current design leans on.
- **Real deletion becomes possible.** Physically removing a vector is a matter of compaction rewriting
  a segment without it — which is also what makes *provable* deletion, the kind regulations are
  starting to require, tractable at all.
- **And, decisively, forking becomes cheap.** Which is the whole point of the next piece.

The obvious worry with fanning a search across many segments is that accuracy might degrade as
segments accumulate. That worry was measured directly before committing to the layout, by partitioning
a real embedding set into growing numbers of segments and comparing the merged-search accuracy against
a single graph over the same data. The result was reassuring in the strongest possible way: accuracy
did *not* degrade with segment count — the cost showed up purely as added latency, roughly proportional
to the number of segments. That distinction matters enormously. It means a background compactor that
falls behind makes queries *slower* but never *wrong* — so compaction is a performance knob, not a
correctness requirement. Had accuracy collapsed instead, a lagging compactor would have silently
returned worse answers, which is a far more dangerous failure mode. The de-risking experiment turned
the layout from a bet into a measured decision.

### 12.2 Per-segment zone maps — making filtered and temporal queries fast

Given segments, a small companion primitive pays for itself many times over. Because segments are
written in time order, each one naturally covers a contiguous slice of history; recording the minimum
and maximum of a timestamp (and any other low-cardinality filter column) for each segment lets a query
skip *whole segments* before touching a single vector. "What did we know as of last December" becomes
"consult only the segments whose time range overlaps December" — segment pruning, the same idea that
makes analytics engines fast, applied to vector segments.

This is nearly free once segments exist and impossible without them, because a single monolithic graph
gives you nowhere to attach the metadata. It is firmly a *storage* primitive — it makes temporal
*filtering* fast — and it stops deliberately short of being a temporal *data model*; deciding what a
fact *means* over time is someone else's job, not the storage engine's.

### 12.3 Branching — the differentiator

This is the capability the layout change exists to enable, and the sharpest statement of what Strata is
for. Because segments are immutable and shared, and the manifest is just a small list pointing at them,
*forking the entire store is copying that small list*. The new branch shares every existing segment
with its parent at zero copying cost; writes on the branch produce new segments visible only to that
branch. That is a manifest copy, not an index rebuild — which is exactly why no existing system offers
it, because none of them put the index somewhere a manifest copy could fork it.

The operations, in the order they will be built — chosen because the agent workload is asymmetric,
forking-and-discarding constantly and merging rarely:

- **Fork** is the cheap manifest copy just described.
- **Abort** discards the branch's own new segments and its manifest. This is the *hot path* — agents
  abandon far more branches than they keep — so unlike a traditional database, where fast commit
  matters most, here fast *abort* is what the whole design optimizes for.
- **Branch-isolated reads** answer queries against a branch's own set of segments, snapshot-consistent,
  never seeing another branch's writes.
- **Merge** replays a branch's logical additions and deletions onto the parent and rebuilds the
  affected segments. Merge is allowed to be correct-but-slow for a long time, because it is rare.

The one genuinely unsolved problem in this picture is the housekeeping: when thousands of short-lived
branches each spawn a few segments, deciding when to compact — and never reclaiming a segment that some
live branch still points at — is the hard part, and it is honestly flagged as open rather than
hand-waved.

### 12.4 The enabling primitives that come along the way

The future filtered and temporal queries need two small things the current engine lacks, and they will
be built as part of the layout work: the ability to combine filter conditions with "and"/"or" rather
than expressing only one at a time, and a first-class notion of when each row was written, so temporal
pruning has a column to prune on. Alongside them comes a time-travel read interface — the versioning to
support it already exists internally; what is missing is the outward-facing way to ask for the store as
it was at an older version.

### 12.5 The small primitives, held deliberately at arm's length

Beyond branching sit a handful of narrow storage facts, each valuable and each stopping deliberately
short of the larger system that would consume it: *staleness tracking*, which records which derived
values are out of date relative to their sources and lets you ask "what is dirty" — without recomputing
anything, because the recomputation is an orchestrator's job, not the storage engine's; *verifiable
deletion*, which turns "provably scrubbed from every segment" into a first-class operation, made
tractable by the segmented layout and about to be legally required; and a *budget-shaped* search
interface, which lets a caller ask for "accuracy at least this high, or cost at most that much" instead
of hand-tuning a raw search-effort dial. Each is days to a couple of weeks of work and none is urgent.

### 12.6 The productization tail

Finally, the unglamorous but necessary remainder: a data-loading path optimized for feeding training
loops efficiently; the ability to run the identical format and manifest logic against cloud object
storage instead of only local disk; and polished language bindings so the engine can be driven
comfortably from other ecosystems. These are engineering, not research — they come once the core and
the differentiator are solid.

---

## 13. What Strata deliberately will *not* be

A design is defined as much by its refusals as its features, and Strata's refusals are principled and
recorded so they are not relitigated every time the surrounding field moves. It will not attempt full
serializability, or distribute across multiple nodes, or grow a full query language and optimizer, or
add a second family of vector index — each of these is a different project, and taking any of them on
steals effort from the thing that is actually differentiated. It will not become a derivation engine
that invokes models on your behalf, nor a query planner that reasons about cost and quality, nor a
belief system that holds opinions about what a "fact" means over time — the moment a storage engine
starts holding opinions about the meaning of data, it stops being a storage engine.

Most pointedly, it will not try to *be* agent memory as a product. The demand for agent memory is real,
but every serious entrant in that category is a data model plus an extraction pipeline plus a retrieval
policy sitting on top of someone else's storage engine — and when the differentiating logic is a set of
prompts, the competitor is whoever makes the model. Strata's role is the opposite one: to be the
substrate that a memory layer is *built on*, providing the one thing all of those systems currently
fake at the application layer and get wrong — atomic, consistent, forkable storage of facts and their
index together. The intended relationship is that memory is the *demonstration*, and Strata is the
engine underneath it.

---

## 14. The single idea underneath everything

If you keep only one thing: nearly every property Strata has — or is going to have — comes from one
architectural decision taken seriously. *Put the vector index inside the transaction engine, on an
immutable, segmented, versioned store.*

From that one decision, atomic multi-structure commits follow, because the index change rides the same
durable, atomic publish as the row change. Snapshot isolation follows, because immutable files and a
versioned manifest make point-in-time consistency the default. Cheap recovery follows, because opening
the store is reading a list, not rebuilding a graph. Provable deletion follows, because compaction can
physically rewrite immutable segments. And branching — the capability nobody else has and the reason
the project sharpened its thesis — follows, because forking a store built this way is copying a small
list of pointers to shared, immutable segments.

Everyone else assembles these properties piecemeal, at the application layer, with eventual consistency
and crossed fingers. Strata's bet is that taking the single hard architectural decision, and paying for
the correctness rigor it demands, yields all of them at once — as consequences of the design rather
than features bolted onto it.
