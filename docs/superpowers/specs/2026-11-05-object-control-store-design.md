# Object control store: design and failure analysis

Status: proposed, final phase of control-plane de-database. Inputs: Phase 0
(`2026-11-02-control-plane-no-database-phase0.md`), the four v1.30 control-store slices, and the two
in-flight seam slices (IAM; jobs/outbox/sites/audit). This document decides the end state; it does
not modify code.

## 0. Decision summary

| # | Decision | Why |
|---|----------|-----|
| D1 | Node control state is one immutable copy-on-write B+tree per table. The only mutable key is a small root object whose CAS is the transaction commit point. | One per-key CAS gives arbitrary multi-table atomicity without journals, replay, or a 2PC coordinator. Crash before the root CAS is invisible; crash after it is fully committed. |
| D2 | Under Raft, the root is **node-local** (`.oss.sys` on that node's drives). Raft orders and replicates commands; every node applies to its own root. Cluster-quorum system objects are not on the live path. | Phase 0 cluster CAS is a linearizable shared cell. Applying the same command on N nodes would race N writers on one key, and a follower must not depend on a network quorum to apply. This mirrors today's per-node `oss.db`. |
| D3 | Rows larger than 16 KiB are chunked content-addressed blobs (64 KiB chunks, 24 MiB cap). | `MAX_INLINE` is 128 KiB; OLAP WASM modules are up to 16 MiB and `COMMAND_MAX_BYTES` is 24 MiB. A whole row can never be a single control object. |
| D4 | Operational node state (`uploads`, `instant_index`, audit, outboxes, sync intents, site cursors/dead letters, usage cache) lives in versioned local files, not in the root. | Those domains are high-frequency, best-effort or lease-based, and do not share an atomicity boundary with control state. Putting them in the root would turn every upload/audit write into a root CAS. |
| D5 | Raft log, vote, meta and snapshot chunks live in `<raft.db_path>.d/` as versioned files; `raft.db` is never opened except by the offline migration tool. | Raft storage needs append/truncate/purge/vote semantics and per-entry bytes up to 24 MiB, not CAS. Raft already replicates the log; object quorum adds cost and no safety. |
| D6 | SQL triggers are replaced by typed write origins plus per-mutation hooks inside `ControlTxn`: 33 raft guards become a capability/policy check; 30 site triggers become explicit gate + changelog + clock code. | No SQL engine remains. Origins (`RaftApply`, `SnapshotInstall`, `SiteApply`, `Local`) are stronger than mutable `active` flags and cannot leak across async tasks. |
| D7 | Hot reads use per-generation copy-on-write maps and a per-request snapshot. Cold reads traverse the pinned tree. | Auth is two point lookups per request today; object traversal is too slow for the data path. Generation tagging makes revocation immediate and coherent on the node. |
| D8 | Migration is stop-the-world, one-way, with offline byte-level verification. No runtime dual-write. | Two stores with different atomicity cannot be updated atomically; dual-write only creates split-brain on partial failure. |
| D9 | Witness loses its SQLite state machine: metadata-only applied/membership file. Promotion of a witness to voter requires restart with full state and snapshot install. | A witness has no data dir and cannot host the object root. It never serves control reads. |
| D10 | `schema_meta` and `runstate_meta` disappear into root metadata; activation markers that must be atomic with data (`raft_feature_level`, runstate/inventory activation, accounts canonical/backfill) are committed in the same root CAS. | Preserves the current "marker and data in one transaction" invariant. |

## 1. Namespace, layout, serialization, versioning

### 1.1 Keys

```
control/state/root                  mutable; CAS commit point (one per node)
control/state/node/<sha256-hex>     immutable B+tree node
control/state/blob/<sha256-hex>/<n> immutable large-value chunk (n decimal)
control/...                         existing Phase 0 control objects (legacy vpc, recertify) unchanged
```

All keys stay inside the reserved `.oss.sys` namespace (`layout::SYS_DIR`), are S3-invisible, and use
only the existing control-key grammar. Node and blob keys are content-addressed create-once objects:
the same bytes always map to the same key; a conflict whose stored content hashes identically is
success, a different content is `CorruptMeta`.

The root is per node, not per cluster: the same key name exists independently on each node's drives.
A node never reads or CASes another node's root.

### 1.2 Object IO

The root and nodes use a new **local control-object IO** API over the existing store kernel:
`get_local_control(key)`, `put_local_control(key, data, expected_version, fence_token)`,
`delete` not used for nodes. It reuses the Phase 0 single-node/local-drive commit path: per-key process
mutex + `flock`, local drive quorum, commit certificate, tombstones, `Indeterminate` semantics. It
skips the cluster fanout and fenced cluster lock. `put_system_object` and
`recertify_system_object_cluster` remain for legacy cluster keys only.

Control objects are **not EC-striped**: the full meta is written to every local drive, so the local
quorum is a majority of the node's registered drives, `W = drives/2 + 1` (integer floor; 1 drive → 1),
independent of the cluster EC configuration. This matters for the documented 4 nodes × 3 drives =
12 slots / EC 8+4 topology: cluster `Q_s = data + 1 = 9` is unreachable on one node, while `W = 2`
is. The data plane and `put_system_object` keep `Q_s = data + 1` unchanged.

Startup takes an exclusive `flock` on the node state directory. Two server processes serving the same
object root are unsupported (same as two today's servers sharing one `oss.db`); the second fails
startup. CAS remains the last line of defense if that lock is bypassed.

### 1.3 Format

Every control-state object starts with a fixed header:

```
magic "OSSC" | u8 kind (1 root, 2 node, 3 blob) | u16 major | u16 min_reader
| u32 payload_len | payload (rmp/serde) | u32 crc32c(payload)
```

- `major` mismatch: reject the node (`Unsupported`/`CorruptMeta`), fail closed.
- `min_reader` is the lowest minor a reader must implement. A reader with
  `KNOWN_MINOR < min_reader` rejects; otherwise it ignores fields it does not know.
- Writers that only append fields with serde defaults keep `min_reader` unchanged. New semantics or
  new required tables set it. Unknown table ids with `required_minor > KNOWN_MINOR` are preserved
  byte-for-byte on rewrite but cause export/install and destructive operations to fail closed rather
  than silently drop state.
- Row payloads reuse the seam row structs. Their field order and `deny_unknown_fields` stay frozen by
  the existing byte-order tests; adding a snapshot row field still requires an OSSCTL version bump.

Root payload (kept under 32 KiB):

```
min_reader, mode, markers: Map<String,String>,
tables: Map<u16, TableRoot { hash, rows, bytes }>,
next_uid, next_job_id, next_run_id, next_site_seq,
applied: Option<LogId {term,index}>, membership_json: Option<String>,
feature_level: u16, created_at
```

`markers` holds the `schema_meta` keys. `next_*` are the allocators formerly provided by SQLite
autoincrement / `runstate_meta`. Root `min_reader`/`major` are checked before any mutation.

Table ids are a fixed registry shared with export/install and goldens. Ordering is a
per-table typed comparator (text keys bytewise, numeric keys numeric, composite keys
length-prefixed); it is not SQLite collation. Export order is defined by the comparator so output is
deterministic regardless of tree shape.

Node payload: `kind (leaf|internal), level, entries`. A leaf entry is
`table_key, row_bytes | BlobRef | tombstone`; an internal entry is `first_key, child_hash`. Nodes are
written at target 32 KiB, split above 64 KiB, merged below 8 KiB. Values above 16 KiB are stored as
`BlobRef {sha256, len, chunks}` and never inflate a node. A row above `COMMAND_MAX_BYTES` (24 MiB) is
rejected (`ControlError::Invalid`); the 16 MiB OLAP module fits in 256 chunks.

Logical deletion writes a leaf tombstone; tombstones are compacted by the next node rewrite. This is
not needed for correctness (reads return None) and keeps deletes cheap.

## 2. Transaction protocol and failure analysis

### 2.1 `ControlTxn` in the object backend

`ObjectControlTxn` pins a root generation and its table roots. It keeps an in-memory overlay:
`BTreeMap<table_id, BTreeMap<key, StagedRow>>`, where a staged row is `Put(row|blob)` or `Delete`.
All reads are overlay-aware: point reads check the overlay, range scans merge the tree iterator with
the staged range. This preserves read-your-writes inside one transaction, including the
`user_delete_guard` last-admin count, natural-key resolution and cascade enumeration.

No `begin`/`rollback` is added. The caller (Raft apply, snapshot install, local façade) owns the
boundary; dropping the txn discards the overlay. `commit()` is backend-specific and is called only by
the owner of the atomic boundary.

### 2.2 Commit algorithm

1. If the overlay is empty and no root metadata changed, return the pinned version (no CAS, no
   generation bump).
2. For each dirty table, bottom-up rewrite only the nodes on changed key paths. Write new nodes with
   create-once semantics; verify any existing object at the hash matches the bytes.
3. Write new blob chunks before the node that references them. Chunks are immutable; identical bytes
   deduplicate.
4. Serialize the new root (same table roots for untouched tables, new roots for dirty tables, updated
   metadata/allocators/`applied`). CAS root with `expected_version = pinned_version` and
   `fence_token = pinned_version + 1`.
5. On `Conflict`: re-read root. If it already contains this transaction's `applied` log id, return
   success (idempotent replay). If this is a local façade write, retry the whole transaction with a
   bounded budget (8) and then return `ControlError::Conflict` (mapped to 409/503). If this is Raft
   apply, rebuild from the new root and retry; apply is the only writer of replicated tables, and
   local allowed writes are rare, so prolonged conflict is a bug and after 8 attempts the state
   machine returns a storage error (fail-stop) rather than applying on a stale base.
6. On `Indeterminate`: re-read root by key and compare version/token. New generation present and
   matching the serialized root bytes: success. Otherwise retry/fail as above. Never assume rollback.
7. Publish the new root generation to the hot cache (`ArcSwap`), record metrics, return.

One root CAS is the linearization point. There is no separate commit marker, no replay, and no
recovery pass that mutates state: readers see generation N or N+1, never a mix.

### 2.3 Concurrent writers, fencing, quorum

- Within a node, a process-wide async mutex serializes commits; CAS detects violations from a second
  process or a missed lock. The startup `flock` makes a second process a startup error.
- Fencing token = pinned physical version + 1. `put_local_control` rejects a token that is not
  strictly greater than the committed one. A writer paused past a newer commit cannot resurrect its
  base.
- Local control read and write use the local majority `W = drives/2 + 1` (full meta on every drive,
  not EC-striped). A successful write puts committed certificates on ≥ `W` local drives; a read
  requires ≥ `W` reachable drives (otherwise `QuorumUnmet`) and elects the max `(fence_token,
  version)` among certified copies. The two quorums intersect (`2W > drives`), so a committed version
  is never missed. Uncertified/corrupt residues are rejected (`CorruptMeta`), never a stale or
  partial answer. There is no cross-node quorum read for live control state: the cluster's
  consistency authority is Raft, exactly as today. Follower reads may lag by apply distance, as
  today; the leader acknowledges a write only after apply.
- Snapshot install and apply are exclusive (`&mut self` in the state machine). A local admin/session
  write may interleave between applies; it commits a root with the same `applied` value and is not
  part of the replicated state.

### 2.4 Raft apply and snapshot install

`ObjectStateMachine::apply` keeps today's **one root commit per entry** (not per batch): if entry k
fails internally, entries before k stay applied and openraft fail-stops, matching the current
per-entry SQLite transaction contract. Each commit includes:

- the command's row mutations,
- any feature activation markers and runstate/inventory activation,
- the new `applied` LogId and current `StoredMembership`,
- updated allocators.

Business rejection (`ControlResponse::Invalid`/`Conflict`) still commits the entry and its applied id;
only an internal invariant error aborts the commit. Replay of an already-applied entry is a no-op
because the root's `applied` is >= that entry. Blank/membership entries commit metadata only.

`install_snapshot` builds a complete new tree from the snapshot's ControlData/RunState, carries over
the local rows the current install preserves (admin users, admin-owned access keys, `set_id`, site
tables, markers), clears admin-target permissions as `install_tx` does today, assigns uids for
non-admin users by username (monotonic `next_uid`, never reused) and switches the root in one CAS with
the snapshot's applied log/membership/feature level. The old tree becomes garbage. Crash
before the CAS leaves the old state; crash after it leaves the new state. This is strictly stronger
than the current multi-statement SQL transaction and needs no `raft_apply_ctx`.

### 2.5 Failure matrix

| Failure | Visible state | Recovery |
|---------|---------------|----------|
| Crash before any node/chunk write | old generation | none; overlay lost |
| Crash after some nodes/chunks, before root CAS | old generation; orphans | async GC removes unreferenced hashes older than retention |
| Root CAS partially certified (`Indeterminate`) | old or new generation, never a mix | re-read root; if new version certified, success; else retry |
| Crash after root CAS, before response | new generation | Raft: replayed entry is a no-op; local façade: caller retry sees new state |
| Node object hash collision/bytes mismatch | none (fail closed) | `CorruptMeta`; runbook: restore root backup or re-add node |
| Blob chunk missing/corrupt | read error, never a partial row | fail closed; restore/GC audit |
| Drive below quorum | read/write error | control plane unavailable; do not serve stale |
| Root missing while `initialized=1` | startup refuses | Raft: wipe node, rejoin, install snapshot; standalone: restore backup |
| Root has higher `major`/`min_reader` | startup refuses | run newer binary; never downgrade |
| Snapshot install crash pre-CAS | old state/applied id | openraft retries install |
| Snapshot install crash post-CAS | new state/applied id | idempotent reinstall |
| Site ship crash after send before cursor save | remote applied; local cursor behind | duplicate resend; LWW tuple rejects the duplicate |
| Site ship cursor saved before crash? (never: save after send) | no loss | at-least-once |
| Concurrent root CAS | one winner | retry policy above; no partial writes |

### 2.6 Restart recovery

Read root, verify header/CRC/major/min_reader and every referenced table root. Rebuild the hot cache
(§4) and the job due queue by scanning the corresponding table; scan local operational files. Remove
orphan nodes/chunks older than the retention window. If the root is absent and no `initialized`
marker exists, bootstrap an empty root. If the root is absent but the marker exists, fail closed. If
the root is present with `applied` but the Raft file log is empty and membership is empty, use the
existing "state lost" refusal (now reading the root instead of `raft_applied`).

## 3. Consistency model

- **Per-request snapshot:** every request captures one root generation and one `Arc<HotView>`. Auth,
  ACL, bucket and policy checks in that request observe one consistent generation. A concurrent commit
  does not change an in-flight request's decisions; the next request sees the new generation.
- **Local writes:** commit is atomic and durably linearized by the root CAS. Reads on the same node are
  snapshot-isolated, monotonic per captured view. The façade returns only after the cache for the new
  generation is published, so read-your-write holds on that node.
- **Raft:** log order is the linearization order. Apply is deterministic: replicated mutations use
  values carried by the command (timestamps, ids, hashes), never local clock/random. Reads on a
  follower are served from its applied generation and may lag; the leader only responds after apply.
  Snapshot export must name its applied log id; export at a pinned root generation is self-consistent.
- **Export:** pin the root (and optionally register a GC pin), walk the table roots, serialize with the
  existing OSSCTL encoders. No locks, no writer pause; GC retention must exceed export duration.
- **Site replication:** unchanged LWW semantics `(ts, priority, origin_site_id, seq)` with a strict
  comparison against `site_entity_clock`. Local gate rejection and changelog+clock append happen in
  the same root CAS as the entity mutation, so a crash cannot split a change from its changelog.
  Applied remote entries use `Origin::SiteApply`: they update the entity and clock, never write a new
  changelog; a lower tuple is skipped exactly as today.

## 4. Cache and hot paths

`HotView` is immutable per root generation and contains copy-on-write maps:

- users by uid and by username; access keys by AK (including kind/parent/owner/expiry/status);
- users' status and account membership/status (for login/auth invalidation);
- buckets by name (`BucketView`, local `set_id`), policies/event/replication configs by bucket;
- permissions by `(uid, bucket)`, accounts by id and members by uid;
- AP/OLAP/VPC registries used per request; site config/sites for gate predicates.

Commit publishes a `CacheDelta` from the actual committed overlay, then swaps the maps. Only domains
touched by the transaction are cloned; runstate-only commits do not clone auth/bucket maps. Bulk
replaces and snapshot install rebuild the affected maps once from the new tree. A property test
compares cache maps with a fresh tree scan after randomized transactions.

Cold reads (lifecycle/inventory/tier rows, job rows, exports, admin lists) traverse the pinned tree
with no cache; they are not on the S3 request path.

Budget (single node, local SSD; validated in the performance gate):

| Path | Target |
|------|--------|
| AK + user + account status auth lookup (cache hit) | ≤ 10 µs CPU; ≤ 200 µs p99 added request latency incl. vault decrypt |
| user_of_key + permission + bucket lookup per request | ≤ 15 µs CPU |
| uncached tree point read | ≤ 2 ms p99 |
| control write commit (one drive) / (multi-drive quorum) | ≤ 20 ms / ≤ 50 ms p99; ≥ 200 commits/s |
| Raft apply per entry | ≤ 50 ms p99; no regression vs SQLite beyond 10% |
| job claim commit | ≤ 20 ms p99; worker loop unchanged |
| startup hot-cache load, 100k users/AKs/buckets | ≤ 5 s |
| snapshot export/import, 100k rows | ≤ 10 s |
| hot cache memory at 100k users/AKs/buckets | ≤ 256 MiB, configurable cap |

Negative lookups are just absent map entries; because the whole map is swapped per generation, creating
an AK is visible on the next request with no TTL and no invalidation race. The mesh fanout for
non-Raft cluster topologies is retained; its results are not cached across generations.

## 5. Triggers become application code

`ControlTxn` is constructed with an origin:

```
enum WriteOrigin { RaftApply { log_id }, SnapshotInstall, SiteApply { peer }, Local { actor } }
```

The origin is a parameter of the only constructor that can obtain a `ControlTxn`; there is no mutable
`raft_apply_ctx`/`apply_ctx` flag. The SQLite startup mode switch becomes a startup assertion that the
root `mode` matches the process configuration; mismatches fail closed.

### 5.1 33 raft guards (11 tables × INSERT/UPDATE/DELETE)

Only `RaftApply` and `SnapshotInstall` may write the replicated tables when `mode=raft`. `Local` is
checked per mutation against the exact current trigger exceptions:

| Table | Local-write exception |
|-------|----------------------|
| users | INSERT: `NEW.is_admin = 1`; UPDATE/DELETE: `OLD.is_admin = 1` |
| access_keys | INSERT: `NEW.kind = 'session'` or owner `NEW.user_id` is admin; UPDATE/DELETE: `OLD.kind = 'session'` or owner `OLD.user_id` is admin |
| permissions | INSERT/UPDATE: target `NEW.user_id` is admin; DELETE: target `OLD.user_id` is admin |
| other 8 replicated tables | never local |

Rejection is `ControlError::Invalid("raft: control write outside apply")` to preserve the observable
error. Standalone (`mode=standalone`) disables the guard entirely, as dropping the triggers does
today. Snapshot install bypasses guards because it is the state machine's install path.

### 5.2 30 site triggers (5 entities × gate/log × INSERT/UPDATE/DELETE)

Active only when `origin=Local`, `sites` is non-empty, `site_config` is seeded. `RaftApply` and
`SnapshotInstall` behave as the dropped triggers in Raft mode. `SiteApply` suppresses the automatic
gate/log but its caller explicitly writes the LWW clock as today.

| Entity | Rows covered | Gate predicate | Payload keys (in order) |
|--------|--------------|----------------|-------------------------|
| bucket | all | INSERT/DELETE always; UPDATE only when versioning, owner_id or object_lock actually change (`IS NOT` null-safe) | upsert: name, owner_username (null if dangling), creation_date, versioning, object_lock; delete: name |
| bucket_policy | all | UPDATE only when policy_json or updated_at change | upsert: bucket, policy_json, updated_at; delete: bucket |
| permission | owner non-admin | all ops | upsert: username, bucket, can_read, can_write, can_delete; delete: same for OLD |
| user | `is_admin=0` only | UPDATE only when username/password_hash/status/created_at change | upsert: username, password_hash, status, created_at; delete: username |
| access_key | `kind='long-term'` and owner non-admin | UPDATE requires OLD and NEW long-term and NEW owner non-admin | upsert: access_key, secret_key, username, status, expires_at, created_at; delete: access_key, username |

Implementation rules that preserve trigger behavior:

- The gate compares the tuple `(now_secs, local_site_priority, local_site_id)` lexicographically to
  the current clock row. A missing clock row compares as NULL and therefore permits the write, as in
  SQLite. A losing local write is rejected before any row mutation, with the same
  `site replication conflict` message.
- Entity id for a permission is `username || char(31) || bucket`; updates use the NEW target, deletes
  the OLD target, exactly as the triggers do.
- After the mutation, append one changelog row with `next_site_seq` (the root allocator replaces
  AUTOINCREMENT), priority/site id from `site_config`, one `now_secs` value per mutation, then
  `INSERT OR IGNORE`/conditional update of `site_entity_clock` using the changelog tuple. Clock and
  changelog are in the same root CAS.
- Payload JSON uses a small SQLite-compatible string escaper so key order, nulls and escaping match
  `json_object` output. A golden corpus captured from SQLite (including quotes, backslashes and
  controls in usernames/keys/policy JSON) is the acceptance oracle.
- Cascades enumerate rows and emit one hook per affected row in the same order as today's SQL:
  user delete deletes access keys, then permissions (user still present so payloads resolve the
  username), then updates buckets, then deletes the user; bucket delete deletes policies, then
  permissions, then the bucket. A bulk `UPDATE buckets SET owner_id = NULL` logs one bucket upsert per
  actually changed row. A delete of an admin-target permission logs nothing.
- Site apply writes `SiteApply` mutations through the same domain methods; the caller's explicit
  `put_clock` is retained. Peer add first writes the peer cursor file (atomic rename), then commits
  `sites` + changelog + clock in one root CAS. A crash between leaves an ignorable orphan cursor;
  the reverse order (paired peer without a cursor) is impossible. Ship-time cursor updates are
  local-file writes, not root CAS; a lost cursor restarts that peer at seq 0, which is safe because
  the LWW tuple rejects duplicates (at-least-once).

No SQLite JSON/scalar subqueries remain; all referenced values are resolved from the txn's maps.

## 6. Snapshot, migration, cutover

### 6.1 Formats and snapshot path

`ControlData` (OSSCTL1/2/3) and `RunState` (`m8.runstate`) are unchanged: same fields, order,
predicates, feature gates. Export builds the payload from a pinned root; install stages a full new
tree and commits once. `buckets.set_id`, admin users, admin-owned keys, site tables and markers stay
node-local across install exactly as the current `install_tx` predicates do; install deletes
non-admin-owned access keys (including session/STS keys of non-admin users) and clears admin-target
permissions, as today. Because `uploads` lives in local files in the object backend, install must
also clear non-admin `owner_id`s in that local upload store (today's `install_tx` step 6) and
rebuild the account-usage cache; this is tested in D3. Golden fixtures captured from the last
SQLite build must round-trip byte-identically through the object backend.

### 6.2 Offline migration (one-way)

1. Stop all servers (blue-green or full stop; no rolling upgrade, matching README). Take a complete
   offline backup of `oss.db`, `raft.db`, and data dirs; record checksums.
2. Preflight: verify versions/feature levels, no unknown higher format, disk space, table counts,
   `PRAGMA integrity_check` while SQLite still exists.
3. Recertify legacy Phase 0 control objects with the existing cluster recertify tool if any legacy
   key lacks a certificate.
4. Export `ControlData` + `RunState` from SQLite with the existing encoders; verify against recertified
   legacy keys.
5. Build the object root: write table nodes/chunks, then root last; write local operational files and
   Raft files (`<raft.db_path>.d/`), preserving log entries, terms, vote, committed index, purge
   index and snapshot bytes.
6. Verify: export from the object root with the same encoders and compare byte-for-byte with step 4;
   compare row counts and per-table hashes; verify every local table count and every Raft log entry
   checksum; run `control-verify` (root CRC, reachability, allocators, clock/seq bounds).
7. Cut over: start the new binary against the object root and local state dir. The first accepted
   write makes the cutover irreversible. Smoke tests: auth, bucket CRUD, snapshot export, site
   status, Raft status.

No runtime dual-write and no live dual-read: they cannot preserve one atomic state across two
backends. "Dual-read" is replaced by the offline byte comparison in step 6. The migration command is
`oss-server control-migrate --from <db> --state-dir <dir> --dry-run|--apply --json` and is built from
the pre-removal commit; it is archived as a migration artifact when the workspace stops compiling
SQLite.

### 6.3 Rollback

- Before the first new write: stop the new server and restore the backed-up SQLite files and old
  binary. This is the only full rollback.
- After the first new write: rollback means restore the backup and lose writes since cutover; there is
  no in-binary reverse writer after SQLite removal. The old deployment must keep its binary and backup
  until the acceptance window closes. This is a deliberate constraint, not an oversight.
- Mixed versions are forbidden: all Raft voters/witnesses migrate together; the new file store still
  carries raft log format v2 bytes, so an old binary cannot read the new directory, and a new binary
  refuses a legacy `raft.db` without migration.

## 7. Fate of every store

| Store | Fate | Notes |
|-------|------|-------|
| users, access_keys, permissions | root table | Replicated subset as today; admin/session rows local but same tree, guarded. Numeric uid stays a local allocator (`next_uid`, monotonic, never reused); all replicated references are natural keys except the existing `requested_by` scalar. |
| buckets, bucket_policies, lifecycle_rules, event_configs, replication_rules, access_points, olap_configs, inventory_configs | root table | `buckets.set_id` local attribute preserved on install. Large OLAP modules chunked. |
| inventory_runs, inventory_cursors, inventory_incremental, tier_restores, lifecycle_cursors | root table | Part of RunState. |
| vpc_endpoints, accounts, account_members, account_usage_ledger | root table | Part of ControlData; export unchanged. |
| jobs, job_tasks, schedule_lease | root table | `next_job_id`/`next_run_id` move to root counters; job due queue is a derived in-memory index. |
| runstate_meta | root metadata counters | Atomic with job/run creation. |
| schema_meta | root `markers` | Identity, mode, activation, backfill markers. |
| site_config, sites, site_changelog, site_entity_clock | root table | Changelog/clock must be atomic with entity writes. Unbounded changelog growth is pre-existing; monitor disk. |
| site_cursors, site_apply_dead | local files | Per-peer cursor (atomic rename) and append-only dead-letter file. |
| uploads, instant_index | local KV files | One record per key under `local/`, atomic tmp+fsync+rename; numeric owner uid valid because uids are never reused. |
| audit_log | daily append-only files | Buffered batch append; retention by file age; console scans recent files. |
| event_outbox, repl_outbox | local append-only queue files | Framed records, CRC, replay to in-memory pending map, lease/claim records, periodic compaction. |
| sync_write_intents, sync_delete_intents | local queue file | Same journal as outboxes; recovery scans unfinished intents as today. |
| account_usage | local cache file | Recomputable; cleared/rebuilt when accounts change. |
| apply_ctx, raft_apply_ctx | deleted | Origin parameter replaces both. |
| raft_applied | root metadata | Applied LogId + membership in the root CAS. |
| raft_log, raft_log_segment, raft_meta, raft_snapshot, raft_snapshot_chunk | `<raft.db_path>.d/` files | Segmented append log, framed CRC; `meta` file vote/committed/last_index/last_purged/format; snapshot dir with manifest + 256 KiB chunks. Writes: entry fsync then meta rename; orphan tail ignored/truncated. |
| raft_kv | deleted (test-only) | Production commands never use it; tests move to an internal root KV probe or are rewritten. |
| witness state_db | `witness/state` metadata file | Identity + applied/membership only; metadata-only state machine. Config `state_db` accepted only for migration input. |
| SQLite schema/migrations/sqlx | deleted at the gate | No `oss.db`, no `raft.db`, no SQLite linked. |
| data-plane `xl.meta`, object data | unchanged | Data plane was never in SQLite. |

## 8. Security and operability

- **Access:** control roots and nodes are only reachable through the internal store API; `.oss.sys` is
  rejected by bucket-name validation and never listed/served by the S3 or console paths. Root/node
  files inherit 0700/0600; only the server user reads them. Backup files must be encrypted at rest
  (they contain password hashes and sealed SKs, as `oss.db` did).
- **Secrets:** values are stored exactly as today (hashes, `enc:v1:` sealed keys). Logs never print
  row values or JWTs; auth cache holds sealed SKs and decrypts per request (no plaintext cache).
- **Observability:** metrics `control_root_generation`, `control_root_commit_seconds`,
  `control_root_conflicts_total`, `control_nodes_written_total`, `control_gc_deleted_total`,
  `control_cache_generation`, `control_auth_cache_hit_total`, `raft_apply_seconds`,
  `raft_follower_lag`, `site_changelog_lag`, `site_apply_dead_total`, plus startup verification
  results. Alerts on commit p99, conflicts, GC backlog, root size, disk usage, changelog growth.
- **Bounds:** node ≤ 64 KiB, root ≤ 32 KiB, row ≤ 24 MiB, blob chunks 64 KiB, hot-cache memory cap,
  local queue file size cap with dead-letter after repeated retry, audit retention, Raft log purge
  and snapshot retention.
- **GC:** mark from current root plus the last K generations (K=32) and active exports/backups;
  delete unreferenced nodes/chunks only after a grace window longer than the longest read/export.
  Tombstones and old snapshots are collected by the same sweeper. A stuck GC is a disk alert, not a
  correctness failure.
- **Runbooks:** root missing/corrupt (restore backup or rejoin Raft node), format too new (upgrade
  binary; never downgrade), drive below quorum (restore drive; control plane stays down rather than
  serve stale), conflict storm (stop second process; inspect flock), GC backlog (raise retention
  disk), snapshot install failure (retry; if persistent, wipe node and re-add), migration failure
  (restore SQLite backup before cutover), site divergence (dead-letter + clock inspection).

## 9. Final hard-gate acceptance

1. Fresh standalone, site and 3-node Raft (voter+witness) startups create no `oss.db`, `raft.db`,
   `state.db` or any SQLite file; only the object root, `local/` files and `<db_path>.d/`.
2. `cargo tree -i sqlx` is empty; `sqlx`, `libsqlite3-sys`, `rusqlite` are absent from
   `Cargo.toml`/`Cargo.lock`; `crates/oss-server/migrations/` and all `Sqlite*` adapters are deleted;
   `rg` CI guards reject reintroduction (including `sqlite://`, `oss.db`, `raft.db` strings outside
   migration/docs).
3. The full seven-gate test suite passes, plus the kill9 suite at commit phases (crash before nodes,
   after nodes, during root CAS, after root) with 100 iterations: every reopen yields the old or new
   generation, never a partial row set. Snapshot-install crash matrix passes. Raft apply replay is
   idempotent.
4. Golden parity: every seam differential suite converted to frozen goldens captured from the last
   SQLite build; OSSCTL1/2/3 and `m8.runstate` exports/imports are byte-identical; all 33 guard
   exceptions and all 30 site-trigger cases (gate conflicts, payloads, cascades, apply suppression,
   clock) match goldens.
5. Concurrency: two independent writers on one key yield exactly one success; apply vs local facade
   loses no committed update; stale fence token is rejected; `Indeterminate` re-read path tested with
   injected partial certificates.
6. Performance budgets of §4 met on the reference host, with no S3 data-plane throughput regression
   >5% vs the SQLite baseline and Raft apply no >10% regression.
7. Security/operability: S3 and console cannot read/list `.oss.sys`; backup/restore and migration
   runbooks rehearsed; metrics exported; drift/GC alarms wired; no plaintext secrets in logs.

## 10. Rejected alternatives and residual risks

- **Per-row objects + journal + commit marker:** readers must detect and resolve in-flight rows,
  recovery must roll forward/back, and partial failures need per-row fencing. The COW root gets the
  same atomicity with one CAS and no recovery writer. Rejected.
- **Per-domain snapshot objects + 2PC across domain roots:** bucket/user/bucket-delete cascades span
  domains; 2PC reintroduces coordinator failure windows and `Indeterminate` multi-key states. A
  single root CAS is simpler and stronger.
- **Keep SQLite behind an interface:** violates the gate (no sqlx/SQLite) and leaves the operational
  burden the project is removing.
- **Raft log/meta/snapshot as control objects:** 24 MiB entries exceed `MAX_INLINE`; append/truncate/
  purge/vote need sequential semantics, not arbitrary CAS; every node would fan out quorum writes for
  data Raft already replicates. Local versioned files are the smallest safe end state.
- **Runtime dual-write during migration:** cannot be made atomic across two backends; rejected in
  favor of stop-the-world byte-verified cutover.

Residual risks, accepted with monitoring: B+tree write amplification at very high job-progress rates
(mitigation: budget gate, then batch only if per-entry failure semantics can be preserved); unbounded
`site_changelog` growth (pre-existing); COW disk amplification until GC (bounded by K generations and
sweeper); witness promotion requires a full restart/snapshot; migration downtime proportional to
control-state size; no rollback after the first post-cutover write except backup restore.
