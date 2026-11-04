# Object control store: phased implementation plan

Status: proposed. Executes the decisions in
`docs/superpowers/specs/2026-11-05-object-control-store-design.md`. Every task is independently
testable, TDD (RED then GREEN), and leaves the workspace green. No production code changes before
this plan is accepted.

## 0. Preconditions and lanes

Required before the integration phase (not before the immediate lane):

- `2026-11-04-control-store-*` slices are merged (vpc/AP/OLAP/lifecycle/inventory/tier/buckets).
- The in-flight **IAM** slice has merged: `crates/xl-store/src/control_store/iam.rs` with
  `IamStore`/`IamTxn`/`IamSnapshotTxn`; users, access keys, permissions, accounts and quota no longer
  need raw SQL on the runtime path.
- The in-flight **jobs/outbox/sites/audit** slice has merged: `crates/xl-store/src/control_store/jobs.rs`
  with `JobStore`/`JobTxn`/`JobSnapshotTxn`, plus its local-domain seams. If any of outbox/sites/audit
  still uses raw SQL after the slice, D2/D3 absorb it before F1.
- Re-read the merged `ControlStore`/`ControlTxn` supertraits: each domain adapter below implements the
  merged trait, not this plan's snapshot of it.

Lanes:

```
immediate (parallel with in-flight slices)
  A1 format/node ─ A2 tree ─ A3 root ─ A4 NodeIo
  A5 local control IO        A6 local files        A7 raft file storage
  A8 golden harness (capture runs once the two slices merge)

after A1-A5
  B1 object txn core ── B2 v1.30 domains ── B3 accounts ── B4 backend selector
  IAM slice  ── C1 IAM tables ── C2 auth cache ── C3 IAM snapshot
  Jobs slice ── D1 jobs ── D2 sites/triggers ── D3 outboxes/audit/uploads ── D4 quota
  B4 + C + D ── E1 hot view/request snapshot ── E2 guard/site matrix
  E ── F1 object state machine ── F2 raft file wiring ── F3 export/install parity
  F3 + A6/A7 ── G1 migrator ── G2 verify/recert ── G3 cutover rehearsal ── H1 SQLite removal ── H2 gate
```

Can start immediately: A1-A8 and B1. B2/B3 depend only on A1-A5 (their traits already landed). C/D
wait for their slices. F waits for B/C/D. G waits for F. H waits for G and a signed cutover.

Global gate for every task: focused tests first, then `cargo check --workspace`,
`cargo test -p xl-store --lib`, `cargo test -p oss-server`, `cargo clippy --workspace --all-targets
-- -D warnings`, `cargo fmt --all --check`. The full seven gates run at phase boundaries.

## 1. Immediate lane: storage primitives

### A1. Object header, node codec, blob codec

Depends: none. Files: `crates/xl-store/src/control_state/mod.rs`, `format.rs`, `node.rs`, `blob.rs`.
Interfaces:

```rust
pub const ROOT_MAGIC: [u8;4] = *b"OSSC";
pub enum ObjectKind { Root = 1, Node = 2, Blob = 3 }
pub struct ObjectHeader { pub kind: ObjectKind, pub major: u16, pub min_reader: u16 }
pub enum LeafValue { Row(Vec<u8>), Blob(BlobRef), Tombstone }
pub struct BlobRef { pub sha256: [u8;32], pub len: u64, pub chunks: u32 }
```

RED: header round-trip and CRC failure tests; node golden bytes; leaf/internal split at 64 KiB;
16 MiB blob round-trip in 64 KiB chunks; 24 MiB+1 rejected; unknown `major`/`min_reader` rejected;
`deny_unknown_fields` row tests remain green. GREEN: implement codecs with existing `rmp`/serde
conventions; no new dependency.

Gate: `cargo test -p xl-store control_state::`.

### A2. COW B+tree per table

Depends: A1. Files: `crates/xl-store/src/control_state/tree.rs`, `io.rs`, `hash.rs`.
Interfaces:

```rust
pub trait NodeIo: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error>;
    async fn put_create(&self, key: &str, bytes: &[u8]) -> Result<(), Error>; // exists+same => Ok
}
pub struct TableTree { root: Option<NodeRef>, rows: u64, bytes: u64 }
impl TableTree {
    pub async fn get(&self, io: &dyn NodeIo, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    pub async fn scan(&self, io: &dyn NodeIo, range: Range<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error>;
    pub fn stage_put(&mut self, key: &[u8], value: LeafValue);
    pub fn stage_delete(&mut self, key: &[u8]);
    pub async fn flush(&mut self, io: &dyn NodeIo) -> Result<(), Error>;
}
```

RED: point/range lookup; overlay read-your-writes; split/merge; node size caps; delete/tombstone;
flush of one key writes only the path (assert node count ≤ depth × 2 + 1); interrupted flush leaves
the old root usable; same bytes dedup. GREEN: persistent path copying; content-addressed nodes.

Gate: `cargo test -p xl-store control_state::tree`.

### A3. Root envelope and allocators

Depends: A1. Files: `crates/xl-store/src/control_state/root.rs`.
Interfaces:

```rust
pub struct RootMeta {
    pub mode: Mode, pub markers: BTreeMap<String,String>,
    pub next_uid: u64, pub next_job_id: u64, pub next_run_id: u64, pub next_site_seq: i64,
    pub applied: Option<AppliedLog>, pub membership_json: Option<String>,
    pub feature_level: u16, pub created_at: i64,
}
pub struct Root { pub major: u16, pub min_reader: u16, pub tables: BTreeMap<u16,TableRoot>, pub meta: RootMeta }
```

RED: root golden bytes; 32 KiB cap; unknown table with `required_minor` preserved and gated;
unknown minor tolerated only when `min_reader <= KNOWN_MINOR`; allocator monotonicity; empty root
bootstrap. GREEN: rmp serde with explicit field order and CRC.

Gate: `cargo test -p xl-store control_state::root`.

### A4. NodeIo fault and crash harness

Depends: A1. Files: `crates/xl-store/src/control_state/io.rs`,
`crates/xl-store/src/control_state/fault_io.rs` (test-only feature).
RED: injected failure after N puts; partial root CAS; hash mismatch; duplicate create with different
bytes must be `CorruptMeta`; duplicate with same bytes is success. GREEN: in-memory ordered NodeIo and
failpoint test double. This harness is reused by A5/B1/F1.

Gate: `cargo test -p xl-store control_state::fault`.

### A5. Local control-object IO

Depends: none (Store kernel exists). Files: `crates/xl-store/src/store/control_local.rs`,
`crates/xl-store/src/store/control.rs` (extract the single-node commit path; no behavior change for
existing cluster objects). Interfaces:

```rust
impl Store {
    pub async fn get_local_control(&self, key: &str) -> Result<Option<SystemObject>>;
    pub async fn put_local_control(&self, key: &str, data: &[u8], expected: Option<u64>, fence: u64) -> Result<u64>;
}
```

RED: CAS/tombstone/inline-cap parity with the single-node `put_system_object`; two Store handles on one
root allow exactly one writer; fence regression rejected; multi-drive quorum failure and
`Indeterminate` read-back; `flock` excludes a second process; restart reopens the committed root;
cluster config never fans out. GREEN: reuse `control_meta`/`write_meta_marker` local branch; new tests
`crates/xl-store/tests/control_local.rs`.

Gate: `cargo test -p xl-store --test control_local`.

### A6. Local operational files

Depends: none. Files: `crates/oss-server/src/local_store/mod.rs`, `kv.rs`, `queue.rs`, `audit.rs`,
`site.rs`, `uploads.rs`.
Interfaces: `LocalKv::{get,put,delete,scan}`, `LocalQueue::{append,scan,pending,claim,ack,dead,compact}`,
`AuditLog::{append,scan,retain}`, `CursorFile::{load,store}`.
RED: atomic tmp+fsync+rename; torn write leaves previous value; queue replay order and CRC; lease
expiry redelivery; compaction preserves pending rows; audit file rotation/retention; uploads KV TTL;
concurrent processes use atomic rename, never torn files. GREEN: versioned framed records; no SQLite.

Gate: `cargo test -p oss-server local_store::`.

### A7. Raft file storage

Depends: none. Files: `crates/oss-server/src/raft/file_log.rs`, `file_meta.rs`, `file_snapshot.rs`,
`crates/oss-server/src/raft/store.rs` (extraction only; SQLite impl stays until H1).
Interfaces: `FileLogStore` implements `RaftLogStorage<TypeConfig>`; `FileSnapshotStore` writes
`snapshot/<id>/manifest` + 256 KiB `chunk-*`; `RaftMetaFile { vote, committed, last_index,
last_purged, log_format }` at `<db_path>.d/meta`.
RED: append/read/truncate/purge/vote openraft trait tests; entry bytes identical to current
`raft_log.data`; torn tail ignored; meta fsync ordering (entry fsync before meta rename); higher
format refused; existing snapshot chunk bytes round-trip; empty dir bootstraps; legacy `raft.db`
regular file makes startup refuse with a migration message. GREEN: segmented per-index files or
one-file-per-segment with in-memory index; no SQLite.

Gate: `cargo test -p oss-server raft::file_`.

### A8. Golden oracle harness

Depends: in-flight slices merged. Files: `scripts/capture_control_goldens.sh`,
`crates/oss-server/tests/support/golden.rs`, `crates/oss-server/tests/goldens/**`.
RED: a test asserts every listed fixture exists and its SHA-256 is recorded in
`goldens/MANIFEST`. GREEN: script runs the SQLite-backed suite on the last SQLite build and writes:
seam row bytes per domain, OSSCTL1/2/3 + `m8.runstate` exports, all 30 site-trigger payloads for a
fixed clock corpus (quotes/backslash/control chars), guard allow/deny matrix, cascade order, and
snapshot install results. These are the permanent oracle after H1.

Gate: `cargo test -p oss-server golden_manifest`.

## 2. Object backend for landed domains

### B1. Object store core and transaction commit

Depends: A1-A5. Files: `crates/oss-server/src/control_store_obj/mod.rs`, `root_io.rs`, `txn.rs`,
`change.rs`, `origin.rs`.
Interfaces: `ObjectControlStore { root: Arc<RootIo> }`, `ObjectControlTxn { pinned: Root, overlay,
origin }`, `ObjectControlTxn::commit(self) -> Result<CommitStamp>`; `RootIo` wraps
`Store::{get_local_control,put_local_control}`, implements `NodeIo` plus root CAS and read-back.
`WriteOrigin` is defined here, constructor-private to apply/install/local façade.

RED: empty txn no CAS; one-row txn writes path + root; crash matrix from the design (before nodes,
after nodes, before root, during root, after root) reopens old/new only; stale token rejected;
`Conflict` retry then success with a concurrent writer; `Indeterminate` re-read; root read-back
verification; cache publication callback fired once; overlay scan used by cascade. GREEN: implement
per design §2; no SQL.

Gate: `cargo test -p oss-server control_store_obj::txn`.

### B2. Bucket and per-bucket config adapters

Depends: B1. Files: `crates/oss-server/src/control_store_obj/bucket.rs`, `bucket_write.rs`,
`bucket_snapshot.rs`, `ap_olap.rs`, `vpc.rs`, `lifecycle.rs`, `inventory.rs`, `tier.rs`.
Implement the merged `BucketStore/BucketTxn/BucketSnapshotTxn`, AP/OLAP/VPC and
lifecycle/inventory/tier traits over `ObjectControlTxn`; natural keys and sort orders from the seam.

RED: run every existing v1.30 domain diff/golden suite against `ObjectControlStore`; parity for
point reads, list/range order, owner scopes (`Admin` vs `User`), missing-owner errors, cascade order
(13-table bucket delete, buckets last), zero-write guard rejections, snapshot export/install/replace,
OLAP module >128 KiB chunked round-trip, VPC replace. GREEN: adapter methods pure tree operations;
no raw SQL; no clock/random in replicated writes.

Gate: object-backend variants of `control_store_*_diff` plus golden parity.

### B3. Accounts and quota

Depends: B1 + IAM slice. Files: `control_store_obj/accounts.rs`, `quota.rs`.
Implement the merged account/member/ledger/quota methods (including `account_buckets_from` semantics
and the hard-quota create path).
RED: parity for account CRUD, membership, ledger, quota baseline seeding, dangling-owner failures.
GREEN: all checks inside the caller txn; no SQL fragments.

Gate: `cargo test -p oss-server control_store_obj::accounts`.

### B4. Backend selector and startup wiring

Depends: B1-B3. Files: `crates/oss-server/src/config.rs`, `state.rs`, `db.rs` (façade only),
`control_store.rs` (dispatch).
Add `[server].control_backend = "sqlite" | "objects"` (default sqlite until H1); route
`Db::control_store()`/`control_write()` and `AppState` through the selected backend. Startup asserts
the root mode/markers match the configured mode; a second process fails on the root flock.
RED: both backends pass startup; unknown backend refused; mode mismatch fails closed; no schema
migrations run in object mode. GREEN: dispatch enum; SQLite path unchanged.

Gate: `cargo test -p oss-server backend_select`.

## 3. IAM object domain (after IAM slice)

### C1. IAM tables, uid allocator, guards, cascades

Depends: B1 + IAM slice merged. Files: `control_store_obj/iam.rs`, `iam_snapshot.rs`.
Implement `IamStore/IamTxn/IamSnapshotTxn` over the tree: users keyed by uid + username unique index;
access keys keyed by access_key + owner index; permissions keyed `(uid,bucket)`; accounts/members/
ledger; `next_uid` monotonic and never reused; local admin/session guard exceptions exactly as the
33-guard table; last-active-admin guard reads the overlay; user/bucket cascades emit site hooks in
SQL order; snapshot rows use username natural keys and current predicates.

RED: run the IAM diff/golden suite against objects; every guard exception allowed and every
non-exception rejected with the exact error; guard rejection writes zero bytes; last-admin and
disabled-admin guards; delete cascade order/payloads; uid stability across restart and snapshot
install (preserve by username, new uid for new user, never reuse); session/parent-chain auth
lookups; permissions replace. GREEN: no raw SQL; reads use overlay where required.

Gate: `cargo test -p oss-server control_store_obj::iam` + IAM goldens.

### C2. Auth hot view and per-request snapshot

Depends: C1, B2. Files: `crates/oss-server/src/control_view.rs`, `state.rs`,
`crates/xl-s3/src/auth.rs`, `crates/xl-s3/src/access.rs`, `crates/xl-s3/src/lib.rs`.
Replace `SqlitePool`-based auth/ACL reads with an injected `ControlAuth`/`ControlAcl` trait served by
the hot view. `HotView` holds generation-tagged copy-on-write users, access keys, account status,
buckets, policies, permissions; commit publishes only dirty domains; snapshot install rebuilds all.

RED: existing auth/ACL suite runs against the view with no SQLite; revocation is visible on the next
request; deleted user invalidates all keys; disabled account/expired/parent-chain cases unchanged;
per-request snapshot does not mix generations; property test cache == tree after randomized commits;
no plaintext SK stored; cache memory cap. GREEN: ArcSwap publication from `CacheDelta`; cold domains
read the pinned tree.

Gate: `cargo test -p oss-server control_view` and `cargo test -p xl-s3`.

### C3. IAM snapshot export/install

Depends: C1. Files: `control_store_obj/iam_snapshot.rs`, `raft/data.rs` (adapter only).
RED: IAM control rows serialize byte-identically to goldens; install preserves admin users/admin keys,
wipes non-admin-owned AKs (including session/STS) and admin-target permissions, preserves `set_id`;
reinstall idempotent; inventory/AP owner usernames resolve. GREEN: export/install through the object
txn; formats unchanged.

Gate: `cargo test -p oss-server control_store_obj::iam_snapshot`.

## 4. Jobs, sites, outboxes, audit (after jobs/outbox/sites/audit slice)

### D1. Jobs/runstate domain

Depends: B1 + jobs slice merged. Files: `control_store_obj/jobs.rs`, `jobs_cache.rs`.
Implement `JobStore/JobTxn/JobSnapshotTxn`, root `next_job_id`/`next_run_id`, dedupe uniqueness,
claim/recover/preempt/fence semantics, schedule lease, purge, job-task pages. Build a derived
in-memory due index from table scans and commit deltas; never scan all tasks per claim.

RED: jobs diff/golden suite against objects; claim concurrency (one winner); expired/leased/
preempted recovery; dedupe `INSERT OR IGNORE` rollback semantics; job completion; purge retention;
counters atomic with insert; crash mid-claim. GREEN: all operations in one txn; due index updated
from committed changes only.

Gate: `cargo test -p oss-server control_store_obj::jobs`.

### D2. Site root domain and trigger hooks

Depends: B1, D1, C1. Files: `control_store_obj/site.rs`, `site_apply.rs`,
`crates/oss-server/src/site.rs` (call-site migration).
Implement `site_config`, `sites`, `site_changelog`, `site_entity_clock` in the root; gate/log hooks
for buckets/policies/permissions/users/access keys; peer snapshot insert; `Origin::SiteApply`
explicit clock writes; cursors/dead letters in local files. Replace `apply_ctx`/`raft_apply_ctx` with
origins; delete their startup updates.

RED: 30-trigger golden matrix with fixed clock: each op/entity payload bytes, gate conflict, no-op
updates do not log, dangling owner null, permission entity id char(31), cascade order, admin
excluded, session/AK exclusion; site apply wins/loses and clock update; peer snapshot and cursor
restart; triggers inactive in Raft mode; local write while `sites` empty does not log. GREEN: exact
§5.2 rules; no SQL.

Gate: `cargo test -p oss-server control_store_obj::site` + site goldens.

### D3. Outboxes, audit, uploads, instant index, sync intents

Depends: A6 + jobs slice. Files: `crates/oss-server/src/{events.rs,replication.rs,audit_queue.rs,
aether.rs,sync.rs,db.rs}` (call sites), `local_store/*`.
Replace every raw SQL use of `event_outbox`, `repl_outbox`, `audit_log`, `uploads`, `instant_index`,
`sync_*_intents`, `account_usage` with the local store. Queue claim/lease/retry/dead-letter
semantics unchanged; audit retention; uploads owner uid remains numeric (uids never reused).
RED: existing worker/queue tests run without SQLite; crash after send before ack redelivers; dead
letter after attempts; compaction under load; audit query/retention; upload completion/expiry; sync
intent recovery. GREEN: no SQL; local files are the only store.

Gate: `cargo test -p oss-server local_store` and event/replication/audit suites.

### D4. Remaining quota/account call sites

Depends: B3, D1. Files: `control_store_obj/quota.rs`, `accounts_usage.rs`, call sites.
Move the last raw-SQL account/quota fragments (the D7 whitelist) into txn methods; account usage
cache reads/writes through the local cache.
RED: quota parity on create/delete/ownership transfer; account usage recompute; dangling account
fail-closed. GREEN: no SQL.

Gate: `cargo test -p oss-server control_store_obj::quota`.

## 5. Cache and policy integration

### E1. Hot view on the request path

Depends: B2-B4, C2, D1. Files: `control_view.rs`, `state.rs`, `routes*`, `xl-s3` call sites.
Every request captures one `RequestView { root_generation, Arc<HotView> }`; auth, ACL, bucket
lookup, versioning/lock/encryption, event/replication config and site gate predicates use it.
RED: end-to-end S3 and console suites with SQLite disabled at runtime; write-then-read same node;
concurrent reader during commit sees old or new only; auth revocation immediate; memory cap.
GREEN: no per-request tree traversal on hot paths.

Gate: `cargo test -p oss-server` + `cargo test -p xl-s3` + e2e smoke.

### E2. Guard and site policy matrix

Depends: D2. Files: `crates/oss-server/tests/control_guards.rs`,
`crates/oss-server/tests/control_site_triggers.rs`.
Table-driven tests for all 33 guard cells and 30 trigger cells, including origins, mode switches,
and zero-write rejections. Green only against the frozen goldens from A8.

Gate: `cargo test -p oss-server --test control_guards --test control_site_triggers`.

## 6. Raft on object state and files

### F1. Object state machine

Depends: E1, E2. Files: `crates/oss-server/src/raft/object_sm.rs`, `raft/apply.rs` (types),
`raft/runstate/store.rs` (adapter).
Implement `RaftStateMachine` with the object root: per-entry commit, feature/activation markers,
applied LogId/membership, runstate install, business-rejection commit, internal-error no-commit.
Witness uses a metadata-only state machine (`witness_sm.rs`) storing applied/membership only.

RED: existing state machine suite against `ObjectStateMachine`; per-entry failure leaves prior
entries applied; replay idempotent; marker+data same root generation; business rejection advances
applied; membership-only entry; witness has no control DB and reports membership.
GREEN: no SQLite state machine in object mode.

Gate: `cargo test -p oss-server raft::object_sm`.

### F2. Raft file storage wiring and recovery

Depends: A7, F1. Files: `raft/mod.rs`, `raft/identity.rs`, `witness.rs`, `raft/cutover/recover.rs`,
`cli.rs`, `config.rs`.
Wire `FileLogStore`/`FileSnapshotStore`; storage dir `<db_path>.d/`; identity checks log state and
root applied/membership; cutover/recover reads/writes files; witness uses its metadata file; config
validation rejects `state_db` except as migration input.

RED: 3-node in-process cluster boots/elects/replicates on files; follower kill/restart catch-up;
snapshot transfer to empty node; log truncate/purge/restart; witness; kill9 at append/meta boundaries;
legacy `raft.db` refuses with actionable message; no `raft.db` opened.
GREEN: openraft traits over files; no SQL.

Gate: `cargo test -p oss-server raft::file` + cluster kill9 suite.

### F3. Snapshot export/install parity and crash matrix

Depends: F1. Files: `raft/data.rs`, `raft/runstate/*`, `control_store_obj/*_snapshot.rs`,
`tests/snapshot_parity.rs`.
RED: OSSCTL1/2/3 and `m8.runstate` golden install/export byte equality through objects; feature-level
accept/refuse matrix; admin/set_id/local preservation and non-admin key/permission wiping exactly as
today; install crash before/after root switch; snapshot with OLAP module chunks; leader/follower
export at pinned applied id. GREEN: single root CAS install.

Gate: `cargo test -p oss-server --test snapshot_parity`.

## 7. Migration and cutover

### G1. Offline migrator

Depends: F3, A6, A7. Files: `crates/oss-server/src/cli/control_migrate.rs`,
`crates/oss-server/src/migrate/{mod,sqlite_source,object_sink,local_sink,raft_sink,report}.rs`.
Reads stopped `oss.db` (and `raft.db`) through the existing SQLite code, writes object root last,
local files, and `<db_path>.d/` files. Modes `--dry-run`, `--apply`, `--json`; no server start; opens
the source read-only; rechecks counts and hashes; never leaves a partially visible root.
RED: fixture SQLite DB with every table → dry-run report; apply → boot new backend; interrupted apply
before root leaves no visible state; rerun after interruption is safe; source checksum unchanged;
Raft fixture preserves vote/committed/last index/log bytes/snapshot.
GREEN: implement sinks over A1-A7; SQLite reads only in the migrator.

Gate: `cargo test -p oss-server migrate::`.

### G2. Verify and recertification reuse

Depends: G1. Files: `migrate/verify.rs`, `cli/control_verify.rs`.
Verification: export from object root with the pre-removal encoders and compare byte-for-byte with the
SQLite export; per-table row counts and canonical hashes; root CRC/reachability/allocator bounds;
site clock/seq sanity; local table counts; Raft log checksums. Run the existing Phase 0 cluster
recertify tool before export and fail migration if any legacy control key is uncertified.
RED: corrupted node/blob/root/raft entry detected with a precise report; recertification required;
report has stable JSON schema. GREEN: reuse `recertify_system_object_cluster`; no new crypto.

Gate: `cargo test -p oss-server migrate::verify`.

### G3. Cutover rehearsal, backup/restore, performance

Depends: G2. Files: `tests/e2e_object_control.sh`, `tests/e2e_object_control_cluster.sh`,
`scripts/bench_control_store.sh`, `docs/superpowers/runbooks/2026-11-05-object-control-cutover.md`.
Rehearse standalone and 3-node Raft: populate SQLite, stop, backup, migrate, verify, start object
backend, run S3/console/site smoke, crash/restart, snapshot transfer, rollback-before-write test.
Measure the design §4 budgets against the SQLite baseline on the reference host.
RED: scripted assertions fail on a dropped row, changed export byte, missing local table, missing Raft
entry, or budget regression. GREEN: scripts pass; record results in the gate report.

Gate: both e2e scripts + benchmark thresholds.

## 8. SQLite removal and hard gate

### H1. Remove SQLite

Depends: G3 signed. Files: delete `crates/oss-server/migrations/`, `crates/oss-server/src/db.rs` SQL
bodies, `crates/oss-server/src/control_store*.rs` SQLite adapters, `xl-s3` sqlx pool/plumbing,
`raft/store.rs` SQLite impls; remove `sqlx`/`sqlite` from all `Cargo.toml`/lock; convert differential
tests to goldens; delete `schema_meta`/`runstate_meta`/outbox/site SQL paths; delete `apply_ctx`/
`raft_apply_ctx` remnants. Keep the DB façade types only where they are backend-neutral names; rename
them in the same commit so no `Sqlite*` symbol survives.

RED: `cargo tree -i sqlx` empty; `rg` guards; full test suite. GREEN: delete in dependency order:
migrator artifact first (archive the pre-removal binary), then server, then xl-s3.

Gate: all seven gates + `cargo tree -i sqlx` empty.

### H2. Final no-database E2E and acceptance report

Depends: H1. Files: `tests/e2e_no_db.sh`, `docs/superpowers/plans/2026-11-05-object-control-gate.md`
(gate record only; generated at execution, not now), CI workflow.
Assert fresh standalone/site/Raft/witness startups create no `*.db`; full E2E, kill9, snapshot
transfer, site replication, auth/console smoke; CI guards reject `sqlx`, `sqlite`, `oss.db`, `raft.db`,
`state.db`, `SqliteControl*`, `migrations/` outside docs and the archived migrator. Record the
acceptance matrix from design §9 and the benchmark numbers.

Gate: CI green + signed gate record.

## 9. Rollback and sequencing constraints

- Each task lands green; the SQLite backend remains the default until H1. No task may leave both
  backends disagreeing in tests.
- A8 goldens must be captured from the merged last-SQLite commit; they are the permanent oracle.
- G1/G2 must be rehearsed on a copy with byte-level verification before any production cutover. The
  migration binary is archived before H1 because the final workspace no longer links SQLite.
- Cutover is one-way. Before the first post-cutover write, rollback = restore the pre-cutover SQLite
  backup and old binary. After it, rollback = restore that backup and accept loss of writes since
  cutover; there is no in-binary reverse writer once SQLite is removed.
- All Raft voters and witnesses migrate together; mixed old/new clusters and rolling upgrades remain
  forbidden. A new node refuses a legacy `raft.db` until migrated; an old node cannot read `.d/`.
- Witness promotion to voter requires a full restart with a data dir and snapshot install; document
  this in the runbook before H1.

## 10. Explicitly deferred / not built

- No runtime dual-write or live dual-read; offline byte comparison only (design §6.2).
- No secondary B+tree index unless a query cannot be served by a primary-key range, cache or derived
  queue; the job due index and IAM natural-key indexes are the only derived ones listed.
- No retention/compaction of `site_changelog`; its unbounded growth is pre-existing and monitored.
- No witness-to-voter promotion in place; it requires restart with data dir and snapshot.
- No migration back to SQLite inside the final binary; the archived pre-removal binary remains the
  migration/rollback artifact.
