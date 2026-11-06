# Object control store — final no-database gate record

Status: **DRAFT / H2 template, rehearsed on the pre-H1 tree (2026-11-06)**. Default mode records
known pre-H1 gaps as `PENDING-H1`; `STRICT_NO_DB=1` (or `--require-no-sqlite`) enforces every item.
The signed record is generated at execution after H1
(`docs/superpowers/specs/2026-11-05-object-control-store-design.md` §9; implementation plan §8 H2).

## Gate identity

| Field | Value |
|-------|-------|
| Gate | H2 final no-database acceptance |
| Candidate commit | `ef41301` (v1.31.0) + H2 gate artifacts; commit to be filled at execution |
| Dirty-worktree snapshot | H2 artifacts only (`git status --porcelain`: CI workflow, guard script, e2e, helpers, doc) |
| Binary | `target/debug/oss-server` sha256 `2c3578fb0da806a9ccb55ca6c13873db4d9c9467f885acff9d51e2172831685d` |
| Reference host | Linux 6.6.155-amd64-desktop-hwe x86_64, 8 vCPU, debug build, local single disk |
| Operator / reviewer | H2 rehearsal / `<fill at execution>` |
| Verdict | **PASS in default mode** (125 pass / 0 fail / 2 pending); **PENDING-H1 in strict mode** |

## Seven gates

Design §9 items 1-7. A gate is PASS only with its evidence command recorded and reproducible from the
candidate commit. Current values are the pre-H1 rehearsal, not the signed result.

| # | Gate | Evidence (command / artifact) | Current status | Misses / notes |
|---|------|-------------------------------|----------------|----------------|
| 1 | No SQLite startup state | `tests/e2e_no_db.sh` sections 1-4: fresh standalone, site, 3-node Raft + witness; migrated standalone, site pair, 3-node Raft + witness; `*.db*` find + `/proc/<pid>/fd` scan | **PASS (default mode)** | `database_url` points at a never-created directory; no `sqlite`/`etilqs` fds. The object backend still uses a `sqlite::memory:` placeholder pool, so unported raw-SQL writes fail with "no such table"; see known gaps. |
| 2 | Dependency and symbol removal | `scripts/check_no_db.sh`; `cargo tree -i sqlx` | PENDING-H1 | Guard: 5,074 matches (see evidence). `cargo tree -i sqlx` still succeeds. `STRICT_NO_DB=1` fails by design. |
| 3 | Full suite + crash matrix | `cargo test --workspace --locked --no-fail-fast`; `tests/e2e_no_db.sh`; kill9 matrix | PENDING-H1 (full suite), PASS (kill9 smoke) | Default e2e restarts a migrated standalone and a Raft voter after `kill -9`; full seven-gate suite and the 100-iteration commit-phase crash matrix run at execution. |
| 4 | Golden parity | frozen goldens from the last-SQLite commit; OSSCTL1/2/3 and `m8.runstate` byte equality | PENDING-H1 | 16 `control_store_*_diff.rs` suites must convert to frozen goldens; object-backend snapshot parity is already covered by F1/F3 tests. |
| 5 | Concurrency | two writers one key; apply vs local facade; stale fence; `Indeterminate` re-read | PENDING-H1 | Owned by E2/F1 suites; no H2-specific regression expected. |
| 6 | Performance budgets | `scripts/bench_control_store.sh` (G3 run, debug binary) | PARTIAL / known misses | See benchmark table. |
| 7 | Security and operability | `.oss.sys` not readable/listable via S3/console; backup/restore and migration runbook rehearsed; metrics/alarms; no plaintext secrets | PENDING-H1 | Runbook `docs/superpowers/runbooks/2026-11-05-object-control-cutover.md` still to be written. |

## Acceptance matrix (design §9)

| # | Acceptance item | Evidence | Current result | H1 action |
|---|-----------------|----------|----------------|-----------|
| 1 | Fresh standalone, site, 3-node Raft (voter + witness) create no `oss.db`, `raft.db`, `state.db` or any SQLite file | `tests/e2e_no_db.sh` §1-§4 | **PASS (default)**, strict pending guards | Re-run strict; expect 0 `*.db*`, 0 SQLite fds. |
| 2 | Full E2E suite | `<record command + output at execution>` | PENDING-H1 | Run `cargo test --workspace --locked` plus the existing e2e scripts on the post-H1 tree. |
| 3 | kill -9 restart | `tests/e2e_no_db.sh` §2 and §4 | **PASS** | Repeat with the 100-iteration commit-phase crash matrix. |
| 4 | Snapshot transfer | `tests/e2e_no_db.sh` §4: empty node 3 joins by snapshot after purge | **PASS**, with pre-H1 caveat | Must also pass without the "one live write after migration" workaround; the applied-LogId bug is listed below. |
| 5 | Site replication | `tests/e2e_no_db.sh` §3: pair both ways, ping, control apply, object ship | **PASS**: pair both ways, ping, control apply visible on peer, versioning + data ship | — |
| 6 | Auth / console smoke | `tests/e2e_no_db.sh` §2-§4: console login/list/create user+bucket, S3 bad-key reject, put/get/head/list, user login on joined voters | **PASS** for migrated and object-native admins; snapshot-joined voter login and console access-key creation pass | — |
| 7 | Benchmark numbers | `scripts/bench_control_store.sh` G3 run | Known misses below | Re-run release build; budgets must be met. |

### Rehearsal evidence (2026-11-06)

- `tests/e2e_no_db.sh` (default, 2026-11-06 final): `PASS=125 FAIL=0 PENDING=2 SKIP=0`, exit 0.
  Only the two H1 workspace guards remain pending; site control replication, versioning/data ship,
  object-native admin bootstrap and snapshot-joined admin login now pass.
- `scripts/check_no_db.sh`: exit 1, `files=702 manifests=7`; dependency 27, `sqlx` 2245, `sqlite`
  2017, DB filenames 203, `SqliteControl*` 563, `migrations/` 18, migrations directory 1 —
  `BLOCKED: 5074 SQLite match(es)`.
- `tests/e2e_object_control_cluster.sh` (G3 canonical): `PASS=70 FAIL=0`, exit 0 — the new gate
  reproduces its snapshot/witness coverage.

### Resolved since the first run (2026-11-06)

Gaps 1–5 below were fixed before the final run: console access-key creation and the db façade
writes (`51cf409`, `68d201e`), site hooks wired into domain writes (`e73f58f`), xl-s3 bucket
versioning/notification/lock writes through the seam (`e91e81c`, `47b76f4`), and object-native
admin bootstrap (`0505810`, `2199bd4`). Gap 6 still needs the e2e's single live-write workaround;
gap 7 is the H1 deletion itself.

## Known pre-H1 gaps surfaced by the gate

These are counted as `PENDING-H1` by `tests/e2e_no_db.sh`; `STRICT_NO_DB=1` turns each into a
failure. None of them was weakened to make default mode green.

| # | Gap | Evidence | H1 action |
|---|-----|----------|-----------|
| 1 | Object console access-key creation is still a raw SQLite write | `crates/oss-server/src/db.rs:1819` `create_access_key` → `SqliteControlTxn` on the in-memory placeholder; endpoint logs `no such table: access_keys` and returns 500 | Route access-key creation through the control seam (part of the DB façade removal). |
| 2 | Object site control writes emit no changelog | `site_hook_*` handlers exist in `crates/oss-server/src/control_store_obj/site_apply.rs`, but `grep site_hook_` finds no production call site; a bucket/user created after pairing never appears on the peer | Call the hooks from the object bucket/user/access-key/policy write paths in the same CAS. |
| 3 | Object S3 bucket-versioning write is raw SQL | `s3api put-bucket-versioning` logs `写桶版本控制状态失败 ... no such table: buckets`, HTTP 500 | Port the `xl-s3` versioning write (xl-s3 SQL item in the H1 checklist). |
| 4 | Object site data ship is not exercised | Blocked by gap 3: without versioning no `repl_outbox` row is enqueued (`repl_outbox.log` length 0), so the peer never receives the object | Enable versioning through the seam, then assert outbox enqueue + peer PUT. |
| 5 | Admin bootstrap is skipped for object nodes | `crates/oss-server/src/state.rs:666` bootstraps only when `!object_backend`; a fresh object root has no admin, and snapshot-joined voters return 401 for the seeded admin | Add object-native admin bootstrap (env or first-run command) and a non-SQLite seed path in the e2e. |
| 6 | Migrated object Raft state machine stores an applied LogId with leader node 0 | Startup: `last_applied=T1-N0-2` vs `committed=T1-N1-2`; adding a voter before any post-migration live command trips openraft `invalid state: purge_upto ... <= snapshot_last_log_id` and kills the Raft core | Persist the applied LogId with its real leader id (or repair it during migration); remove the e2e's single live-write workaround. |
| 7 | Workspace still links and references SQLite | Guard counts in the evidence above; `cargo tree -i sqlx` non-empty | Delete `sqlx`/`libsqlite3-sys`, `SqliteControl*`, migrations and the SQL bodies; make `no-db-guard` blocking. |

## Benchmarks (current values)

Recorded from the G3 benchmark run (`scripts/bench_control_store.sh`) on the reference host, debug
binary, single node. Values are quoted from the G3 lane and are not re-measured by H2.

| Metric | Current | Design §4 target | Verdict |
|--------|---------|------------------|---------|
| Control write p99 | ~0.64 s | ≤ 20 ms (single drive) / ≤ 50 ms (multi-drive quorum) | **KNOWN MISS** — pre-H1, debug binary |
| Control write throughput | 23.7 /s | ≥ 200 commits/s | **KNOWN MISS** — pre-H1, debug binary |
| Snapshot import (100k rows) | 8.3 s | ≤ 10 s | within budget |
| Snapshot export (100k rows) | 1.5 s | ≤ 10 s | within budget |
| Startup hot-cache load (100k users/AKs/buckets) | 1.0 s | ≤ 5 s | within budget |

Both misses must be re-measured on the H1 release build before gate 6 can pass. No budget was
weakened to make this table green.

## Rollback

- Archive the **pre-removal binary** before H1: `archive/pre-h1/oss-server` (outside the workspace
  guard's scan). It is the only binary that still links SQLite and can read `oss.db`/`raft.db`.
- **Before the first post-cutover write:** rollback = stop all nodes, restore the pre-cutover
  `oss.db`, `raft.db` and data-dir backup, start the archived binary. The object root is ignored.
- **After the first post-cutover write:** rollback = restore that backup and accept loss of writes
  since cutover. The final binary has no reverse writer.
- All voters and witnesses migrate together. Mixed old/new clusters and rolling upgrades are
  forbidden. A new node refuses a legacy `raft.db` until migrated; an old node cannot read `.d/`.
- Witness-to-voter promotion requires a full restart with a data dir and snapshot install.

## H1 checklist (from the SQLite audit)

| Area | Current footprint | Required change |
|------|-------------------|-----------------|
| `xl-s3` SQL | `auth.rs` 30 + `access.rs` 117 + `sts.rs` 18 + `ap.rs` 5 `sqlx` refs / pools; bucket-versioning write also raw SQL | Replace `SqlitePool` plumbing with the backend-neutral auth view and control seam; no `sqlx` in `xl-s3`. |
| DB façade | `crates/oss-server/src/db.rs` 7,287 lines, 229 `sqlx::` refs; `create_access_key` raw `SqliteControlTxn` | Delete SQL bodies; keep backend-neutral façade types/method names; remove `apply_ctx`/`raft_apply_ctx` remnants. |
| Control-store adapters | `SqliteControlStore`, `SqliteControlTxn`, `SqliteControlBackend`, `SqliteQuota`; 563 refs incl. 16 diff tests | Delete the SQLite adapters; rename backend-neutral façade types so no `Sqlite*` symbol survives. |
| Site hooks | Object `site_hook_*` handlers implemented but not called by production write paths | Call the hooks from object bucket/user/access-key/policy writes inside the root CAS. |
| Raft SQLite stores | `raft/store.rs` `SqliteLogStore`/`SqliteStateMachine` (80 refs), `raft/runstate/store.rs` (6 `sqlx`), `identity.rs`, `compat.rs`, `cutover/`, `apply/quota.rs`, `apply/tier_restore.rs` | Use the file log/object state machine on every path; delete SQLite stores and one-shot compat readers. |
| Raft applied LogId | Migrated object state machine reports `T1-N0-*` applied vs `T1-N1-*` committed; snapshot before a live write panics | Persist/repair the real applied LogId and add a migration regression test. |
| Admin bootstrap | `state.rs` seeds `OSS_ADMIN_USER` only when `!object_backend` | Object-native bootstrap (env or first-run command) and a SQLite-free e2e seed. |
| Migrations | `crates/oss-server/migrations/` (12 `.sql` files) and `include_str!` sites in `db.rs` | Delete the directory and the includes. |
| Tests | 16 `crates/oss-server/tests/control_store_*_diff.rs`; SQLite fixtures (`raft_s71`, `raft_quota_ledger`, migrator fixtures) | Convert differential suites to frozen goldens captured from the last-SQLite commit; convert or delete SQLite-fixture tests; keep the `raft_guard` source auditor. |
| Cargo | workspace `Cargo.toml:72`, `crates/oss-server/Cargo.toml:49`, `crates/xl-s3/Cargo.toml:23`, `Cargo.lock` | Remove `sqlx`; regenerate the lock; `cargo tree -i sqlx` empty. |
| Guard / CI | `scripts/check_no_db.sh`, `.github/workflows/ci.yml` `no-db-guard` job (continue-on-error) | Guard exits 0; remove continue-on-error/TODO, require the job, re-enable push/PR triggers. |
| Archive / rollback | `archive/pre-h1/oss-server` not yet present | Archive the pre-removal binary and rehearse restore before H1. |

## Sign-off

| Role | Name | Date | Signature / commit |
|------|------|------|--------------------|
| H1/H2 owner | | | |
| Reviewer | | | |
| Release approver | | | |
