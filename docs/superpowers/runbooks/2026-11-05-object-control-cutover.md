# Object control cutover runbook (SQLite → object control root)

Status: **draft, sign at release**. Applies to the one-way cutover that removes SQLite in v1.32.0.
The cutover tool is the archived v1.31.0 binary; the v1.32.0 binary has no SQLite code and no in-tree
migrator, and rejects `control-migrate` as a removed operation.

Design: `docs/superpowers/specs/2026-11-05-object-control-store-design.md` §6. Gate record:
`docs/superpowers/plans/2026-11-05-object-control-gate.md`.

## Artifacts

| Artifact | Value |
|---|---|
| Cutover binary | `oss-server-v1.31.0-linux-x86_64` from GitHub release `v1.31.0` |
| URL | `https://github.com/erikwang2013/oss/releases/download/v1.31.0/oss-server-v1.31.0-linux-x86_64` |
| sha256 | `fc2e96fd0eb1777724da7b9ad07b4ad6520da7373bc9780099d8a211560b1ae` |
| Target binary | v1.32.0 or later (object control root only) |
| Source files | `oss.db` (control DB), `raft.db` (Raft DB), data drives |
| Target files | `<data_dir>/.oss.sys/control/` (root/nodes/blobs), `<data_dir>/.oss.sys/local/`, `<raft.db_path>.d/` |

`control-migrate` reads the SQLite sources read-only, verifies the source checksum before and after,
writes local operational files and Raft files first, and commits the object root last in one CAS. It is
idempotent: a rerun after a successful apply verifies the existing root (`status=already-applied`).

## Preconditions

- All nodes are on the same v1.31.0 deployment and the same feature level. All Raft voters and
  witnesses migrate together; mixed old/new clusters and rolling upgrades are forbidden.
- A maintenance window covers the whole cluster. Clients are drained; no uploads, jobs, or site
  replication are in flight.
- The archived binary was downloaded and its sha256 verified on every node.
- Free space on every data drive is at least the current control DB size plus the expected object root
  (the source is not modified, so budget 2× the control DB and Raft DB).
- The old binary and the backup are retained until the acceptance window closes.

## 1. Stop all nodes

1. Remove every node from the client load balancer / DNS and wait for in-flight requests to drain.
2. Stop every oss-server process, including Raft voters and witnesses. Do not leave a writer running
   against `oss.db` or `raft.db`.
3. Confirm no process holds the files:

   ```bash
   pgrep -af oss-server
   fuser -v /path/to/oss.db /path/to/raft.db
   ```

4. Keep the console, metrics, and site endpoints down for the duration of the cutover.

## 2. Back up the SQLite state

Use the real paths from the old configuration (`database_url`, `[raft].db_path`, `data_dir` /
`data_dirs`). Run on every node:

```bash
ts=$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "/srv/oss-backup/$ts"
cp -a /path/to/oss.db /path/to/raft.db /srv/oss/data "/srv/oss-backup/$ts/"
sha256sum /path/to/oss.db /path/to/raft.db > "/srv/oss-backup/$ts/SHA256SUMS"
sqlite3 /path/to/oss.db 'PRAGMA integrity_check;'
sqlite3 /path/to/raft.db 'PRAGMA integrity_check;'
```

Both integrity checks must print `ok`. Record the checksums in the change ticket. The backup is the
only full rollback path; keep it until acceptance closes.

## 3. Download and verify the cutover binary

```bash
asset=oss-server-v1.31.0-linux-x86_64
curl -fL --retry 3 -o "$asset" \
  "https://github.com/erikwang2013/oss/releases/download/v1.31.0/$asset"
echo "fc2e96fd0eb1777724da7b9ad07b4ad6520da7373bc9780099d8a211560b1ae  $asset" | sha256sum -c -
chmod +x "$asset"
```

A checksum mismatch is a hard stop: do not run the binary. Verify the release tag and asset name
against the GitHub release page.

## 4. Dry-run on every node

```bash
./"$asset" control-migrate \
  --from /path/to/oss.db \
  --state-dir /srv/oss/data \
  --raft-db /path/to/raft.db \
  --dry-run --json > /srv/oss-backup/migrate-dry-run.json
```

`--dry-run` opens no target path. Check the JSON report:

- `status` is `dry-run`;
- `source_sha256` matches the backup checksum;
- `sqlite_table_counts` and `control_table_counts` are non-zero for the expected tables;
- `feature` is consistent with the old deployment;
- `warnings` is reviewed (for example, audit rows carrying account ids are dropped because the
  local-store audit schema has no such field).

If the report shows a missing table, an unknown higher feature level, or a count that does not match
the old system, stop and investigate before applying.

**Multi-drive nodes.** `--state-dir` is one drive root, and control objects are full-meta on every
local drive (not EC-striped). For a node with `data_dirs`, run the tool once per data drive, using that
drive as `--state-dir`, with the same `--from` and `--raft-db`. The first data dir is also the server's
local-state root; duplicate `local/` copies on the other drives are harmless. Run `--dry-run` for every
drive before any `--apply`.

## 5. Apply on every node

```bash
./"$asset" control-migrate \
  --from /path/to/oss.db \
  --state-dir /srv/oss/data \
  --raft-db /path/to/raft.db \
  --apply --json > /srv/oss-backup/migrate-apply.json
```

The command fails closed if the source checksum changes during the run, if a target object does not
match, or if the root format is unsupported. Verify:

- `status` is `applied` (or `already-applied` on a rerun);
- `object.root_hash`, `object.snapshot_sha256`, and `object.runstate_sha256` are present;
- `local` and `raft` reports are present and their counts match the dry-run;
- every table count matches the dry-run;
- `/path/to/oss.db` and `/path/to/raft.db` still match `SHA256SUMS`;
- `<data_dir>/.oss.sys/control/state/root`, `<data_dir>/.oss.sys/local/`, and
  `<raft.db_path>.d/` exist.

If a drive needs to be retried, rerun the same command; do not delete a partially written object
root. Orphans are harmless and are collected by the target binary's GC.

## 6. Start the v1.32 binary

1. Update `oss.toml` on every node: remove `database_url`, `[server].control_backend`, `[cluster]`,
   and `[stretch]`. Keep `data_dir`/`data_dirs`, `[site]`, `[raft]`, and `[raft].db_path` (the new
   binary uses `<db_path>.d/`). If the old `[raft].state_db` is still present, remove it.
2. Start the v1.32 binary on every node, voters first.
3. Confirm startup fails closed if it sees a legacy `raft.db` that was not migrated.

## 7. Smoke tests

Run on at least one standalone or site node and on the Raft leader:

```bash
curl -fsS http://127.0.0.1:9000/health/ready
token=$(curl -s -X POST http://127.0.0.1:9000/api/v1/login \
  -H 'content-type: application/json' \
  -d '{"username":"<admin>","password":"<password>"}' |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')
curl -fsS http://127.0.0.1:9000/api/v1/users -H "authorization: Bearer $token"
aws --endpoint-url http://127.0.0.1:9000 s3api list-buckets
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://cutover-smoke
aws --endpoint-url http://127.0.0.1:9000 s3 cp /etc/hostname s3://cutover-smoke/hostname
aws --endpoint-url http://127.0.0.1:9000 s3api head-object --bucket cutover-smoke --key hostname
```

- **Auth / console**: admin login, user list, bucket list, access-key list.
- **S3**: create bucket, put/get/head/list object; delete the smoke bucket afterwards.
- **Site** (if configured): `GET /api/v1/sites`, ping each peer, verify control apply reaches the peer.
- **Snapshot**: `GET /api/v1/raft/status` reports the expected leader/applied index;
  `GET /metrics | grep oss_raft_snapshot_bytes` is non-zero after a snapshot. For a real install test,
  add an empty voter or witness and confirm it installs from snapshot and serves auth.
- **No SQLite**: `find <data_dir> -name 'oss.db' -o -name 'raft.db' -o -name 'state.db'` is empty; the
  process fd table contains no SQLite files.

If any smoke test fails, stop and use the rollback below before accepting writes.

## 8. Accept and keep the backup

The cutover is irreversible at the first accepted client write after the new binary starts. Keep the
backup and the archived binary until the acceptance window closes (at least one full backup cycle and
the agreed observation period).

## Rollback

**Before the first new write**

1. Stop every v1.32 node.
2. Restore `oss.db`, `raft.db`, and the data drives from the cold backup. If a drive cannot be restored
   in place, restore the whole data directory, not just `.oss.sys`.
3. Start the old v1.31.0 binary (or the archived cutover binary) on the restored state.
4. Confirm auth, S3, and (if configured) site/raft status. Remove `.oss.sys/control` and
   `<raft.db_path>.d/` only after the restore is verified; they are not read by the old binary.

**After the first new write**

There is no reverse writer. Rollback means restoring the cold backup and accepting loss of every
write since cutover; `.oss.sys/control` and `<raft.db_path>.d/` are discarded with the restore. If the
post-cutover writes cannot be lost, the only option is to stay forward and fix the issue on the new
binary.

**Mixed versions are not a rollback path.** All voters and witnesses migrate together. A new binary
refuses a legacy `raft.db` without migration; an old binary cannot read `<raft.db_path>.d/`.

## Failure handling

| Symptom | Action |
|---|---|
| Dry-run count/feature mismatch | Stop; compare old schema/feature level; do not apply |
| Apply fails on source checksum change | Stop all writers; restore source from backup; rerun dry-run |
| Apply fails on a target mismatch | Do not delete the root; rerun `--apply`; if it persists, restore the backup and escalate |
| Local/Raft sink written, root not committed | Rerun `--apply`; idempotent; old generation remains authoritative |
| New binary refuses startup: root mode mismatch | Check the old deployment mode (standalone vs Raft); restore the correct backup and retry the migration with the matching deployment |
| New binary refuses startup: legacy `raft.db` | The Raft sink did not complete; rerun `--apply` with the correct `--raft-db` |
| Quorum unmet on a multi-drive node | Verify every local drive has a certified root; rerun `--apply` per missing drive |
| Smoke auth fails | Stop; rollback before accepting writes; inspect `root_hash`/table counts in the apply report |

## References

- Design §6 (snapshot, migration, cutover) and §9 (acceptance):
  `docs/superpowers/specs/2026-11-05-object-control-store-design.md`
- H2 gate record: `docs/superpowers/plans/2026-11-05-object-control-gate.md`
- Release asset: GitHub release `v1.31.0`, `oss-server-v1.31.0-linux-x86_64`
