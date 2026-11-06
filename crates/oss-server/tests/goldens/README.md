# SQLite control-plane goldens (task A8) — frozen

Permanent byte-level oracle captured from the SQLite-backed control plane at
v1.31.0, before H1 removed SQLite. The tree no longer contains a capture target:
the goldens are frozen and must not be edited or regenerated. Every file is
generated output; do not hand-edit.

## Provenance

`scripts/capture_control_goldens.sh` ran the ignored test
`golden_capture::capture_control_goldens` against the last SQLite commit and
rewrote `MANIFEST` (sorted `<sha256>  <path>` lines). Both the script and the
capture target were deleted in v1.32.0 with the SQLite control plane.

| Path | What it freezes |
| --- | --- |
| `rows/` | One JSON value per frozen row type (serde snapshot rows and non-serde local rows). |
| `snapshots/export_*.bin` | OSSCTL1/2/3 and `m8.runstate` export envelopes, byte-exact. |
| `snapshots/install_*.bin` | Re-export after installing the matching export into a fresh control state machine; asserted byte-identical to the export. |
| `snapshots/runstate_extension.*` | `SnapshotExtension {version:1, payload_b64}` and its decoded JSON. |
| `site_triggers/gate/*.json` | All 15 mutations rejected by a future site clock; exact error text. |
| `site_triggers/log/*.json` | The same 15 mutations after clearing the clock: exact trigger payload bytes (including quote/backslash/control-character escaping), clock tuple, cascade order where applicable. 30 cases total. |
| `site_triggers/exclusions.json` | Admin user, session key, admin-owned key, admin-target permission never log. |
| `site_triggers/apply_suppression.json` | `apply_ctx.active=1` suppresses site triggers; resetting restores logging. |
| `site_triggers/raft_mode_inactive.json` | Raft mode has 0 site triggers and 33 raft guards; apply writes log nothing. |
| `site_triggers/user_delete_cascade.json` | User delete child-row/changelog order. |
| `guards/matrix.json` | 11 tables × INSERT/UPDATE/DELETE = 33 cells, each denied outside apply with `raft: control write outside apply`, allowed inside; plus 12 local exceptions. |
| `cascade/bucket_delete_order.json` | Production `ControlCommand::BucketDelete` order across 13 dependent tables (trace triggers) plus `buckets` last, survivor counts, changelog order. |
| `cascade/install_predicates.json` | OSSCTL1 install keeps `buckets.set_id` and admins/admin AKs; deletes non-admin AKs (incl. session), clears admin-target permissions and non-admin upload owners, removes target-only buckets. |

## Determinism and normalization

All ids, site ids, clocks fed to the gate, fixture timestamps, and user/bucket/
policy/AK values are fixed literals. The only nondeterministic columns are the
SQLite `strftime('%s','now')` values written by the site triggers:

- `site_changelog.ts`
- `site_changelog.created_at`
- `site_entity_clock.ts`

They are captured as `0` (the gate probe itself is recorded as
`"now+3600 (normalized)"`). No other field is excluded. Rows are ordered by
primary key/natural key in every query, and JSON is written with serde's stable
struct field order (or `serde_json::Map` insertion order).

Capture never touches the network: it builds on local fixtures, uses only
`127.0.0.1` placeholder endpoints, and runs Cargo with `--offline`.

## Verify

```bash
cargo test -p oss-server --test golden_manifest   # existence, hashes, no extras
```

Regeneration would require reconstructing the v1.31.0 SQLite build; the
goldens intentionally stay at that byte-exact snapshot.
