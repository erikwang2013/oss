#!/usr/bin/env bash
# G3 standalone cutover rehearsal: SQLite fixture → backup → control-migrate →
# object backend (standalone) → S3/console/site smoke → restart → rollback.
#
# Assertions are the design §6/§9 contract, not the current implementation:
# a red check is a real blocker and is reported as such (see the result table).
#
# Prereq: cargo build -p oss-server; aws-cli; curl; python3; sqlite3.
# Usage: OSS_BIN=target/debug/oss-server tests/e2e_object_control.sh
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${OSS_BIN:-$PWD/target/debug/oss-server}"
[ -x "$BIN" ] || { echo "missing oss-server binary (cargo build -p oss-server, or set OSS_BIN)"; exit 2; }
BIN=$(realpath "$BIN")
for c in aws curl python3 sqlite3 sha256sum; do command -v "$c" >/dev/null || { echo "need $c"; exit 2; }; done

PASSES=0; FAILS=0; SKIPS=0
pass() { PASSES=$((PASSES+1)); echo "  PASS: $*"; }
fail() { FAILS=$((FAILS+1)); echo "FAIL: $*"; }
skip() { SKIPS=$((SKIPS+1)); echo "  SKIP: $*"; }
ck() { if "$@"; then pass "$*"; else fail "$*"; fi; }
ck_eq() { if [ "$1" = "$2" ]; then pass "$3"; else fail "$3 (got '$1', want '$2')"; fi; }
finish() {
  echo "== result =="
  echo "PASS=$PASSES FAIL=$FAILS SKIP=$SKIPS"
  [ "$FAILS" -ne 0 ] && { echo "FAIL: object standalone rehearsal has $FAILS failing assertion(s)"; exit 1; }
  echo "PASS: object standalone cutover rehearsal"
}

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
P_SRC=$(free_port); P_OBJ=$(free_port); USED="$P_SRC $P_OBJ"
[ "$P_SRC" != "$P_OBJ" ] || { echo "port collision"; exit 2; }
W=$(mktemp -d); PIDS=""
ADMIN="oc-rehearsal"; APW="oc-rehearsal-pw-123"; ALICE_PW="alice-pw-123"
JWT_SECRET="dev-only-change-me-0123456789abcdef"
TOKEN=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
cleanup() {
  local p
  for p in $PIDS; do kill -9 "$p" 2>/dev/null; done
  wait 2>/dev/null
  if [ "${KEEP:-0}" = 1 ]; then echo "WORK=$W"; else rm -rf "$W"; fi
}
trap cleanup EXIT
stop_proc() { local p="$1"; [ -n "$p" ] && { kill -9 "$p" 2>/dev/null; wait "$p" 2>/dev/null; }; }

wait_http() { # port pid [path]
  local port=$1 pid=$2 path="${3:-/health/live}"
  for _ in $(seq 1 160); do
    kill -0 "$pid" 2>/dev/null || return 1
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$port$path" 2>/dev/null || true)" = 200 ] && return 0
    sleep 0.25
  done
  return 1
}
start_serve() { # cfg port log -> pid in STARTED
  local cfg=$1 port=$2 log=$3 pid
  ( cd "$(dirname "$cfg")" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" exec "$BIN" --config "$cfg" serve >"$log" 2>&1 ) &
  pid=$!; PIDS="$PIDS $pid"; STARTED=$pid
  wait_http "$port" "$pid"
}
stop_started() { stop_proc "$STARTED"; STARTED=""; }
login() { curl -s --max-time 10 -X POST "http://127.0.0.1:$1/api/v1/login" -H 'content-type: application/json' \
  -d "{\"username\":\"$2\",\"password\":\"$3\"}" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get("token",""))'; }
mint() { curl -s --max-time 10 -X POST "http://127.0.0.1:$1/api/v1/access-keys" \
  -H "authorization: Bearer $2" -H 'content-type: application/json' -d '{}'; }
jget() { python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
v=d.get(sys.argv[1],"")
print(",".join(map(str,v)) if isinstance(v,list) else (str(v).lower() if isinstance(v,bool) else v))' "$1"; }
code() { curl -s -o /dev/null -w '%{http_code}' --max-time "${3:-10}" ${2:+-H "authorization: Bearer $2"} "$1"; }
mig_json_field() { # file dotted-path e.g. object.snapshot_sha256
python3 - "$1" "$2" <<'PY'
import json, sys
s = open(sys.argv[1], encoding="utf-8").read()
i = s.find("{")
d = json.loads(s[i:]) if i >= 0 else {}
for k in sys.argv[2].split("."):
    if isinstance(d, dict):
        d = d.get(k)
    else:
        d = None
        break
print("" if d is None else (json.dumps(d, sort_keys=True) if isinstance(d, (dict, list)) else d))
PY
}
no_sqlite_fds() { ! ls -l "/proc/$1/fd" 2>/dev/null | grep -Eq '(oss|raft|state)\.db'; }
cfg() { # dir port backend (objects|sqlite)
  local d="$W/$1" port=$2 backend=$3
  mkdir -p "$d/data" "$d/staging"
  cat > "$d/oss.toml" <<EOF
listen = "127.0.0.1:$port"
data_dir = "$d/data"
staging_dir = "$d/staging"
database_url = "sqlite://$d/oss.db?mode=rwc"
jwt_secret = "$JWT_SECRET"
max_put_bytes = 268435456
groups = []
[server]
control_backend = "$backend"
[site]
name = "oc-standalone"
region = "us-east-1"
priority = 10
token = "$TOKEN"
EOF
}

echo "== 1. SQLite fixture (admin, user, bucket, access key, object) =="
cfg src "$P_SRC" sqlite
if start_serve "$W/src/oss.toml" "$P_SRC" "$W/src.log"; then pass "SQLite fixture started"; else fail "SQLite fixture start"; tail -20 "$W/src.log"; exit 1; fi
T=$(login "$P_SRC" "$ADMIN" "$APW"); ck [ -n "$T" ]
BC=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "http://127.0.0.1:$P_SRC/api/v1/buckets" \
  -H "authorization: Bearer $T" -H 'content-type: application/json' -d '{"name":"oc-bucket"}')
ck_eq "$BC" "204" "console create bucket (SQLite)"
UC=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "http://127.0.0.1:$P_SRC/api/v1/users" \
  -H "authorization: Bearer $T" -H 'content-type: application/json' -d "{\"username\":\"alice\",\"password\":\"$ALICE_PW\"}")
ck_eq "$UC" "200" "console create user (SQLite)"
M=$(mint "$P_SRC" "$T")
AK=$(printf '%s' "$M" | jget access_key); SK=$(printf '%s' "$M" | jget secret_key)
ck [ -n "$AK" ]; ck [ -n "$SK" ]
head -c 65536 /dev/urandom > "$W/one.bin"
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
AWS="aws --endpoint-url http://127.0.0.1:$P_SRC s3"
ck $AWS cp "$W/one.bin" "s3://oc-bucket/one.bin" >/dev/null 2>&1
stop_started
ck [ ! -e "$W/src/raft.db" ]
SHA_BEFORE=$(sha256sum "$W/src/oss.db" | cut -d' ' -f1)
cp -a "$W/src" "$W/backup"
ck [ -f "$W/backup/oss.db" ]

echo "== 2. control-migrate dry-run / apply / idempotent export =="
"$BIN" control-migrate --from "$W/src/oss.db" --state-dir "$W/drydata" --raft-db "$W/src/raft.db" --dry-run --json > "$W/dry.json" 2>"$W/dry.err"
DRC=$?
ck_eq "$DRC" "0" "dry-run exits 0"
ck_eq "$(mig_json_field "$W/dry.json" status)" "dry-run" "dry-run status"
ck_eq "$(mig_json_field "$W/dry.json" source_sha256)" "$SHA_BEFORE" "dry-run source sha256 unchanged"
ck [ ! -e "$W/drydata" ]
ck_eq "$(sha256sum "$W/src/oss.db" | cut -d' ' -f1)" "$SHA_BEFORE" "dry-run did not modify source"

"$BIN" control-migrate --from "$W/src/oss.db" --state-dir "$W/src/data" --raft-db "$W/src/raft.db" --apply --json > "$W/apply.json" 2>"$W/apply.err"
ck_eq "$?" "0" "apply exits 0"
ck_eq "$(mig_json_field "$W/apply.json" status)" "applied" "apply status"
ck_eq "$(mig_json_field "$W/apply.json" mode)" "standalone" "apply mode standalone"
ck_eq "$(mig_json_field "$W/apply.json" source_sha256)" "$SHA_BEFORE" "apply source sha256 unchanged"
SNAP=$(mig_json_field "$W/apply.json" object.snapshot_sha256)
RUN=$(mig_json_field "$W/apply.json" object.runstate_sha256)
ROOT=$(mig_json_field "$W/apply.json" object.root_hash)
ck [ -n "$SNAP" ]; ck [ -n "$RUN" ]; ck [ -n "$ROOT" ]
ck_eq "$(mig_json_field "$W/apply.json" object.snapshot_sha256)" "$(mig_json_field "$W/apply.json" source_snapshot_sha256)" "export bytes equal SQLite export"
ck_eq "$(mig_json_field "$W/apply.json" object.runstate_sha256)" "$(mig_json_field "$W/apply.json" source_runstate_sha256)" "runstate bytes equal SQLite export"
ck [ -f "$W/src/data/.oss.sys/control__XLDIR__state__XLDIR__root/xl.meta" ]

"$BIN" control-migrate --from "$W/src/oss.db" --state-dir "$W/src/data" --raft-db "$W/src/raft.db" --apply --json > "$W/again.json" 2>"$W/again.err"
ck_eq "$?" "0" "rerun exits 0"
ck_eq "$(mig_json_field "$W/again.json" status)" "already-applied" "rerun is idempotent"
ck_eq "$(mig_json_field "$W/again.json" object.snapshot_sha256)" "$SNAP" "rerun snapshot unchanged"
ck_eq "$(mig_json_field "$W/again.json" object.runstate_sha256)" "$RUN" "rerun runstate unchanged"

echo "== 3. independent-state install (snapshot export/import parity) =="
cp -a "$W/backup" "$W/src2"
"$BIN" control-migrate --from "$W/src2/oss.db" --state-dir "$W/src2/data" --raft-db "$W/src2/raft.db" --apply --json > "$W/install.json" 2>"$W/install.err"
ck_eq "$?" "0" "fresh-state install exits 0"
ck_eq "$(mig_json_field "$W/install.json" status)" "applied" "fresh-state install applied"
ck_eq "$(mig_json_field "$W/install.json" object.snapshot_sha256)" "$SNAP" "install snapshot bytes identical"
ck_eq "$(mig_json_field "$W/install.json" object.runstate_sha256)" "$RUN" "install runstate bytes identical"
ck_eq "$(mig_json_field "$W/install.json" object.table_counts)" "$(mig_json_field "$W/apply.json" object.table_counts)" "install table counts identical"
ck_eq "$(sha256sum "$W/src2/oss.db" | cut -d' ' -f1)" "$SHA_BEFORE" "install did not modify its source"

echo "== 4. object standalone: reads, data-plane writes, no SQLite =="
# database_url points at a directory that must never be created: any SQLite
# open in object mode fails closed instead of silently creating oss.db.
cfg obj "$P_OBJ" objects
# Same data/staging roots the migrator wrote; only the SQLite URL is moved to a
# path that must never be created.
sed -i "s|data_dir = \"$W/obj/data\"|data_dir = \"$W/src/data\"|; \
        s|staging_dir = \"$W/obj/staging\"|staging_dir = \"$W/src/staging\"|; \
        s|database_url = \"sqlite://$W/obj/oss.db?mode=rwc\"|database_url = \"sqlite://$W/no-sqlite/oss.db?mode=rwc\"|" "$W/obj/oss.toml"
if start_serve "$W/obj/oss.toml" "$P_OBJ" "$W/obj.log"; then pass "object backend started"; else fail "object backend start"; tail -40 "$W/obj.log"; finish; fi
OBJ_PID=$STARTED
ck no_sqlite_fds "$OBJ_PID"
ck [ ! -e "$W/no-sqlite/oss.db" ]
ck [ -z "$(find "$W/obj/data" -maxdepth 3 -name '*.db' -print -quit)" ]
TO=$(login "$P_OBJ" "$ADMIN" "$APW"); ck [ -n "$TO" ]
ck_eq "$(code "http://127.0.0.1:$P_OBJ/api/v1/users" "$TO")" "200" "console users read"
ck_eq "$(code "http://127.0.0.1:$P_OBJ/api/v1/buckets" "$TO")" "200" "console buckets read"
ck_eq "$(code "http://127.0.0.1:$P_OBJ/api/v1/site" "$TO")" "200" "site identity read"
ck_eq "$(code "http://127.0.0.1:$P_OBJ/api/v1/sites" "$TO")" "200" "site list read"
USERS=$(curl -s --max-time 10 "http://127.0.0.1:$P_OBJ/api/v1/users" -H "authorization: Bearer $TO")
printf '%s' "$USERS" | grep -q "\"username\":\"$ADMIN\"" && pass "migrated admin visible" || fail "migrated admin visible"
printf '%s' "$USERS" | grep -q '"username":"alice"' && pass "migrated user visible" || fail "migrated user visible"
AWS2="aws --endpoint-url http://127.0.0.1:$P_OBJ s3"
ck $AWS2 cp "s3://oc-bucket/one.bin" "$W/one.back" >/dev/null 2>&1
ck cmp -s "$W/one.bin" "$W/one.back"
head -c 4096 /dev/urandom > "$W/two.bin"
ck $AWS2 cp "$W/two.bin" "s3://oc-bucket/two.bin" >/dev/null 2>&1
ck $AWS2 cp "s3://oc-bucket/two.bin" "$W/two.back" >/dev/null 2>&1
ck cmp -s "$W/two.bin" "$W/two.back"
# Design §6.2 smoke includes bucket CRUD. These are expected to work in object
# mode and are asserted, not skipped: red = product blocker.
BC2=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "http://127.0.0.1:$P_OBJ/api/v1/buckets" \
  -H "authorization: Bearer $TO" -H 'content-type: application/json' -d '{"name":"oc-bucket-2"}')
ck_eq "$BC2" "204" "object mode console create bucket"
UC2=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "http://127.0.0.1:$P_OBJ/api/v1/users" \
  -H "authorization: Bearer $TO" -H 'content-type: application/json' -d '{"username":"bob","password":"bob-pw-1234"}')
ck_eq "$UC2" "200" "object mode console create user"

echo "== 5. restart object backend; no SQLite, data persists =="
stop_started
if start_serve "$W/obj/oss.toml" "$P_OBJ" "$W/obj2.log"; then pass "object backend restarted"; else fail "object backend restart"; tail -40 "$W/obj2.log"; finish; fi
OBJ2_PID=$STARTED
ck no_sqlite_fds "$OBJ2_PID"
ck [ ! -e "$W/no-sqlite/oss.db" ]
TO2=$(login "$P_OBJ" "$ADMIN" "$APW"); ck [ -n "$TO2" ]
ck_eq "$(code "http://127.0.0.1:$P_OBJ/api/v1/users" "$TO2")" "200" "users readable after restart"
ck $AWS2 cp "s3://oc-bucket/one.bin" "$W/one.back2" >/dev/null 2>&1
ck cmp -s "$W/one.bin" "$W/one.back2"
stop_started

echo "== 6. rollback before first accepted control write =="
rm -rf "$W/src/data" "$W/src/staging" "$W/src/oss.db" "$W/src/oss.db-wal" "$W/src/oss.db-shm"
cp -a "$W/backup/." "$W/src/"
ck [ ! -e "$W/src/data/.oss.sys/control__XLDIR__state__XLDIR__root" ]
ck_eq "$(sha256sum "$W/src/oss.db" | cut -d' ' -f1)" "$SHA_BEFORE" "restored SQLite checksum"
if start_serve "$W/src/oss.toml" "$P_SRC" "$W/rollback.log"; then pass "SQLite rollback server started"; else fail "SQLite rollback start"; tail -30 "$W/rollback.log"; finish; fi
TR=$(login "$P_SRC" "$ADMIN" "$APW"); ck [ -n "$TR" ]
RUSERS=$(curl -s --max-time 10 "http://127.0.0.1:$P_SRC/api/v1/users" -H "authorization: Bearer $TR")
printf '%s' "$RUSERS" | grep -q '"username":"alice"' && pass "rollback keeps alice" || fail "rollback keeps alice"
ck_eq "$(code "http://127.0.0.1:$P_SRC/api/v1/buckets" "$TR")" "200" "rollback bucket read"
ck aws --endpoint-url "http://127.0.0.1:$P_SRC" s3 cp "s3://oc-bucket/one.bin" "$W/rollback.one" >/dev/null 2>&1
ck cmp -s "$W/one.bin" "$W/rollback.one"
ck_eq "$(sha256sum "$W/src/oss.db" | cut -d' ' -f1)" "$SHA_BEFORE" "rollback source still byte-identical"
stop_started
finish
