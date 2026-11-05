#!/usr/bin/env bash
# G3 control-store benchmark: SQLite+file-Raft baseline vs the object backend.
#
# Measures the design §4 budgets that are observable from outside the process:
#   auth view hit (S3 GET proxy), control write commit / Raft apply (console
#   bucket create through single-node Raft), startup hot-cache load (100k
#   users+access keys), snapshot export/import (control-migrate dry-run/apply).
# Job claim has no HTTP trigger and still uses a raw SQLite pool, so it is
# reported as blocked, not faked.
#
# Usage: OSS_BIN=/path/to/oss-server scripts/bench_control_store.sh
set -uo pipefail
cd "$(dirname "$0")/.."
BIN=${OSS_BIN:-target/debug/oss-server}
BIN=$(realpath "$BIN") || { echo "oss-server not found (set OSS_BIN)" >&2; exit 1; }
W=$(mktemp -d)
PIDS=""
cleanup() {
  for p in $PIDS; do kill -9 "$p" 2>/dev/null; done
  wait 2>/dev/null
  if [ "${KEEP:-0}" = 1 ]; then echo "WORK=$W"; else rm -rf "$W"; fi
}
trap cleanup EXIT

pass() { echo "  PASS: $*"; PASSES=$((PASSES + 1)); }
fail() { echo "  FAIL: $*"; FAILS=$((FAILS + 1)); }
PASSES=0 FAILS=0
finish() {
  echo
  echo "PASS=$PASSES FAIL=$FAILS"
  [ "$FAILS" -ne 0 ] && exit 1
  exit 0
}

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
le() { awk -v a="$1" -v b="$2" 'BEGIN{exit !(a<=b)}'; }
jf() { # dotted path from stdin (JSON may be preceded by tracing lines)
  python3 -c 'import json,sys
s=sys.stdin.read(); i=s.find("{")
d=json.loads(s[i:]) if i >= 0 else {}
for k in sys.argv[1].split("."):
    d=d[int(k)] if k.isdigit() else d[k]
print(d)' "$1"
}
ck() { if "$@" >/dev/null 2>&1; then pass "$*"; else fail "$*"; fi; }
ck_eq() { if [ "$1" = "$2" ]; then pass "$3"; else fail "$3 (got '$1', want '$2')"; fi; }

ADMIN=benchadmin; APW=benchadmin-pw-123; ALICE=alice; ALPW=alice-pw-123
JWT=dev-only-change-me-0123456789abcdef
RAFT_TOKEN=$(python3 -c 'import secrets;print(secrets.token_hex(32))')
SITE_TOKEN=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
PORT=$(free_port)
USERS_N=${USERS_N:-50000}
ROWS=$((USERS_N * 2 + 3)) # bulk users+keys plus admin, alice and alice's key
IMPORT_TIMEOUT=${IMPORT_TIMEOUT:-180} # seconds; the 100k-row import is O(N log N) node writes
WARM=20
AUTH_N=300
WRITE_N=200
WRITE_WORKERS=8

wait_http() { # port pid
  local i
  for i in $(seq 1 240); do
    kill -0 "$2" 2>/dev/null || return 1
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$1/health/live" 2>/dev/null || true)" = 200 ] && return 0
    sleep 0.1
  done
  return 1
}
login() { # port user password
  curl -s --max-time 20 -X POST "http://127.0.0.1:$1/api/v1/login" -H 'content-type: application/json' \
    -d "{\"username\":\"$2\",\"password\":\"$3\"}" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get("token",""))'
}
no_sqlite_fds() { ! ls -l "/proc/$1/fd" 2>/dev/null | grep -Eq '/(oss|raft)\.db( |$)'; }

cat >"$W/oss.toml" <<EOF
listen = "127.0.0.1:$PORT"
data_dir = "$W/data"
staging_dir = "$W/staging"
database_url = "sqlite://$W/oss.db?mode=rwc"
jwt_secret = "$JWT"
max_put_bytes = 268435456
groups = []
[server]
control_backend = "sqlite"
[site]
name = "bench"
region = "us-east-1"
priority = 10
token = "$SITE_TOKEN"
[raft]
node_id = 1
role = "voter"
bootstrap = true
advertise_endpoint = "http://127.0.0.1:$PORT"
cluster_token = "$RAFT_TOKEN"
db_path = "$W/raft.db"
election_timeout_ms = 1000
heartbeat_interval_ms = 200
rpc_timeout_ms = 1000
write_timeout_ms = 3000
feature_level = 3
EOF

start_timed() { # backend log
  local t0 t1
  t0=$(date +%s%N)
  (cd "$W" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" \
    exec "$BIN" --config "$W/oss.toml" serve >"$W/$2" 2>&1) &
  STARTED=$!
  PIDS="$PIDS $STARTED"
  if wait_http "$PORT" "$STARTED"; then
    t1=$(date +%s%N)
    START_MS=$(((t1 - t0) / 1000000))
    return 0
  fi
  START_MS=-1
  return 1
}
stop_started() { kill -9 "$STARTED" 2>/dev/null; wait "$STARTED" 2>/dev/null; }

cat >"$W/bench.py" <<'PY'
import hashlib, hmac, http.client, json, sys, threading, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone


def sign(host, path, ak, sk, region="us-east-1"):
    now = datetime.now(timezone.utc)
    amz = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    payload = hashlib.sha256(b"").hexdigest()
    ch = f"host:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{amz}\n"
    sh = "host;x-amz-content-sha256;x-amz-date"
    req = f"GET\n{path}\n\n{ch}\n{sh}\n{payload}"
    scope = f"{day}/{region}/s3/aws4_request"
    sts = f"AWS4-HMAC-SHA256\n{amz}\n{scope}\n{hashlib.sha256(req.encode()).hexdigest()}"
    k = ("AWS4" + sk).encode()
    for part in (day, region, "s3", "aws4_request"):
        k = hmac.new(k, part.encode(), hashlib.sha256).digest()
    sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
    return {
        "host": host,
        "x-amz-content-sha256": payload,
        "x-amz-date": amz,
        "authorization": f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, "
                         f"SignedHeaders={sh}, Signature={sig}",
    }


def pct(v, p):
    s = sorted(v)
    return s[min(len(s) - 1, int(round(p * (len(s) - 1))))]


def s3get(port, ak, sk, bucket, key, n, warm):
    host = f"127.0.0.1:{port}"
    path = f"/{bucket}/{key}"
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
    headers = sign(host, path, ak, sk)

    def one():
        conn.request("GET", path, headers=headers)
        r = conn.getresponse()
        r.read()
        return r.status

    for _ in range(warm):
        if one() != 200:
            raise SystemExit("s3 warm-up failed")
    ts = []
    for _ in range(n):
        t0 = time.perf_counter_ns()
        st = one()
        ts.append((time.perf_counter_ns() - t0) / 1e6)
        if st != 200:
            raise SystemExit(f"s3 GET {st}")
    print(json.dumps({"n": n, "p50_ms": pct(ts, .50), "p99_ms": pct(ts, .99),
                      "mean_ms": sum(ts) / len(ts)}))


def writes(port, token, bucket, n, warm, workers):
    local = threading.local()
    errs = []
    lock = threading.Lock()
    body = json.dumps({"Version": "2012-10-17", "Statement": []})
    hdr = {"authorization": "Bearer " + token, "content-type": "application/json"}

    def conn():
        c = getattr(local, "c", None)
        if c is None:
            c = local.c = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        return c

    def one(i):
        path = f"/api/v1/buckets/{bucket}/policy"
        t0 = time.perf_counter_ns()
        try:
            c = conn()
            c.request("PUT", path, body, hdr)
            r = c.getresponse()
            data = r.read()
            st = r.status
        except Exception as e:  # count transport errors instead of hanging
            with lock:
                errs.append(str(e))
            return None
        dt = (time.perf_counter_ns() - t0) / 1e6
        if st != 204:
            with lock:
                errs.append(f"{st}: {data[:120]!r}")
            return None
        return dt

    for i in range(warm):
        one(-1 - i)
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        ts = [x for x in ex.map(one, range(n)) if x is not None]
    wall = time.perf_counter() - t0
    out = {"ok": len(ts), "fail": len(errs), "errors": errs[:3], "wall_s": wall,
           "rps": len(ts) / wall if wall else 0.0}
    if ts:
        out.update({"p50_ms": pct(ts, .50), "p99_ms": pct(ts, .99), "mean_ms": sum(ts) / len(ts)})
    print(json.dumps(out))


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "s3get":
        s3get(int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6],
              int(sys.argv[7]), int(sys.argv[8]))
    elif cmd == "writes":
        writes(int(sys.argv[2]), sys.argv[3], sys.argv[4], int(sys.argv[5]),
               int(sys.argv[6]), int(sys.argv[7]))
    else:
        raise SystemExit("unknown command")
PY

echo "== environment =="
echo "  host=$(uname -sr) cpus=$(nproc)"
echo "  binary=$BIN mtime=$(stat -c %y "$BIN" | cut -d. -f1)"
echo "  rows=$ROWS auth_n=$AUTH_N writes=$WRITE_N/$WRITE_WORKERS workers import_timeout=${IMPORT_TIMEOUT}s"

echo "== 1. SQLite+file-Raft fixture =="
if start_timed sqlite sqlite-fixture.log; then pass "SQLite fixture started"; else fail "SQLite fixture start"; tail -30 "$W/sqlite-fixture.log"; finish; fi
T=$(login "$PORT" "$ADMIN" "$APW"); ck [ -n "$T" ]
BC=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 -X POST "http://127.0.0.1:$PORT/api/v1/buckets" \
  -H "authorization: Bearer $T" -H 'content-type: application/json' -d '{"name":"oc-bucket"}')
ck_eq "$BC" "204" "fixture console bucket"
UC=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 -X POST "http://127.0.0.1:$PORT/api/v1/users" \
  -H "authorization: Bearer $T" -H 'content-type: application/json' -d "{\"username\":\"$ALICE\",\"password\":\"$ALPW\"}")
ck_eq "$UC" "200" "fixture console user"
TA=$(login "$PORT" "$ALICE" "$ALPW"); ck [ -n "$TA" ]
ABC=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 -X POST "http://127.0.0.1:$PORT/api/v1/buckets" \
  -H "authorization: Bearer $TA" -H 'content-type: application/json' -d '{"name":"alice-bucket"}')
ck_eq "$ABC" "204" "fixture alice bucket"
M=$(curl -s --max-time 20 -X POST "http://127.0.0.1:$PORT/api/v1/access-keys" \
  -H "authorization: Bearer $TA" -H 'content-type: application/json' -d '{}')
AK=$(printf '%s' "$M" | python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("access_key",""))
except Exception: print("")')
SK=$(printf '%s' "$M" | python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("secret_key",""))
except Exception: print("")')
ck [ -n "$AK" ]; ck [ -n "$SK" ]
printf 'bench-object\n' >"$W/one.bin"
if AWS_ACCESS_KEY_ID=$AK AWS_SECRET_ACCESS_KEY=$SK AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url "http://127.0.0.1:$PORT" s3 cp "$W/one.bin" s3://alice-bucket/one.bin >/dev/null 2>&1; then
  pass "fixture S3 put"
else
  fail "fixture S3 put"
fi
stop_started

# The fixture is stopped; open the guard window the same way an apply does so
# bulk rows can be seeded without going through the Raft log.
sqlite3 "$W/oss.db" <<SQL
UPDATE raft_apply_ctx SET active = 1 WHERE id = 1;
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < $USERS_N)
INSERT INTO users(username,password_hash,is_admin,status,created_at)
SELECT 'bench_user_'||i, (SELECT password_hash FROM users WHERE username='$ALICE'), 0, 'active', 1 FROM n;
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < $USERS_N)
INSERT INTO access_keys(access_key,secret_key,user_id,status,created_at)
SELECT 'BENCHAK'||printf('%013d',i), printf('%040d',i),
       (SELECT id FROM users WHERE username='$ALICE'), 'active', 1 FROM n;
UPDATE raft_apply_ctx SET active = 0 WHERE id = 1;
SQL
ck_eq "$(sqlite3 "$W/oss.db" 'SELECT count(*) FROM users')" "$((USERS_N + 2))" "fixture user rows"
ck_eq "$(sqlite3 "$W/oss.db" 'SELECT count(*) FROM access_keys')" "$((USERS_N + 1))" "fixture access-key rows"
# G1 currently rejects an empty runstate whose encoder always emits a level-3
# extension; mark the fixture as level 3 so the migration path can be measured.
sqlite3 "$W/oss.db" "INSERT INTO schema_meta(key,value) VALUES('raft_feature_level','3') ON CONFLICT(key) DO UPDATE SET value='3'"

echo "== 2. SQLite baseline: startup, auth, control write =="
if start_timed sqlite sqlite-bench.log; then pass "SQLite baseline started"; else fail "SQLite baseline start"; tail -30 "$W/sqlite-bench.log"; finish; fi
SQL_START_MS=$START_MS
T=$(login "$PORT" "$ADMIN" "$APW"); ck [ -n "$T" ]
S3_SQL=$(python3 "$W/bench.py" s3get "$PORT" "$AK" "$SK" alice-bucket one.bin "$AUTH_N" "$WARM")
ck [ "$(printf '%s' "$S3_SQL" | jf p99_ms | awk '{print ($1 > 0)}')" = 1 ]
WRITE_SQL=$(python3 "$W/bench.py" writes "$PORT" "$T" alice-bucket "$WRITE_N" 5 "$WRITE_WORKERS")
ck_eq "$(printf '%s' "$WRITE_SQL" | jf fail)" "0" "SQLite control writes all accepted"
stop_started
# Baseline writes are part of the fixture now; checksum what is actually migrated.
SHA_BEFORE=$(sha256sum "$W/oss.db" | cut -d' ' -f1)

echo "== 3. snapshot export/import and object startup =="
T0=$(date +%s%N)
"$BIN" control-migrate --from "$W/oss.db" --state-dir "$W/data" --raft-db "$W/raft.db" --dry-run --json >"$W/dry.json" 2>&1
DRC=$?
EXPORT_MS=$((($(date +%s%N) - T0) / 1000000))
ck_eq "$DRC" "0" "snapshot export (dry-run)"
ck_eq "$(jf status <"$W/dry.json")" "dry-run" "export report status"
T0=$(date +%s%N)
timeout "$IMPORT_TIMEOUT" "$BIN" control-migrate --from "$W/oss.db" --state-dir "$W/data" --raft-db "$W/raft.db" --apply --json >"$W/apply.json" 2>&1
ARC=$?
IMPORT_MS=$((($(date +%s%N) - T0) / 1000000))
if [ "$ARC" = 124 ]; then
  FAILS=$((FAILS + 1))
  if ! le "$EXPORT_MS" 10000; then
    FAILS=$((FAILS + 1))
    echo "  FAIL: snapshot export over 10s ($EXPORT_MS ms)"
  fi
  echo "  FAIL: snapshot import did not finish within ${IMPORT_TIMEOUT}s (budget 10s); object phase skipped"
  S3S=$(printf '%s' "$S3_SQL" | jf p99_ms)
  WS=$(printf '%s' "$WRITE_SQL" | jf p99_ms)
  RS=$(printf '%s' "$WRITE_SQL" | jf rps)
  printf '  %-38s %-22s %10s %10s  %s\n' "metric" "budget" "sqlite" "objects" "verdict"
  printf '  %-38s %-22s %10s %10s  %s\n' "auth S3 GET p99 (proxy, ms)" "<=200us added (proxy)" "$S3S" "SKIP" "SKIP"
  printf '  %-38s %-22s %10s %10s  %s\n' "control write p99 (ms)" "<=20ms one drive" "$WS" "SKIP" "SKIP"
  printf '  %-38s %-22s %10s %10s  %s\n' "control write throughput (req/s)" ">=200/s" "$RS" "SKIP" "SKIP"
  printf '  %-38s %-22s %10s %10s  %s\n' "snapshot export, ${ROWS} rows (ms)" "<=10000ms" "n/a" "$EXPORT_MS" "$(le "$EXPORT_MS" 10000 && echo PASS || echo FAIL)"
  printf '  %-38s %-22s %10s %10s  %s\n' "snapshot import, ${ROWS} rows (ms)" "<=10000ms" "n/a" ">${IMPORT_TIMEOUT}000" "FAIL"
  finish
fi
ck_eq "$ARC" "0" "snapshot import (apply)"
ck_eq "$(jf status <"$W/apply.json")" "applied" "import report status"
ck_eq "$(sha256sum "$W/oss.db" | cut -d' ' -f1)" "$SHA_BEFORE" "migration left source byte-identical"
for ext in "" -wal -shm; do
  if [ -e "$W/raft.db$ext" ]; then mv "$W/raft.db$ext" "$W/raft.db.legacy$ext"; fi
done
sed -i 's/control_backend = "sqlite"/control_backend = "objects"/' "$W/oss.toml"
if start_timed objects object-bench.log; then pass "object baseline started"; else fail "object baseline start"; tail -40 "$W/object-bench.log"; finish; fi
OBJ_START_MS=$START_MS
ck no_sqlite_fds "$STARTED"
T=$(login "$PORT" "$ADMIN" "$APW"); ck [ -n "$T" ]

echo "== 4. object backend: auth, control write =="
S3_OBJ=$(python3 "$W/bench.py" s3get "$PORT" "$AK" "$SK" alice-bucket one.bin "$AUTH_N" "$WARM")
ck [ "$(printf '%s' "$S3_OBJ" | jf p99_ms | awk '{print ($1 > 0)}')" = 1 ]
WRITE_OBJ=$(python3 "$W/bench.py" writes "$PORT" "$T" alice-bucket "$WRITE_N" 5 "$WRITE_WORKERS")
ck_eq "$(printf '%s' "$WRITE_OBJ" | jf fail)" "0" "object control writes all accepted"
ck no_sqlite_fds "$STARTED"
stop_started

echo "== 5. design §4 budgets =="
fmt() { printf '%.3f' "$1"; }
S3S=$(printf '%s' "$S3_SQL" | jf p99_ms); S3O=$(printf '%s' "$S3_OBJ" | jf p99_ms)
WS=$(printf '%s' "$WRITE_SQL" | jf p99_ms); WO=$(printf '%s' "$WRITE_OBJ" | jf p99_ms)
RS=$(printf '%s' "$WRITE_SQL" | jf rps); RO=$(printf '%s' "$WRITE_OBJ" | jf rps)
printf '  %-38s %-22s %10s %10s  %s\n' "metric" "budget" "sqlite" "objects" "verdict"
printf '  %-38s %-22s %10s %10s  %s\n' "auth S3 GET p99 (proxy, ms)" "<=200us added (proxy)" "$(fmt "$S3S")" "$(fmt "$S3O")" "$(le "$S3O" "$(awk -v b="$S3S" 'BEGIN{print b*1.1+1}')" && echo PASS || echo FAIL)"
printf '  %-38s %-22s %10s %10s  %s\n' "control write p99 (ms)" "<=20ms one drive" "$(fmt "$WS")" "$(fmt "$WO")" "$(le "$WO" 20 && echo PASS || echo FAIL)"
printf '  %-38s %-22s %10s %10s  %s\n' "raft apply p99 (ms)" "<=50ms" "$(fmt "$WS")" "$(fmt "$WO")" "$(le "$WO" 50 && echo PASS || echo FAIL)"
printf '  %-38s %-22s %10s %10s  %s\n' "control write throughput (req/s)" ">=200/s" "$(fmt "$RS")" "$(fmt "$RO")" "$(le 200 "$RO" && echo PASS || echo FAIL)"
START_V=$(if [ "$USERS_N" -ge 50000 ]; then le "$OBJ_START_MS" 5000 && echo PASS || echo FAIL; else echo "SKIP (reduced rows)"; fi)
EXPORT_V=$(if [ "$USERS_N" -ge 50000 ]; then le "$EXPORT_MS" 10000 && echo PASS || echo FAIL; else echo "SKIP (reduced rows)"; fi)
printf '  %-38s %-22s %10s %10s  %s\n' "startup hot load, ${ROWS} rows (ms)" "<=5000ms" "$SQL_START_MS" "$OBJ_START_MS" "$START_V"
printf '  %-38s %-22s %10s %10s  %s\n' "snapshot export, ${ROWS} rows (ms)" "<=10000ms" "n/a" "$EXPORT_MS" "$EXPORT_V"
printf '  %-38s %-22s %10s %10s  %s\n' "snapshot import, ${ROWS} rows (ms)" "<=10000ms" "n/a" "$IMPORT_MS" "$(le "$IMPORT_MS" 10000 && echo PASS || echo FAIL)"
printf '  %-38s %-22s %10s %10s  %s\n' "job claim p99" "<=20ms" "SKIP" "SKIP" "BLOCKED"
le "$WO" "$(awk -v b="$WS" 'BEGIN{print b*1.1+1}')" || fail "object control-write p99 regressed >10% vs SQLite ($WO > $WS)"
le "$S3O" "$(awk -v b="$S3S" 'BEGIN{print b*1.1+1}')" || fail "object auth p99 regressed >10% vs SQLite ($S3O > $S3S)"
le "$WO" 20 || fail "control write p99 over 20ms one-drive budget ($WO)"
le "$WO" 50 || fail "Raft apply p99 over 50ms budget ($WO)"
le 200 "$RO" || fail "control write throughput under 200/s ($RO)"
if [ "$USERS_N" -ge 50000 ]; then
  le "$OBJ_START_MS" 5000 || fail "object startup over 5s ($OBJ_START_MS ms)"
  le "$EXPORT_MS" 10000 || fail "snapshot export over 10s ($EXPORT_MS ms)"
fi
le "$IMPORT_MS" 10000 || fail "snapshot import over 10s ($IMPORT_MS ms)"
echo
echo "  notes: auth row is end-to-end S3 GET (HTTP+S3+storage), not isolated auth-view CPU;"
echo "         the §4 10us CPU / 200us added-latency budget needs in-process instrumentation."
echo "         job claim is not externally observable and object mode still calls the raw SQLite pool."
echo "         rows=${ROWS}; startup/export verdicts only apply to the default 100k-row fixture."
echo "         SQLite startup: ${SQL_START_MS:-n/a} ms; object startup: ${OBJ_START_MS:-n/a} ms."
finish
