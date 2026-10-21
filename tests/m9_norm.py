#!/usr/bin/env python3
"""M9 控制面 11 张复制表的三端比对（plan §2.2/§7 验收①）。

用法: m9_norm.py db_a db_b db_c users_csv buckets_csv

三个库：A/B 的 oss.db + witness 的 control.db。只比较本脚本创建的 m9* 实体，
站点本地行排除：bootstrap 管理员 m9admin 及其 AK、会话 AK（kind<>'long-term'）。

归一化规则：
- 外键（users.id / access_keys.user_id / permissions.user_id / 各表的 owner_id）
  一律换成 username；管理员属主统一为 'admin'（A/B 写各自本地管理员 id，
  witness 写哨兵 0，三者语义相同，数字 id 不可比）。
- 时间戳（created_at/updated_at）由 raft 命令携带，apply 不取本地时钟，
  因此三端必须逐字节相等，全部纳入比较。
- buckets.set_id 是本地物理 set 归属（raft/data.rs ruling #5：不入复制/快照），
  站点本地值，排除。
- access_keys.secret_key 纳入比较：Raft 模式下 SK 由 leader 在 append 前封装一次，
  apply/快照安装原样写入，且 raft 模式禁止 apply 外透明重封（auth.rs H2），
  故三端逐字节一致；未配置密钥库时存明文，同样一致。不是站点本地列。
"""
import json
import sqlite3
import sys

ADMIN = "m9admin"


def owner(username):
    """owner_id 的自然键：本地管理员行与 witness 哨兵 0 都归一为 'admin'。"""
    return "admin" if username is None or username == ADMIN else username


def dump(path):
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    return {
        "users": {
            r[0]: list(r[1:])
            for r in c.execute(
                "SELECT username, password_hash, is_admin, status, created_at "
                "FROM users WHERE username LIKE 'm9%' AND username <> ?",
                (ADMIN,),
            )
        },
        "access_keys": {
            r[0]: list(r[1:])
            for r in c.execute(
                "SELECT a.access_key, a.secret_key, u.username, a.status, a.expires_at, "
                "a.created_at, a.parent_ak, a.session_policy, a.kind "
                "FROM access_keys a JOIN users u ON u.id = a.user_id "
                "WHERE u.username LIKE 'm9%' AND u.username <> ? AND a.kind = 'long-term'",
                (ADMIN,),
            )
        },
        "permissions": {
            f"{r[0]}|{r[1]}": list(r[2:])
            for r in c.execute(
                "SELECT u.username, p.bucket, p.can_read, p.can_write, p.can_delete "
                "FROM permissions p JOIN users u ON u.id = p.user_id "
                "WHERE u.username LIKE 'm9%' AND u.username <> ?",
                (ADMIN,),
            )
        },
        # set_id 是本地物理 set（ruling #5），不参与比较
        "buckets": {
            r[0]: [owner(r[1])]
            + list(r[2:])
            for r in c.execute(
                "SELECT b.name, u.username, b.creation_date, b.versioning, b.object_lock, "
                "b.default_encryption, b.object_lock_default_mode, "
                "b.object_lock_default_days, b.replication_mode, b.deleting "
                "FROM buckets b LEFT JOIN users u ON u.id = b.owner_id "
                "WHERE b.name LIKE 'm9b-%'"
            )
        },
        "bucket_policies": {
            r[0]: list(r[1:])
            for r in c.execute(
                "SELECT bucket, policy_json, updated_at FROM bucket_policies "
                "WHERE bucket LIKE 'm9b-%'"
            )
        },
        "lifecycle_rules": {
            f"{r[0]}|{r[1]}": list(r[2:])
            for r in c.execute(
                "SELECT bucket, prefix, rule_id, expire_days, expiration_days, "
                "transition_days, transition_class, transition_tier, enabled, updated_at "
                "FROM lifecycle_rules WHERE bucket LIKE 'm9b-%'"
            )
        },
        "event_configs": {
            r[0]: list(r[1:])
            for r in c.execute(
                "SELECT bucket, config_json, updated_at FROM event_configs "
                "WHERE bucket LIKE 'm9b-%'"
            )
        },
        "replication_rules": {
            r[0]: list(r[1:])
            for r in c.execute(
                "SELECT bucket, rules_json, updated_at FROM replication_rules "
                "WHERE bucket LIKE 'm9b-%'"
            )
        },
        "access_points": {
            r[0]: [r[1], owner(r[2])] + list(r[3:])
            for r in c.execute(
                "SELECT a.name, a.bucket, u.username, a.policy_json, a.created_at, "
                "a.updated_at FROM access_points a "
                "LEFT JOIN users u ON u.id = a.owner_id WHERE a.name LIKE 'm9ap%'"
            )
        },
        "olap_configs": {
            r[0]: list(r[1:])
            for r in c.execute(
                "SELECT access_point, kind, endpoint, hex(module), module_sha256, "
                "timeout_ms, max_response_bytes, updated_at FROM olap_configs "
                "WHERE access_point LIKE 'm9ap%'"
            )
        },
        "inventory_configs": {
            f"{r[0]}|{r[1]}": [owner(r[2])] + list(r[3:])
            for r in c.execute(
                "SELECT i.bucket, i.id, u.username, i.config_json, i.updated_at "
                "FROM inventory_configs i LEFT JOIN users u ON u.id = i.owner_id "
                "WHERE i.bucket LIKE 'm9b-%'"
            )
        },
    }


def main():
    try:
        a, b, c = (dump(x) for x in sys.argv[1:4])
    except sqlite3.Error as e:
        print("db error:", e)
        return 2
    if a != b or a != c:
        print(json.dumps({"a": a, "b": b, "c": c}, ensure_ascii=False, sort_keys=True))
        return 1
    # 存在性断言：防止“三端都为空”被当成一致而假 PASS。
    # 脚本在每张未覆盖表里恰好创建 1 行；users/buckets 的期望键由调用方给出。
    need = sys.argv[4].split(",") + sys.argv[5].split(",")
    miss = [k for k in need if k and k not in a["users"] and k not in a["buckets"]]
    if miss:
        print("missing:", miss)
        return 3
    # 每张表本脚本恰好创建的行数；防止三端同时缺行被当成“一致”。
    want = {"access_keys": 1, "permissions": 1, "bucket_policies": 2,
            "lifecycle_rules": 1, "event_configs": 1, "replication_rules": 1,
            "access_points": 1, "olap_configs": 1, "inventory_configs": 1}
    bad = {t: len(a[t]) for t, n in want.items() if len(a[t]) != n}
    if bad:
        print("missing: 行数不符，实得", bad, "期望", want)
        return 3
    print("ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
