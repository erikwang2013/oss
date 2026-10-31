-- feature 9：复制账户用量账本（strict 模式的唯一执行依据）。
-- account_id 是稳定字符串（可为 printf('%012d', uid) 的个人账户），因此不设外键。
CREATE TABLE IF NOT EXISTS account_usage_ledger (
    account_id   TEXT PRIMARY KEY,
    used_bytes   INTEGER NOT NULL DEFAULT 0,
    object_count INTEGER NOT NULL DEFAULT 0
);
