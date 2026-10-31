-- feature 8：AP 动态 VPC 端点注册表（随 Raft 复制/OSSCTL3 快照携带）。
CREATE TABLE IF NOT EXISTS vpc_endpoints (
    vpce_id TEXT PRIMARY KEY,
    vpc_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
