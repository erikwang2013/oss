//! inventory 域行类型与后端中立读写 trait。

use serde::{Deserialize, Serialize};

use super::{ControlError, OwnerRef};

/// inventory 配置快照行（自然键 `(bucket, id)`；字段顺序即快照字节顺序）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryConfigEntry {
    pub bucket: String,
    pub id: String,
    pub owner: OwnerRef,
    pub config_json: String,
    pub updated_at: i64,
}

/// inventory 配置运行期视图：属主解析为用户行（`None` = 用户行缺失/悬空）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryConfigView {
    pub bucket: String,
    pub id: String,
    pub owner: Option<OwnerRef>,
    /// 本地外键投影（原 `inventory_configs.owner_id`）；不是 seam 身份键。
    pub owner_uid: i64,
    pub config_json: String,
    pub updated_at: i64,
}

/// inventory 运行行（`m8.runstate` 扩展）。
///
/// 身份键是 `(bucket, config_id, run_key)`；本地自增 `id` 是不可变行属性
/// （快照顺序、执行器 API、console 展示需要），**不是**身份键。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct InventoryRunRow {
    pub id: i64,
    pub bucket: String,
    pub config_id: String,
    pub run_key: String,
    pub started_at: i64,
    pub state: String,
    pub job_id: Option<i64>,
    pub manifest_key: Option<String>,
    pub error: Option<String>,
    pub created_at: i64,
    pub fence_term: i64,
    pub fence_leader: i64,
}

/// inventory 增量游标行（`m8.runstate` 扩展）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct InventoryCursorRow {
    pub bucket: String,
    pub config_id: String,
    pub cursor: String,
    pub config_updated_at: i64,
    pub updated_at: i64,
    pub fence_term: i64,
    pub fence_leader: i64,
}

/// inventory 增量开关行（`m8.runstate` 扩展）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct InventoryIncrementalRow {
    pub bucket: String,
    pub config_id: String,
    pub enabled: bool,
    pub updated_at: i64,
}

/// 只读 inventory 访问（运行期视图带本地外键投影 `owner_uid`）。
#[async_trait::async_trait]
pub trait InventoryStore: Send + Sync {
    /// 运行期口径；`owner_uid` 为本地外键投影，`owner = None` 表示用户行缺失。
    async fn inventory_config(
        &self,
        bucket: &str,
        id: &str,
    ) -> Result<Option<InventoryConfigView>, ControlError>;
    /// 按 `id` 升序。
    async fn inventory_configs(
        &self,
        bucket: &str,
    ) -> Result<Vec<InventoryConfigView>, ControlError>;
    /// 按 `(bucket, id)` 升序。
    async fn inventory_configs_all(&self) -> Result<Vec<InventoryConfigView>, ControlError>;
    async fn inventory_config_count(&self, bucket: &str) -> Result<i64, ControlError>;
    /// 按本地自增 id 读运行行（id 是行属性，不是身份键）。
    async fn inventory_run(&self, id: i64) -> Result<Option<InventoryRunRow>, ControlError>;
    /// 按自然键 `(bucket, config_id, run_key)` 读运行行。
    async fn inventory_run_by_key(
        &self,
        bucket: &str,
        config_id: &str,
        run_key: &str,
    ) -> Result<Option<InventoryRunRow>, ControlError>;
    async fn inventory_cursor(
        &self,
        bucket: &str,
        config_id: &str,
    ) -> Result<Option<InventoryCursorRow>, ControlError>;
    async fn inventory_incremental(
        &self,
        bucket: &str,
        config_id: &str,
    ) -> Result<Option<InventoryIncrementalRow>, ControlError>;
}

/// 调用方事务内的 inventory 写访问；本 trait 不 begin/commit/rollback。
#[async_trait::async_trait]
pub trait InventoryTxn: Send {
    /// 解析属主自然键 → 本地 uid；DELETE 该配置的 cursor + incremental；upsert config。
    async fn inventory_config_upsert(
        &mut self,
        row: &InventoryConfigEntry,
    ) -> Result<(), ControlError>;
    /// DELETE cursor + incremental + config；返回是否确有 config 行被删。
    async fn inventory_config_delete(
        &mut self,
        bucket: &str,
        id: &str,
    ) -> Result<bool, ControlError>;
    /// 快照导出（严格口径），按 `(bucket, id)` 升序。
    async fn inventory_configs(&mut self) -> Result<Vec<InventoryConfigEntry>, ControlError>;
    /// 快照安装：DELETE 全表 configs，再按入参顺序 INSERT（不碰 cursor/incremental）。
    async fn replace_inventory_configs(
        &mut self,
        rows: &[InventoryConfigEntry],
    ) -> Result<(), ControlError>;
    /// 快照导出：按 `id` 升序。
    async fn inventory_runs(&mut self) -> Result<Vec<InventoryRunRow>, ControlError>;
    /// 快照安装：DELETE 全表 runs，再按入参顺序 INSERT（显式保留 id）。
    async fn replace_inventory_runs(
        &mut self,
        rows: &[InventoryRunRow],
    ) -> Result<(), ControlError>;
    /// 快照导出：按 `(bucket, config_id)` 升序。
    async fn inventory_cursors(&mut self) -> Result<Vec<InventoryCursorRow>, ControlError>;
    async fn replace_inventory_cursors(
        &mut self,
        rows: &[InventoryCursorRow],
    ) -> Result<(), ControlError>;
    async fn inventory_incrementals(&mut self) -> Result<Vec<InventoryIncrementalRow>, ControlError>;
    async fn replace_inventory_incrementals(
        &mut self,
        rows: &[InventoryIncrementalRow],
    ) -> Result<(), ControlError>;
    async fn inventory_run(&mut self, id: i64) -> Result<Option<InventoryRunRow>, ControlError>;
    async fn inventory_run_by_key(
        &mut self,
        bucket: &str,
        config_id: &str,
        run_key: &str,
    ) -> Result<Option<InventoryRunRow>, ControlError>;
    /// 自然键 upsert；返回是否恰好 1 行。`id` 是行内不可变属性（调用方预检）。
    async fn inventory_run_upsert(
        &mut self,
        row: &InventoryRunRow,
    ) -> Result<bool, ControlError>;
    /// `INSERT OR IGNORE`；插入成功返回本地 id。
    async fn inventory_run_create(
        &mut self,
        bucket: &str,
        config_id: &str,
        run_key: &str,
        created_at: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<Option<i64>, ControlError>;
    async fn inventory_run_set_job(
        &mut self,
        run_id: i64,
        job_id: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<(), ControlError>;
    async fn inventory_run_started(
        &mut self,
        run_id: i64,
        started_at: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<(), ControlError>;
    async fn inventory_run_finish(
        &mut self,
        run_id: i64,
        state: &str,
        manifest_key: Option<&str>,
        error: Option<&str>,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<(), ControlError>;
    async fn inventory_cursor(
        &mut self,
        bucket: &str,
        config_id: &str,
    ) -> Result<Option<InventoryCursorRow>, ControlError>;
    /// upsert；fence 回退时 `Ok(false)`（零写）。
    async fn inventory_cursor_upsert(
        &mut self,
        row: &InventoryCursorRow,
    ) -> Result<bool, ControlError>;
    /// `fence_term = None` 为单机无 fence 口径；`Some(t)` 仅删 `fence_term <= t`。
    async fn inventory_cursor_delete(
        &mut self,
        bucket: &str,
        config_id: &str,
        fence_term: Option<i64>,
    ) -> Result<(), ControlError>;
    async fn inventory_incremental(
        &mut self,
        bucket: &str,
        config_id: &str,
    ) -> Result<Option<InventoryIncrementalRow>, ControlError>;
    async fn inventory_incremental_upsert(
        &mut self,
        row: &InventoryIncrementalRow,
    ) -> Result<bool, ControlError>;
    async fn inventory_incremental_delete(
        &mut self,
        bucket: &str,
        config_id: &str,
    ) -> Result<(), ControlError>;
    /// DELETE cursor + incremental（InventoryClear 语义）。
    async fn inventory_clear(&mut self, bucket: &str, config_id: &str)
    -> Result<(), ControlError>;
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    /// 旧快照行（与 `raft/data.rs` 的 `InventoryConfigRow` 声明顺序一致）。
    #[derive(Serialize)]
    struct LegacyConfig {
        bucket: String,
        id: String,
        owner: super::OwnerRef,
        config_json: String,
        updated_at: i64,
    }

    #[test]
    fn inventory_config_rmp_matches_snapshot_field_order() {
        let row = super::InventoryConfigEntry {
            bucket: "b1".into(),
            id: "cfg".into(),
            owner: super::OwnerRef::User("alice".into()),
            config_json: r#"{"destination":{"bucket":"d"}}"#.into(),
            updated_at: 8,
        };
        let bytes = rmp_serde::to_vec_named(&row).unwrap();
        let legacy = LegacyConfig {
            bucket: row.bucket.clone(),
            id: row.id.clone(),
            owner: row.owner.clone(),
            config_json: row.config_json.clone(),
            updated_at: row.updated_at,
        };
        assert_eq!(
            bytes,
            rmp_serde::to_vec_named(&legacy).unwrap(),
            "字段名与顺序必须与旧快照行逐字节一致"
        );
        assert_eq!(
            rmp_serde::from_slice::<super::InventoryConfigEntry>(&bytes).unwrap(),
            row
        );
    }

    #[test]
    fn inventory_config_rejects_unknown_fields() {
        #[derive(Serialize)]
        struct WithExtra {
            bucket: String,
            id: String,
            owner: super::OwnerRef,
            config_json: String,
            updated_at: i64,
            future_field: u8,
        }
        let bytes = rmp_serde::to_vec_named(&WithExtra {
            bucket: "b".into(),
            id: "i".into(),
            owner: super::OwnerRef::Admin,
            config_json: "{}".into(),
            updated_at: 1,
            future_field: 2,
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<super::InventoryConfigEntry>(&bytes).is_err());
    }

    /// 旧 `m8.runstate` 扩展行（与 `raft/runstate/model.rs` 声明顺序一致）。
    #[derive(Serialize)]
    struct LegacyRun {
        id: i64,
        bucket: String,
        config_id: String,
        run_key: String,
        started_at: i64,
        state: String,
        job_id: Option<i64>,
        manifest_key: Option<String>,
        error: Option<String>,
        created_at: i64,
        fence_term: i64,
        fence_leader: i64,
    }

    fn run() -> super::InventoryRunRow {
        super::InventoryRunRow {
            id: 7,
            bucket: "b1".into(),
            config_id: "cfg".into(),
            run_key: "k".into(),
            started_at: 1,
            state: "running".into(),
            job_id: Some(2),
            manifest_key: None,
            error: Some("e".into()),
            created_at: 3,
            fence_term: 4,
            fence_leader: 5,
        }
    }

    #[test]
    fn inventory_run_rmp_matches_runstate_extension() {
        let row = run();
        let bytes = rmp_serde::to_vec_named(&row).unwrap();
        let legacy = LegacyRun {
            id: row.id,
            bucket: row.bucket.clone(),
            config_id: row.config_id.clone(),
            run_key: row.run_key.clone(),
            started_at: row.started_at,
            state: row.state.clone(),
            job_id: row.job_id,
            manifest_key: row.manifest_key.clone(),
            error: row.error.clone(),
            created_at: row.created_at,
            fence_term: row.fence_term,
            fence_leader: row.fence_leader,
        };
        assert_eq!(bytes, rmp_serde::to_vec_named(&legacy).unwrap());
        assert_eq!(
            rmp_serde::from_slice::<super::InventoryRunRow>(&bytes).unwrap(),
            row
        );
    }

    #[test]
    fn inventory_run_rejects_unknown_fields() {
        #[derive(Serialize)]
        struct WithExtra {
            id: i64,
            bucket: String,
            config_id: String,
            run_key: String,
            started_at: i64,
            state: String,
            job_id: Option<i64>,
            manifest_key: Option<String>,
            error: Option<String>,
            created_at: i64,
            fence_term: i64,
            fence_leader: i64,
            future_field: u8,
        }
        let bytes = rmp_serde::to_vec_named(&WithExtra {
            id: 1,
            bucket: "b".into(),
            config_id: "c".into(),
            run_key: "k".into(),
            started_at: 0,
            state: "running".into(),
            job_id: None,
            manifest_key: None,
            error: None,
            created_at: 0,
            fence_term: 0,
            fence_leader: 0,
            future_field: 9,
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<super::InventoryRunRow>(&bytes).is_err());
    }

    #[derive(Serialize)]
    struct LegacyCursor {
        bucket: String,
        config_id: String,
        cursor: String,
        config_updated_at: i64,
        updated_at: i64,
        fence_term: i64,
        fence_leader: i64,
    }

    #[test]
    fn inventory_cursor_rmp_matches_runstate_extension() {
        let row = super::InventoryCursorRow {
            bucket: "b1".into(),
            config_id: "cfg".into(),
            cursor: "c".into(),
            config_updated_at: 4,
            updated_at: 5,
            fence_term: 6,
            fence_leader: 7,
        };
        let bytes = rmp_serde::to_vec_named(&row).unwrap();
        let legacy = LegacyCursor {
            bucket: row.bucket.clone(),
            config_id: row.config_id.clone(),
            cursor: row.cursor.clone(),
            config_updated_at: row.config_updated_at,
            updated_at: row.updated_at,
            fence_term: row.fence_term,
            fence_leader: row.fence_leader,
        };
        assert_eq!(bytes, rmp_serde::to_vec_named(&legacy).unwrap());
        assert_eq!(
            rmp_serde::from_slice::<super::InventoryCursorRow>(&bytes).unwrap(),
            row
        );
    }

    #[test]
    fn inventory_cursor_rejects_unknown_fields() {
        #[derive(Serialize)]
        struct WithExtra {
            bucket: String,
            config_id: String,
            cursor: String,
            config_updated_at: i64,
            updated_at: i64,
            fence_term: i64,
            fence_leader: i64,
            future_field: u8,
        }
        let bytes = rmp_serde::to_vec_named(&WithExtra {
            bucket: "b".into(),
            config_id: "c".into(),
            cursor: "cur".into(),
            config_updated_at: 1,
            updated_at: 2,
            fence_term: 3,
            fence_leader: 4,
            future_field: 5,
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<super::InventoryCursorRow>(&bytes).is_err());
    }

    #[derive(Serialize)]
    struct LegacyIncremental {
        bucket: String,
        config_id: String,
        enabled: bool,
        updated_at: i64,
    }

    #[test]
    fn inventory_incremental_rmp_matches_runstate_extension() {
        let row = super::InventoryIncrementalRow {
            bucket: "b1".into(),
            config_id: "cfg".into(),
            enabled: true,
            updated_at: 3,
        };
        let bytes = rmp_serde::to_vec_named(&row).unwrap();
        let legacy = LegacyIncremental {
            bucket: row.bucket.clone(),
            config_id: row.config_id.clone(),
            enabled: row.enabled,
            updated_at: row.updated_at,
        };
        assert_eq!(bytes, rmp_serde::to_vec_named(&legacy).unwrap());
        assert_eq!(
            rmp_serde::from_slice::<super::InventoryIncrementalRow>(&bytes).unwrap(),
            row
        );
    }

    #[test]
    fn inventory_incremental_rejects_unknown_fields() {
        #[derive(Serialize)]
        struct WithExtra {
            bucket: String,
            config_id: String,
            enabled: bool,
            updated_at: i64,
            future_field: u8,
        }
        let bytes = rmp_serde::to_vec_named(&WithExtra {
            bucket: "b".into(),
            config_id: "c".into(),
            enabled: false,
            updated_at: 1,
            future_field: 2,
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<super::InventoryIncrementalRow>(&bytes).is_err());
    }
}
