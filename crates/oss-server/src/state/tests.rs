//! `state` 模块的单元测试（原内联模块迁移，测试项不变）。

use super::*;

mod login_limiter_tests {
    use super::*;

    /// 5 次失败 → 锁定；锁定期内即使口令正确也拒（check 先于校验）；成功清零。
    #[test]
    fn user_lockout_and_reset() {
        let l = LoginLimiter::new();
        for _ in 0..USER_FAILS_MAX {
            assert!(l.check("u").is_ok());
            l.on_failure("u");
        }
        assert!(
            l.check("u").is_err(),
            "第 {} 次失败后应锁定",
            USER_FAILS_MAX
        );
        // 别的账号不受影响
        assert!(l.check("v").is_ok());
        // 过期解锁（直接拨表：把锁定窗口拨到过去）
        {
            let mut st = l.inner.lock().unwrap();
            let w = st.per_user.get_mut("u").unwrap();
            w.locked_until = limiter_now() - 1;
            w.window_start = limiter_now() - 1;
        }
        assert!(l.check("u").is_ok(), "锁定过期应放行");
        // 成功后窗口清零
        l.on_failure("w");
        l.on_success("w");
        assert!(l.check("w").is_ok());
        assert!(!l.inner.lock().unwrap().per_user.contains_key("w"));
    }

    /// 全局失败上限：跨账号洪水被整体暂拒；窗口滑动后恢复。
    #[test]
    fn global_flood_ceiling() {
        let l = LoginLimiter::new();
        for i in 0..GLOBAL_FAILS_MAX {
            l.on_failure(&format!("flood{i}"));
        }
        assert!(l.check("anyone").is_err(), "全局失败上限应暂拒全体");
        // 拨回窗口 → 恢复
        {
            let mut st = l.inner.lock().unwrap();
            st.global.1 = limiter_now() - GLOBAL_WINDOW_SECS - 1;
        }
        assert!(l.check("anyone").is_ok(), "窗口滑过后应恢复");
    }
}

mod set_group_tests {
    use crate::config::sets_nodes_by_set;

    fn node(id: &str, set: Option<u16>) -> crate::config::NodeToml {
        crate::config::NodeToml {
            id: id.into(),
            addr: "127.0.0.1:1".into(),
            drive_count: 4,
            set,
        }
    }

    #[test]
    fn groups_by_set_with_default_zero() {
        let groups = sets_nodes_by_set(&[
            node("n1", None),
            node("n2", None),
            node("n1", Some(1)),
            node("n2", Some(1)),
        ])
        .unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[1].len(), 2);
        assert_eq!(groups[0][0].id, "n1");
        assert_eq!(groups[1][0].id, "n1", "同 id 跨 set 各一条");
    }

    #[test]
    fn non_contiguous_sets_rejected() {
        // 只有 set=1：缺 set0 → fail-closed（拓扑数组下标会错位）
        let err = sets_nodes_by_set(&[node("n1", Some(1))]).unwrap_err();
        assert!(err.contains("连续"), "{err}");
        let err = sets_nodes_by_set(&[node("n1", None), node("n2", Some(2))]).unwrap_err();
        assert!(err.contains("连续"), "{err}");
    }
}

mod stretch_startup_tests {
    use super::*;

    fn drives(tmp: &tempfile::TempDir) -> Vec<PathBuf> {
        let d = tmp.path().join("d0");
        std::fs::create_dir_all(&d).unwrap();
        vec![d]
    }

    async fn empty_db() -> Db {
        Db::connect("sqlite::memory:").await.unwrap()
    }

    #[tokio::test]
    async fn fresh_store_ok_then_marker_skips_emptiness() {
        let db = empty_db().await;
        let tmp = tempfile::tempdir().unwrap();
        let ds = drives(&tmp);
        std::fs::create_dir_all(ds[0].join(".oss.sys")).unwrap();
        assert!(stretch_startup_check(&db, &ds).await.unwrap());
        db.schema_meta_set_if_absent("stretch_initialized", "1")
            .await
            .unwrap();
        // 非首次启动不再要求空库/空盘（历史数据正常服务）
        db.insert_bucket("old-b", None).await.unwrap();
        assert!(!stretch_startup_check(&db, &ds).await.unwrap());
    }

    #[tokio::test]
    async fn fresh_store_with_missing_dirs_ok() {
        let db = empty_db().await;
        let tmp = tempfile::tempdir().unwrap();
        let missing = vec![tmp.path().join("not-created-yet")];
        assert!(stretch_startup_check(&db, &missing).await.unwrap());
    }

    #[tokio::test]
    async fn old_store_with_buckets_rejected() {
        let db = empty_db().await;
        let tmp = tempfile::tempdir().unwrap();
        let ds = drives(&tmp);
        db.insert_bucket_full("b", None, 0, "async").await.unwrap();
        let e = stretch_startup_check(&db, &ds).await.unwrap_err();
        assert!(e.contains("全新空库"), "{e}");
        assert!(e.contains("buckets 共 1 行"), "{e}");
    }

    #[tokio::test]
    async fn sync_bucket_rejected() {
        let db = empty_db().await;
        let tmp = tempfile::tempdir().unwrap();
        let ds = drives(&tmp);
        db.insert_bucket_full("s", None, 0, "sync").await.unwrap();
        let e = stretch_startup_check(&db, &ds).await.unwrap_err();
        assert!(e.contains("sync 桶 1 个"), "{e}");
    }

    #[tokio::test]
    async fn bucket_dir_rejected() {
        let db = empty_db().await;
        let tmp = tempfile::tempdir().unwrap();
        let ds = drives(&tmp);
        std::fs::create_dir_all(ds[0].join("oldbucket")).unwrap();
        let e = stretch_startup_check(&db, &ds).await.unwrap_err();
        assert!(e.contains("全新空存储"), "{e}");
    }

    #[tokio::test]
    async fn site_outbox_rows_rejected_even_after_marker() {
        let db = empty_db().await;
        let tmp = tempfile::tempdir().unwrap();
        let ds = drives(&tmp);
        db.schema_meta_set_if_absent("stretch_initialized", "1")
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO repl_outbox (bucket, key, version_id, etag, size, remote_id, \
             dest_bucket, target_kind) VALUES ('b','k','v','e',1,'r','b','site')",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let e = stretch_startup_check(&db, &ds).await.unwrap_err();
        assert!(e.contains("target_kind='site'"), "{e}");
    }
}

mod ap_endpoint_registry_tests {
    use super::*;
    use crate::config::{ApToml, ApVpcEndpointToml};

    fn ap_toml(account_id: &str) -> ApToml {
        ApToml {
            vpc_endpoints: vec![ApVpcEndpointToml {
                vpce_id: "vpce-abc".into(),
                vpc_id: "vpc-abc".into(),
                account_id: account_id.into(),
                state: "active".into(),
            }],
        }
    }

    /// 启动校验：账户存在且 active 才放行；查无/停用失败；空表零改动。
    #[tokio::test]
    async fn validates_account_exists_and_active() {
        let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
        let id = db.create_account("acct", None).await.unwrap();
        validate_ap_endpoints(&db, &ap_toml(&id)).await.unwrap();
        validate_ap_endpoints(&db, &ApToml::default())
            .await
            .unwrap();

        let e = validate_ap_endpoints(&db, &ap_toml("999999999999"))
            .await
            .unwrap_err();
        assert!(e.contains("不存在"), "{e}");

        db.set_account_status(&id, "disabled").await.unwrap();
        let e = validate_ap_endpoints(&db, &ap_toml(&id)).await.unwrap_err();
        assert!(e.contains("disabled"), "{e}");
    }
}
