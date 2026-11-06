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

    /// C1：唯一用户名洪水必须被硬上限兜住，per_user 不得无界增长。
    #[test]
    fn per_user_entries_capped() {
        let l = LoginLimiter::new();
        for i in 0..USER_ENTRIES_MAX + 64 {
            l.on_failure(&format!("flood-{i}"));
        }
        let st = l.inner.lock().unwrap();
        assert_eq!(st.per_user.len(), USER_ENTRIES_MAX, "条目数应被硬上限截断");
    }

    /// C1：插入前淘汰过期窗口——过期条目让位，活跃条目保留。
    #[test]
    fn expired_windows_evicted_on_insert() {
        let l = LoginLimiter::new();
        let now = limiter_now();
        {
            let mut st = l.inner.lock().unwrap();
            st.per_user.insert(
                "stale".into(),
                UserWindow {
                    fails: 1,
                    window_start: now - USER_WINDOW_SECS - 1,
                    locked_until: 0,
                },
            );
            st.per_user.insert(
                "active".into(),
                UserWindow {
                    fails: 1,
                    window_start: now,
                    locked_until: 0,
                },
            );
        }
        l.on_failure("fresh");
        let st = l.inner.lock().unwrap();
        assert!(!st.per_user.contains_key("stale"), "过期窗口应被清掉");
        assert!(st.per_user.contains_key("active"));
        assert!(st.per_user.contains_key("fresh"));
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

mod object_admin_tests {
    use super::*;
    use crate::control_backend::ObjectControl;
    use xl_store::control_state::root::Mode;
    use xl_store::store::{Store, StoreConfig};

    async fn attach_db(store: Arc<Store>) -> Db {
        let object = ObjectControl::open(store, Mode::Standalone, None)
            .await
            .unwrap();
        let db = Db::connect_object().await.unwrap();
        db.attach_object_control(object).unwrap();
        db
    }

    async fn object_db(dir: &std::path::Path) -> (Db, Arc<Store>) {
        std::fs::create_dir_all(dir).unwrap();
        let mut cfg = StoreConfig::new(dir);
        cfg.fsync = false;
        let store = Arc::new(Store::open(cfg).await.unwrap());
        let db = attach_db(Arc::clone(&store)).await;
        (db, store)
    }

    #[tokio::test]
    async fn fresh_object_root_seeds_env_admin() {
        let dir = tempfile::tempdir().unwrap();
        let (db, _store) = object_db(&dir.path().join("data")).await;
        assert!(
            ensure_object_admin(&db, "env-admin", "env-pw-123456")
                .await
                .unwrap()
        );
        let uid = db
            .verify_user("env-admin", "env-pw-123456")
            .await
            .unwrap()
            .expect("环境管理员必须能登录");
        assert_eq!(
            db.get_user(uid).await.unwrap(),
            Some(("env-admin".into(), true, true))
        );
        assert_eq!(
            db.schema_meta("admin_username").await.unwrap().as_deref(),
            Some("env-admin"),
            "配对/身份用的 admin_username 须写入"
        );
    }

    #[tokio::test]
    async fn restart_keeps_admin_and_password() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let (db, store) = object_db(&root).await;
        assert!(
            ensure_object_admin(&db, "env-admin", "env-pw-123456")
                .await
                .unwrap()
        );
        let uid = db
            .verify_user("env-admin", "env-pw-123456")
            .await
            .unwrap()
            .unwrap();
        db.set_password(uid, "changed-pw-123").await.unwrap();
        drop(db);

        // 重启：同一对象根重新装配（admin_username marker 已存在 → 不再建号/改密）。
        let db = attach_db(store).await;
        assert!(
            !ensure_object_admin(&db, "env-admin", "env-pw-123456")
                .await
                .unwrap()
        );
        assert_eq!(
            db.verify_user("env-admin", "changed-pw-123").await.unwrap(),
            Some(uid)
        );
        assert_eq!(
            db.verify_user("env-admin", "env-pw-123456").await.unwrap(),
            None,
            "环境口令不得覆盖已有账号口令"
        );
        assert_eq!(db.count_admins().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn deleted_env_admin_is_not_resurrected() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let (db, store) = object_db(&root).await;
        assert!(
            ensure_object_admin(&db, "env-admin", "env-pw-123456")
                .await
                .unwrap()
        );
        let uid = db
            .verify_user("env-admin", "env-pw-123456")
            .await
            .unwrap()
            .unwrap();
        db.create_user("second-admin", "second-pw-123", true)
            .await
            .unwrap();
        assert!(db.delete_user(uid).await.unwrap());
        assert!(!db.user_exists("env-admin").await.unwrap());
        drop(db);

        let db = attach_db(store).await;
        assert!(
            !ensure_object_admin(&db, "env-admin", "env-pw-123456")
                .await
                .unwrap()
        );
        assert!(
            !db.user_exists("env-admin").await.unwrap(),
            "有意删除的管理员不得在重启时复活"
        );
    }

    #[tokio::test]
    async fn existing_non_admin_is_not_promoted() {
        let dir = tempfile::tempdir().unwrap();
        let (db, _store) = object_db(&dir.path().join("data")).await;
        db.create_user("env-admin", "user-pw-123456", false)
            .await
            .unwrap();
        assert!(
            !ensure_object_admin(&db, "env-admin", "env-pw-123456")
                .await
                .unwrap()
        );
        let uid = db
            .verify_user("env-admin", "user-pw-123456")
            .await
            .unwrap()
            .expect("已存在账号保持原口令");
        assert_eq!(
            db.get_user(uid).await.unwrap(),
            Some(("env-admin".into(), false, true)),
            "同名非管理员不得被提升"
        );
        assert_eq!(
            db.verify_user("env-admin", "env-pw-123456").await.unwrap(),
            None
        );
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
        use crate::control_backend::ObjectControl;
        use xl_store::control_state::root::Mode;
        use xl_store::store::{Store, StoreConfig};
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = StoreConfig::new(dir.path());
        cfg.fsync = false;
        let store = Arc::new(Store::open(cfg).await.unwrap());
        let object = ObjectControl::open(store, Mode::Standalone, None)
            .await
            .unwrap();
        let db = Db::connect_object().await.unwrap();
        db.attach_object_control(object).unwrap();
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
