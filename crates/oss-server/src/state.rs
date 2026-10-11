//! 请求间共享的应用状态。

use std::sync::Arc;

use crate::config::AppConfig;
use crate::db::Db;
use jwt_rust::config::JwtConfig;
use jwt_rust::integrations::JwtAuth;
use xl_store::store::{Store, StoreConfig};

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<AppConfig>,
    pub db: Arc<Db>,
    pub store: Arc<Store>,
    pub ingest: Arc<crate::aether::IngestSink>,
    /// 控制面（`/api/v1`）与上传口（`/aetherupload`）共用的 JWT 门面：
    /// 登录用它签发，路由层用同一实例的 `JwtLayer` 校验（跨面 token 因此天然互通）。
    pub jwt: Arc<JwtAuth>,
}

impl AppState {
    /// 启动自举：读配置、建目录、连库、开存储、引导管理员与桶。
    /// serve 与运维命令（`cli::run`）共用这一条序列。
    pub async fn load(
        cfg_path: &std::path::Path,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let cfg = AppConfig::load(cfg_path)?;

        std::fs::create_dir_all(&cfg.data_dir)?;
        std::fs::create_dir_all(&cfg.staging_dir)?;

        let db = Db::connect(&cfg.database_url).await?;
        let store = Store::open(StoreConfig::new(&cfg.data_dir)).await?;

        // M1 还没有注册界面：用环境变量引导出第一个管理员（幂等，已存在则跳过）。
        // OSS_ADMIN_USER / OSS_ADMIN_PASSWORD 未设置时不做任何事。
        // 空凭据一律拒绝建号：空口令账号会被空口令登录命中（M1 的 verify_user 只做哈希比对，无口令强度校验）。
        match (
            std::env::var("OSS_ADMIN_USER"),
            std::env::var("OSS_ADMIN_PASSWORD"),
        ) {
            (Ok(user), Ok(pass)) if !user.is_empty() && !pass.is_empty() => {
                // 已存在的同名账号一律不覆盖（含口令不同的情况）
                if !db.user_exists(&user).await? {
                    db.create_user(&user, &pass, true).await?;
                    tracing::info!(user = %user, "创建管理员账号");
                }
            }
            (Ok(user), Ok(pass)) => {
                tracing::warn!(
                    user_empty = user.is_empty(),
                    pass_empty = pass.is_empty(),
                    "管理员凭据为空字符串，跳过引导（空口令账号会被空登录命中）"
                );
            }
            (Ok(_), Err(_)) | (Err(_), Ok(_)) => {
                tracing::warn!("OSS_ADMIN_USER / OSS_ADMIN_PASSWORD 只设置了一个，跳过管理员引导");
            }
            (Err(_), Err(_)) => {}
        }

        // 每个上传分组对应的桶，启动时确保存在（内核 + 控制面各一份记录）
        for g in &cfg.groups {
            if !store.bucket_exists(&g.bucket).await? {
                store.create_bucket(&g.bucket).await?;
            }
            db.insert_bucket(&g.bucket, None).await?;
        }

        Ok(Self::new(cfg, db, store))
    }

    pub fn new(cfg: AppConfig, db: Db, store: Store) -> Self {
        // except 只有 `/login`：console 路由经 nest 时 axum 的 StripPrefix 会先把
        // `/api/v1` 前缀剥掉，JwtLayer 看到的是 `/login`；aether 面路径不含 `/login` 段
        // （`/aetherupload/display/login` 之类整段不匹配），照常校验。
        // secret 短于 32 字节是配置错误（jwt-rust 的 256 位下限），启动即失败。
        let jwt = Arc::new(
            JwtAuth::from_config(JwtConfig {
                secret_key: cfg.jwt_secret.clone(),
                default_expire: 12 * 3600,
                middleware: jwt_rust::config::MiddlewareConfig {
                    except: vec!["/login".into()],
                },
                ..Default::default()
            })
            .expect("jwt_secret 非法：jwt-rust 要求至少 32 字符（256 位）"),
        );
        let cfg = Arc::new(cfg);
        let db = Arc::new(db);
        let ingest = Arc::new(crate::aether::IngestSink {
            store: Arc::new(store),
            db: Arc::clone(&db),
            cfg: Arc::clone(&cfg),
        });
        Self {
            cfg,
            db,
            store: Arc::clone(&ingest.store),
            ingest,
            jwt,
        }
    }
}
