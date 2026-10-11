//! 请求间共享的应用状态。

use std::sync::Arc;

use crate::config::AppConfig;
use crate::db::Db;
use jwt_rust::config::JwtConfig;
use jwt_rust::integrations::JwtAuth;
use xl_store::store::Store;

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
