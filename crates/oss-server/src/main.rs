//! e-cat App 组装与启动自举。

use ecat::App;
use ecat::transport_http::HttpServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 先初始化日志：App::run() 里也有一次 init（has_been_set 守卫，不会重复），
    // 但启动自举（管理员引导/建桶）发生在 run() 之前，不提前 init 这些日志会被静默丢弃。
    ecat_logging::init();

    let cfg_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "oss.toml".to_string());
    let cfg = oss_server::config::AppConfig::load(std::path::Path::new(&cfg_path))?;

    std::fs::create_dir_all(&cfg.data_dir)?;
    std::fs::create_dir_all(&cfg.staging_dir)?;

    let db = oss_server::db::Db::connect(&cfg.database_url).await?;
    let store =
        xl_store::store::Store::open(xl_store::store::StoreConfig::new(&cfg.data_dir)).await?;

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

    let listen = cfg.listen.clone();
    let state = oss_server::state::AppState::new(cfg, db, store);
    let router = oss_server::router::build_router(state);

    let mut app = App::builder()
        .name("oss")
        .server(HttpServer::new(listen).router(router))
        .build()?;
    app.run().await?;
    Ok(())
}
