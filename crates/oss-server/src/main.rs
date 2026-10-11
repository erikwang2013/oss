//! `oss-server [serve|groups|build|clean <days>]` 的命令行壳：
//! 解析参数 → 启动自举（`AppState::load`）→ 交给 `cli::run`。
//! 自举与运维命令共用同一条序列，避免 serve 与 cron 看到两套行为。

use oss_server::cli;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 先初始化日志：App::run() 里也有一次 init（has_been_set 守卫，不会重复），
    // 但启动自举（管理员引导/建桶）发生在 run() 之前，不提前 init 这些日志会被静默丢弃。
    ecat_logging::init();

    // args_os：非 UTF-8 参数不该 panic（101），走与解析错误同一条用法 + exit 2
    let args: Vec<String> = match std::env::args_os()
        .skip(1)
        .map(|a| a.into_string())
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(args) => args,
        Err(_) => {
            eprint!("{}", cli::USAGE);
            std::process::exit(2);
        }
    };
    let cmd = match cli::parse_args(&args) {
        Ok(cmd) => cmd,
        Err(usage) => {
            eprint!("{usage}");
            std::process::exit(2);
        }
    };

    let state = oss_server::state::AppState::load(std::path::Path::new(cli::CONFIG_PATH)).await?;

    let code = cli::run(cmd, state).await?;
    if code != 0 {
        std::process::exit(code);
    }
    Ok(())
}
