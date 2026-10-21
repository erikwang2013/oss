//! `oss-server [serve|groups|build|clean <days>]` 的命令行壳：
//! 解析参数 → 启动自举（`AppState::load`）→ 交给 `cli::run`。
//! 自举与运维命令共用同一条序列，避免 serve 与 cron 看到两套行为。

use oss_server::cli;

#[tokio::main]
async fn main() -> std::process::ExitCode {
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
    // `--config` 先摘出：witness/成员管理命令不得走 AppState::load（会二次打开 raft.db）。
    let (cfg_path, args) = match cli::split_config(&args) {
        Ok(v) => v,
        Err(usage) => {
            eprint!("{usage}");
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

    let code = match if cli::needs_full_state(&cmd) {
        let state = match oss_server::state::AppState::load(&cfg_path).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Error: {e}");
                return std::process::ExitCode::FAILURE;
            }
        };
        cli::run(cmd, state).await
    } else {
        cli::run_offline(cmd, &cfg_path).await
    } {
        Ok(c) => c,
        Err(e) => {
            // Display 而非 Debug：配置错误里的引号不再被转义（运维/负例脚本可直读）。
            eprintln!("Error: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };
    if code != 0 {
        std::process::ExitCode::from(code.clamp(1, 255) as u8)
    } else {
        std::process::ExitCode::SUCCESS
    }
}
