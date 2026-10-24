//! 磁盘预留的跨进程账目：子进程持占位，父进程必须看得到（同一文件系统锚点）。

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use oss_server::tier::{filesystem_available, reserve_free_space, reserved_bytes};

const CHILD_DIR_ENV: &str = "OSS_TIER_RESERVE_CHILD_DIR";
const CHILD_BYTES_ENV: &str = "OSS_TIER_RESERVE_CHILD_BYTES";

/// 子进程入口：持有占位直到 stdin 关闭。正常 `cargo test` 无 env → 直接通过。
#[test]
fn reservation_child_holds() {
    let Ok(dir) = std::env::var(CHILD_DIR_ENV) else {
        return;
    };
    let bytes: u64 = std::env::var(CHILD_BYTES_ENV)
        .expect("子进程缺字节数")
        .parse()
        .expect("子进程字节数非法");
    let reservation = reserve_free_space(Path::new(&dir), bytes, 0).expect("子进程预留失败");
    println!("holding");
    std::io::stdout().flush().unwrap();
    // 父进程读完一行后写一行再关闭；读到 EOF 前一直持有。
    let mut line = String::new();
    let _ = std::io::stdin().read_line(&mut line);
    drop(reservation);
}

fn wait_for<T: PartialEq>(want: T, mut f: impl FnMut() -> T) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if f() == want {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    f() == want
}

/// 跨进程：子进程的稀疏占位进入同一锚点账目；父进程基于可用空间的申请被拒
/// （若只看本进程已持有量就会放行）；子进程退出后账目归零。
#[test]
fn reservation_is_shared_across_processes() {
    let d = tempfile::tempdir().unwrap();
    let dir = d.path().to_path_buf();
    let baseline = reserved_bytes(&dir).unwrap();
    let hold: u64 = 1 << 20; // 1 MiB 稀疏占位，不真占数据块

    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("reservation_child_holds")
        .arg("--nocapture")
        .env(CHILD_DIR_ENV, &dir)
        .env(CHILD_BYTES_ENV, hold.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("启动子进程失败");
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        let n = stdout.read_line(&mut line).expect("读取子进程输出失败");
        if n == 0 {
            panic!("子进程未就绪即退出");
        }
        if line.contains("holding") {
            break;
        }
    }

    // 子进程仍持有：账目 = baseline + hold（跨进程可见）。
    assert!(
        wait_for(baseline + hold, || reserved_bytes(&dir).unwrap()),
        "父进程看不到子进程的预留：{} ≠ {}",
        reserved_bytes(&dir).unwrap(),
        baseline + hold
    );

    // 再申请一份完整可用空间：只可能因子进程的 hold 被拒。
    let avail = filesystem_available(&dir).unwrap();
    let err = reserve_free_space(&dir, avail, 0).unwrap_err();
    assert!(err.contains("磁盘剩余空间不足"), "{err}");

    // 放行子进程：占位删除，账目回到 baseline。
    child.stdin.take().unwrap().write_all(b"\n").unwrap();
    assert!(child.wait().expect("等待子进程失败").success());
    assert!(
        wait_for(baseline, || reserved_bytes(&dir).unwrap()),
        "子进程退出后预留未释放：{} ≠ {baseline}",
        reserved_bytes(&dir).unwrap()
    );
}
