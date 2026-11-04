//! `oss-server control-recertify`：集群 legacy 控制对象补证书（默认 dry-run）。
//!
//! 键列表在任何扫描前完成校验（空列表/重复/非 `control/`/非法键都拒绝）；集群能力
//! 门禁与写入证据由 xl-store 的 scan/recertify 负责。逐键继续、最后汇总，任一键失败
//! 退出 1；`Indeterminate` 必须按“可能已提交”处理，绝不假设回滚。

use std::collections::BTreeSet;
use std::path::Path;

use xl_store::error::Error as StoreError;
use xl_store::store::{LegacyControlState, RecertifyOutcome, Store};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// 读操作者提供的键列表：`#` 注释与空行忽略（兼容 CRLF）。任何非法输入都在
/// 第一次扫描前整份拒绝——漏键仍是残余风险，但错误键绝不静默跳过。
pub fn load_keys(path: &Path) -> Result<Vec<String>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("读取键列表 {} 失败：{e}", path.display()))?;
    let mut keys = Vec::new();
    let mut seen = BTreeSet::new();
    for (n, line) in text.lines().enumerate() {
        let key = line.trim();
        if key.is_empty() || key.starts_with('#') {
            continue;
        }
        if !key.starts_with("control/") {
            return Err(format!("第 {} 行不以 control/ 开头：{key:?}", n + 1));
        }
        xl_store::layout::validate_key(key).map_err(|e| format!("第 {} 行键非法：{e}", n + 1))?;
        if !seen.insert(key.to_string()) {
            return Err(format!("第 {} 行重复键：{key:?}", n + 1));
        }
        keys.push(key.to_string());
    }
    if keys.is_empty() {
        return Err("键列表为空：至少需要一个 control/ 对象".into());
    }
    Ok(keys)
}

struct GroupRow {
    version: u64,
    copies: usize,
    certified: usize,
    max_token: u64,
}

struct KeyReport {
    key: String,
    state: &'static str,
    version: Option<u64>,
    q_s: u16,
    copies: usize,
    certified: usize,
    groups: Vec<GroupRow>,
    token: Option<u64>,
}

fn state_name(state: LegacyControlState) -> &'static str {
    match state {
        LegacyControlState::Missing => "missing",
        LegacyControlState::AlreadyCommitted => "already-committed",
        LegacyControlState::Certifiable => "certifiable",
        LegacyControlState::CompletionNeeded => "completion-needed",
    }
}

async fn one(store: &Store, key: &str, apply: bool) -> Result<KeyReport, StoreError> {
    // apply 也先扫描取得报告口径；写前的权威证据仍由 recertify 在锁内重扫。
    let scan = store.scan_legacy_control_object(key).await?;
    let mut r = KeyReport {
        key: key.to_string(),
        state: state_name(scan.state),
        version: scan.chosen.map(|i| scan.groups[i].version),
        q_s: scan.q_s,
        copies: scan.copies.len(),
        certified: scan.chosen.map_or(0, |i| scan.groups[i].certified),
        groups: scan
            .groups
            .iter()
            .map(|g| GroupRow {
                version: g.version,
                copies: g.copies,
                certified: g.certified,
                max_token: g.max_token,
            })
            .collect(),
        token: None,
    };
    if apply {
        match store.recertify_system_object_cluster(key).await? {
            RecertifyOutcome::Missing => r.state = "missing",
            RecertifyOutcome::AlreadyCommitted { version, certified } => {
                r.state = "already-committed";
                r.version = Some(version);
                r.certified = certified;
            }
            RecertifyOutcome::Certified {
                version,
                certified: flipped,
                token,
            } => {
                r.state = "certified";
                r.version = Some(version);
                r.certified = flipped;
                r.token = Some(token);
            }
        }
    }
    Ok(r)
}

fn human_line(r: &KeyReport) -> String {
    let version = r.version.map_or_else(|| "-".into(), |v| format!("v{v}"));
    match r.state {
        "missing" => format!("{}: missing（无副本，no-op）", r.key),
        "already-committed" => format!(
            "{}: already-committed {version}（证书 {} 份，no-op）",
            r.key, r.certified
        ),
        "certified" => format!(
            "{}: certified {version}（本次翻转 {} 槽，token {}）",
            r.key,
            r.certified,
            r.token.unwrap_or(0)
        ),
        state => format!(
            "{}: {state} {version}（Q_s={}，副本 {}，证书 {}，组 {}）",
            r.key,
            r.q_s,
            r.copies,
            r.certified,
            r.groups.len()
        ),
    }
}

struct ExecOutcome {
    stdout: String,
    stderr: String,
    code: i32,
}

/// 执行并渲染输出；`run` 只负责打印。逐键继续：单键失败计入汇总但不中断后续键。
async fn execute(store: &Store, keys: &[String], apply: bool, json: bool) -> ExecOutcome {
    let mut stdout = String::new();
    let mut stderr = String::new();
    let mut entries = Vec::with_capacity(keys.len());
    let mut failed = 0usize;
    let mut missing = 0usize;
    for key in keys {
        match one(store, key, apply).await {
            Ok(r) => {
                missing += usize::from(r.state == "missing");
                if json {
                    entries.push(serde_json::json!({
                        "key": r.key,
                        "ok": true,
                        "state": r.state,
                        "version": r.version,
                        "q_s": r.q_s,
                        "copies": r.copies,
                        "certified": r.certified,
                        "token": r.token,
                        "groups": r.groups.iter().map(|g| serde_json::json!({
                            "version": g.version,
                            "copies": g.copies,
                            "certified": g.certified,
                            "max_token": g.max_token,
                        })).collect::<Vec<_>>(),
                    }));
                } else {
                    stdout.push_str(&human_line(&r));
                    stdout.push('\n');
                }
            }
            Err(e) => {
                failed += 1;
                let msg = if matches!(e, StoreError::Indeterminate(_)) {
                    format!("可能已提交（Indeterminate），禁止假设回滚；请重跑同一命令核对：{e}")
                } else {
                    e.to_string()
                };
                if json {
                    entries.push(serde_json::json!({
                        "key": key,
                        "ok": false,
                        "error": msg,
                    }));
                } else {
                    stderr.push_str(&format!("Error: {key}: {msg}\n"));
                }
            }
        }
    }
    if json {
        stdout.push_str(&format!(
            "{}\n",
            serde_json::json!({
                "applied": apply,
                "ok": failed == 0,
                "missing": missing,
                "keys": entries,
            })
        ));
    } else {
        let nothing = if failed == 0 && missing == keys.len() {
            "；nothing to do"
        } else {
            ""
        };
        stdout.push_str(&format!(
            "control-recertify{}: {} 键 / missing {} / 失败 {}{nothing}\n",
            if apply {
                "（apply）"
            } else {
                "（dry-run）"
            },
            keys.len(),
            missing,
            failed,
        ));
    }
    ExecOutcome {
        stdout,
        stderr,
        code: if failed == 0 { 0 } else { 1 },
    }
}

/// 执行一条 `control-recertify`。返回进程退出码；键列表/环境错误走 `Err`。
pub async fn run(
    store: &Store,
    keys_path: &Path,
    apply: bool,
    json: bool,
) -> Result<i32, BoxError> {
    let keys = load_keys(keys_path)?;
    if store.cluster().is_none() {
        return Err(
            "control-recertify 仅支持集群部署；单节点请用库 API recertify_system_object".into(),
        );
    }
    let out = execute(store, &keys, apply, json).await;
    print!("{}", out.stdout);
    eprint!("{}", out.stderr);
    Ok(out.code)
}

#[cfg(test)]
#[path = "control_recertify_tests.rs"]
mod tests;
