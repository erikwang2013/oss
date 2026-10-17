//! 运行配置：`oss.toml` 反序列化。

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub listen: String,
    /// 单盘形态的数据目录（`data_dirs` 未配置时使用）。M2b-2 起多盘用 `data_dirs`。
    pub data_dir: PathBuf,
    /// 多盘形态的数据盘数组（M2b-2）；配置了它则忽略 `data_dir`。
    /// EC 8+4 需 ≥ 12 盘；单盘 = 无冗余（仍有 EC 形态，见 xl-store `open` 的盘数裁决）。
    #[serde(default)]
    pub data_dirs: Option<Vec<PathBuf>>,
    /// EC 配置节 `[ec]`：enabled（默认 true）/ data（8）/ parity（4）/ inline_threshold（128 KiB）。
    #[serde(default)]
    pub ec: EcToml,
    pub staging_dir: PathBuf,
    pub database_url: String,
    pub jwt_secret: String,
    /// 单个 S3 PutObject 允许的最大字节数（内存上限）。
    /// ponytail: 存储内核自 M4 切片 2 起分块流式，此值更多是**防护上限**（挡异常客户端
    /// 的超大单请求），不再是内存必要约束；要放宽时按业务调整。
    #[serde(default = "default_put_limit")]
    pub max_put_bytes: usize,
    #[serde(default)]
    pub groups: Vec<GroupTarget>,
    /// M3 集群节 `[cluster]`；未配置 = 单节点（v1.4.0 行为）。
    #[serde(default)]
    pub cluster: Option<ClusterToml>,
    /// M6 切片 1：Kafka 通知目标 `[[kafka.targets]]`（静态配置，同集群 secret 的配置文件信任）。
    #[serde(default)]
    pub kafka: KafkaToml,
}

/// `[[kafka.targets]]`：桶通知 ARN `arn:oss:sqs::<id>:kafka` 的 id → 连接与主题映射。
/// **deny_unknown_fields**（P3-f 复查补）：`[[kafka.target]]` 这类拼错必须启动即拒，
/// 不得静默退回 Noop（事件全丢）。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaToml {
    #[serde(default)]
    pub targets: Vec<KafkaTarget>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaTarget {
    /// 与桶通知 ARN 中的 `<id>` 对应（白名单 `[A-Za-z0-9._-]{1,64}`，见 notify.rs）
    pub id: String,
    pub brokers: Vec<String>,
    pub topic: String,
}

/// `[cluster]`：扁平静态拓扑（设计 §5.3——无 DC/Region/AZ，M3+ 同样适用）。
#[derive(Debug, Clone, Deserialize)]
pub struct ClusterToml {
    /// 本节点 id（必须在 `nodes` 表内）。
    pub node_id: String,
    /// 集群内共享令牌（节点间 gRPC 的 metadata 校验；明文传输，内网假设）。
    pub secret: String,
    #[serde(default)]
    pub nodes: Vec<NodeToml>,
    /// M4 切片 5 放置开关：`"v2"` = 展开放置（扩容前全节点同步开启；混版会写失败可诊断）。
    #[serde(default)]
    pub placement: Option<String>,
}

/// 拓扑表中的一个节点。`addr` = 该节点 gRPC 监听地址（HTTP 端口另配）。
#[derive(Debug, Clone, Deserialize)]
pub struct NodeToml {
    pub id: String,
    pub addr: String,
    pub drive_count: u16,
    /// M4 切片 8：所属 set（缺省 0 = 单 set 部署；同物理节点可在多个 set 各出一条，
    /// 同 id 各条的 addr 必须一致——由 ClusterSets::new 校验）。
    #[serde(default)]
    pub set: Option<u16>,
}

/// `[ec]` 配置节（映射到 xl-store 的 `EcConfig` + `inline_threshold`）。
#[derive(Debug, Clone, Deserialize)]
pub struct EcToml {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_ec_data")]
    pub data: u8,
    #[serde(default = "default_ec_parity")]
    pub parity: u8,
    #[serde(default = "default_inline_threshold")]
    pub inline_threshold: u64,
}

impl Default for EcToml {
    fn default() -> Self {
        Self {
            enabled: true,
            data: default_ec_data(),
            parity: default_ec_parity(),
            inline_threshold: default_inline_threshold(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_ec_data() -> u8 {
    8
}
fn default_ec_parity() -> u8 {
    4
}
fn default_inline_threshold() -> u64 {
    // 与 xl-store meta::MAX_INLINE 一致（128 KiB）；超出会被 Store::open 拒绝
    128 * 1024
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GroupTarget {
    /// aetherupload 的分组名（**不能含下划线**）
    pub group: String,
    pub bucket: String,
    pub max_size: u64,
    pub extensions: Vec<String>,
}

fn default_put_limit() -> usize {
    256 * 1024 * 1024
}

impl AppConfig {
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("读取配置文件 {} 失败: {e}", path.display()))?;
        let cfg: Self = toml::from_str(&text)?;
        cfg.kafka_validate()
            .map_err(|e| format!("配置校验失败：{e}"))?;
        Ok(cfg)
    }

    /// M6 切片 1（P3-f）：`[[kafka.targets]]` 校验——id 白名单/唯一、brokers/topic 非空。
    /// 启动即 fail-closed（错配置不静默进死信）。
    pub fn kafka_validate(&self) -> Result<(), String> {
        let mut seen = std::collections::HashSet::new();
        for t in &self.kafka.targets {
            if t.id.is_empty()
                || t.id.len() > 64
                || !t
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
            {
                return Err(format!(
                    "[[kafka.targets]] id 非法：{:?}（白名单 [A-Za-z0-9._-]，≤64）",
                    t.id
                ));
            }
            if !seen.insert(t.id.clone()) {
                return Err(format!("[[kafka.targets]] id 重复：{}", t.id));
            }
            if t.brokers.is_empty() || t.brokers.iter().any(|b| b.trim().is_empty()) {
                return Err(format!("[[kafka.targets]] {} 的 brokers 不能为空", t.id));
            }
            if t.topic.trim().is_empty() {
                return Err(format!("[[kafka.targets]] {} 的 topic 不能为空", t.id));
            }
        }
        Ok(())
    }

    pub fn bucket_for_group(&self, group: &str) -> Option<&str> {
        self.groups
            .iter()
            .find(|g| g.group == group)
            .map(|g| g.bucket.as_str())
    }

    /// 密钥兜底解析（容器/无人值守部署；**显式填写优先，绝不覆盖**）：
    /// - 空 `jwt_secret` / 空集群 `secret` / 缺管理员口令 → 优先复用 `OSS_SECRETS_FILE`
    ///   已有值，缺失才生成（64 hex = 32 字节）；文件用 flock 串行化——
    ///   集群多节点并发首启共享同一份（后到者阻塞后重读，不会各生成一套）。
    /// - 未设文件：单机允许进程内生成（warn，重启即换）；**集群一律报错**——
    ///   各节点随机值 mesh 鉴权互不相认，属静默致命。
    /// - 管理员口令：`OSS_ADMIN_PASSWORD` 非空优先；空/缺**且配了文件**才生成（单机裸跑
    ///   行为不变：无口令就不引导管理员）。
    pub fn resolve_secrets(&mut self) -> Result<ResolvedSecrets, String> {
        let file = std::env::var("OSS_SECRETS_FILE")
            .ok()
            .filter(|p| !p.trim().is_empty());
        let admin_env = std::env::var("OSS_ADMIN_PASSWORD").ok();
        self.resolve_secrets_with(
            file.as_deref().map(std::path::Path::new),
            admin_env.as_deref(),
        )
    }

    /// 与 [`Self::resolve_secrets`] 同逻辑，但文件/环境显式传入（可单测，不碰进程环境）。
    pub(crate) fn resolve_secrets_with(
        &mut self,
        file: Option<&std::path::Path>,
        admin_env: Option<&str>,
    ) -> Result<ResolvedSecrets, String> {
        let need_jwt = self.jwt_secret.is_empty();
        let need_cluster = self.cluster.as_ref().is_some_and(|c| c.secret.is_empty());
        let need_admin = admin_env.map(|s| s.is_empty()).unwrap_or(true) && file.is_some();

        let mut out = ResolvedSecrets {
            admin_password: admin_env.filter(|s| !s.is_empty()).map(str::to_string),
            generated: false,
            secrets_file: file.map(std::path::Path::to_path_buf),
        };
        if !(need_jwt || need_cluster || need_admin) {
            return Ok(out);
        }
        if file.is_none() && (need_jwt || need_cluster) && self.cluster.is_some() {
            return Err(
                "集群的 jwt_secret/secret 未填写且未设 OSS_SECRETS_FILE——各节点必须共享同一份密钥（docker 栈已自动配置该文件）".into(),
            );
        }

        let mut have = std::collections::BTreeMap::<String, String>::new();
        let mut lock_file: Option<std::fs::File> = None;
        if let Some(f) = file {
            if let Some(dir) = f.parent() {
                let _ = std::fs::create_dir_all(dir);
            }
            let fh = std::fs::File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false) // 只复用不截断：已有值要重读；覆盖写发生在取得锁之后
                .open(f)
                .map_err(|e| format!("打开密钥文件 {} 失败: {e}", f.display()))?;
            // 跨进程串行：并发首启时后到者在锁上阻塞，重读拿到先到者已写入的同一份
            fh.lock()
                .map_err(|e| format!("锁密钥文件 {} 失败: {e}", f.display()))?;
            let text = std::fs::read_to_string(f).unwrap_or_default();
            for line in text.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((k, v)) = line.split_once('=') {
                    let v = v.trim().trim_matches('"');
                    if !v.is_empty() {
                        have.insert(k.trim().to_string(), v.to_string());
                    }
                }
            }
            lock_file = Some(fh);
        }

        if need_jwt {
            self.jwt_secret = get_or_gen(&mut have, "JWT_SECRET", &mut out.generated);
        }
        if need_cluster && let Some(c) = self.cluster.as_mut() {
            c.secret = get_or_gen(&mut have, "CLUSTER_SECRET", &mut out.generated);
        }
        if need_admin {
            out.admin_password = Some(get_or_gen(&mut have, "ADMIN_PASSWORD", &mut out.generated));
        }

        match (file, out.generated) {
            (Some(f), true) => {
                std::fs::write(f, render_secrets(&have))
                    .map_err(|e| format!("写密钥文件 {} 失败: {e}", f.display()))?;
                #[cfg(unix)]
                {
                    // 0644：容器内 oss(10001) 落盘、宿主用户（UID 多半不同）需直读口令——
                    // 0600 会让「本地保存一份」变成只有 docker exec 才看得到。
                    // 文件在 gitignore 的宿主目录内，能碰它的用户本就握有 docker（root 等价）。
                    use std::os::unix::fs::PermissionsExt as _;
                    let _ = std::fs::set_permissions(f, std::fs::Permissions::from_mode(0o644));
                }
                drop(lock_file);
            }
            (Some(_), false) => drop(lock_file),
            (None, true) => tracing::warn!(
                "密钥未填且未设 OSS_SECRETS_FILE——本次进程内随机生成，重启即更换（生产请设置该环境变量）"
            ),
            (None, false) => {}
        }
        Ok(out)
    }
}

/// [`AppConfig::resolve_secrets`] 的返回值。
#[derive(Debug)]
pub struct ResolvedSecrets {
    /// 管理员引导口令（环境变量优先；配了密钥文件时可能是生成值）。None = 不引导。
    pub admin_password: Option<String>,
    /// 本次是否发生生成（调用方可据此提示「口令见文件」）。
    pub generated: bool,
    pub secrets_file: Option<std::path::PathBuf>,
}

/// 复用已有值；缺失则生成（64 hex = 32 字节，满足 jwt ≥32B 与集群令牌要求）。
fn get_or_gen(
    have: &mut std::collections::BTreeMap<String, String>,
    key: &str,
    generated: &mut bool,
) -> String {
    if let Some(v) = have.get(key) {
        return v.clone();
    }
    *generated = true;
    let v = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    have.insert(key.to_string(), v.clone());
    v
}

fn render_secrets(have: &std::collections::BTreeMap<String, String>) -> String {
    let mut s = String::from(
        "# oss 自动生成的密钥（首次启动生成并保存；删除本文件可整体轮换，集群各节点共享同一份）\n\
         # 想用自有值：直接改这里，或在 oss.toml / 环境变量里显式填写（显式值优先）\n",
    );
    for k in ["JWT_SECRET", "CLUSTER_SECRET", "ADMIN_PASSWORD"] {
        if let Some(v) = have.get(k) {
            s.push_str(k);
            s.push('=');
            s.push_str(v);
            s.push('\n');
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_cfg() -> AppConfig {
        AppConfig {
            listen: "127.0.0.1:0".into(),
            data_dir: "d".into(),
            data_dirs: None,
            ec: Default::default(),
            staging_dir: "s".into(),
            database_url: "sqlite::memory:".into(),
            jwt_secret: String::new(),
            max_put_bytes: 1024,
            groups: vec![],
            kafka: KafkaToml::default(),
            cluster: None,
        }
    }

    fn cluster_cfg() -> AppConfig {
        let mut c = base_cfg();
        c.cluster = Some(ClusterToml {
            node_id: "n1".into(),
            secret: String::new(),
            nodes: vec![],
            placement: None,
        });
        c
    }

    /// 显式填写优先：已有值一律不动，管理员口令走环境。
    #[test]
    fn resolve_keeps_explicit_values_untouched() {
        let mut c = base_cfg();
        c.jwt_secret = "explicit-32-bytes-secret-value-abcdef".into();
        let r = c.resolve_secrets_with(None, Some("pw")).unwrap();
        assert!(!r.generated);
        assert_eq!(c.jwt_secret, "explicit-32-bytes-secret-value-abcdef");
        assert_eq!(r.admin_password.as_deref(), Some("pw"));
    }

    /// 首启生成并落盘；二启（模拟重启/集群另一节点）复用同一份（flock 串行化的同进程面）。
    #[test]
    fn resolve_generates_persists_and_reuses() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("secrets.env");

        let mut c1 = cluster_cfg();
        let r1 = c1.resolve_secrets_with(Some(&f), None).unwrap();
        assert!(r1.generated);
        assert_eq!(c1.jwt_secret.len(), 64, "32 字节 hex");
        assert_eq!(c1.cluster.as_ref().unwrap().secret.len(), 64);
        assert!(f.exists(), "应落盘到本地");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(&f).unwrap().permissions().mode() & 0o777,
                0o644,
                "宿主用户需可直读（权限=0644）"
            );
        }
        let jwt1 = c1.jwt_secret.clone();
        let cluster1 = c1.cluster.as_ref().unwrap().secret.clone();
        let admin1 = r1.admin_password.clone().unwrap();

        let mut c2 = cluster_cfg();
        let r2 = c2.resolve_secrets_with(Some(&f), None).unwrap();
        assert!(!r2.generated, "已有值不应触发再生成");
        assert_eq!(c2.jwt_secret, jwt1);
        assert_eq!(c2.cluster.as_ref().unwrap().secret, cluster1);
        assert_eq!(r2.admin_password.unwrap(), admin1);
    }

    /// 集群密钥缺失且未配文件 → 报错（各节点随机值会静默破坏 mesh 鉴权）。
    #[test]
    fn resolve_cluster_without_file_errors() {
        let mut c = cluster_cfg();
        let e = c.resolve_secrets_with(None, None).unwrap_err();
        assert!(e.contains("OSS_SECRETS_FILE"), "{e}");
    }

    /// 单机无文件：进程内生成 jwt（warn），但不生成管理员口令（保持裸跑不引导语义）。
    #[test]
    fn resolve_single_without_file_generates_in_memory_only() {
        let mut c = base_cfg();
        let r = c.resolve_secrets_with(None, None).unwrap();
        assert!(r.generated);
        assert_eq!(c.jwt_secret.len(), 64);
        assert!(
            r.admin_password.is_none(),
            "无文件时不生成管理员口令（裸跑无口令即不引导）"
        );
    }

    /// M6 切片 1（P3-f）：Kafka 目标配置校验矩阵（启动 fail-closed）。
    #[test]
    fn kafka_targets_validation_matrix() {
        let mk = |id: &str, brokers: Vec<&str>, topic: &str| KafkaTarget {
            id: id.into(),
            brokers: brokers.into_iter().map(str::to_string).collect(),
            topic: topic.into(),
        };
        let cfg = |targets| AppConfig {
            listen: "127.0.0.1:0".into(),
            data_dir: std::path::PathBuf::from("/tmp/x"),
            data_dirs: None,
            ec: Default::default(),
            staging_dir: std::path::PathBuf::from("/tmp/x"),
            database_url: "sqlite::memory:".into(),
            jwt_secret: "k".repeat(32),
            max_put_bytes: 1,
            groups: vec![],
            cluster: None,
            kafka: KafkaToml { targets },
        };
        // 合法
        assert!(
            cfg(vec![mk("t1", vec!["127.0.0.1:9092"], "events")])
                .kafka_validate()
                .is_ok()
        );
        // 非法 id（路径穿越/超长）
        assert!(
            cfg(vec![mk("../x", vec!["b:1"], "t")])
                .kafka_validate()
                .is_err()
        );
        assert!(
            cfg(vec![mk("a/b", vec!["b:1"], "t")])
                .kafka_validate()
                .is_err()
        );
        assert!(
            cfg(vec![mk(&"a".repeat(65), vec!["b:1"], "t")])
                .kafka_validate()
                .is_err()
        );
        // 重复 id / 空 brokers / 空 broker 串 / 空 topic
        assert!(
            cfg(vec![mk("t1", vec!["b:1"], "x"), mk("t1", vec!["b:2"], "y")])
                .kafka_validate()
                .is_err()
        );
        assert!(cfg(vec![mk("t1", vec![], "x")]).kafka_validate().is_err());
        assert!(
            cfg(vec![mk("t1", vec![" "], "x")])
                .kafka_validate()
                .is_err()
        );
        assert!(
            cfg(vec![mk("t1", vec!["b:1"], " ")])
                .kafka_validate()
                .is_err()
        );
    }
}
