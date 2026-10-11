//! 列举机制：桶目录扫描 + ListObjectsV2 的过滤/分页（`Store` 的第二个 impl 块，
//! 从 store.rs 拆出；pub 面不变）。

use crate::error::{Error, Result};
use crate::layout;

use super::{ListRequest, ListResult, Store};

const DEFAULT_MAX_KEYS: usize = 1000;

impl Store {
    /// 列举：直接走盘。M1 每个对象读一次 xl.meta（O(n) 次系统调用），
    /// M4 用 scrub 的命名空间索引优化（ponytail: 先对，再快）。
    pub async fn list_objects(&self, bucket: &str, req: &ListRequest) -> Result<ListResult> {
        self.ensure_bucket(bucket).await?;
        let max = req.max_keys.unwrap_or(DEFAULT_MAX_KEYS).min(1000);
        // max-keys=0：空页且不截断（报截断会让客户端拿同一个 token 空转）
        if max == 0 {
            return Ok(ListResult::default());
        }
        // 空 delimiter 视同未指定：find("") 命中每个键的首字节，会把整份键都当 common prefix
        let delimiter = req.delimiter.as_deref().filter(|d| !d.is_empty());

        let mut keys = self.collect_keys(bucket).await?;
        keys.sort();

        let prefix = req.prefix.clone().unwrap_or_default();
        let start_after = req.continuation_token.clone();

        let mut objects = Vec::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut is_truncated = false;
        let mut next_token = None;

        for k in keys {
            if !k.starts_with(&prefix) {
                continue;
            }
            if let Some(after) = &start_after {
                // token = 本页「未返回的第一个 key」，恢复时包含该 key 本身
                if &k < after {
                    continue;
                }
            }
            if let Some(d) = delimiter {
                let rest = &k[prefix.len()..];
                if let Some(pos) = rest.find(d) {
                    let cp = format!("{prefix}{}", &rest[..pos + d.len()]);
                    // 去重检查在容量判断之前：已发过的 cp 不再占位、也不触发截断，
                    // 否则下一页会拿同一个 token 原地打转（空页或重复 cp）
                    if !common_prefixes.contains(&cp) {
                        if objects.len() + common_prefixes.len() >= max {
                            is_truncated = true;
                            next_token = Some(k.clone());
                            break;
                        }
                        common_prefixes.push(cp);
                    }
                    continue;
                }
            }
            if objects.len() + common_prefixes.len() >= max {
                is_truncated = true;
                next_token = Some(k.clone());
                break;
            }
            if let Some(m) = self.read_meta(bucket, &k).await? {
                objects.push(self.load_object_info(bucket, &k, &m)?);
            }
        }
        common_prefixes.sort();
        Ok(ListResult {
            objects,
            common_prefixes,
            is_truncated,
            next_token,
        })
    }

    // 可见性等价于原先在 crate::store 里的私有：store 子树（含 store::tests）可用，子树外不可见。
    pub(in crate::store) async fn collect_keys(&self, bucket: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut entries =
            match tokio::fs::read_dir(layout::bucket_dir(&self.cfg.root, bucket)).await {
                Ok(e) => e,
                // 桶被并发删除的窗口：映射 NoSuchBucket，而不是把裸 IO 错误抛给上层
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(Error::NoSuchBucket(bucket.to_string()));
                }
                Err(e) => return Err(e.into()),
            };
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            // 杂物目录（手工塞入、崩溃残留）解出的键非法：跳过并告警，不能让一个目录名
            // 把整桶列举打挂；真实对象的目录名由 object_dir_name 生成，必过同一套校验。
            match layout::key_from_dir_name(&name) {
                Ok(k) => keys.push(k),
                Err(e) => tracing::warn!(dir = %name, error = %e, "跳过非法对象目录"),
            }
        }
        Ok(keys)
    }
}
