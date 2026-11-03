//! Phase 0：控制对象（`.oss.sys` 保留命名空间内的 system-object）。
//!
//! CAS = 串行化（本机两层对象锁：进程内 per-key mutex + `flock`；集群再加 fenced 集群锁）+
//! quorum 读旧 meta + `expected_version`/`fence_token` 校验 + fenced `write_meta` 提交（提交侧
//! 再按已提交 token 拒绝过期写）。只要两个 Store 打开同一 root（同进程或不同进程），同一 key
//! 的读-检-写互斥；`flock` 是本地文件系统上的 advisory 锁，不覆盖网络文件系统/跨机器。
//!
//! 集群分支：锁 token 由 [`Store::acquire_fenced_cluster_lock`] 持久化发放并作为提交 token
//! （调用方 token 仍按 Task 2 契约校验单调性）。读**全量扇出**：覆盖 ≥ `Q_s` 槽才允许
//! 判定（缺席证明另要求 `2*Q_s > N`），可见版本 = 任一**带提交证书**的副本——不要求
//! 内容相同的副本数达到 `Q_s`（`Q_s` 只约束覆盖与提交翻转数）。覆盖不足/版本分叉/
//! 无证书但有副本 → fail-closed。
//!
//! 写失败契约：第一阶段失败 = 未提交（读不可见）；第二阶段（提交证书）失败但已落
//! 1..<Q_s 份证书 → [`Error::Indeterminate`]——写**可能已提交**。任何控制写错误都必须
//! 按 key 重读核对 token/version，绝不假设回滚。
//!
//! 键生命周期：control key 是 create-once 命名——创建后只做 CAS 更新，删除写 tombstone
//! （普通版本：`control_deleted = Some(true)`、数据为空，占用版本号并参与 CAS）；delete
//! 幂等（不存在/已 tombstone → `Ok(false)`）。tombstone 后重建必须用 put 以更高版本/token
//! 越过它，旧 token 永久失效。集群控制写要求 `N >= data + 2`（1 个 preserve 槽 +
//! `Q_s` 个翻转槽；写容错 = `N - data - 2`）。删除后的版本号读接口不可见（get 返回
//! None），只能按「delete 成功即 +1」推算。
//! 集群枚举不做：Phase 0 的 control manifest 携带键集合，留待 Phase 1。

use md5::Digest as _;

use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, InlineData, PartRef, Version, XlMeta};

use super::{LockGuard, Store};

/// 控制对象写锁：集群下 fenced 集群锁先声明 → 先 Drop（与获取顺序相反：
/// 本机 mutex → flock → fenced 集群锁）。
struct ControlGuard {
    fenced: Option<super::lock::FencedLockGuard>,
    _local: LockGuard,
}

impl ControlGuard {
    /// 集群租约是否丢失（单机恒 false）——写入口在提交点前检查。
    fn lease_lost(&self) -> bool {
        self.fenced.as_ref().is_some_and(|g| g.lease_lost())
    }

    /// 提交 token：集群 = 持久化锁发放的 token（并校验其严格大于已提交 token）；
    /// 单机/多盘 = 调用方提供的 token（Task 2 语义逐字节不变）。
    fn commit_token(&self, caller_token: u64, cur_fence: u64) -> Result<u64> {
        let Some(g) = &self.fenced else {
            return Ok(caller_token);
        };
        if g.token <= cur_fence {
            return Err(Error::Conflict(format!(
                "fence token 已过期：锁发放 {} ≤ 已提交 {cur_fence}",
                g.token
            )));
        }
        Ok(g.token)
    }
}

/// 控制对象键前缀：`object_dir_system` 只拒绝与系统子项同名的首段，`control` 不在其列。
pub(super) const CONTROL_PREFIX: &str = "control/";

#[derive(Debug, Clone)]
pub struct SystemObject {
    pub key: String,
    pub data: Vec<u8>,
    pub version: u64,
    pub fence_token: u64,
    pub deleted: bool,
}

impl Store {
    /// 写控制对象（CAS）：`expected_version`/`fence_token` 与当前 meta 不符 → `Error::Conflict`。
    /// 返回新版本号（每次成功写入 +1）。
    ///
    /// 集群模式下调用方 token 只是 CAS 前置条件：提交/存储的是 fenced 锁发放的 token
    /// （每次成功获取都推进，失败的 CAS 尝试也会留下空洞，因此会高于调用方 token）。
    ///
    /// 失败可能已提交：返回 [`Error::Indeterminate`] 时部分证书可能已落盘——调用方
    /// 必须按 key 重读核对 token/version，绝不假设回滚。
    pub async fn put_system_object(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<u64>,
        fence_token: u64,
    ) -> Result<u64> {
        validate_control_key(key)?;
        if data.len() as u64 > meta::MAX_INLINE {
            return Err(Error::InvalidArgument(format!(
                "控制对象 {} 超过内联上限 {}：{} 字节",
                key.escape_debug(),
                meta::MAX_INLINE,
                data.len()
            )));
        }
        let floor = self.control_token_floor(key).await?;
        let guard = self.control_lock(key, floor).await?;
        // 获取后即时自检（与数据面写入口一致）
        if guard.lease_lost() {
            return Err(Error::QuorumUnmet("集群锁租约在获取后即失效".into()));
        }
        // CAS 只认带提交证书的版本：未提交的第一阶段半翻版本对客户端不可见，
        // 也不能让它把后续合法 CAS 永久顶成 Conflict。无证书且存在损坏副本 → fail-closed
        // （版本/token 高水位不可信）；无证书仅存在未提交副本 → 允许在已观察版本上重建。
        let st = self.read_control(layout::SYS_DIR, key).await?;
        if st.certified.is_none() && st.corrupt > 0 {
            return Err(Error::CorruptMeta(format!(
                "控制对象 {key:?} 无提交证书且存在 {} 份损坏副本：拒绝推进版本",
                st.corrupt
            )));
        }
        let (cur_version, cur_fence) = control_state(st.certified.as_ref());
        check_cas(
            cur_version,
            cur_fence.max(st.max_token),
            expected_version,
            fence_token,
        )?;
        let commit_token = guard.commit_token(fence_token, cur_fence.max(st.max_token))?;
        // 版本推进越过**所有已观察副本**（含未提交半翻）：崩溃写者的版本绝不重用。
        let version = st
            .max_version
            .checked_add(1)
            .ok_or_else(|| Error::CorruptMeta(format!("控制对象 {key:?} 版本号溢出")))?;
        // 全部证书槽都是候选：写者取首个可准备的槽保留旧副本（节点宕机/盘不可写时换一个）
        let preserve = st.certified_slots.clone();
        let m = control_meta(key, version, commit_token, Some(data), false);
        // 提交点前复查租约（无 fencing 窗口收窄到两次检查之间；与 put.rs:1058 同款）
        if guard.lease_lost() {
            return Err(Error::QuorumUnmet("集群锁租约丢失，写入中止".into()));
        }
        self.commit_control_meta(
            key,
            &m,
            st.certified.as_ref(),
            commit_token,
            &guard,
            &preserve,
        )
        .await?;
        Ok(version)
    }

    /// 读控制对象（tombstone → None）。只认带提交证书的版本：未提交半翻不可见；
    /// 集群覆盖不足 → `CorruptMeta`（fail-closed）；无已提交副本 → 缺席。
    pub async fn get_system_object(&self, key: &str) -> Result<Option<SystemObject>> {
        validate_control_key(key)?;
        let Some(m) = self.read_control_strict(layout::SYS_DIR, key).await? else {
            return Ok(None);
        };
        if m.control_deleted == Some(true) {
            return Ok(None);
        }
        Ok(Some(SystemObject {
            key: key.to_string(),
            data: m
                .latest()
                .and_then(|v| v.inline.as_ref())
                .map(|i| i.bytes.clone())
                .unwrap_or_default(),
            version: m.control_version.unwrap_or(0),
            fence_token: m.fence_token.unwrap_or(0),
            deleted: false,
        }))
    }

    /// 删控制对象（CAS 同 put）：写 tombstone 版本。返回是否真的发生了删除
    /// （不存在/已删除 → false，幂等；无证书残骸 → `CorruptMeta`，需先 put/recertify 修复）。
    /// 返回 [`Error::Indeterminate`] 时 tombstone 可能已提交——必须按 key 重读核对，
    /// 绝不假设回滚。
    pub async fn delete_system_object(
        &self,
        key: &str,
        expected_version: Option<u64>,
        fence_token: u64,
    ) -> Result<bool> {
        validate_control_key(key)?;
        let floor = self.control_token_floor(key).await?;
        let guard = self.control_lock(key, floor).await?;
        if guard.lease_lost() {
            return Err(Error::QuorumUnmet("集群锁租约在获取后即失效".into()));
        }
        let st = self.read_control(layout::SYS_DIR, key).await?;
        if st.certified.is_none() && st.corrupt > 0 {
            return Err(Error::CorruptMeta(format!(
                "控制对象 {key:?} 无提交证书且存在 {} 份损坏副本：拒绝推进版本",
                st.corrupt
            )));
        }
        let (cur_version, cur_fence) = control_state(st.certified.as_ref());
        check_cas(
            cur_version,
            cur_fence.max(st.max_token),
            expected_version,
            fence_token,
        )?;
        let commit_token = guard.commit_token(fence_token, cur_fence.max(st.max_token))?;
        let Some(old) = st.certified else {
            // 无证书副本存在 = 未提交第一阶段残骸或旧版无证书 meta。`Ok(false)` 会谎报
            // 「不存在/已删除」而 get/list 仍 fail-closed；必须显式要求修复（put 推进
            // 版本越过残骸，或 recertify 给旧 meta 补证书）。
            if st.copies > 0 {
                return Err(Error::CorruptMeta(format!(
                    "控制对象 {key:?} 无提交证书但存在 {} 份副本（未提交残骸或旧版 meta）：拒绝删除；先用 put 推进版本或 recertify 补证书",
                    st.copies
                )));
            }
            return Ok(false);
        };
        if old.control_deleted == Some(true) {
            return Ok(false);
        }
        let preserve = st.certified_slots.clone();
        let version = st
            .max_version
            .checked_add(1)
            .ok_or_else(|| Error::CorruptMeta(format!("控制对象 {key:?} 版本号溢出")))?;
        let m = control_meta(key, version, commit_token, None, true);
        if guard.lease_lost() {
            return Err(Error::QuorumUnmet("集群锁租约丢失，写入中止".into()));
        }
        self.commit_control_meta(key, &m, Some(&old), commit_token, &guard, &preserve)
            .await?;
        Ok(true)
    }

    /// Phase 1 升级门（仅限**真正的单机部署**：一个节点、全部盘本地）：给旧版
    /// （`control_committed = None`）控制 meta **原地补提交证书**。单机读集完整，
    /// rank-max 选举与本地 ≥`Q_s` 提交证明成立。
    ///
    /// 集群模式的旧版无证书 meta **不受支持**（本方法返回 `Unsupported`）：节点只能
    /// 看到自己的槽；标准 12 槽拓扑上单节点 prepare quorum 不满足（工具不可用），而
    /// 本地盘数 ≥ `data+1` 的节点可能把失败写残骸认证后覆盖本机已提交副本。集群迁移
    /// 需要单独的跨节点 raw-copy 扫描（同一 `(control_version, content)` 组至少 `Q_s`
    /// 份才可认证，否则 `CorruptMeta`），**尚未实现**。
    ///
    /// 在 fenced 锁内重读后写入：版本/数据不变，只置 `control_committed = Some(true)`，
    /// token 取锁发放值（严格高于所有已观察 token）。幂等：已有证书或键不存在 →
    /// `Ok(false)`；`Ok(true)` = 本次新写了证书。损坏副本/非控制形态/新代码第一阶段
    /// 残骸（`Some(false)`）→ `CorruptMeta`（绝不认证不确定内容；残骸由 put 推进版本覆盖）。
    /// `write_meta_marker` 在部分证书落盘时可返回 [`Error::Indeterminate`]：该版本可能
    /// 已对读可见，重试幂等（已认证 → `Ok(false)`），不得假设回滚。
    pub async fn recertify_system_object(&self, key: &str) -> Result<bool> {
        if self.cfg.cluster.is_some() {
            return Err(Error::Unsupported(
                "cluster legacy control metas are unsupported; recertify requires a genuine \
                 single-node deployment (one node, all drives local)"
                    .into(),
            ));
        }
        validate_control_key(key)?;
        let floor = self.control_token_floor(key).await?;
        let guard = self.control_lock(key, floor).await?;
        if guard.lease_lost() {
            return Err(Error::QuorumUnmet("集群锁租约在获取后即失效".into()));
        }
        // 锁内重读：certified 已可见 → 无需迁移（幂等）。
        let st = self.read_control(layout::SYS_DIR, key).await?;
        if st.certified.is_some() {
            return Ok(false);
        }
        if st.corrupt > 0 {
            return Err(Error::CorruptMeta(format!(
                "控制对象 {key:?} 存在 {} 份损坏副本：拒绝补证书",
                st.corrupt
            )));
        }
        if st.copies == 0 {
            return Ok(false);
        }
        // 全量扇出取 rank 最大副本作为待认证内容（与数据面读选举同口径）。
        let Some(mut raw) = self.read_meta_full(layout::SYS_DIR, key).await? else {
            return Err(Error::CorruptMeta(format!(
                "控制对象 {key:?} 观察到 {} 份副本但 meta 扫描为空：拒绝补证书",
                st.copies
            )));
        };
        if raw.control_version.is_none() && raw.fence_token.is_none() {
            return Err(Error::CorruptMeta(format!(
                "控制对象 {key:?} 的 meta 不是控制对象形态：拒绝补证书"
            )));
        }
        if let Some(c) = raw.control_committed {
            // Some(true) 已被 certified 分支覆盖；Some(false) = 新代码第一阶段残骸。
            return Err(Error::CorruptMeta(format!(
                "控制对象 {key:?} 带 control_committed={c:?}（非旧版无证书形态）：拒绝补证书；未提交残骸由 put 推进版本覆盖"
            )));
        }
        let caller_token = st
            .max_token
            .checked_add(1)
            .ok_or_else(|| Error::CorruptMeta(format!("控制对象 {key:?} fence token 溢出")))?;
        let commit_token = guard.commit_token(caller_token, st.max_token)?;
        raw.control_committed = Some(true);
        raw.fence_token = Some(commit_token);
        // 提交点前复查租约（与 put/delete 同款）
        if guard.lease_lost() {
            return Err(Error::QuorumUnmet("集群锁租约丢失，补证书中止".into()));
        }
        let flipped = self
            .write_meta_marker(layout::SYS_DIR, key, &raw, commit_token, &[])
            .await?;
        if flipped == 0 {
            return Err(Error::QuorumUnmet(format!(
                "控制对象 {key:?} 补证书 0 槽（未提交）"
            )));
        }
        Ok(true)
    }

    /// 列控制对象键（前缀过滤；tombstone 不出现）。排序保证结果稳定。
    pub async fn list_system_objects(&self, prefix: &str) -> Result<Vec<String>> {
        if self.cfg.cluster.is_some() {
            // Phase 0 的 control manifest 携带键集合；集群枚举推迟到 Phase 1（不新增 list RPC）。
            return Err(Error::Unsupported(
                "集群 system-object 枚举推迟到 Phase 1（Phase 0 由 control manifest 携带键集合）"
                    .into(),
            ));
        }
        if !prefix.starts_with(CONTROL_PREFIX) {
            return Err(Error::InvalidKey(format!(
                "控制对象前缀必须以 {CONTROL_PREFIX} 开头：{:?}",
                prefix.escape_debug()
            )));
        }
        // 全盘并集（同 scan_keys 口径）：掉线盘/半翻盘不得让 put 可写的键从列举里消失。
        let mut names = std::collections::BTreeSet::new();
        for drive in &self.cfg.drives {
            let mut entries = match tokio::fs::read_dir(layout::sys_dir(drive, "")).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            while let Some(entry) = entries.next_entry().await? {
                if !entry.file_type().await?.is_dir() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().into_owned();
                if let Ok(key) = layout::key_from_dir_name(&name)
                    && key.starts_with(prefix)
                {
                    names.insert(key);
                }
            }
        }
        let mut out = Vec::new();
        for key in names {
            // tombstone 仍占目录：读已提交 meta 过滤（未提交副本不得让键消失）
            if let Some(m) = self.read_control_strict(layout::SYS_DIR, &key).await?
                && m.control_deleted != Some(true)
            {
                out.push(key);
            }
        }
        Ok(out)
    }

    /// 集群锁发放的 token 下限：**所有已观察副本**（含崩溃写者留下的未提交副本）中
    /// 最大的 `fence_token`（无则 0）。token 一旦出现在任何槽就不得复用。高水位可能
    /// 落后于已提交 meta（单机→集群升级、`.fence` 丢失），没有下限时每次获取只抬 1，
    /// 写入会以 Conflict 永久楔死；单机无锁发放，直接 0。
    async fn control_token_floor(&self, key: &str) -> Result<u64> {
        if self.cfg.cluster.is_none() {
            return Ok(0);
        }
        Ok(self.read_control(layout::SYS_DIR, key).await?.max_token)
    }

    /// CAS 串行化：本机两层（进程内 mutex + `flock`，跨进程互斥）；集群再叠加 fenced
    /// 集群锁——先本机后集群，与数据面「对象锁 → 集群锁」顺序一致。集群分支不能复用
    /// `lock_object`：它会先取同 key 的数据面租约锁，锁表拒绝同 holder 的 fenced 重入。
    async fn control_lock(&self, key: &str, min_token: u64) -> Result<ControlGuard> {
        let local = self.lock_object_local(layout::SYS_DIR, key).await?;
        let fenced = if self.cfg.cluster.is_some() {
            Some(
                self.acquire_fenced_cluster_lock(layout::SYS_DIR, key, min_token)
                    .await?,
            )
        } else {
            None
        };
        Ok(ControlGuard {
            fenced,
            _local: local,
        })
    }

    /// 两阶段提交控制对象 meta：第一阶段写**未提交**数据，但保留 `preserve` 槽上的
    /// 已提交旧副本原样（崩溃/第二阶段零证书时读回退旧版本，而不是缺席——Critical A）；
    /// 第二阶段写同 token 的提交证书（`control_committed = Some(true)`）。
    /// 只有证书落盘后读才可见——第一阶段崩溃留下的半翻副本永远不可见。
    /// 第二阶段失败时绝不回滚第一阶段数据（证书是原子性判据；数据残骸由下次写覆盖）；
    /// 已落部分证书 → `Error::Indeterminate`（可能已提交，调用方须重读核对）。
    async fn commit_control_meta(
        &self,
        key: &str,
        m: &XlMeta,
        old: Option<&XlMeta>,
        fence_token: u64,
        guard: &ControlGuard,
        preserve: &[u16],
    ) -> Result<()> {
        // 单盘（无集群）：唯一 rename 就是提交点，直接写带证书的 meta——不存在
        // 「第一阶段覆盖后第二阶段零证书」的丢失窗口，无需两阶段。
        if self.cfg.cluster.is_none() && self.cfg.drives.len() == 1 {
            if guard.lease_lost() {
                return Err(Error::QuorumUnmet("集群锁租约丢失，写入中止".into()));
            }
            let mut marker = m.clone();
            marker.control_committed = Some(true);
            let marked = self
                .write_meta_marker(layout::SYS_DIR, key, &marker, fence_token, preserve)
                .await?;
            if marked == 0 {
                return Err(Error::QuorumUnmet(format!(
                    "控制对象 {key:?} 提交标记 0 槽（未提交）"
                )));
            }
            return Ok(());
        }
        let flipped = self
            .write_meta_fenced_preserving(layout::SYS_DIR, key, m, old, fence_token, preserve)
            .await?;
        if flipped == 0 {
            return Err(Error::QuorumUnmet(format!(
                "控制对象 {key:?} meta 未提交（0 槽翻转）"
            )));
        }
        // 标记阶段前复查租约：租约丢失时更新写者可能已介入，不得再发证书
        if guard.lease_lost() {
            return Err(Error::QuorumUnmet(
                "集群锁租约丢失，提交标记中止（第一阶段数据未提交）".into(),
            ));
        }
        let mut marker = m.clone();
        marker.control_committed = Some(true);
        let marked = self
            .write_meta_marker(layout::SYS_DIR, key, &marker, fence_token, preserve)
            .await?;
        if marked == 0 {
            return Err(Error::QuorumUnmet(format!(
                "控制对象 {key:?} 提交标记 0 槽（第一阶段数据未提交，读不可见）"
            )));
        }
        Ok(())
    }
}

fn validate_control_key(key: &str) -> Result<()> {
    layout::validate_key(key)?;
    if !key.starts_with(CONTROL_PREFIX) {
        return Err(Error::InvalidKey(format!(
            "{}: 不以 {CONTROL_PREFIX} 开头",
            key.escape_debug()
        )));
    }
    Ok(())
}

fn control_state(old: Option<&XlMeta>) -> (u64, u64) {
    old.map_or((0, 0), |m| {
        (m.control_version.unwrap_or(0), m.fence_token.unwrap_or(0))
    })
}

fn check_cas(
    cur_version: u64,
    cur_fence: u64,
    expected_version: Option<u64>,
    fence_token: u64,
) -> Result<()> {
    if expected_version.unwrap_or(0) != cur_version {
        return Err(Error::Conflict(format!(
            "控制对象版本不匹配：期望 {}，当前 {cur_version}",
            expected_version.unwrap_or(0)
        )));
    }
    if fence_token <= cur_fence {
        return Err(Error::Conflict(format!(
            "fence token 已过期：{fence_token} ≤ {cur_fence}"
        )));
    }
    Ok(())
}

/// 单版本控制对象 meta：数据内联（数据面读路径不经过它，PartRef 仅满足解码/兼容契约）。
fn control_meta(
    key: &str,
    version: u64,
    fence_token: u64,
    data: Option<&[u8]>,
    deleted: bool,
) -> XlMeta {
    let bytes = data.unwrap_or_default();
    let version_entry = Version {
        version_id: uuid::Uuid::nil(),
        mod_time_nanos: super::now_nanos(),
        size: bytes.len() as u64,
        etag: hex::encode(md5::Md5::digest(bytes)),
        content_type: None,
        user_metadata: Default::default(),
        part: PartRef {
            uuid: uuid::Uuid::new_v4().to_string(),
            part: 1,
            size: bytes.len() as u64,
            sha256: hex::encode(sha2::Sha256::digest(bytes)),
        },
        parts: None,
        part_sizes: None,
        checksum: None,
        data_layout: None,
        inline: data.map(|d| InlineData { bytes: d.to_vec() }),
        is_delete_marker: None,
        is_latest: Some(true),
        epoch: Some(version),
        encryption: None,
        lock: None,
        replication_status: None,
        remote_version_id: None,
        tags: None,
        storage_class: None,
        tier: None,
    };
    XlMeta {
        object_key: key.to_string(),
        versions: vec![version_entry],
        control_version: Some(version),
        fence_token: Some(fence_token),
        control_deleted: deleted.then_some(true),
        // 第一阶段数据；提交证书由 commit_control_meta 的第二阶段写入
        control_committed: Some(false),
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use crate::store::{Store, StoreConfig};

    async fn test_store() -> (Store, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(StoreConfig::new(dir.path())).await.unwrap();
        (s, dir)
    }

    fn assert_conflict<T: std::fmt::Debug>(r: crate::error::Result<T>) {
        match r {
            Err(Error::Conflict(_)) => {}
            other => panic!("应为 Conflict，实际：{other:?}"),
        }
    }

    #[tokio::test]
    async fn system_object_cas_and_tombstone() {
        let (s, _d) = test_store().await;
        assert_eq!(
            s.put_system_object("control/a", b"one", None, 1)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            &s.get_system_object("control/a")
                .await
                .unwrap()
                .unwrap()
                .data[..],
            b"one"
        );
        assert_conflict(s.put_system_object("control/a", b"two", Some(0), 2).await);
        assert_eq!(
            s.put_system_object("control/a", b"two", Some(1), 2)
                .await
                .unwrap(),
            2
        );
        assert!(
            s.delete_system_object("control/a", Some(2), 3)
                .await
                .unwrap()
        );
        assert!(s.get_system_object("control/a").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn system_object_fence_and_key_validation() {
        let (s, _d) = test_store().await;
        // fence token 必须严格递增（缺失按 0，故首个 token 必须 ≥ 1）
        assert_conflict(s.put_system_object("control/a", b"x", None, 0).await);
        s.put_system_object("control/a", b"x", None, 2)
            .await
            .unwrap();
        assert_conflict(s.put_system_object("control/a", b"y", Some(1), 2).await);
        // CAS 版本不匹配
        assert_conflict(s.put_system_object("control/a", b"y", Some(0), 3).await);
        // 只允许 control/ 前缀
        assert!(matches!(
            s.put_system_object("other/a", b"x", None, 9).await,
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(
            s.get_system_object("other/a").await,
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(
            s.list_system_objects("other/").await,
            Err(Error::InvalidKey(_))
        ));
        // 内联上限
        let big = vec![0u8; crate::meta::MAX_INLINE as usize + 1];
        assert!(matches!(
            s.put_system_object("control/big", &big, None, 1).await,
            Err(Error::InvalidArgument(_))
        ));
    }

    /// 同一 expected_version 的并发写者只能有一个成功。两个独立 Store 句柄共用一个
    /// root（≈ 两个进程各开一个 Store）：per-Store 内存锁不共享，互斥必须由 flock 提供。
    #[tokio::test]
    async fn system_object_concurrent_cas_allows_exactly_one_writer() {
        let dir = tempfile::tempdir().unwrap();
        let s1 = std::sync::Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
        let s2 = std::sync::Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
        let handles = [s1.clone(), s2.clone()];
        let results = futures::future::join_all((1..=8u64).map(|token| {
            let s = handles[(token as usize) % 2].clone();
            async move { s.put_system_object("control/race", b"x", None, token).await }
        }))
        .await;
        assert_eq!(
            results.iter().filter(|r| r.is_ok()).count(),
            1,
            "只允许一个写者成功：{results:?}"
        );
        for r in results.iter().filter(|r| r.is_err()) {
            assert!(
                matches!(r, Err(Error::Conflict(_))),
                "其余必须是 Conflict：{r:?}"
            );
        }
        assert_eq!(
            s1.get_system_object("control/race")
                .await
                .unwrap()
                .unwrap()
                .version,
            1
        );
    }

    /// tombstone 后重建：按 tombstone 的版本 CAS 继续写，数据/版本/token 全部前进。
    #[tokio::test]
    async fn system_object_delete_then_put_resurrects() {
        let (s, _d) = test_store().await;
        assert_eq!(
            s.put_system_object("control/r", b"one", None, 1)
                .await
                .unwrap(),
            1
        );
        assert!(
            s.delete_system_object("control/r", Some(1), 2)
                .await
                .unwrap()
        );
        assert!(s.get_system_object("control/r").await.unwrap().is_none());
        // tombstone 占版本 2（delete 只回 bool，版本按契约 +1 推算）
        assert_eq!(
            s.put_system_object("control/r", b"two", Some(2), 3)
                .await
                .unwrap(),
            3
        );
        let obj = s.get_system_object("control/r").await.unwrap().unwrap();
        assert_eq!(&obj.data[..], b"two");
        assert_eq!((obj.version, obj.fence_token, obj.deleted), (3, 3, false));
    }

    #[tokio::test]
    async fn system_object_list_excludes_tombstones_and_filters_prefix() {
        let (s, _d) = test_store().await;
        s.put_system_object("control/a", b"1", None, 1)
            .await
            .unwrap();
        s.put_system_object("control/dir/b", b"2", None, 1)
            .await
            .unwrap();
        s.put_system_object("control/c", b"3", None, 1)
            .await
            .unwrap();
        assert_eq!(
            s.list_system_objects("control/").await.unwrap(),
            vec!["control/a", "control/c", "control/dir/b"]
        );
        assert_eq!(
            s.list_system_objects("control/dir/").await.unwrap(),
            vec!["control/dir/b"]
        );
        s.delete_system_object("control/c", Some(1), 2)
            .await
            .unwrap();
        assert_eq!(
            s.list_system_objects("control/").await.unwrap(),
            vec!["control/a", "control/dir/b"]
        );
    }
}
