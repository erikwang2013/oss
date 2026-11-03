//! Phase 3 前置：集群 legacy 控制对象补证书。
//!
//! 只读扫描（本 Task）：全槽观察 → 解码 → 规范内容分组 → 状态判定。写入（fenced
//! 锁 + 现有标记阶段）在 Task 4 追加，绝不新增提交协议。
//!
//! 分组口径：`clone` meta → 剥 `fence_token`/`control_committed` → `encode` → sha256；
//! 组键 `(control_version, hash)`。两阶段提交/补偿回滚只改 token/证书，不改变组；
//! 同版本不同内容 = 分叉，整键 fail-closed。

use std::collections::BTreeMap;

use crate::error::{Error, Result};
use crate::layout;
use crate::mesh::MetaRead;
use crate::meta::XlMeta;

use super::{Store, control};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegacyControlState {
    Missing,
    AlreadyCommitted,
    Certifiable,
    CompletionNeeded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecertifyOutcome {
    Missing,
    AlreadyCommitted {
        version: u64,
        certified: usize,
    },
    Certified {
        version: u64,
        certified: usize,
        token: u64,
    },
}

#[derive(Clone, Debug)]
pub struct LegacyControlCopy {
    pub node: String,
    pub slot: u16,
    pub version: u64,
    pub token: u64,
    pub certified: bool,
    pub content: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyContentGroup {
    pub version: u64,
    pub content: [u8; 32],
    pub copies: usize,
    pub certified: usize,
    pub max_token: u64,
}

#[derive(Clone, Debug)]
pub struct LegacyControlScan {
    pub key: String,
    pub n: u16,
    pub q_s: u16,
    /// 确定性观察到的槽数（必须 == n；命中或 NotFound）。
    pub observed: u16,
    pub copies: Vec<LegacyControlCopy>,
    pub groups: Vec<LegacyContentGroup>,
    pub chosen: Option<usize>,
    pub state: LegacyControlState,
    /// 候选组最高 token 的代表副本（Task 4 写证书用）。
    chosen_meta: Option<XlMeta>,
}

/// 规范内容：忽略 token 与提交证书（两阶段/补偿副本同组），其余逐字节参与。
pub(super) fn canonical_content(m: &XlMeta) -> Result<[u8; 32]> {
    let mut c = m.clone();
    c.fence_token = None;
    c.control_committed = None;
    Ok(crate::mesh::content_hash(&c.encode()?))
}

/// 状态判定（纯函数，供扫描与单元测试共用）。
///
/// - 无证书：唯一 `copies >= Q_s` 组 = 可认证；0/多个 = `CorruptMeta`。
/// - 有证书：取 rank `(max_token, version)` 最高的证书组；rank 并列（内容不同）=
///   同版本分叉；`certified >= Q_s` = 已提交；总副本 `>= Q_s` = 可补齐；否则拒绝。
///   有证书时绝不认证 legacy-only 组（已提交状态不得被改写）。
pub(super) fn classify(
    groups: &[LegacyContentGroup],
    q_s: usize,
) -> Result<(Option<usize>, LegacyControlState)> {
    if groups.is_empty() {
        return Ok((None, LegacyControlState::Missing));
    }
    let certs: Vec<usize> = groups
        .iter()
        .enumerate()
        .filter(|(_, g)| g.certified > 0)
        .map(|(i, _)| i)
        .collect();
    if certs.is_empty() {
        let quorum: Vec<usize> = groups
            .iter()
            .enumerate()
            .filter(|(_, g)| g.copies >= q_s)
            .map(|(i, _)| i)
            .collect();
        return match quorum.as_slice() {
            [only] => Ok((Some(*only), LegacyControlState::Certifiable)),
            [] => Err(Error::CorruptMeta(format!(
                "无提交证书且无同内容组达到 Q_s={q_s}"
            ))),
            _ => Err(Error::CorruptMeta(format!(
                "无提交证书但有 {} 个同内容组达到 Q_s={q_s}：无法唯一认证",
                quorum.len()
            ))),
        };
    }
    let rank = |i: usize| (groups[i].max_token, groups[i].version);
    let max_rank = certs.iter().map(|&i| rank(i)).max().expect("非空");
    let top: Vec<usize> = certs
        .iter()
        .copied()
        .filter(|&i| rank(i) == max_rank)
        .collect();
    if top.len() > 1 {
        return Err(Error::CorruptMeta(format!(
            "证书组 rank 并列（token={}, v{}）且内容不同：同版本分叉",
            max_rank.0, max_rank.1
        )));
    }
    let i = top[0];
    let g = &groups[i];
    if g.certified >= q_s {
        Ok((Some(i), LegacyControlState::AlreadyCommitted))
    } else if g.copies >= q_s {
        Ok((Some(i), LegacyControlState::CompletionNeeded))
    } else {
        Err(Error::CorruptMeta(format!(
            "最高 rank 证书组 v{} 仅 {} 份副本（证书 {} 份，Q_s={q_s}）：无法补齐",
            g.version, g.copies, g.certified
        )))
    }
}

impl Store {
    /// 全槽扫描并分类一个 legacy 控制对象（只读；任何不可判定都 fail-closed）。
    pub async fn scan_legacy_control_object(&self, key: &str) -> Result<LegacyControlScan> {
        control::validate_control_key(key)?;
        let Some(cl) = self.cfg.cluster.as_deref() else {
            return Err(Error::Unsupported(
                "control-recertify 仅支持集群部署；单节点请用 recertify_system_object".into(),
            ));
        };
        self.ensure_control_raw_compatible().await?;
        let q_s = self.cfg.ec.data as usize + 1;
        let n = cl.sets.flat.slot_count() as usize;
        // 唯一性证明要求任意两个 Q_s 观察集相交；标记写还要求 preserve + Q_s 空间。
        if 2 * q_s <= n {
            return Err(Error::Unsupported(format!(
                "补证书要求 2*Q_s > N（当前 Q_s={q_s}, N={n}）：拓扑无法证明唯一性，fail-closed"
            )));
        }
        if n < self.cfg.ec.data as usize + 2 {
            return Err(Error::Unsupported(format!(
                "补证书要求 N >= data+2（当前 N={n}, data={}）：无标记写空间，fail-closed",
                self.cfg.ec.data
            )));
        }
        let self_id = cl.sets.flat.self_node().id.clone();
        let req = MetaRead {
            bucket: layout::SYS_DIR.into(),
            key: key.to_string(),
        };

        // 全槽观察：本机 + 全部 peer 并发（任何缺口都不下结论）。
        let local = self.mesh_read_control_raw(req.clone()).await?;
        if local.slots_ok as usize != self.cfg.drives.len() {
            return Err(Error::CorruptMeta(format!(
                "控制对象 {key:?} 本机覆盖 {}/{} 槽：存在不在册盘",
                local.slots_ok,
                self.cfg.drives.len()
            )));
        }
        let mut observed = local.slots_ok as usize;
        let mut raw: Vec<(String, Vec<u16>, Vec<bytes::Bytes>)> = vec![(
            self_id.clone(),
            local.copies.iter().map(|c| c.slot).collect(),
            local.copies.into_iter().map(|c| c.meta).collect(),
        )];
        let remotes = cl
            .sets
            .flat
            .nodes
            .iter()
            .filter(|node| node.id != self_id)
            .map(|node| {
                let transport = &cl.transport;
                let req = req.clone();
                async move {
                    let resp = transport
                        .read_control_raw(&node.id, req)
                        .await
                        .map_err(|e| {
                            Error::CorruptMeta(format!(
                                "控制对象 raw 扫描失败：节点 {}：{e}",
                                node.id
                            ))
                        })?;
                    if resp.slots_ok != node.drive_count as u32 {
                        return Err(Error::CorruptMeta(format!(
                            "控制对象 raw 扫描覆盖不全：节点 {} {}/{} 槽",
                            node.id, resp.slots_ok, node.drive_count
                        )));
                    }
                    Ok::<_, Error>((
                        node.id.clone(),
                        resp.slots_ok as usize,
                        resp.copies.iter().map(|c| c.slot).collect::<Vec<_>>(),
                        resp.copies.into_iter().map(|c| c.meta).collect(),
                    ))
                }
            });
        for (node, slots_ok, slots, metas) in futures::future::try_join_all(remotes).await? {
            observed += slots_ok;
            raw.push((node, slots, metas));
        }
        if observed != n {
            return Err(Error::CorruptMeta(format!(
                "控制对象 {key:?} 全槽观察 {observed}/{n}：覆盖不全，fail-closed"
            )));
        }

        // 解码 + 规范分组（重复槽/损坏/非控制/第一阶段残骸 → 整键拒绝）。
        let mut seen_slots = std::collections::BTreeSet::new();
        let mut copies = Vec::new();
        let mut groups: BTreeMap<(u64, [u8; 32]), LegacyContentGroup> = BTreeMap::new();
        let mut reps: BTreeMap<(u64, [u8; 32]), XlMeta> = BTreeMap::new();
        let mut version_hash: BTreeMap<u64, [u8; 32]> = BTreeMap::new();
        for (node, slots, metas) in raw {
            for (slot, bytes) in slots.into_iter().zip(metas) {
                if !seen_slots.insert(slot) {
                    return Err(Error::CorruptMeta(format!(
                        "控制对象 {key:?} 槽号重复：{slot}"
                    )));
                }
                let m = XlMeta::decode(&bytes).map_err(|e| {
                    Error::CorruptMeta(format!("控制对象 {key:?} 槽 {slot} 解码失败：{e}"))
                })?;
                if m.object_key != key {
                    return Err(Error::CorruptMeta(format!(
                        "控制对象 {key:?} 槽 {slot} 键不一致：{:?}",
                        m.object_key
                    )));
                }
                if m.control_version.is_none() && m.fence_token.is_none() {
                    return Err(Error::CorruptMeta(format!(
                        "控制对象 {key:?} 槽 {slot} 非控制形态（无 version/token）"
                    )));
                }
                if m.control_committed == Some(false) {
                    return Err(Error::CorruptMeta(format!(
                        "控制对象 {key:?} 槽 {slot} 是 Phase 0 第一阶段残骸（Some(false)）：拒绝迁移"
                    )));
                }
                let version = m.control_version.unwrap_or(0);
                let token = m.fence_token.unwrap_or(0);
                let content = canonical_content(&m)?;
                if let Some(prev) = version_hash.insert(version, content)
                    && prev != content
                {
                    return Err(Error::CorruptMeta(format!(
                        "控制对象 {key:?} 同版本 v{version} 分叉：多份不同内容"
                    )));
                }
                let certified = m.control_committed == Some(true);
                let g = groups
                    .entry((version, content))
                    .or_insert(LegacyContentGroup {
                        version,
                        content,
                        copies: 0,
                        certified: 0,
                        max_token: 0,
                    });
                g.copies += 1;
                g.certified += usize::from(certified);
                g.max_token = g.max_token.max(token);
                let key_group = (version, content);
                let take = reps
                    .get(&key_group)
                    .is_none_or(|prev| prev.fence_token.unwrap_or(0) <= token);
                if take {
                    reps.insert(key_group, m);
                }
                copies.push(LegacyControlCopy {
                    node: node.clone(),
                    slot,
                    version,
                    token,
                    certified,
                    content,
                });
            }
        }
        copies.sort_by_key(|c| c.slot);
        let groups: Vec<LegacyContentGroup> = groups.into_values().collect();
        let (chosen, state) = classify(&groups, q_s)?;
        let chosen_meta = chosen.and_then(|i| {
            let g = &groups[i];
            reps.get(&(g.version, g.content)).cloned()
        });
        Ok(LegacyControlScan {
            key: key.to_string(),
            n: n as u16,
            q_s: q_s as u16,
            observed: observed as u16,
            copies,
            groups,
            chosen,
            state,
            chosen_meta,
        })
    }

    /// 集群补证书：fenced 锁内重扫（权威证据）→ 现有标记阶段写提交证书。
    /// 绝不调用 `commit_control_meta` 的数据阶段，版本/内容/tombstone 原样保留。
    /// `Indeterminate` 原样上抛（可能已提交）；重跑走“已有部分证书 + 总副本 ≥ Q_s”
    /// 的补齐分支收敛。
    pub async fn recertify_system_object_cluster(&self, key: &str) -> Result<RecertifyOutcome> {
        control::validate_control_key(key)?;
        if self.cfg.cluster.is_none() {
            return Err(Error::Unsupported(
                "control-recertify 仅支持集群部署；单节点请用 recertify_system_object".into(),
            ));
        }
        let floor = self.control_token_floor(key).await?;
        let guard = self.control_lock(key, floor).await?;
        if guard.lease_lost() {
            return Err(Error::QuorumUnmet("集群锁租约在获取后即失效".into()));
        }
        let scan = self.scan_legacy_control_object(key).await?;
        let Some(chosen) = scan.chosen else {
            return Ok(RecertifyOutcome::Missing);
        };
        let version = scan.groups[chosen].version;
        if scan.state == LegacyControlState::AlreadyCommitted {
            return Ok(RecertifyOutcome::AlreadyCommitted {
                version,
                certified: scan.groups[chosen].certified,
            });
        }
        // 锁 token 必须严格大于**所有已观察** token（含未提交残骸的高水位）。
        let max_token = scan.copies.iter().map(|c| c.token).max().unwrap_or(0);
        let commit_token = guard.commit_token(0, max_token)?;
        let mut marker = scan
            .chosen_meta
            .expect("Certifiable/CompletionNeeded 必有代表副本");
        marker.control_committed = Some(true);
        marker.fence_token = Some(commit_token);
        if guard.lease_lost() {
            return Err(Error::QuorumUnmet("集群锁租约丢失，补证书中止".into()));
        }
        let flipped = self
            .write_meta_marker(layout::SYS_DIR, key, &marker, commit_token, &[])
            .await?;
        if flipped == 0 {
            return Err(Error::QuorumUnmet(format!(
                "控制对象 {key:?} 补证书 0 槽（未提交）"
            )));
        }
        Ok(RecertifyOutcome::Certified {
            version,
            certified: flipped,
            token: commit_token,
        })
    }
}
