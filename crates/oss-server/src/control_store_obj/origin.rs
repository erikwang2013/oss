//! Typed write origins (design §5). Every transaction carries one; there is no
//! mutable `apply_ctx` flag. Only [`WriteOrigin`]'s constructor functions can be
//! called outside this module: variants are `#[non_exhaustive]`, so downstream
//! crates cannot forge one by struct literal.

use xl_store::control_state::root::AppliedLog;

/// Why a control transaction is being written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOrigin {
    /// Raft entry apply: the only writer of replicated tables.
    #[non_exhaustive]
    RaftApply { log_id: AppliedLog },
    /// Snapshot install: replaces the whole state in one root CAS.
    #[non_exhaustive]
    SnapshotInstall,
    /// Applied remote site entry (LWW); the caller writes its clock explicitly.
    #[non_exhaustive]
    SiteApply { peer: String },
    /// Local façade/admin write.
    #[non_exhaustive]
    Local { actor: String },
}

// The apply/install/site constructors are called by B2/C/D adapters; per-target
// dead-code analysis sees only a subset of them.
#[allow(dead_code)]
impl WriteOrigin {
    pub(crate) fn raft_apply(log_id: AppliedLog) -> Self {
        Self::RaftApply { log_id }
    }

    pub(crate) fn snapshot_install() -> Self {
        Self::SnapshotInstall
    }

    pub(crate) fn site_apply(peer: impl Into<String>) -> Self {
        Self::SiteApply { peer: peer.into() }
    }

    pub(crate) fn local(actor: impl Into<String>) -> Self {
        Self::Local {
            actor: actor.into(),
        }
    }

    /// Applied log position this transaction advances, when it is a replicated write.
    pub fn log_id(&self) -> Option<AppliedLog> {
        match self {
            Self::RaftApply { log_id } => Some(*log_id),
            _ => None,
        }
    }

    pub fn is_replicated(&self) -> bool {
        matches!(self, Self::RaftApply { .. } | Self::SnapshotInstall)
    }
}
