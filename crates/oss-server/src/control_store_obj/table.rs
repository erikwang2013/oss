//! Fixed u16 table-id registry shared by transactions, export/install and goldens
//! (design §1.3 and §7). IDs are grouped by domain and never renumbered: an id is
//! part of the on-disk root format.

/// Control tables stored in the object root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum TableId {
    Users = 1,
    AccessKeys = 2,
    Permissions = 3,
    Accounts = 10,
    AccountMembers = 11,
    AccountUsageLedger = 12,
    Buckets = 20,
    BucketPolicies = 21,
    LifecycleRules = 22,
    EventConfigs = 23,
    ReplicationRules = 24,
    AccessPoints = 25,
    OlapConfigs = 26,
    InventoryConfigs = 27,
    InventoryRuns = 30,
    InventoryCursors = 31,
    InventoryIncremental = 32,
    TierRestores = 33,
    LifecycleCursors = 34,
    Jobs = 40,
    JobTasks = 41,
    ScheduleLease = 42,
    SiteConfig = 50,
    Sites = 51,
    SiteChangelog = 52,
    SiteEntityClock = 53,
    VpcEndpoints = 60,
}

impl TableId {
    /// Every table this binary knows, ascending by id.
    pub const ALL: &'static [TableId] = &[
        Self::Users,
        Self::AccessKeys,
        Self::Permissions,
        Self::Accounts,
        Self::AccountMembers,
        Self::AccountUsageLedger,
        Self::Buckets,
        Self::BucketPolicies,
        Self::LifecycleRules,
        Self::EventConfigs,
        Self::ReplicationRules,
        Self::AccessPoints,
        Self::OlapConfigs,
        Self::InventoryConfigs,
        Self::InventoryRuns,
        Self::InventoryCursors,
        Self::InventoryIncremental,
        Self::TierRestores,
        Self::LifecycleCursors,
        Self::Jobs,
        Self::JobTasks,
        Self::ScheduleLease,
        Self::SiteConfig,
        Self::Sites,
        Self::SiteChangelog,
        Self::SiteEntityClock,
        Self::VpcEndpoints,
    ];

    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    /// SQL/export name of the table (stable golden identity).
    pub const fn name(self) -> &'static str {
        match self {
            Self::Users => "users",
            Self::AccessKeys => "access_keys",
            Self::Permissions => "permissions",
            Self::Accounts => "accounts",
            Self::AccountMembers => "account_members",
            Self::AccountUsageLedger => "account_usage_ledger",
            Self::Buckets => "buckets",
            Self::BucketPolicies => "bucket_policies",
            Self::LifecycleRules => "lifecycle_rules",
            Self::EventConfigs => "event_configs",
            Self::ReplicationRules => "replication_rules",
            Self::AccessPoints => "access_points",
            Self::OlapConfigs => "olap_configs",
            Self::InventoryConfigs => "inventory_configs",
            Self::InventoryRuns => "inventory_runs",
            Self::InventoryCursors => "inventory_cursors",
            Self::InventoryIncremental => "inventory_incremental",
            Self::TierRestores => "tier_restores",
            Self::LifecycleCursors => "lifecycle_cursors",
            Self::Jobs => "jobs",
            Self::JobTasks => "job_tasks",
            Self::ScheduleLease => "schedule_lease",
            Self::SiteConfig => "site_config",
            Self::Sites => "sites",
            Self::SiteChangelog => "site_changelog",
            Self::SiteEntityClock => "site_entity_clock",
            Self::VpcEndpoints => "vpc_endpoints",
        }
    }

    /// Reader minor this table needs; 0 until a table gains new semantics.
    pub const fn required_minor(self) -> u16 {
        0
    }

    pub const fn from_u16(id: u16) -> Option<Self> {
        Some(match id {
            1 => Self::Users,
            2 => Self::AccessKeys,
            3 => Self::Permissions,
            10 => Self::Accounts,
            11 => Self::AccountMembers,
            12 => Self::AccountUsageLedger,
            20 => Self::Buckets,
            21 => Self::BucketPolicies,
            22 => Self::LifecycleRules,
            23 => Self::EventConfigs,
            24 => Self::ReplicationRules,
            25 => Self::AccessPoints,
            26 => Self::OlapConfigs,
            27 => Self::InventoryConfigs,
            30 => Self::InventoryRuns,
            31 => Self::InventoryCursors,
            32 => Self::InventoryIncremental,
            33 => Self::TierRestores,
            34 => Self::LifecycleCursors,
            40 => Self::Jobs,
            41 => Self::JobTasks,
            42 => Self::ScheduleLease,
            50 => Self::SiteConfig,
            51 => Self::Sites,
            52 => Self::SiteChangelog,
            53 => Self::SiteEntityClock,
            60 => Self::VpcEndpoints,
            _ => return None,
        })
    }
}

/// Registry order used by export/install compatibility checks.
pub const KNOWN_TABLES: &[TableId] = TableId::ALL;

pub fn known_table_ids() -> Vec<u16> {
    KNOWN_TABLES.iter().map(|t| t.as_u16()).collect()
}
