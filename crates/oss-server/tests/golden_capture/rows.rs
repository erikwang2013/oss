//! Natural-key seam row bytes for serde snapshot/runstate rows. Local rows
//! (no `Serialize`) live in `local_rows.rs`.

use super::fixtures;
use crate::golden;

pub fn capture() {
    let l1 = fixtures::level_one();
    let l2 = fixtures::level_two();
    let l3 = fixtures::level_three();
    let rs = fixtures::runstate();

    golden::write_serde("rows/bucket/bucket.json", &l1.buckets[0]);
    golden::write_serde("rows/bucket/bucket_policy.json", &l1.bucket_policies[0]);
    golden::write_serde("rows/bucket/event_config.json", &l1.event_configs[0]);
    golden::write_serde(
        "rows/bucket/replication_rule.json",
        &l1.replication_rules[0],
    );

    golden::write_serde("rows/ap_olap_vpc/access_point.json", &l1.access_points[0]);
    golden::write_serde("rows/ap_olap_vpc/olap_config.json", &l1.olap_configs[0]);
    golden::write_serde("rows/ap_olap_vpc/vpc_endpoint.json", &fixtures::vpc());

    golden::write_serde(
        "rows/lifecycle_inventory_tier/lifecycle_rule.json",
        &l2.lifecycle_rules[0],
    );
    golden::write_serde(
        "rows/lifecycle_inventory_tier/lifecycle_cursor.json",
        &rs.lifecycle_cursors[0],
    );
    golden::write_serde(
        "rows/lifecycle_inventory_tier/inventory_config.json",
        &l1.inventory_configs[0],
    );
    golden::write_serde(
        "rows/lifecycle_inventory_tier/inventory_run.json",
        &rs.inventory_runs[0],
    );
    golden::write_serde(
        "rows/lifecycle_inventory_tier/inventory_cursor.json",
        &rs.inventory_cursors[0],
    );
    golden::write_serde(
        "rows/lifecycle_inventory_tier/inventory_incremental.json",
        &rs.inventory_incremental[0],
    );
    golden::write_serde(
        "rows/lifecycle_inventory_tier/tier_restore.json",
        &rs.tier_restores[0],
    );

    golden::write_serde("rows/iam_accounts/user.json", &l1.users[0]);
    golden::write_serde("rows/iam_accounts/access_key.json", &l1.access_keys[0]);
    golden::write_serde("rows/iam_accounts/permission.json", &l1.permissions[0]);
    golden::write_serde("rows/iam_accounts/account.json", &l3.accounts[0]);
    golden::write_serde(
        "rows/iam_accounts/account_member.json",
        &l3.account_members[0],
    );
    golden::write_serde(
        "rows/iam_accounts/account_usage_ledger.json",
        &fixtures::ledger(),
    );

    golden::write_serde("rows/jobs_runstate/jobs.json", &rs.jobs);
    golden::write_serde("rows/jobs_runstate/job_task.json", &rs.job_tasks[0]);
    golden::write_serde(
        "rows/jobs_runstate/schedule_lease.json",
        &rs.schedule_lease.as_ref().unwrap(),
    );
    golden::write_serde("rows/jobs_runstate/run_state.json", &rs);
}
