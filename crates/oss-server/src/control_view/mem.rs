//! Rough resident-byte estimate used by the memory cap (design §4).

use super::*;

impl HotView {
    /// Rough resident bytes: map slots plus string payloads.
    pub fn estimated_bytes(&self) -> usize {
        let mut n = std::mem::size_of::<Self>();
        for u in self.users.values() {
            n += 64 + u.username.len() + u.password_hash.len();
        }
        for (ak, k) in self.keys.iter() {
            n += 96 + ak.len() + k.stored_secret.len();
            n += k.session_policy.as_ref().map_or(0, String::len);
            n += k.parent_ak.as_ref().map_or(0, String::len);
        }
        for (uid, perms) in self.permissions.iter() {
            n += 16 + std::mem::size_of_val(uid);
            for (bucket, row) in perms {
                n += 64 + bucket.len() + std::mem::size_of_val(row);
            }
        }
        for a in self.accounts.values() {
            n += 80 + a.id.len() + a.name.len() + a.admin_uid.map_or(0, |_| 8);
        }
        for m in self.members.values() {
            n += 48 + m.account_id.len() + m.role.len();
        }
        for b in self.buckets.values() {
            n += 96 + b.name.len();
            for s in [&b.default_encryption, &b.object_lock_default_mode]
                .into_iter()
                .flatten()
            {
                n += s.len();
            }
        }
        for p in self.policies.values() {
            n += 48 + p.bucket.len() + p.policy_json.len();
        }
        for e in self.events.values() {
            n += 48 + e.bucket.len() + e.config_json.len();
        }
        for a in self.aps.values() {
            n += 96 + a.name.len() + a.bucket.len();
            n += a.policy_json.as_ref().map_or(0, String::len);
            if let OwnerRef::User(name) = &a.owner {
                n += name.len();
            }
        }
        for c in self.olap.values() {
            n += 96 + c.access_point.len() + c.kind.len();
            n += c.endpoint.as_ref().map_or(0, String::len);
            n += c.module.as_ref().map_or(0, Vec::len);
        }
        for v in self.vpcs.values() {
            n += 80 + v.vpce_id.len() + v.vpc_id.len() + v.account_id.len() + v.state.len();
        }
        for s in self.sites.values() {
            n += 200
                + s.peer_id.len()
                + s.remote_site_id.len()
                + s.name.len()
                + s.endpoint.len()
                + s.region.len()
                + s.s3_access_key.len()
                + s.s3_secret_key.len()
                + s.peer_site_token.len();
        }
        if let Some(c) = &self.site_config {
            n += 64 + c.id.len() + c.name.len() + c.region.len() + c.site_token.len();
        }
        n
    }
}
