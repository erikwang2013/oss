//! Content addressing and the reserved control-state key namespace (design §1.1).

use sha2::Digest as _;

pub const ROOT_KEY: &str = "control/state/root";
pub const NODE_PREFIX: &str = "control/state/node/";
pub const BLOB_PREFIX: &str = "control/state/blob/";

/// Full-object SHA-256, the content address for nodes and blobs.
pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    sha2::Sha256::digest(bytes).into()
}

pub fn node_key(hash: &[u8; 32]) -> String {
    format!("{NODE_PREFIX}{}", hex::encode(hash))
}

/// `n` is the decimal chunk index (design §1.1).
pub fn blob_key(hash: &[u8; 32], n: u32) -> String {
    format!("{BLOB_PREFIX}{}/{n}", hex::encode(hash))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_empty_known_vector() {
        assert_eq!(
            hex::encode(sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn key_layout() {
        assert_eq!(ROOT_KEY, "control/state/root");
        assert_eq!(
            node_key(&[0xab; 32]),
            format!("control/state/node/{}", "ab".repeat(32))
        );
        assert_eq!(
            blob_key(&[0x01; 32], 7),
            format!("control/state/blob/{}/7", "01".repeat(32))
        );
    }
}
