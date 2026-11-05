//! A8 gate: every golden listed in MANIFEST exists and matches its SHA-256,
//! the manifest is sorted, and no unlisted file lives under `tests/goldens`.
//!
//! Regenerate with `scripts/capture_control_goldens.sh`; never hand-edit the
//! manifest or fixture bytes.

#[path = "support/golden.rs"]
mod golden;

use std::collections::BTreeSet;
use std::path::Path;

use sha2::{Digest, Sha256};

#[test]
fn golden_manifest() {
    let root = golden::root();
    let manifest_path = root.join("MANIFEST");
    let text = std::fs::read_to_string(&manifest_path)
        .unwrap_or_else(|e| panic!("missing {}: {e}", manifest_path.display()));

    let mut listed = BTreeSet::new();
    let mut previous: Option<String> = None;
    let mut count = 0usize;
    for (n, line) in text.lines().enumerate() {
        assert!(!line.is_empty(), "MANIFEST line {} is empty", n + 1);
        let (hash, rel) = line.split_once("  ").unwrap_or_else(|| {
            panic!(
                "MANIFEST line {} must be `<sha256>  <path>`: {line:?}",
                n + 1
            )
        });
        assert!(
            hash.len() == 64 && hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')),
            "MANIFEST line {} hash is not 64 lowercase hex chars: {hash:?}",
            n + 1
        );
        assert!(
            !rel.is_empty()
                && !rel.starts_with('/')
                && !rel.contains('\\')
                && rel
                    .split('/')
                    .all(|s| !s.is_empty() && s != "." && s != ".."),
            "MANIFEST line {} path must be a clean relative path: {rel:?}",
            n + 1
        );
        if let Some(prev) = &previous {
            assert!(
                prev < &rel.to_string(),
                "MANIFEST is not sorted: {prev:?} before {rel:?}"
            );
        }
        previous = Some(rel.to_string());
        assert!(
            listed.insert(rel.to_string()),
            "duplicate MANIFEST path {rel}"
        );

        let bytes = std::fs::read(root.join(rel))
            .unwrap_or_else(|e| panic!("MANIFEST lists {rel}, but it cannot be read: {e}"));
        let got = hex::encode(Sha256::digest(&bytes));
        assert_eq!(got, hash, "SHA-256 mismatch for golden {rel}");
        count += 1;
    }
    assert!(count > 0, "MANIFEST lists no fixtures");

    let mut on_disk = Vec::new();
    walk(&root, &root, &mut on_disk);
    for rel in &on_disk {
        assert!(
            rel == "MANIFEST" || listed.contains(rel),
            "unlisted file under tests/goldens: {rel}"
        );
    }
    assert!(
        on_disk.len() == count + 1,
        "expected {} files plus MANIFEST, found {} on disk",
        count,
        on_disk.len()
    );
}

#[test]
fn golden_helpers_load_and_compare() {
    let bytes = golden::load("rows/bucket/bucket.json");
    golden::assert_bytes("rows/bucket/bucket.json", &bytes);
    let value = golden::load_json("rows/bucket/bucket.json");
    assert_eq!(value["name"], "bucket\"q\\b\u{1}");
    golden::assert_json("rows/bucket/bucket.json", &value);
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .collect();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            walk(root, &p, out);
        } else {
            out.push(
                p.strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/"),
            );
        }
    }
}
