use std::collections::BTreeMap;

use super::*;
use crate::control_state::format;
use crate::error::Error;

fn table(hash_byte: u8, rows: u64, bytes: u64, required_minor: u16) -> TableRoot {
    TableRoot {
        hash: [hash_byte; 32],
        rows,
        bytes,
        required_minor,
    }
}

fn sample() -> Root {
    let mut tables = BTreeMap::new();
    tables.insert(7, table(0x11, 3, 30, 0));
    let mut markers = BTreeMap::new();
    markers.insert("k".to_string(), "v".to_string());
    Root {
        major: KNOWN_MAJOR,
        min_reader: KNOWN_MINOR,
        tables,
        meta: RootMeta {
            mode: Mode::Standalone,
            markers,
            next_uid: 2,
            next_job_id: 3,
            next_run_id: 4,
            next_site_seq: 5,
            applied: Some(AppliedLog { term: 9, index: 10 }),
            membership_json: Some("{}".to_string()),
            feature_level: 1,
            created_at: 100,
        },
    }
}

#[test]
fn empty_root_bootstrap() {
    let root = Root::empty(Mode::Raft, 1234);
    assert_eq!(root.major, KNOWN_MAJOR);
    assert_eq!(root.min_reader, KNOWN_MINOR);
    assert!(root.tables.is_empty());
    assert_eq!(root.meta.mode, Mode::Raft);
    assert_eq!(root.meta.created_at, 1234);
    assert_eq!(root.meta.next_uid, 1);
    assert_eq!(root.meta.next_job_id, 1);
    assert_eq!(root.meta.next_run_id, 1);
    assert_eq!(root.meta.next_site_seq, 1);
    assert_eq!(root.meta.applied, None);
    assert_eq!(root.meta.membership_json, None);
    let bytes = encode_root(&root).unwrap();
    assert_eq!(decode_root(&bytes).unwrap(), root);
}

#[test]
fn golden_root_bytes() {
    let bytes = encode_root(&sample()).unwrap();
    assert_eq!(hex::encode(&bytes), GOLDEN_ROOT_HEX);
}

#[test]
fn root_roundtrip_preserves_tables_and_meta() {
    let root = sample();
    let bytes = encode_root(&root).unwrap();
    let got = decode_root(&bytes).unwrap();
    assert_eq!(got, root);
    assert_eq!(got.table(7), Some(&table(0x11, 3, 30, 0)));
    assert_eq!(got.table(8), None);
}

#[test]
fn root_32kib_cap_enforced() {
    let mut root = Root::empty(Mode::Standalone, 1);
    root.meta
        .markers
        .insert("big".into(), "x".repeat(40 * 1024));
    assert!(matches!(encode_root(&root), Err(Error::InvalidArgument(_))));
}

#[test]
fn unknown_table_with_required_minor_is_preserved_then_gated() {
    let mut root = sample();
    root.tables.insert(999, table(0x22, 1, 2, 1));
    let bytes = encode_root(&root).unwrap();
    let got = decode_root(&bytes).unwrap();
    assert_eq!(got.tables.get(&999), Some(&table(0x22, 1, 2, 1)));
    assert_eq!(got.required_minor(), 1);
    assert!(matches!(
        got.check_required_minor(),
        Err(Error::Unsupported(_))
    ));
    assert!(matches!(
        got.check_compatible(&[7, 999]),
        Err(Error::Unsupported(_))
    ));
}

#[test]
fn unknown_table_with_known_minor_is_readable_but_not_exportable() {
    let mut root = sample();
    root.tables.insert(999, table(0x22, 1, 2, 0));
    let got = decode_root(&encode_root(&root).unwrap()).unwrap();
    assert_eq!(got.required_minor(), 0);
    got.check_required_minor().unwrap();
    assert_eq!(got.unknown_tables(&[7]), vec![999]);
    assert!(matches!(
        got.check_compatible(&[7]),
        Err(Error::Unsupported(_))
    ));
    got.check_compatible(&[7, 999]).unwrap();
}

#[test]
fn unknown_payload_fields_are_ignored() {
    #[derive(serde::Serialize)]
    struct FutureRoot<'a> {
        major: u16,
        min_reader: u16,
        tables: &'a BTreeMap<u16, TableRoot>,
        meta: &'a RootMeta,
        future_field: u32,
    }
    let root = sample();
    let future = FutureRoot {
        major: root.major,
        min_reader: root.min_reader,
        tables: &root.tables,
        meta: &root.meta,
        future_field: 0xdead_beef,
    };
    let bytes = format::encode_payload(format::ObjectKind::Root, KNOWN_MAJOR, KNOWN_MINOR, &future)
        .unwrap();
    let got = decode_root(&bytes).unwrap();
    assert_eq!(got, root);
}

#[test]
fn header_payload_version_mismatch_rejected() {
    // Header says major 1/min_reader 0, payload claims a different reader minor.
    let mut tampered = sample();
    tampered.min_reader = 1;
    let payload = rmp_serde::to_vec_named(&tampered).unwrap();
    let bytes = format::encode_object(
        &format::ObjectHeader {
            kind: format::ObjectKind::Root,
            major: KNOWN_MAJOR,
            min_reader: KNOWN_MINOR,
        },
        &payload,
    )
    .unwrap();
    assert!(matches!(decode_root(&bytes), Err(Error::CorruptMeta(_))));
}

#[test]
fn wrong_kind_and_oversize_root_rejected() {
    let node = format::encode_payload(
        format::ObjectKind::Node,
        KNOWN_MAJOR,
        KNOWN_MINOR,
        &sample(),
    )
    .unwrap();
    assert!(matches!(decode_root(&node), Err(Error::CorruptMeta(_))));

    let payload = vec![0u8; ROOT_MAX_BYTES + 1];
    let bytes = format::encode_object(
        &format::ObjectHeader {
            kind: format::ObjectKind::Root,
            major: KNOWN_MAJOR,
            min_reader: KNOWN_MINOR,
        },
        &payload,
    )
    .unwrap();
    assert!(matches!(decode_root(&bytes), Err(Error::CorruptMeta(_))));
}

#[test]
fn allocators_are_monotonic_and_never_reused() {
    let mut meta = Root::empty(Mode::Standalone, 0).meta;
    assert_eq!(meta.alloc_uid().unwrap(), 1);
    assert_eq!(meta.alloc_uid().unwrap(), 2);
    assert_eq!(meta.next_uid, 3);
    meta.observe_uid(10).unwrap();
    assert_eq!(meta.alloc_uid().unwrap(), 11);
    meta.observe_uid(5).unwrap();
    assert_eq!(meta.next_uid, 12);

    assert_eq!(meta.alloc_job_id().unwrap(), 1);
    meta.observe_job_id(41).unwrap();
    assert_eq!(meta.alloc_job_id().unwrap(), 42);

    assert_eq!(meta.alloc_run_id().unwrap(), 1);
    meta.observe_run_id(7).unwrap();
    assert_eq!(meta.alloc_run_id().unwrap(), 8);

    assert_eq!(meta.alloc_site_seq().unwrap(), 1);
    meta.observe_site_seq(99).unwrap();
    assert_eq!(meta.alloc_site_seq().unwrap(), 100);
}

#[test]
fn allocator_overflow_rejected() {
    let mut meta = Root::empty(Mode::Standalone, 0).meta;
    meta.next_uid = u64::MAX;
    assert!(matches!(meta.alloc_uid(), Err(Error::InvalidArgument(_))));
    assert!(matches!(
        meta.observe_uid(u64::MAX),
        Err(Error::InvalidArgument(_))
    ));
    meta.next_site_seq = i64::MAX;
    assert!(matches!(
        meta.alloc_site_seq(),
        Err(Error::InvalidArgument(_))
    ));
}

#[test]
fn table_registry_set_and_remove() {
    let mut root = Root::empty(Mode::Standalone, 0);
    root.set_table(1, table(1, 0, 0, 0));
    root.set_table(2, table(2, 0, 0, 0));
    assert_eq!(root.tables.len(), 2);
    root.set_table(1, table(3, 1, 1, 0));
    assert_eq!(root.table(1).unwrap().hash, [3; 32]);
    assert_eq!(root.remove_table(1), Some(table(3, 1, 1, 0)));
    assert!(root.table(1).is_none());
}

#[test]
fn applied_log_and_mode_serde() {
    let log = AppliedLog { term: 4, index: 8 };
    let bytes = rmp_serde::to_vec_named(&log).unwrap();
    assert_eq!(rmp_serde::from_slice::<AppliedLog>(&bytes).unwrap(), log);
    for mode in [Mode::Standalone, Mode::Raft] {
        let bytes = rmp_serde::to_vec_named(&mode).unwrap();
        assert_eq!(rmp_serde::from_slice::<Mode>(&bytes).unwrap(), mode);
    }
}

const GOLDEN_ROOT_HEX: &str = "4f5353430100010000000000fd84a56d616a6f7201aa6d696e5f72656164657200a67461626c6573810784a468617368dc00201111111111111111111111111111111111111111111111111111111111111111a4726f777303a562797465731eae72657175697265645f6d696e6f7200a46d6574618aa46d6f6465aa7374616e64616c6f6e65a76d61726b65727381a16ba176a86e6578745f75696402ab6e6578745f6a6f625f696403ab6e6578745f72756e5f696404ad6e6578745f736974655f73657105a76170706c69656482a47465726d09a5696e6465780aaf6d656d626572736869705f6a736f6ea27b7dad666561747572655f6c6576656c01aa637265617465645f617464e241a917";
