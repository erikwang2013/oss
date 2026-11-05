//! Empty-runstate snapshot encoding: below feature 3 the `m8.runstate`
//! extension is omitted; at level >= 3 output stays byte-identical to the A8
//! goldens.

use oss_server::raft::data::{self, ControlData};
use oss_server::raft::runstate::{RunState, decode_runstate_snapshot, encode_runstate_snapshot};

fn golden(name: &str) -> Vec<u8> {
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/goldens/snapshots")
            .join(name),
    )
    .unwrap()
}

/// An empty runstate is not level-3 data: below 3 the extension is omitted and
/// the bytes equal the plain snapshot at the same level.
#[test]
fn empty_runstate_below_level_three_omits_extension() {
    let data = ControlData::default();
    for level in [1, 2] {
        let bytes = encode_runstate_snapshot(&data, &RunState::default(), level).unwrap();
        assert_eq!(bytes, data::encode_snapshot_at(&data, level).unwrap());
        let decoded = data::decode_snapshot(&bytes).unwrap();
        assert!(decoded.extensions.is_empty());
        assert_eq!(decoded.feature_level, level);
        assert!(decode_runstate_snapshot(&bytes).unwrap().1.is_empty());
    }
}

/// Level >= 3 keeps writing the `m8.runstate` extension even when empty, so the
/// A8 golden (and production snapshots) stay byte-identical.
#[test]
fn empty_level_three_snapshot_matches_a8_golden() {
    let want = golden("export_ossctl3.bin");
    let data = data::decode_snapshot(&want).unwrap().data;
    let bytes = encode_runstate_snapshot(&data, &RunState::default(), 3).unwrap();
    assert_eq!(bytes, want, "empty level-3 snapshot bytes changed");
}

/// Non-empty runstate output is unchanged at levels 3/4/5 (A8 golden + the
/// extension payload is level-independent).
#[test]
fn nonempty_runstate_bytes_unchanged_above_level_three() {
    let want = golden("export_ossctl3_runstate.bin");
    let (data, rs) = decode_runstate_snapshot(&want).unwrap();
    assert_eq!(encode_runstate_snapshot(&data, &rs, 3).unwrap(), want);

    let want_ext = data::decode_snapshot(&want).unwrap().extensions;
    for level in [4, 5] {
        let bytes = encode_runstate_snapshot(&data, &rs, level).unwrap();
        let decoded = data::decode_snapshot(&bytes).unwrap();
        assert_eq!(decoded.feature_level, level);
        assert_eq!(decoded.data, data);
        assert_eq!(decoded.extensions, want_ext);
        assert_eq!(decode_runstate_snapshot(&bytes).unwrap().1, rs);
    }
}
