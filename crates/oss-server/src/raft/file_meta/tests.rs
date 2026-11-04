use std::fs;

use openraft::{CommittedLeaderId, LogId, Vote};

use super::*;

fn log_id(term: u64, index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}

#[test]
fn raft_file_meta_atomic_roundtrip_and_stale_tmp_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    assert!(RaftMetaFile::load(path).unwrap().is_none());
    let meta = RaftMetaFile {
        vote: Some(Vote::new_committed(7, 1)),
        committed: Some(log_id(3, 11)),
        last_index: Some(log_id(3, 11)),
        last_purged: Some(log_id(2, 4)),
        log_format: CURRENT_LOG_FORMAT,
    };
    meta.store(path).unwrap();
    assert_eq!(RaftMetaFile::load(path).unwrap(), Some(meta.clone()));

    // A crash before rename leaves only the tmp file; the committed meta must win.
    fs::write(path.join(META_TMP), b"not json").unwrap();
    assert_eq!(RaftMetaFile::load(path).unwrap(), Some(meta));
}

#[test]
fn raft_file_meta_refuses_unknown_formats() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    for (format, readable) in [(1u32, true), (2, true), (0, false), (3, false), (99, false)] {
        let meta = RaftMetaFile {
            log_format: format,
            ..RaftMetaFile::default()
        };
        fs::write(RaftMetaFile::path(path), serde_json::to_vec(&meta).unwrap()).unwrap();
        match RaftMetaFile::load(path) {
            Ok(loaded) => {
                assert!(readable, "format {format} must fail closed");
                assert_eq!(loaded.unwrap().log_format, format);
            }
            Err(e) => {
                assert!(!readable, "format {format} must be readable");
                let msg = e.to_string();
                assert!(msg.contains("unsupported raft log format"), "{msg}");
                if format > CURRENT_LOG_FORMAT {
                    assert!(msg.contains("upgrade"), "{msg}");
                }
            }
        }
    }
}

#[test]
fn raft_file_meta_corrupt_and_inverted_bounds_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    fs::write(RaftMetaFile::path(path), b"{not json").unwrap();
    let err = RaftMetaFile::load(path).unwrap_err().to_string();
    assert!(err.contains("corrupt raft meta file"), "{err}");

    let meta = RaftMetaFile {
        last_index: Some(log_id(4, 2)),
        last_purged: Some(log_id(5, 9)),
        ..RaftMetaFile::default()
    };
    fs::write(RaftMetaFile::path(path), serde_json::to_vec(&meta).unwrap()).unwrap();
    let err = RaftMetaFile::load(path).unwrap_err().to_string();
    assert!(err.contains("below last_purged"), "{err}");
}
