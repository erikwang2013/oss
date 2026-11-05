//! Golden fixture loader and byte/JSON comparisons (task A8).
//!
//! Fixtures are frozen artifacts under `tests/goldens`; `MANIFEST` pins their
//! SHA-256. Capturing is a separate ignored test driven by
//! `scripts/capture_control_goldens.sh`; this module is read-only.

#![allow(dead_code)]

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::Serialize;

/// Directory holding every golden plus `MANIFEST`.
pub fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/goldens")
}

pub fn path(rel: &str) -> PathBuf {
    root().join(rel)
}

pub fn load(rel: &str) -> Vec<u8> {
    std::fs::read(path(rel))
        .unwrap_or_else(|e| panic!("golden {rel} is missing or unreadable: {e}"))
}

pub fn load_json(rel: &str) -> serde_json::Value {
    serde_json::from_slice(&load(rel))
        .unwrap_or_else(|e| panic!("golden {rel} is not valid JSON: {e}"))
}

/// Capture-side write (ignored test only). Creates parents; bytes are exact.
pub fn write(rel: &str, bytes: &[u8]) {
    let p = path(rel);
    std::fs::create_dir_all(p.parent().expect("golden path has a parent")).unwrap();
    std::fs::write(&p, bytes).unwrap_or_else(|e| panic!("write golden {rel}: {e}"));
}

pub fn write_serde<T: Serialize>(rel: &str, value: &T) {
    let mut bytes = serde_json::to_vec_pretty(value).unwrap();
    bytes.push(b'\n');
    write(rel, &bytes);
}

/// Byte-exact comparison; failure names the fixture and shows the first mismatch.
pub fn assert_bytes(rel: &str, actual: &[u8]) {
    let expected = load(rel);
    if expected != actual {
        panic!(
            "golden {rel} bytes differ\n{}",
            diff_bytes(&expected, actual)
        );
    }
}

/// JSON-value comparison (formatting is still pinned byte-exactly by MANIFEST).
pub fn assert_json(rel: &str, actual: &serde_json::Value) {
    let expected = load_json(rel);
    if expected != *actual {
        let e = serde_json::to_string_pretty(&expected).unwrap();
        let a = serde_json::to_string_pretty(actual).unwrap();
        panic!("golden {rel} JSON differs\n{}", diff_text(&e, &a));
    }
}

fn diff_bytes(expected: &[u8], actual: &[u8]) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "expected {} bytes, actual {} bytes",
        expected.len(),
        actual.len()
    );
    let at = expected
        .iter()
        .zip(actual)
        .position(|(a, b)| a != b)
        .unwrap_or(expected.len().min(actual.len()));
    let _ = writeln!(out, "first difference at byte {at}");
    let lo = at.saturating_sub(24);
    let hi_e = (at + 24).min(expected.len());
    let hi_a = (at + 24).min(actual.len());
    let _ = writeln!(out, "expected[..]: {}", escape(&expected[lo..hi_e]));
    let _ = writeln!(out, "actual  [..]: {}", escape(&actual[lo..hi_a]));
    out
}

fn diff_text(expected: &str, actual: &str) -> String {
    let e: Vec<&str> = expected.lines().collect();
    let a: Vec<&str> = actual.lines().collect();
    let at = e
        .iter()
        .zip(&a)
        .position(|(x, y)| x != y)
        .unwrap_or(e.len().min(a.len()));
    let lo = at.saturating_sub(2);
    let hi = (at + 3).max(lo);
    let mut out = String::new();
    let _ = writeln!(out, "first differing line {}:", at + 1);
    for i in lo..hi.min(e.len().max(a.len())) {
        let want = e.get(i).copied().unwrap_or("<missing>");
        let got = a.get(i).copied().unwrap_or("<missing>");
        let mark = if want == got { ' ' } else { '!' };
        let _ = writeln!(out, "{mark} {:>4} expected: {want:?}", i + 1);
        let _ = writeln!(out, "{mark} {:>4} actual  : {got:?}", i + 1);
    }
    out
}

fn escape(bytes: &[u8]) -> String {
    let mut out = String::new();
    for &b in bytes {
        match b {
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x20..=0x7e => out.push(b as char),
            _ => {
                let _ = write!(out, "\\x{b:02x}");
            }
        }
    }
    out
}
