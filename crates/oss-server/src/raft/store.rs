//! Raft 文件日志/对象快照共用的 entry 编码与错误映射。
//!
//! 大 entry 的 JSON 用 zlib 压缩、`z1:` + base64 存盘；旧明文条目（`{` 开头）照常解码。
//! 快照超过 [`SNAPSHOT_CHUNK_BYTES`] 时文件快照按固定大小分块。

// openraft StorageError 体积由第三方类型固定，trait 签名不能改。
#![allow(clippy::result_large_err)]

use std::io::{Read, Write};

use base64::Engine as _;
use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use openraft::StorageError;
use openraft::{AnyError, Entry, ErrorSubject, ErrorVerb, StorageIOError};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::TypeConfig;

/// 快照单块上限：超过它才分块，单块因此有界；不超过时保持整块格式。
pub(crate) const SNAPSHOT_CHUNK_BYTES: usize = 256 * 1024;

/// 明文 JSON 达到该长度才尝试压缩；小 entry 保持明文（运维/集成测试直接读）。
const ENTRY_COMPRESS_MIN_BYTES: usize = 1024;
/// 压缩 entry 的 magic。明文 Entry JSON 必以 `{` 开头，前缀无歧义。
const ENTRY_COMPRESS_MAGIC: &str = "z1:";
/// 解压上限：命令体上限 24 MiB + Entry 外壳余量；超限按损坏 fail-closed。
const ENTRY_JSON_MAX_BYTES: u64 = 32 * 1024 * 1024;

pub(crate) fn storage_err(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    e: impl std::error::Error + 'static,
) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::new(subject, verb, AnyError::new(&e)),
    }
}

fn ser_err(subject: ErrorSubject<u64>, verb: ErrorVerb, e: serde_json::Error) -> StorageError<u64> {
    storage_err(subject, verb, e)
}

fn encode<T: Serialize>(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    v: &T,
) -> Result<String, StorageError<u64>> {
    serde_json::to_string(v).map_err(|e| ser_err(subject, verb, e))
}

fn decode<T: DeserializeOwned>(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    s: &str,
) -> Result<T, StorageError<u64>> {
    serde_json::from_str(s).map_err(|e| ser_err(subject, verb, e))
}

pub(crate) fn log_err(verb: ErrorVerb, msg: impl std::fmt::Display) -> StorageError<u64> {
    storage_err(
        ErrorSubject::Logs,
        verb,
        std::io::Error::other(msg.to_string()),
    )
}

pub(crate) fn log_corrupt(msg: impl std::fmt::Display) -> StorageError<u64> {
    log_err(ErrorVerb::Read, format_args!("corrupt raft log: {msg}"))
}

/// 序列化 entry；大载荷尝试 zlib+base64，只有确实更小才用压缩形态（`z1:` 前缀）。
pub(crate) fn encode_entry(entry: &Entry<TypeConfig>) -> Result<String, StorageError<u64>> {
    let json = encode(ErrorSubject::Log(entry.log_id), ErrorVerb::Write, entry)?;
    if json.len() < ENTRY_COMPRESS_MIN_BYTES {
        return Ok(json);
    }
    let io_err =
        |e: std::io::Error| storage_err(ErrorSubject::Log(entry.log_id), ErrorVerb::Write, e);
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
    enc.write_all(json.as_bytes()).map_err(io_err)?;
    let compressed = enc.finish().map_err(io_err)?;
    let b64 = base64::engine::general_purpose::STANDARD.encode(compressed);
    if ENTRY_COMPRESS_MAGIC.len() + b64.len() < json.len() {
        Ok(format!("{ENTRY_COMPRESS_MAGIC}{b64}"))
    } else {
        Ok(json)
    }
}

/// 解码 entry，并返回原始 JSON 字节数（用于限制内存）。任何损坏都 fail-closed。
pub(crate) fn decode_entry_with_len(
    s: &str,
) -> Result<(Entry<TypeConfig>, usize), StorageError<u64>> {
    let Some(b64) = s.strip_prefix(ENTRY_COMPRESS_MAGIC) else {
        // 旧库/小 entry：明文 JSON。
        return Ok((decode(ErrorSubject::Logs, ErrorVerb::Read, s)?, s.len()));
    };
    let corrupt = |what: String| log_corrupt(format!("compressed entry: {what}"));
    let compressed = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| corrupt(e.to_string()))?;
    let mut json = Vec::new();
    ZlibDecoder::new(compressed.as_slice())
        .take(ENTRY_JSON_MAX_BYTES + 1)
        .read_to_end(&mut json)
        .map_err(|e| corrupt(e.to_string()))?;
    if json.len() as u64 > ENTRY_JSON_MAX_BYTES {
        return Err(corrupt(format!(
            "decompressed size exceeds {ENTRY_JSON_MAX_BYTES} bytes"
        )));
    }
    let entry = serde_json::from_slice(&json)
        .map_err(|e| log_corrupt(format!("compressed entry json: {e}")))?;
    Ok((entry, json.len()))
}
