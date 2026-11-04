//! Large control values: content-addressed 64 KiB chunks with a 24 MiB cap
//! (design §1.3/D3). Chunks are immutable; `BlobRef` lives in the leaf entry so
//! a huge row never inflates a tree node.

use serde::{Deserialize, Serialize};

use super::format::{self, KNOWN_MAJOR, KNOWN_MINOR, ObjectKind};
use super::hash;
use crate::error::{Error, Result};

pub const CHUNK_SIZE: usize = 64 * 1024;
/// `COMMAND_MAX_BYTES`: a single control row/command may not exceed 24 MiB.
pub const MAX_BLOB_BYTES: usize = 24 * 1024 * 1024;
pub const MAX_CHUNKS: u32 = (MAX_BLOB_BYTES / CHUNK_SIZE) as u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    /// SHA-256 of the full (unframed) value.
    pub sha256: [u8; 32],
    /// Full value length in bytes.
    pub len: u64,
    pub chunks: u32,
}

impl BlobRef {
    pub fn validate(&self) -> Result<()> {
        if self.len > MAX_BLOB_BYTES as u64 {
            return Err(Error::InvalidArgument(format!(
                "blob length {} exceeds {MAX_BLOB_BYTES}",
                self.len
            )));
        }
        let expected = self.len.div_ceil(CHUNK_SIZE as u64) as u32;
        if self.chunks != expected {
            return Err(Error::InvalidArgument(format!(
                "blob chunk count {} != expected {expected} for {} bytes",
                self.chunks, self.len
            )));
        }
        Ok(())
    }
}

/// Split a value into raw chunk payloads and its content address.
///
/// An empty value has zero chunks (no object is written).
pub fn split(bytes: &[u8]) -> Result<(BlobRef, Vec<Vec<u8>>)> {
    if bytes.len() > MAX_BLOB_BYTES {
        return Err(Error::InvalidArgument(format!(
            "blob {} exceeds {MAX_BLOB_BYTES} bytes",
            bytes.len()
        )));
    }
    let chunks: Vec<Vec<u8>> = bytes.chunks(CHUNK_SIZE).map(<[u8]>::to_vec).collect();
    let blob = BlobRef {
        sha256: hash::sha256(bytes),
        len: bytes.len() as u64,
        chunks: chunks.len() as u32,
    };
    blob.validate()?;
    Ok((blob, chunks))
}

/// Reassemble and verify a value; any mismatch is `CorruptMeta`, never a partial row.
pub fn join(blob: &BlobRef, chunks: &[Vec<u8>]) -> Result<Vec<u8>> {
    blob.validate()?;
    if chunks.len() != blob.chunks as usize {
        return Err(Error::CorruptMeta(format!(
            "blob has {} chunks, ref says {}",
            chunks.len(),
            blob.chunks
        )));
    }
    let mut out = Vec::with_capacity(blob.len as usize);
    for (i, chunk) in chunks.iter().enumerate() {
        let last = i + 1 == chunks.len();
        if (!last && chunk.len() != CHUNK_SIZE) || (last && chunk.len() > CHUNK_SIZE) {
            return Err(Error::CorruptMeta(format!(
                "blob chunk {i} has invalid length {}",
                chunk.len()
            )));
        }
        out.extend_from_slice(chunk);
    }
    if out.len() as u64 != blob.len {
        return Err(Error::CorruptMeta(format!(
            "blob length {} != ref {}",
            out.len(),
            blob.len
        )));
    }
    if hash::sha256(&out) != blob.sha256 {
        return Err(Error::CorruptMeta("blob sha256 mismatch".into()));
    }
    Ok(out)
}

/// Frame one chunk as a `kind=Blob` control object.
pub fn encode_chunk(chunk: &[u8]) -> Result<Vec<u8>> {
    if chunk.len() > CHUNK_SIZE {
        return Err(Error::InvalidArgument(format!(
            "blob chunk {} exceeds {CHUNK_SIZE}",
            chunk.len()
        )));
    }
    format::encode_payload(ObjectKind::Blob, KNOWN_MAJOR, KNOWN_MINOR, &chunk.to_vec())
}

pub fn decode_chunk(bytes: &[u8]) -> Result<Vec<u8>> {
    let (header, chunk) = format::decode_payload::<Vec<u8>>(bytes)?;
    if header.kind != ObjectKind::Blob {
        return Err(Error::CorruptMeta("expected blob object".into()));
    }
    if chunk.len() > CHUNK_SIZE {
        return Err(Error::CorruptMeta(format!(
            "blob chunk {} exceeds {CHUNK_SIZE}",
            chunk.len()
        )));
    }
    Ok(chunk)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i.wrapping_mul(31) + 7) as u8).collect()
    }

    #[test]
    fn empty_blob_roundtrip() {
        let (r, chunks) = split(b"").unwrap();
        assert_eq!(r.len, 0);
        assert_eq!(r.chunks, 0);
        assert_eq!(r.sha256, crate::control_state::hash::sha256(b""));
        assert!(chunks.is_empty());
        assert_eq!(join(&r, &chunks).unwrap(), b"");
    }

    #[test]
    fn sixteen_mib_roundtrip_in_64k_chunks() {
        let data = pattern(16 * 1024 * 1024);
        let (r, chunks) = split(&data).unwrap();
        assert_eq!(r.len, data.len() as u64);
        assert_eq!(r.chunks, 256);
        assert_eq!(chunks.len(), 256);
        assert!(chunks.iter().all(|c| c.len() == CHUNK_SIZE));

        // Every chunk survives its own OSSC frame.
        let mut framed = Vec::new();
        for chunk in &chunks {
            let obj = encode_chunk(chunk).unwrap();
            framed.push(decode_chunk(&obj).unwrap());
        }
        assert_eq!(join(&r, &framed).unwrap(), data);
    }

    #[test]
    fn exact_24mib_accepted_one_more_rejected() {
        let max = vec![0u8; MAX_BLOB_BYTES];
        let (r, _) = split(&max).unwrap();
        assert_eq!(r.chunks, 384);
        assert!(split(&vec![0u8; MAX_BLOB_BYTES + 1]).is_err());
    }

    #[test]
    fn join_detects_corruption() {
        let data = pattern(CHUNK_SIZE * 2 + 10);
        let (r, mut chunks) = split(&data).unwrap();
        chunks[1][0] ^= 1;
        assert!(matches!(join(&r, &chunks), Err(Error::CorruptMeta(_))));

        let (r, chunks) = split(&data).unwrap();
        assert!(matches!(join(&r, &chunks[..1]), Err(Error::CorruptMeta(_))));

        let (mut r, chunks) = split(&data).unwrap();
        r.len += 1;
        assert!(matches!(join(&r, &chunks), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn chunk_frame_rejects_oversize_and_wrong_kind() {
        assert!(matches!(
            encode_chunk(&vec![0u8; CHUNK_SIZE + 1]),
            Err(Error::InvalidArgument(_))
        ));
        let root = format::encode_object(
            &format::ObjectHeader {
                kind: format::ObjectKind::Root,
                major: format::KNOWN_MAJOR,
                min_reader: format::KNOWN_MINOR,
            },
            b"x",
        )
        .unwrap();
        assert!(matches!(decode_chunk(&root), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn blob_ref_serde_is_stable() {
        let r = BlobRef {
            sha256: [7; 32],
            len: 123,
            chunks: 2,
        };
        let bytes = rmp_serde::to_vec_named(&r).unwrap();
        let got: BlobRef = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(got, r);
    }
}
