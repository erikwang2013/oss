//! Fixed framing for every control-state object (design §1.3):
//!
//! `"OSSC" | kind u8 | major u16 | min_reader u16 | payload_len u32 | payload | crc32c u32`
//!
//! Multi-byte integers are big-endian and the checksum is CRC-32C (Castagnoli)
//! over the payload only. Version gates are enforced before a payload is returned.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

pub const ROOT_MAGIC: [u8; 4] = *b"OSSC";
pub const MAGIC: [u8; 4] = ROOT_MAGIC;
/// Only format this binary writes and reads (design §1.3: higher major fails closed).
pub const KNOWN_MAJOR: u16 = 1;
/// Lowest reader minor this binary implements. Unknown fields are ignored by serde.
pub const KNOWN_MINOR: u16 = 0;
pub const HEADER_LEN: usize = 4 + 1 + 2 + 2 + 4;
pub const TRAILER_LEN: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ObjectKind {
    Root = 1,
    Node = 2,
    Blob = 3,
}

impl TryFrom<u8> for ObjectKind {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Root),
            2 => Ok(Self::Node),
            3 => Ok(Self::Blob),
            other => Err(Error::CorruptMeta(format!(
                "control object: unknown kind {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectHeader {
    pub kind: ObjectKind,
    pub major: u16,
    pub min_reader: u16,
}

impl ObjectHeader {
    /// Fail closed when the object needs a newer writer major or reader minor.
    pub fn check_compatible(&self) -> Result<()> {
        if self.major != KNOWN_MAJOR {
            return Err(Error::Unsupported(format!(
                "control object major {} unsupported (known {KNOWN_MAJOR})",
                self.major
            )));
        }
        if self.min_reader > KNOWN_MINOR {
            return Err(Error::Unsupported(format!(
                "control object needs reader minor {} (known {KNOWN_MINOR})",
                self.min_reader
            )));
        }
        Ok(())
    }
}

pub fn encode_object(header: &ObjectHeader, payload: &[u8]) -> Result<Vec<u8>> {
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| Error::Encode("control object payload exceeds u32".into()))?;
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len() + TRAILER_LEN);
    out.extend_from_slice(&MAGIC);
    out.push(header.kind as u8);
    out.extend_from_slice(&header.major.to_be_bytes());
    out.extend_from_slice(&header.min_reader.to_be_bytes());
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(payload);
    out.extend_from_slice(&crc32c::crc32c(payload).to_be_bytes());
    Ok(out)
}

/// Parse and verify framing, returns the header and a borrowed payload.
pub fn decode_object(bytes: &[u8]) -> Result<(ObjectHeader, &[u8])> {
    if bytes.len() < HEADER_LEN + TRAILER_LEN {
        return Err(Error::CorruptMeta(format!(
            "control object: truncated ({} bytes)",
            bytes.len()
        )));
    }
    if bytes[..4] != MAGIC {
        return Err(Error::CorruptMeta("control object: bad magic".into()));
    }
    let kind = ObjectKind::try_from(bytes[4])?;
    let major = u16::from_be_bytes([bytes[5], bytes[6]]);
    let min_reader = u16::from_be_bytes([bytes[7], bytes[8]]);
    let payload_len = u32::from_be_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]) as usize;
    let expected = HEADER_LEN + payload_len + TRAILER_LEN;
    if bytes.len() != expected {
        return Err(Error::CorruptMeta(format!(
            "control object: length {} != declared {expected}",
            bytes.len()
        )));
    }
    let payload = &bytes[HEADER_LEN..HEADER_LEN + payload_len];
    let crc_at = HEADER_LEN + payload_len;
    let crc = u32::from_be_bytes([
        bytes[crc_at],
        bytes[crc_at + 1],
        bytes[crc_at + 2],
        bytes[crc_at + 3],
    ]);
    if crc != crc32c::crc32c(payload) {
        return Err(Error::CorruptMeta("control object: crc32c mismatch".into()));
    }
    let header = ObjectHeader {
        kind,
        major,
        min_reader,
    };
    header.check_compatible()?;
    Ok((header, payload))
}

pub fn encode_payload<T: Serialize + ?Sized>(
    kind: ObjectKind,
    major: u16,
    min_reader: u16,
    value: &T,
) -> Result<Vec<u8>> {
    let payload =
        rmp_serde::to_vec_named(value).map_err(|e| Error::Encode(format!("rmp encode: {e}")))?;
    encode_object(
        &ObjectHeader {
            kind,
            major,
            min_reader,
        },
        &payload,
    )
}

pub fn decode_payload<T: DeserializeOwned>(bytes: &[u8]) -> Result<(ObjectHeader, T)> {
    let (header, payload) = decode_object(bytes)?;
    let value = rmp_serde::from_slice(payload)
        .map_err(|e| Error::CorruptMeta(format!("rmp decode: {e}")))?;
    Ok((header, value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn header_roundtrip_keeps_payload() {
        let header = ObjectHeader {
            kind: ObjectKind::Node,
            major: KNOWN_MAJOR,
            min_reader: KNOWN_MINOR,
        };
        let bytes = encode_object(&header, b"payload").unwrap();
        let (got, payload) = decode_object(&bytes).unwrap();
        assert_eq!(got, header);
        assert_eq!(payload, b"payload");
    }

    #[test]
    fn golden_header_bytes() {
        // magic "OSSC" | kind node=2 | major 1 | min_reader 0 | len 3 | "abc" | crc32c.
        let bytes = encode_object(
            &ObjectHeader {
                kind: ObjectKind::Node,
                major: 1,
                min_reader: 0,
            },
            b"abc",
        )
        .unwrap();
        assert_eq!(
            hex::encode(&bytes),
            "4f535343020001000000000003616263364b3fb7"
        );
    }

    #[test]
    fn crc_is_crc32c_castagnoli() {
        assert_eq!(crc32c::crc32c(b"abc"), 0x364b_3fb7);
        let bytes = encode_object(
            &ObjectHeader {
                kind: ObjectKind::Blob,
                major: 1,
                min_reader: 0,
            },
            b"abc",
        )
        .unwrap();
        assert_eq!(&bytes[bytes.len() - 4..], &0x364b_3fb7u32.to_be_bytes());
    }

    #[test]
    fn crc_mismatch_rejected() {
        let mut bytes = encode_object(
            &ObjectHeader {
                kind: ObjectKind::Root,
                major: 1,
                min_reader: 0,
            },
            b"payload",
        )
        .unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        assert!(matches!(decode_object(&bytes), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn truncated_header_rejected() {
        let bytes = encode_object(
            &ObjectHeader {
                kind: ObjectKind::Root,
                major: 1,
                min_reader: 0,
            },
            b"",
        )
        .unwrap();
        for cut in 0..bytes.len() {
            assert!(
                matches!(decode_object(&bytes[..cut]), Err(Error::CorruptMeta(_))),
                "truncation at {cut} accepted"
            );
        }
    }

    #[test]
    fn trailing_bytes_rejected() {
        let mut bytes = encode_object(
            &ObjectHeader {
                kind: ObjectKind::Root,
                major: 1,
                min_reader: 0,
            },
            b"x",
        )
        .unwrap();
        bytes.push(0);
        assert!(matches!(decode_object(&bytes), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn bad_magic_rejected() {
        let mut bytes = encode_object(
            &ObjectHeader {
                kind: ObjectKind::Root,
                major: 1,
                min_reader: 0,
            },
            b"x",
        )
        .unwrap();
        bytes[0] = b'X';
        assert!(matches!(decode_object(&bytes), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn unknown_kind_rejected() {
        let mut bytes = encode_object(
            &ObjectHeader {
                kind: ObjectKind::Root,
                major: 1,
                min_reader: 0,
            },
            b"x",
        )
        .unwrap();
        bytes[4] = 9;
        assert!(matches!(decode_object(&bytes), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn unknown_major_rejected() {
        let bytes = encode_object(
            &ObjectHeader {
                kind: ObjectKind::Root,
                major: 2,
                min_reader: 0,
            },
            b"x",
        )
        .unwrap();
        assert!(matches!(decode_object(&bytes), Err(Error::Unsupported(_))));
    }

    #[test]
    fn too_new_min_reader_rejected() {
        let bytes = encode_object(
            &ObjectHeader {
                kind: ObjectKind::Root,
                major: 1,
                min_reader: 1,
            },
            b"x",
        )
        .unwrap();
        assert!(matches!(decode_object(&bytes), Err(Error::Unsupported(_))));
    }

    #[test]
    fn typed_payload_roundtrip() {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Row {
            name: String,
            n: u64,
        }
        let row = Row {
            name: "a".into(),
            n: 7,
        };
        let bytes = encode_payload(ObjectKind::Root, KNOWN_MAJOR, KNOWN_MINOR, &row).unwrap();
        let (header, got) = decode_payload::<Row>(&bytes).unwrap();
        assert_eq!(header.kind, ObjectKind::Root);
        assert_eq!(got, row);
    }
}
