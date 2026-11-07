# Vendored dependency patches

These path crates are wired through `[patch.crates-io]` in the workspace
`Cargo.toml`. They are not forks for features; each patch removes a published
advisory while keeping the public API and on-disk behavior unchanged.

| Crate | Upstream | Patch |
| --- | --- | --- |
| `ecat-metrics` | 7.3.0 | `prometheus` uses `default-features = false`, which removes `protobuf 2.28.0` (RUSTSEC-2024-0437). Only the text encoder is used. Re-check when ecat-metrics starts requiring prometheus 0.14. |
| `reed-solomon-erasure` | 6.0.0 | `lru 0.7.8` -> `0.18.2` (RUSTSEC-2026-0253) and `parking_lot 0.11` -> `0.12` (drops unmaintained `instant`). Only `LruCache::new` takes `NonZeroUsize`; the cache is not part of the encoded format, and the coding math is untouched. Re-check when upstream releases a fixed 6.x or a compatible replacement. |

Do not edit the vendored source beyond these patches without rerunning the
full workspace + strict no-db gates and the A8 goldens.
