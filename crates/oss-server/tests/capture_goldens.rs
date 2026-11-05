//! Golden capture entry point (task A8). Ignored by default; the only supported
//! way to run it is `scripts/capture_control_goldens.sh`, which sets
//! `CAPTURE_CONTROL_GOLDENS=1` and rebuilds the manifest afterwards.

mod common;
#[path = "support/golden.rs"]
mod golden;
mod golden_capture;
