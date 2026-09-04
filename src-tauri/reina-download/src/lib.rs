//! Segmented, resumable HTTP downloader for ReinaManager's install pipeline.
//!
//! One call to [`download`] transfers one file. Multi-task queueing, ordering,
//! and the "how many installs at once" policy belong to the caller; a
//! [`SharedBudget`] passed through [`DownloadOptions`] caps total connections
//! across concurrent calls.
//!
//! # Contract
//!
//! - The file is split into fixed-size pieces (default 4 MiB). Progress within
//!   each piece is tracked as a contiguous byte count, so an interruption loses
//!   at most the unflushed page cache, not whole pieces.
//! - State lives in a sidecar control file (`target` + [`CONTROL_SUFFIX`]),
//!   written atomically about every 2 seconds after an `fdatasync`, and only
//!   ever *under*-reporting what is durable. Target present with no control
//!   file means the download is complete.
//! - Resume identity is `expected_size` plus the caller's `identity` checksum
//!   string. The URL is not part of the identity: a refreshed signed link
//!   resumes where the old one stopped. Without an identity, changed
//!   `ETag`/`Last-Modified` validators discard the partial file.
//! - Cancellation (pause and cancel look the same here) flushes state and
//!   returns [`Outcome::Cancelled`] quickly; it is never an error.
//! - Servers that ignore `Range` get a single-stream fallback that restarts
//!   from zero on interruption.
//!
//! The caller owns final content verification (sha256/blake3): this crate
//! guarantees byte placement, not content.

mod control;
mod engine;
mod error;
mod fsx;
mod http;
mod limiter;
mod progress;
mod types;

pub use control::{CONTROL_SUFFIX, artifact_paths, control_path};
pub use engine::download;
pub use error::DownloadError;
pub use progress::{Phase, Progress};
// Re-exported so callers can drive cancellation without adding tokio-util.
pub use tokio_util::sync::CancellationToken;
pub use types::{DEFAULT_PIECE_SIZE, DownloadOptions, DownloadRequest, Outcome, SharedBudget};
