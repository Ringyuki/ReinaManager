use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::time::Duration;

use crate::error::DownloadError;
use crate::limiter::Gate;

/// Default piece size: small enough that a pause or crash loses seconds, large
/// enough that a multi-gigabyte file stays under a few thousand requests.
pub const DEFAULT_PIECE_SIZE: u64 = 4 * 1024 * 1024;

/// What to download and where to put it.
#[derive(Debug, Clone)]
pub struct DownloadRequest {
    /// Source URL. May differ from the URL used on a previous attempt; resume
    /// keys on size and identity, not URL.
    pub url: String,
    /// Destination path. The control file is `target` + [`crate::CONTROL_SUFFIX`].
    pub target: PathBuf,
    /// Size the caller expects, from the install request. The server's reported
    /// size must match this.
    pub expected_size: u64,
    /// Stable content identity such as `"sha256:abc…"`. When present, changed
    /// `ETag`/`Last-Modified` do not abort a resume, because the caller verifies
    /// the finished file anyway.
    pub identity: Option<String>,
}

/// A shared cap on concurrent connections across several downloads, so running
/// two installs at once cannot flood one CDN.
#[derive(Debug, Clone)]
pub struct SharedBudget(pub(crate) Gate);

impl SharedBudget {
    /// Creates a budget allowing `max` concurrent connections in total.
    #[must_use]
    pub fn new(max: usize) -> Self {
        Self(Gate::new(max))
    }
}

/// Tunables for a single download. [`Default`] matches the values documented in
/// the design proposal.
#[derive(Debug, Clone)]
pub struct DownloadOptions {
    pub piece_size: u64,
    pub min_connections: usize,
    pub initial_connections: usize,
    pub max_connections: usize,
    /// Disconnect and retry a piece after this long without any body bytes.
    pub stall_timeout: Duration,
    /// How often durable progress is flushed to the control file.
    pub commit_interval: Duration,
    /// How often a progress snapshot is pushed to the watch channel.
    pub progress_interval: Duration,
    /// Grow the connection target after this many consecutive piece successes.
    pub grow_after_successes: u32,
    /// Fail the whole download after this many consecutive retryable failures.
    pub max_consecutive_failures: u32,
    /// Optional cross-download connection budget.
    pub budget: Option<SharedBudget>,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            piece_size: DEFAULT_PIECE_SIZE,
            min_connections: 1,
            initial_connections: 4,
            max_connections: 8,
            stall_timeout: Duration::from_secs(20),
            commit_interval: Duration::from_secs(2),
            progress_interval: Duration::from_millis(250),
            grow_after_successes: 8,
            max_consecutive_failures: 20,
            budget: None,
        }
    }
}

impl DownloadOptions {
    #[must_use]
    pub fn connections(&self) -> RangeInclusive<usize> {
        self.min_connections..=self.max_connections
    }

    pub(crate) fn validate(&self) -> Result<(), DownloadError> {
        if self.piece_size == 0 {
            return Err(DownloadError::InvalidConfig(
                "piece_size must be positive".into(),
            ));
        }
        if self.min_connections == 0 {
            return Err(DownloadError::InvalidConfig(
                "min_connections must be >= 1".into(),
            ));
        }
        if self.max_connections < self.min_connections {
            return Err(DownloadError::InvalidConfig(
                "max_connections must be >= min_connections".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn clamped_initial(&self) -> usize {
        self.initial_connections
            .clamp(self.min_connections, self.max_connections)
    }
}

/// How a download ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The target file is complete and the control file removed.
    Completed,
    /// Cancellation was observed; progress is persisted for a later resume.
    Cancelled,
}
