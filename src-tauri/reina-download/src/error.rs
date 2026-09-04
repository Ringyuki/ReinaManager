use std::fmt;
use std::path::PathBuf;

/// Errors returned by [`crate::download`].
///
/// Cancellation is not an error: a cancelled download returns
/// [`crate::Outcome::Cancelled`] after persisting its state.
#[derive(Debug)]
pub enum DownloadError {
    /// The server answered with a status the downloader cannot recover from,
    /// or a retryable status whose retry budget has been exhausted.
    Http { status: u16, retryable: bool },
    /// The server's reported size does not match the size the caller expects.
    SizeMismatch { expected: u64, actual: u64 },
    /// The remote resource changed identity while downloading and no checksum
    /// was available to justify continuing.
    RemoteChanged(String),
    /// A response violated the HTTP range contract (bad `Content-Range`,
    /// unexpected body length, compressed body, and so on).
    Protocol(String),
    /// Transport failure: connect, TLS, reset, stall.
    Network(String),
    /// Local filesystem failure while writing data or control state.
    Disk(std::io::Error),
    /// The target path already exists, is not a completed download, and there
    /// is no control file describing it.
    TargetConflict(PathBuf),
    /// Consecutive retryable failures exceeded the configured budget.
    RetriesExhausted {
        attempts: u32,
        last: Box<DownloadError>,
    },
    /// The caller supplied an invalid request or option.
    InvalidConfig(String),
}

impl DownloadError {
    /// Whether another attempt at the same operation could reasonably succeed.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Http { retryable, .. } => *retryable,
            Self::Protocol(_) | Self::Network(_) => true,
            Self::SizeMismatch { .. }
            | Self::RemoteChanged(_)
            | Self::Disk(_)
            | Self::TargetConflict(_)
            | Self::RetriesExhausted { .. }
            | Self::InvalidConfig(_) => false,
        }
    }

    /// The HTTP status associated with this failure, looking through
    /// [`Self::RetriesExhausted`].
    #[must_use]
    pub fn http_status(&self) -> Option<u16> {
        match self {
            Self::Http { status, .. } => Some(*status),
            Self::RetriesExhausted { last, .. } => last.http_status(),
            _ => None,
        }
    }

    /// Stable machine-readable name for logging and UI mapping.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Http { .. } => "http",
            Self::SizeMismatch { .. } => "size_mismatch",
            Self::RemoteChanged(_) => "remote_changed",
            Self::Protocol(_) => "protocol",
            Self::Network(_) => "network",
            Self::Disk(_) => "disk",
            Self::TargetConflict(_) => "target_conflict",
            Self::RetriesExhausted { .. } => "retries_exhausted",
            Self::InvalidConfig(_) => "invalid_config",
        }
    }
}

impl fmt::Display for DownloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http { status, retryable } => {
                if *retryable {
                    write!(f, "retryable HTTP status {status}")
                } else {
                    write!(f, "HTTP status {status}")
                }
            }
            Self::SizeMismatch { expected, actual } => {
                write!(
                    f,
                    "size mismatch: expected {expected} bytes, server reports {actual}"
                )
            }
            Self::RemoteChanged(detail) => write!(f, "remote resource changed: {detail}"),
            Self::Protocol(detail) => write!(f, "HTTP protocol violation: {detail}"),
            Self::Network(detail) => write!(f, "network error: {detail}"),
            Self::Disk(error) => write!(f, "disk error: {error}"),
            Self::TargetConflict(path) => {
                write!(
                    f,
                    "target already exists without a control file: {}",
                    path.display()
                )
            }
            Self::RetriesExhausted { attempts, last } => {
                write!(
                    f,
                    "gave up after {attempts} consecutive failures; last error: {last}"
                )
            }
            Self::InvalidConfig(detail) => write!(f, "invalid configuration: {detail}"),
        }
    }
}

impl std::error::Error for DownloadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Disk(error) => Some(error),
            Self::RetriesExhausted { last, .. } => Some(last.as_ref()),
            _ => None,
        }
    }
}

impl From<std::io::Error> for DownloadError {
    fn from(error: std::io::Error) -> Self {
        Self::Disk(error)
    }
}

pub(crate) fn map_reqwest(error: &reqwest::Error) -> DownloadError {
    if error.is_timeout() {
        return DownloadError::Network(format!("request timed out: {error}"));
    }
    if error.is_connect() {
        return DownloadError::Network(format!("connection failed: {error}"));
    }
    if error.is_redirect() {
        return DownloadError::Protocol(format!("redirect policy violated: {error}"));
    }
    if error.is_body() || error.is_decode() {
        return DownloadError::Network(format!("body transfer failed: {error}"));
    }
    if error.is_builder() || error.is_request() {
        return DownloadError::InvalidConfig(error.to_string());
    }
    DownloadError::Network(error.to_string())
}
