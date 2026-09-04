//! Progress reporting: a shared atomic snapshot and a phase enum.
//!
//! A dedicated emitter task samples these atomics on a fixed short interval and
//! pushes a [`Progress`] into the caller's `watch` channel, so UI updates stay
//! smooth regardless of how the transfer bunches up.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

/// Coarse stage of a download, surfaced to the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Phase {
    /// Sending the initial range probe.
    Probing = 0,
    /// Downloading with parallel range requests.
    Downloading = 1,
    /// Server does not support ranges; downloading in one stream.
    SingleStream = 2,
    /// Flushing the last bytes and removing the control file.
    Finalizing = 3,
    /// Finished successfully.
    Done = 4,
    /// Stopped for pause or cancel.
    Cancelled = 5,
}

impl Phase {
    #[must_use]
    pub(crate) fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Probing,
            1 => Self::Downloading,
            2 => Self::SingleStream,
            3 => Self::Finalizing,
            5 => Self::Cancelled,
            _ => Self::Done,
        }
    }
}

/// A point-in-time snapshot delivered over the progress channel.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Progress {
    pub phase: Phase,
    /// Total size in bytes; `0` until the probe completes.
    pub total: u64,
    /// Bytes flushed to disk and recorded in the control file. Survives a crash.
    pub committed: u64,
    /// Bytes written to the page cache. Monotonic; use this for the UI bar.
    pub written: u64,
    /// Smoothed transfer speed in bytes per second.
    pub speed_bps: f64,
    /// Active connection count target.
    pub connections: usize,
    /// Cumulative retry count.
    pub retries: u32,
}

impl Progress {
    /// The value to seed the caller's `watch` channel with.
    #[must_use]
    pub fn initial() -> Self {
        Self {
            phase: Phase::Probing,
            total: 0,
            committed: 0,
            written: 0,
            speed_bps: 0.0,
            connections: 0,
            retries: 0,
        }
    }
}

/// Atomic backing store updated by the workers and committer.
#[derive(Debug)]
pub(crate) struct Shared {
    phase: AtomicU8Cell,
    total: AtomicU64,
    pub written: AtomicU64,
    pub committed: AtomicU64,
    pub connections: AtomicUsize,
    pub retries: AtomicU32,
}

impl Shared {
    pub(crate) fn new() -> Self {
        Self {
            phase: AtomicU8Cell::new(Phase::Probing as u8),
            total: AtomicU64::new(0),
            written: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            connections: AtomicUsize::new(0),
            retries: AtomicU32::new(0),
        }
    }

    pub(crate) fn set_phase(&self, phase: Phase) {
        self.phase.0.store(phase as u8, Ordering::Relaxed);
    }

    pub(crate) fn set_total(&self, total: u64) {
        self.total.store(total, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self, speed_bps: f64) -> Progress {
        Progress {
            phase: Phase::from_u8(self.phase.0.load(Ordering::Relaxed)),
            total: self.total.load(Ordering::Relaxed),
            committed: self.committed.load(Ordering::Relaxed),
            written: self.written.load(Ordering::Relaxed),
            speed_bps,
            connections: self.connections.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
        }
    }
}

// A tiny newtype so the struct field reads clearly.
#[derive(Debug)]
struct AtomicU8Cell(std::sync::atomic::AtomicU8);

impl AtomicU8Cell {
    fn new(value: u8) -> Self {
        Self(std::sync::atomic::AtomicU8::new(value))
    }
}
