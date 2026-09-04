//! Concurrency control.
//!
//! [`Gate`] is a resizable counting limiter used for the cross-download budget.
//! [`Adaptive`] tracks a per-download connection target that halves on rate
//! limiting and grows back after a run of successes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// A resizable async counting semaphore.
#[derive(Debug, Clone)]
pub(crate) struct Gate {
    inner: Arc<GateInner>,
}

#[derive(Debug)]
struct GateInner {
    state: Mutex<GateState>,
    notify: Notify,
}

#[derive(Debug)]
struct GateState {
    max: usize,
    in_flight: usize,
}

/// Releases one slot on drop.
#[derive(Debug)]
pub(crate) struct GatePermit {
    inner: Arc<GateInner>,
}

impl Gate {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            inner: Arc::new(GateInner {
                state: Mutex::new(GateState {
                    max: max.max(1),
                    in_flight: 0,
                }),
                notify: Notify::new(),
            }),
        }
    }

    /// Waits for a slot, or returns `None` if `cancel` fires first.
    pub(crate) async fn acquire(&self, cancel: &CancellationToken) -> Option<GatePermit> {
        loop {
            let notified = {
                let mut state = self.lock();
                if state.in_flight < state.max {
                    state.in_flight += 1;
                    return Some(GatePermit {
                        inner: Arc::clone(&self.inner),
                    });
                }
                self.inner.notify.notified()
            };
            tokio::select! {
                biased;
                () = cancel.cancelled() => return None,
                () = notified => {}
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, GateState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

impl Drop for GatePermit {
    fn drop(&mut self) {
        {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.in_flight = state.in_flight.saturating_sub(1);
        }
        self.inner.notify.notify_one();
    }
}

/// Per-download adaptive connection target.
#[derive(Debug)]
pub(crate) struct Adaptive {
    min: usize,
    max: usize,
    target: AtomicUsize,
    grow_after: u32,
    successes: AtomicUsize,
    timing: Mutex<Timing>,
}

#[derive(Debug)]
struct Timing {
    last_change: Option<Instant>,
    not_before: Instant,
}

impl Adaptive {
    pub(crate) fn new(min: usize, initial: usize, max: usize, grow_after: u32) -> Self {
        Self {
            min: min.max(1),
            max: max.max(min.max(1)),
            target: AtomicUsize::new(initial.clamp(min.max(1), max.max(min.max(1)))),
            grow_after,
            successes: AtomicUsize::new(0),
            timing: Mutex::new(Timing {
                last_change: None,
                not_before: Instant::now(),
            }),
        }
    }

    pub(crate) fn current(&self) -> usize {
        self.target.load(Ordering::Relaxed)
    }

    /// Delay to wait before the next request, from a `Retry-After`-driven pause.
    pub(crate) fn remaining_pause(&self) -> Duration {
        let timing = self.lock();
        timing.not_before.saturating_duration_since(Instant::now())
    }

    /// Records a rate limit observed by an attempt that started at
    /// `attempt_started`. Halves the target once per change window and extends
    /// the shared pause by `delay`.
    pub(crate) fn on_rate_limited(&self, attempt_started: Instant, delay: Duration) {
        let now = Instant::now();
        let mut timing = self.lock();
        timing.not_before = timing.not_before.max(now + delay);
        self.decrease(attempt_started, now, &mut timing);
        self.successes.store(0, Ordering::Relaxed);
    }

    pub(crate) fn on_success(&self) {
        let streak = self.successes.fetch_add(1, Ordering::Relaxed) + 1;
        if streak >= self.grow_after as usize {
            self.successes.store(0, Ordering::Relaxed);
            let current = self.current();
            if current < self.max {
                self.target.store(current + 1, Ordering::Relaxed);
            }
        }
    }

    fn decrease(&self, attempt_started: Instant, now: Instant, timing: &mut Timing) {
        // Only one reduction per window, so a burst of failures from the same
        // batch does not collapse the target all the way to the floor.
        if timing
            .last_change
            .is_some_and(|last| attempt_started <= last)
        {
            return;
        }
        let current = self.current();
        if current <= self.min {
            return;
        }
        let next = current.div_ceil(2).max(self.min);
        self.target.store(next, Ordering::Relaxed);
        timing.last_change = Some(now);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Timing> {
        self.timing
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn gate_caps_in_flight() {
        let gate = Gate::new(2);
        let cancel = CancellationToken::new();
        let a = gate.acquire(&cancel).await.unwrap();
        let b = gate.acquire(&cancel).await.unwrap();
        let cancel2 = cancel.clone();
        let pending = tokio::spawn({
            let gate = gate.clone();
            async move { gate.acquire(&cancel2).await.is_some() }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!pending.is_finished());
        drop(a);
        assert!(pending.await.unwrap());
        drop(b);
    }

    #[test]
    fn adaptive_halves_then_grows() {
        let adaptive = Adaptive::new(1, 8, 8, 2);
        let start = Instant::now();
        adaptive.on_rate_limited(start, Duration::from_millis(0));
        assert_eq!(adaptive.current(), 4);
        // Same window: a second failure from an earlier attempt does not reduce again.
        adaptive.on_rate_limited(start, Duration::from_millis(0));
        assert_eq!(adaptive.current(), 4);
        adaptive.on_success();
        adaptive.on_success();
        assert_eq!(adaptive.current(), 5);
    }

    #[test]
    fn adaptive_respects_floor() {
        let adaptive = Adaptive::new(2, 2, 8, 4);
        adaptive.on_rate_limited(Instant::now(), Duration::from_millis(0));
        assert_eq!(adaptive.current(), 2);
    }
}
