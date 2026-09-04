//! Download orchestration: probe, piece scheduling, committer, finalize.

use std::collections::VecDeque;
use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use reqwest::header::{ETAG, LAST_MODIFIED, RANGE};
use reqwest::{Client, StatusCode};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::control::{ControlFile, control_path, piece_len};
use crate::error::{DownloadError, map_reqwest};
use crate::fsx;
use crate::http;
use crate::limiter::{Adaptive, Gate};
use crate::progress::{Phase, Progress, Shared};
use crate::types::{DownloadOptions, DownloadRequest, Outcome};

const BASE_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Downloads `request.url` into `request.target`, resuming from the control
/// file when one is present. See the crate documentation for the contract.
///
/// # Errors
///
/// Returns a [`DownloadError`] describing the first unrecoverable failure.
/// Cancellation is reported as `Ok(Outcome::Cancelled)`, never as an error.
pub async fn download(
    request: DownloadRequest,
    options: DownloadOptions,
    client: Client,
    progress: watch::Sender<Progress>,
    cancel: CancellationToken,
) -> Result<Outcome, DownloadError> {
    options.validate()?;
    let control_file_path = control_path(&request.target);

    // A target without a control file is either an already-finished download
    // or a foreign file we must not touch. A corrupt control file still counts
    // as "a download was in progress": start that download over.
    let control_file_exists = control_file_path.exists();
    let existing_control = match ControlFile::load(&control_file_path) {
        Ok(existing) => existing,
        Err(error) => {
            log::warn!("discarding unreadable control file: {error}");
            None
        }
    };
    if !control_file_exists {
        if let Ok(metadata) = std::fs::metadata(&request.target) {
            if metadata.len() == request.expected_size {
                return Ok(Outcome::Completed);
            }
            return Err(DownloadError::TargetConflict(request.target.clone()));
        }
    }

    let shared = Arc::new(Shared::new());
    shared.set_total(request.expected_size);
    let emitter = spawn_emitter(
        Arc::clone(&shared),
        progress.clone(),
        options.progress_interval,
    );
    let result = run(
        &request,
        &options,
        &client,
        Arc::clone(&shared),
        existing_control,
        &control_file_path,
        &cancel,
    )
    .await;

    match &result {
        Ok(Outcome::Completed) => shared.set_phase(Phase::Done),
        Ok(Outcome::Cancelled) => shared.set_phase(Phase::Cancelled),
        Err(_) => {}
    }
    emitter.abort();
    let _ = progress.send(shared.snapshot(0.0));
    result
}

#[allow(clippy::too_many_lines)]
async fn run(
    request: &DownloadRequest,
    options: &DownloadOptions,
    client: &Client,
    shared: Arc<Shared>,
    existing_control: Option<ControlFile>,
    control_file_path: &std::path::Path,
    cancel: &CancellationToken,
) -> Result<Outcome, DownloadError> {
    // Probe.
    shared.set_phase(Phase::Probing);
    let probe = tokio::select! {
        biased;
        () = cancel.cancelled() => return Ok(Outcome::Cancelled),
        probe = probe_with_retry(client, &request.url, options, &shared) => probe?,
    };
    if let Some(total) = probe.total {
        if total != request.expected_size {
            return Err(DownloadError::SizeMismatch {
                expected: request.expected_size,
                actual: total,
            });
        }
    }
    let size = request.expected_size;

    // Reconcile any previous state with what the server now reports.
    let mut control = reconcile(existing_control, request, &probe, options.piece_size, size);
    if !probe.range_supported {
        // A server that ignores ranges cannot resume a partial file.
        control.pieces.iter_mut().for_each(|written| *written = 0);
    }

    // Open the target and persist the control file before the first byte, so
    // "target present, control absent" always means a finished download.
    let target_path = request.target.clone();
    let file = tokio::task::spawn_blocking(move || fsx::open_target(&target_path, size, true))
        .await
        .map_err(join_error)??;
    let file = Arc::new(file);
    control.save(control_file_path)?;

    let pieces: Arc<Vec<AtomicU64>> = Arc::new(
        control
            .pieces
            .iter()
            .map(|written| AtomicU64::new(*written))
            .collect(),
    );
    let already = control.written_total();
    shared.written.store(already, Ordering::Relaxed);
    shared.committed.store(already, Ordering::Relaxed);

    if size == 0 || control.is_complete() {
        return finalize(&shared, &file, control_file_path)
            .await
            .map(|()| Outcome::Completed);
    }

    let committer = Committer {
        file: Arc::clone(&file),
        pieces: Arc::clone(&pieces),
        template: control,
        path: control_file_path.to_path_buf(),
        shared: Arc::clone(&shared),
    };
    let committer_task =
        spawn_committer(committer.clone(), options.commit_interval, cancel.clone());

    let outcome = if probe.range_supported {
        shared.set_phase(Phase::Downloading);
        run_segmented(
            request, options, client, &shared, &pieces, &file, size, cancel,
        )
        .await
    } else {
        shared.set_phase(Phase::SingleStream);
        run_single_stream(
            request, options, client, &shared, &pieces, &file, size, cancel,
        )
        .await
    };

    // Stop the periodic committer, then always take one final durable commit so
    // pause/cancel/errors never lose more than the page cache.
    committer_task.abort();
    let _ = committer_task.await;
    let commit_result = committer.commit().await;

    match outcome {
        Ok(Outcome::Completed) => {
            commit_result?;
            finalize(&shared, &file, control_file_path).await?;
            Ok(Outcome::Completed)
        }
        Ok(Outcome::Cancelled) => {
            commit_result?;
            Ok(Outcome::Cancelled)
        }
        Err(error) => Err(error),
    }
}

// ---------------------------------------------------------------------------
// Probe

#[derive(Debug, Clone)]
struct ProbeResult {
    total: Option<u64>,
    range_supported: bool,
    etag: Option<String>,
    last_modified: Option<String>,
}

/// Retries the probe on retryable failures (429, 5xx, transport errors) with
/// the same backoff policy as piece requests.
async fn probe_with_retry(
    client: &Client,
    url: &str,
    options: &DownloadOptions,
    shared: &Arc<Shared>,
) -> Result<ProbeResult, DownloadError> {
    const PROBE_ATTEMPTS: u32 = 5;
    let mut attempt = 0u32;
    loop {
        match probe(client, url).await {
            Ok(result) => return Ok(result),
            Err(error) if error.is_retryable() && attempt + 1 < PROBE_ATTEMPTS => {
                attempt += 1;
                shared.retries.fetch_add(1, Ordering::Relaxed);
                log::debug!("probe attempt {attempt} failed, retrying: {error}");
                tokio::time::sleep(backoff(attempt).min(options.stall_timeout)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn probe(client: &Client, url: &str) -> Result<ProbeResult, DownloadError> {
    let response = client
        .get(url)
        .header(RANGE, "bytes=0-0")
        .send()
        .await
        .map_err(|error| map_reqwest(&error))?;
    let status = response.status();
    let headers = response.headers().clone();
    let etag = http::header_string(&headers, ETAG);
    let last_modified = http::header_string(&headers, LAST_MODIFIED);

    if status == StatusCode::PARTIAL_CONTENT {
        http::ensure_identity(&headers)?;
        let range = http::content_range(&headers)?
            .ok_or_else(|| DownloadError::Protocol("206 without Content-Range".into()))?;
        return Ok(ProbeResult {
            total: range.total,
            range_supported: true,
            etag,
            last_modified,
        });
    }
    if status == StatusCode::OK {
        // Server ignored the range: fall back to a single stream.
        http::ensure_identity(&headers)?;
        let total = http::content_length(&headers)?;
        return Ok(ProbeResult {
            total,
            range_supported: false,
            etag,
            last_modified,
        });
    }
    if status == StatusCode::RANGE_NOT_SATISFIABLE {
        let value = headers
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| DownloadError::Protocol("416 without Content-Range".into()))?;
        let total = http::parse_unsatisfied_total(value)?;
        return Ok(ProbeResult {
            total: Some(total),
            range_supported: true,
            etag,
            last_modified,
        });
    }
    Err(http::status_error(status))
}

fn reconcile(
    existing: Option<ControlFile>,
    request: &DownloadRequest,
    probe: &ProbeResult,
    piece_size: u64,
    size: u64,
) -> ControlFile {
    let fresh = || {
        ControlFile::new(
            size,
            piece_size,
            request.identity.clone(),
            probe.etag.clone(),
            probe.last_modified.clone(),
        )
    };
    let Some(control) = existing else {
        return fresh();
    };
    if control.size != size || control.piece_size != piece_size {
        log::info!("discarding control file: geometry changed");
        return fresh();
    }
    if let (Some(ours), Some(theirs)) = (&request.identity, &control.identity) {
        if ours == theirs {
            // Same declared content: validator drift (CDN edges disagreeing on
            // ETag) is tolerated because the caller verifies the final hash.
            return control;
        }
        log::info!("discarding control file: content identity changed");
        return fresh();
    }
    // No checksum to fall back on: validators are the only safety net.
    let etag_matches = match (&control.etag, &probe.etag) {
        (Some(stored), Some(current)) => stored == current,
        _ => true,
    };
    let modified_matches = match (&control.last_modified, &probe.last_modified) {
        (Some(stored), Some(current)) => stored == current,
        _ => true,
    };
    if etag_matches && modified_matches {
        control
    } else {
        log::info!("discarding control file: remote validators changed");
        fresh()
    }
}

// ---------------------------------------------------------------------------
// Segmented mode

struct WorkerReport {
    index: u64,
    attempt: u32,
    result: Result<(), WorkerFail>,
}

struct WorkerFail {
    error: DownloadError,
    rate_limited: bool,
    retry_after: Option<Duration>,
    attempt_started: Instant,
}

#[allow(clippy::too_many_arguments)]
async fn run_segmented(
    request: &DownloadRequest,
    options: &DownloadOptions,
    client: &Client,
    shared: &Arc<Shared>,
    pieces: &Arc<Vec<AtomicU64>>,
    file: &Arc<File>,
    size: u64,
    cancel: &CancellationToken,
) -> Result<Outcome, DownloadError> {
    let adaptive = Arc::new(Adaptive::new(
        options.min_connections,
        options.clamped_initial(),
        options.max_connections,
        options.grow_after_successes,
    ));
    let budget: Option<Gate> = options.budget.as_ref().map(|budget| budget.0.clone());
    let mut pending: VecDeque<(u64, u32)> = (0..pieces.len() as u64)
        .filter(|index| {
            pieces[usize::try_from(*index).expect("piece index fits usize")].load(Ordering::Relaxed)
                < piece_len(size, options.piece_size, *index)
        })
        .map(|index| (index, 0))
        .collect();
    let mut tasks: JoinSet<WorkerReport> = JoinSet::new();
    let mut consecutive_failures = 0u32;

    let outcome = loop {
        if cancel.is_cancelled() {
            break Ok(Outcome::Cancelled);
        }
        while tasks.len() < adaptive.current() {
            let Some((index, attempt)) = pending.pop_front() else {
                break;
            };
            tasks.spawn(piece_worker(
                client.clone(),
                request.url.clone(),
                Arc::clone(file),
                Arc::clone(pieces),
                Arc::clone(shared),
                Arc::clone(&adaptive),
                budget.clone(),
                cancel.clone(),
                options.piece_size,
                size,
                index,
                attempt,
                options.stall_timeout,
            ));
        }
        shared.connections.store(tasks.len(), Ordering::Relaxed);
        if tasks.is_empty() {
            if pending.is_empty() {
                break Ok(Outcome::Completed);
            }
            // Target shrank below queued work between iterations; yield briefly.
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }

        let joined = tokio::select! {
            biased;
            () = cancel.cancelled() => break Ok(Outcome::Cancelled),
            joined = tasks.join_next() => joined,
        };
        let Some(joined) = joined else { continue };
        let report = joined.map_err(join_error)?;
        match report.result {
            Ok(()) => {
                adaptive.on_success();
                consecutive_failures = 0;
            }
            Err(fail) => {
                if !fail.error.is_retryable() {
                    break Err(fail.error);
                }
                shared.retries.fetch_add(1, Ordering::Relaxed);
                consecutive_failures += 1;
                if consecutive_failures > options.max_consecutive_failures {
                    break Err(DownloadError::RetriesExhausted {
                        attempts: consecutive_failures,
                        last: Box::new(fail.error),
                    });
                }
                if fail.rate_limited {
                    adaptive.on_rate_limited(
                        fail.attempt_started,
                        fail.retry_after.unwrap_or(BASE_BACKOFF),
                    );
                } else {
                    log::debug!(
                        "piece {} attempt {} failed, requeueing: {}",
                        report.index,
                        report.attempt + 1,
                        fail.error
                    );
                }
                pending.push_back((report.index, report.attempt + 1));
            }
        }
    };

    tasks.shutdown().await;
    outcome
}

#[allow(clippy::too_many_arguments)]
async fn piece_worker(
    client: Client,
    url: String,
    file: Arc<File>,
    pieces: Arc<Vec<AtomicU64>>,
    shared: Arc<Shared>,
    adaptive: Arc<Adaptive>,
    budget: Option<Gate>,
    cancel: CancellationToken,
    piece_size: u64,
    size: u64,
    index: u64,
    attempt: u32,
    stall_timeout: Duration,
) -> WorkerReport {
    let report = |result| WorkerReport {
        index,
        attempt,
        result,
    };
    let cancelled = || {
        report(Err(WorkerFail {
            error: DownloadError::Network("cancelled".into()),
            rate_limited: false,
            retry_after: None,
            attempt_started: Instant::now(),
        }))
    };

    // Backoff for retries, then any shared rate-limit pause, then the budget.
    let delay = backoff(attempt).max(adaptive.remaining_pause());
    if !delay.is_zero() {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return cancelled(),
            () = tokio::time::sleep(delay) => {}
        }
    }
    let _permit = match &budget {
        Some(gate) => match gate.acquire(&cancel).await {
            Some(permit) => Some(permit),
            None => return cancelled(),
        },
        None => None,
    };

    let attempt_started = Instant::now();
    let result = fetch_piece(
        &client,
        &url,
        &file,
        &pieces,
        &shared,
        &cancel,
        piece_size,
        size,
        index,
        stall_timeout,
        attempt_started,
    )
    .await;
    report(result)
}

#[allow(clippy::too_many_arguments)]
async fn fetch_piece(
    client: &Client,
    url: &str,
    file: &Arc<File>,
    pieces: &Arc<Vec<AtomicU64>>,
    shared: &Arc<Shared>,
    cancel: &CancellationToken,
    piece_size: u64,
    size: u64,
    index: u64,
    stall_timeout: Duration,
    attempt_started: Instant,
) -> Result<(), WorkerFail> {
    let fail = |error: DownloadError, rate_limited, retry_after| WorkerFail {
        error,
        rate_limited,
        retry_after,
        attempt_started,
    };
    let plain = |error: DownloadError| fail(error, false, None);

    let slot = &pieces[usize::try_from(index).expect("piece index fits usize")];
    let piece_start = index * piece_size;
    let len = piece_len(size, piece_size, index);
    let already = slot.load(Ordering::Relaxed);
    if already >= len {
        return Ok(());
    }
    let range_start = piece_start + already;
    let range_end = piece_start + len - 1;

    let response = client
        .get(url)
        .header(RANGE, format!("bytes={range_start}-{range_end}"))
        .send()
        .await
        .map_err(|error| plain(map_reqwest(&error)))?;
    let status = response.status();
    if status != StatusCode::PARTIAL_CONTENT {
        let retry_after = http::retry_after(response.headers(), SystemTime::now());
        let error = if status == StatusCode::OK {
            DownloadError::Protocol("server stopped honoring range requests".into())
        } else {
            http::status_error(status)
        };
        let rate_limited = status == StatusCode::TOO_MANY_REQUESTS;
        return Err(fail(error, rate_limited, retry_after));
    }
    http::ensure_identity(response.headers()).map_err(&plain)?;
    let range = http::content_range(response.headers())
        .map_err(&plain)?
        .ok_or_else(|| plain(DownloadError::Protocol("206 without Content-Range".into())))?;
    if range.start != range_start || range.end != range_end {
        return Err(plain(DownloadError::Protocol(format!(
            "Content-Range mismatch: asked {range_start}-{range_end}, got {}-{}",
            range.start, range.end
        ))));
    }
    if let Some(length) = http::content_length(response.headers()).map_err(&plain)? {
        if length != len - already {
            return Err(plain(DownloadError::Protocol(format!(
                "Content-Length mismatch for piece {index}: expected {}, got {length}",
                len - already
            ))));
        }
    }

    let mut response = response;
    let mut written = already;
    loop {
        if cancel.is_cancelled() {
            // Report as retryable network interruption; the outer loop sees the
            // token and stops before requeueing.
            return Err(plain(DownloadError::Network("cancelled".into())));
        }
        let chunk = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(plain(DownloadError::Network("cancelled".into()))),
            chunk = tokio::time::timeout(stall_timeout, response.chunk()) => match chunk {
                Err(_) => {
                    return Err(plain(DownloadError::Network(format!(
                        "no data for {} s on piece {index}",
                        stall_timeout.as_secs()
                    ))));
                }
                Ok(result) => result.map_err(|error| plain(map_reqwest(&error)))?,
            },
        };
        let Some(chunk) = chunk else { break };
        if chunk.is_empty() {
            continue;
        }
        let chunk_len = chunk.len() as u64;
        if written + chunk_len > len {
            return Err(plain(DownloadError::Protocol(format!(
                "piece {index} body exceeded {len} bytes"
            ))));
        }
        let offset = piece_start + written;
        let write_file = Arc::clone(file);
        tokio::task::spawn_blocking(move || fsx::write_all_at(&write_file, offset, &chunk))
            .await
            .map_err(|error| plain(join_error(error)))?
            .map_err(|error| plain(DownloadError::Disk(error)))?;
        written += chunk_len;
        slot.store(written, Ordering::Relaxed);
        shared.written.fetch_add(chunk_len, Ordering::Relaxed);
    }
    if written != len {
        return Err(plain(DownloadError::Network(format!(
            "piece {index} ended early: {written} of {len} bytes"
        ))));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Single-stream fallback

#[allow(clippy::too_many_arguments)]
async fn run_single_stream(
    request: &DownloadRequest,
    options: &DownloadOptions,
    client: &Client,
    shared: &Arc<Shared>,
    pieces: &Arc<Vec<AtomicU64>>,
    file: &Arc<File>,
    size: u64,
    cancel: &CancellationToken,
) -> Result<Outcome, DownloadError> {
    shared.connections.store(1, Ordering::Relaxed);
    let mut attempt = 0u32;
    loop {
        if cancel.is_cancelled() {
            return Ok(Outcome::Cancelled);
        }
        if attempt > 0 {
            let delay = backoff(attempt);
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(Outcome::Cancelled),
                () = tokio::time::sleep(delay) => {}
            }
        }
        match stream_once(request, options, client, shared, pieces, file, size, cancel).await {
            Ok(true) => return Ok(Outcome::Completed),
            Ok(false) => return Ok(Outcome::Cancelled),
            Err(error) if error.is_retryable() => {
                shared.retries.fetch_add(1, Ordering::Relaxed);
                attempt += 1;
                if attempt > options.max_consecutive_failures {
                    return Err(DownloadError::RetriesExhausted {
                        attempts: attempt,
                        last: Box::new(error),
                    });
                }
                // Restart from zero: the server does not honor ranges.
                for slot in pieces.iter() {
                    slot.store(0, Ordering::Relaxed);
                }
                shared.written.store(0, Ordering::Relaxed);
                log::warn!("single-stream download restarting (attempt {attempt}): {error}");
            }
            Err(error) => return Err(error),
        }
    }
}

/// One full pass. `Ok(true)` = complete, `Ok(false)` = cancelled.
#[allow(clippy::too_many_arguments)]
async fn stream_once(
    request: &DownloadRequest,
    options: &DownloadOptions,
    client: &Client,
    shared: &Arc<Shared>,
    pieces: &Arc<Vec<AtomicU64>>,
    file: &Arc<File>,
    size: u64,
    cancel: &CancellationToken,
) -> Result<bool, DownloadError> {
    let response = client
        .get(&request.url)
        .send()
        .await
        .map_err(|error| map_reqwest(&error))?;
    let status = response.status();
    if status != StatusCode::OK {
        return Err(http::status_error(status));
    }
    http::ensure_identity(response.headers())?;
    if let Some(length) = http::content_length(response.headers())? {
        if length != size {
            return Err(DownloadError::SizeMismatch {
                expected: size,
                actual: length,
            });
        }
    }

    let mut response = response;
    let mut absolute = 0u64;
    loop {
        let chunk = tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(false),
            chunk = tokio::time::timeout(options.stall_timeout, response.chunk()) => match chunk {
                Err(_) => return Err(DownloadError::Network("single stream stalled".into())),
                Ok(result) => result.map_err(|error| map_reqwest(&error))?,
            },
        };
        let Some(chunk) = chunk else { break };
        if chunk.is_empty() {
            continue;
        }
        let chunk_len = chunk.len() as u64;
        if absolute + chunk_len > size {
            return Err(DownloadError::Protocol(
                "body exceeded expected size".into(),
            ));
        }
        let offset = absolute;
        let write_file = Arc::clone(file);
        tokio::task::spawn_blocking(move || fsx::write_all_at(&write_file, offset, &chunk))
            .await
            .map_err(join_error)?
            .map_err(DownloadError::Disk)?;
        absolute += chunk_len;
        shared.written.store(absolute, Ordering::Relaxed);
        // Contiguous progress: fill every fully or partially covered piece.
        let mut cursor = absolute;
        for (index, slot) in pieces.iter().enumerate() {
            let len = piece_len(size, options.piece_size, index as u64);
            let value = cursor.min(len);
            slot.store(value, Ordering::Relaxed);
            cursor -= value;
            if cursor == 0 {
                break;
            }
        }
    }
    if absolute != size {
        return Err(DownloadError::Network(format!(
            "single stream ended early: {absolute} of {size} bytes"
        )));
    }
    Ok(true)
}

// ---------------------------------------------------------------------------
// Committer, finalize, emitter

#[derive(Clone)]
struct Committer {
    file: Arc<File>,
    pieces: Arc<Vec<AtomicU64>>,
    template: ControlFile,
    path: std::path::PathBuf,
    shared: Arc<Shared>,
}

impl Committer {
    /// Snapshot piece counters, make the data durable, then record the
    /// snapshot. Ordering guarantees the control file never over-claims.
    async fn commit(&self) -> Result<(), DownloadError> {
        let snapshot: Vec<u64> = self
            .pieces
            .iter()
            .map(|slot| slot.load(Ordering::Relaxed))
            .collect();
        let total: u64 = snapshot.iter().sum();
        let mut control = self.template.clone();
        control.pieces = snapshot;
        let file = Arc::clone(&self.file);
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            file.sync_data()?;
            control.save(&path)
        })
        .await
        .map_err(join_error)?
        .map_err(DownloadError::Disk)?;
        self.shared.committed.store(total, Ordering::Relaxed);
        Ok(())
    }
}

fn spawn_committer(
    committer: Committer,
    interval: Duration,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(100)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // first tick is immediate; skip it
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    if let Err(error) = committer.commit().await {
                        log::warn!("periodic commit failed: {error}");
                    }
                }
            }
        }
    })
}

async fn finalize(
    shared: &Arc<Shared>,
    file: &Arc<File>,
    control_file_path: &std::path::Path,
) -> Result<(), DownloadError> {
    shared.set_phase(Phase::Finalizing);
    let file = Arc::clone(file);
    tokio::task::spawn_blocking(move || file.sync_all())
        .await
        .map_err(join_error)?
        .map_err(DownloadError::Disk)?;
    fsx::remove_if_exists(control_file_path)?;
    Ok(())
}

fn spawn_emitter(
    shared: Arc<Shared>,
    sender: watch::Sender<Progress>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(50)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut previous_written = shared.written.load(Ordering::Relaxed);
        let mut previous_at = Instant::now();
        let mut smoothed = 0.0f64;
        loop {
            ticker.tick().await;
            let written = shared.written.load(Ordering::Relaxed);
            let now = Instant::now();
            let elapsed = now.saturating_duration_since(previous_at).as_secs_f64();
            if elapsed > 0.0 {
                let instant_speed = written.saturating_sub(previous_written) as f64 / elapsed;
                smoothed = if smoothed == 0.0 {
                    instant_speed
                } else {
                    0.3 * instant_speed + 0.7 * smoothed
                };
            }
            previous_written = written;
            previous_at = now;
            let _ = sender.send(shared.snapshot(smoothed));
        }
    })
}

// ---------------------------------------------------------------------------
// Helpers

fn backoff(attempt: u32) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }
    let exponent = attempt.saturating_sub(1).min(6);
    let base = BASE_BACKOFF.saturating_mul(1 << exponent).min(MAX_BACKOFF);
    // Cheap jitter without a rand dependency: sub-millisecond clock noise.
    let jitter_ms = u64::from(
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|value| value.subsec_nanos())
            .unwrap_or(0))
            % 250,
    );
    base + Duration::from_millis(jitter_ms)
}

fn join_error(error: tokio::task::JoinError) -> DownloadError {
    DownloadError::Disk(std::io::Error::other(format!(
        "worker task failed: {error}"
    )))
}
