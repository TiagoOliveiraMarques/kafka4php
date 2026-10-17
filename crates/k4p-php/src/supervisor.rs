//! The runtime thread: starts PHP, supervises the workers, shuts PHP down.
//!
//! Supervising is the hard-timeout watchdog (abandon a worker whose request is past
//! its deadline, answering `HardTimeout`), keeping `threads` live workers (replacing
//! recycled, abandoned and unusable ones), and joining exited threads. It runs here,
//! not in the execute futures, so a request times out and its capacity comes back
//! whether or not anyone still polls its future.

use std::mem;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Instant;

use k4p_core::ExecutionError;
use k4p_php_sys::{InitError, RuntimeConfig};

use crate::log;
use crate::state::{Busy, Phase, Shared, State, Worker, WorkerId, reject};
use crate::worker;

/// What [`PhpPool::shutdown`](crate::PhpPool::shutdown) did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Requests abandoned (answered `HardTimeout`) during shutdown: still running at
    /// the end of the grace period, or past their hard timeout before it.
    pub abandoned_at_shutdown: usize,
    /// Abandoned threads still running PHP at the end (including the above).
    pub abandoned_threads: usize,
    /// `false` if PHP was left running because abandoned threads still use it
    /// (the process is expected to exit), or if it failed to shut down.
    pub runtime_shut_down: bool,
}

/// Body of the runtime thread. Reports the init result on `init`, then supervises
/// until the pool has closed and every live worker exited.
pub(crate) fn run(
    shared: Arc<Shared>,
    config: RuntimeConfig,
    init: Sender<Result<(), InitError>>,
) -> ShutdownReport {
    // SAFETY: `PhpPool::start` lets this happen once per process. This thread outlives
    // every worker that may run PHP: it shuts PHP down only after all of them exited
    // (and not at all while an abandoned one may still run it). The callback is
    // thread-safe, never unwinds and ignores its (null) user data.
    let started =
        unsafe { k4p_php_sys::runtime_init(&config, Some(log::callback), std::ptr::null_mut()) };
    if let Err(e) = started {
        let _ = init.send(Err(e));
        return ShutdownReport::default();
    }
    {
        // Before reporting success, so the pool starts with its full capacity.
        let mut st = shared.lock();
        fill(&shared, &mut st);
        shared.publish_health(&st);
    }
    let _ = init.send(Ok(()));

    let mut report = supervise(&shared);
    if report.abandoned_threads == 0 {
        // SAFETY: every worker called `thread_shutdown` and was joined; none can be
        // spawned any more (the pool is closed).
        report.runtime_shut_down = unsafe { k4p_php_sys::runtime_shutdown() };
    } else {
        tracing::warn!(
            abandoned = report.abandoned_threads,
            "PHP runtime left running: abandoned threads may still use it"
        );
    }
    report
}

fn supervise(shared: &Arc<Shared>) -> ShutdownReport {
    let mut abandoned_at_shutdown = 0;
    let mut st = shared.lock();
    loop {
        let now = Instant::now();

        // Watchdog. While closing, deadlines are cut to the end of the grace period.
        let expired: Vec<WorkerId> = st
            .workers
            .iter()
            .filter(|(_, w)| w.busy.as_ref().is_some_and(|b| b.deadline <= now))
            .map(|(&id, _)| id)
            .collect();
        let timed_out: Vec<Busy> = expired.into_iter().map(|id| abandon(&mut st, id)).collect();
        if st.phase != Phase::Running {
            abandoned_at_shutdown += timed_out.len();
        }
        // Queued jobs left after the grace period. (Workers are then all busy, so the
        // watchdog wakes up at the latest at the end of the grace period.)
        if matches!(st.phase, Phase::Closing { deadline } if deadline <= now) {
            for job in st.queue.drain(..) {
                reject(job, "PHP pool shut down before the message ran");
            }
        }

        // Keep `threads` workers while there is (or may be) work for them.
        if st.phase == Phase::Running || !st.queue.is_empty() {
            fill(shared, &mut st);
        }
        shared.publish_health(&st);
        // Answered only now, so callers see the replacement and the new health.
        for busy in timed_out {
            let _ = busy.reply.send(Err(ExecutionError::HardTimeout));
        }

        let exited = mem::take(&mut st.exited);
        if !exited.is_empty() {
            drop(st);
            for handle in exited {
                let _ = handle.join(); // a panic was already reported by the panic hook
            }
            st = shared.lock();
            continue;
        }

        if st.phase != Phase::Running && st.workers.is_empty() {
            return ShutdownReport {
                abandoned_at_shutdown,
                abandoned_threads: st.abandoned.len(),
                runtime_shut_down: false,
            };
        }

        let wake_at = st
            .workers
            .values()
            .filter_map(|w| w.busy.as_ref().map(|b| b.deadline))
            .min();
        st = match wake_at {
            Some(at) => {
                let timeout = at.saturating_duration_since(now);
                shared
                    .supervisor
                    .wait_timeout(st, timeout)
                    .map_or_else(|e| e.into_inner().0, |r| r.0)
            }
            None => shared
                .supervisor
                .wait(st)
                .unwrap_or_else(|e| e.into_inner()),
        };
    }
}

/// Gives up on worker `id`: it cannot be killed, so it is left to finish (then it
/// exits) or hang. Returns its request, to be answered `HardTimeout`.
fn abandon(st: &mut State, id: WorkerId) -> Busy {
    let worker = st.workers.remove(&id).expect("expired worker is live");
    let busy = worker.busy.expect("expired worker is busy");
    st.abandoned.insert(id, worker.handle);
    st.abandoned_total += 1;
    let c = &busy.context;
    tracing::warn!(
        thread = id,
        topic = %c.topic,
        partition = c.partition,
        offset = c.offset,
        attempt = c.attempt,
        "PHP request exceeded the hard timeout; thread abandoned"
    );
    busy
}

/// Spawns workers until there are `threads` live ones (or the OS refuses).
fn fill(shared: &Arc<Shared>, st: &mut State) {
    while st.workers.len() < shared.settings.threads && spawn(shared, st) {}
}

/// Spawns a worker;`false` (and the pool unhealthy) if the OS refused.
fn spawn(shared: &Arc<Shared>, st: &mut State) -> bool {
    let id = st.next_id;
    st.next_id += 1;
    let worker_shared = Arc::clone(shared);
    // Inserted before the worker can take the lock, so it always finds itself.
    match thread::Builder::new()
        .name(format!("php-{id}"))
        .spawn(move || worker::run(worker_shared, id))
    {
        Ok(handle) => {
            st.workers.insert(id, Worker { handle, busy: None });
            true
        }
        Err(e) => {
            tracing::error!("could not spawn a PHP thread: {e}");
            st.spawn_failed = true;
            false
        }
    }
}
