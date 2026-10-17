//! State shared by the pool handle, the PHP worker threads and the supervisor.
//!
//! One mutex guards everything (job queue, workers, counters); it is held only for
//! short bookkeeping, never while PHP runs. Two condvars: `work` wakes idle workers
//! (job queued, pool closing), `supervisor` wakes the supervisor (request started,
//! thread exited, pool closing).

use std::collections::{HashMap, VecDeque};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use k4p_core::{ExecutionError, InboundMessage, Outcome};
use tokio::sync::{oneshot, watch};

use crate::PhpConfig;
use crate::log::MessageContext;

pub(crate) type WorkerId = u64;

/// What [`PhpPool::health`](crate::PhpPool::health) reports.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolHealth {
    /// PHP threads serving requests (excludes abandoned ones). Equals `capacity()`
    /// except briefly while a thread is replaced.
    pub live_threads: usize,
    /// Abandoned threads still stuck in their request.
    pub abandoned: usize,
    /// Threads abandoned since the pool started.
    pub abandoned_total: u64,
    /// Threads replaced after `max_requests_per_thread` requests.
    pub recycled_total: u64,
    /// `abandoned_total >= max_abandoned_threads`, or a replacement thread could not
    /// be spawned. Sticky: the process should restart (DESIGN.md §7.3).
    pub unhealthy: bool,
}

#[derive(Debug)]
pub(crate) struct Settings {
    pub threads: usize,
    pub max_requests_per_thread: u64,
    pub hard_timeout: Duration,
    pub max_abandoned_threads: u64,
}

#[derive(Debug)]
pub(crate) struct Job {
    pub msg: InboundMessage,
    pub reply: oneshot::Sender<Outcome>,
}

/// A request in progress on a live worker.
#[derive(Debug)]
pub(crate) struct Busy {
    pub deadline: Instant,
    pub reply: oneshot::Sender<Outcome>,
    pub context: MessageContext,
}

#[derive(Debug)]
pub(crate) struct Worker {
    pub handle: JoinHandle<()>,
    pub busy: Option<Busy>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Running,
    /// No new jobs; in-flight and queued ones may finish until `deadline`.
    Closing {
        deadline: Instant,
    },
}

#[derive(Debug)]
pub(crate) struct State {
    pub phase: Phase,
    pub queue: VecDeque<Job>,
    /// Live workers, inserted by the supervisor right after spawning.
    pub workers: HashMap<WorkerId, Worker>,
    /// Abandoned threads still running PHP (handle joined once they exit).
    pub abandoned: HashMap<WorkerId, JoinHandle<()>>,
    /// Threads that left PHP for good, to be joined by the supervisor.
    pub exited: Vec<JoinHandle<()>>,
    pub next_id: WorkerId,
    pub abandoned_total: u64,
    pub recycled_total: u64,
    pub spawn_failed: bool,
}

impl State {
    /// When a request starting now must end: after the hard timeout, or at the end of
    /// the grace period if the pool is closing. The watchdog enforces both alike.
    pub fn request_deadline(&self, hard_timeout: Duration) -> Instant {
        let deadline = Instant::now() + hard_timeout;
        match self.phase {
            Phase::Running => deadline,
            Phase::Closing { deadline: grace } => deadline.min(grace),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Shared {
    pub settings: Settings,
    state: Mutex<State>,
    pub work: Condvar,
    pub supervisor: Condvar,
    health: watch::Sender<PoolHealth>,
}

impl Shared {
    pub fn new(config: &PhpConfig) -> Self {
        Self {
            settings: Settings {
                threads: config.threads.get(),
                max_requests_per_thread: config.max_requests_per_thread.get(),
                hard_timeout: config.hard_timeout,
                max_abandoned_threads: config.max_abandoned_threads.get() as u64,
            },
            state: Mutex::new(State {
                phase: Phase::Running,
                queue: VecDeque::new(),
                workers: HashMap::new(),
                abandoned: HashMap::new(),
                exited: Vec::new(),
                next_id: 1,
                abandoned_total: 0,
                recycled_total: 0,
                spawn_failed: false,
            }),
            work: Condvar::new(),
            supervisor: Condvar::new(),
            health: watch::Sender::new(PoolHealth::default()),
        }
    }

    /// Every update under the lock is a few non-panicking steps, so a poisoned lock
    /// (a panic elsewhere on that thread) still guards consistent state.
    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn health(&self) -> PoolHealth {
        self.health.borrow().clone()
    }

    pub fn health_watch(&self) -> watch::Receiver<PoolHealth> {
        self.health.subscribe()
    }

    /// Recomputes the health from `st`; call after changing workers or counters.
    pub fn publish_health(&self, st: &State) {
        let health = PoolHealth {
            live_threads: st.workers.len(),
            abandoned: st.abandoned.len(),
            abandoned_total: st.abandoned_total,
            recycled_total: st.recycled_total,
            unhealthy: st.spawn_failed || st.abandoned_total >= self.settings.max_abandoned_threads,
        };
        self.health.send_if_modified(|h| {
            let changed = *h != health;
            *h = health;
            changed
        });
    }

    // -- pool handle side ----------------------------------------------------------

    /// Queues `msg`; `None` once the pool is closing.
    pub fn submit(&self, msg: InboundMessage) -> Option<oneshot::Receiver<Outcome>> {
        let mut st = self.lock();
        if st.phase != Phase::Running {
            return None;
        }
        let (reply, rx) = oneshot::channel();
        st.queue.push_back(Job { msg, reply });
        self.work.notify_one();
        Some(rx)
    }

    /// Stops accepting jobs; what is queued or in flight may finish within `grace`.
    /// In-flight requests get the end of the grace period as their deadline.
    pub fn close(&self, grace: Duration) {
        let mut st = self.lock();
        if st.phase == Phase::Running {
            let deadline = Instant::now() + grace;
            st.phase = Phase::Closing { deadline };
            for busy in st.workers.values_mut().filter_map(|w| w.busy.as_mut()) {
                busy.deadline = busy.deadline.min(deadline);
            }
        }
        self.work.notify_all();
        self.supervisor.notify_one();
    }

    // -- worker side ---------------------------------------------------------------

    /// Waits for the next job and marks worker `id` busy with it (starting its hard
    /// timeout). `None` when the pool is closing and the queue is empty.
    pub fn next_job(&self, id: WorkerId) -> Option<InboundMessage> {
        let mut st = self.lock();
        loop {
            if let Some(job) = st.queue.pop_front() {
                if job.reply.is_closed() {
                    continue; // the caller dropped the execute future
                }
                let busy = Busy {
                    deadline: st.request_deadline(self.settings.hard_timeout),
                    reply: job.reply,
                    context: MessageContext::of(&job.msg),
                };
                st.workers
                    .get_mut(&id)
                    .expect("only live workers wait")
                    .busy = Some(busy);
                self.supervisor.notify_one();
                return Some(job.msg);
            }
            if st.phase != Phase::Running {
                return None;
            }
            st = self.work.wait(st).unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Delivers worker `id`'s outcome. `false` if the worker was abandoned meanwhile
    /// (the caller already got `HardTimeout`; the outcome is dropped).
    pub fn finish_job(&self, id: WorkerId, outcome: Outcome) -> bool {
        let busy = self.lock().workers.get_mut(&id).and_then(|w| w.busy.take());
        match busy {
            Some(busy) => {
                let _ = busy.reply.send(outcome); // receiver gone: nobody waits
                true
            }
            None => false,
        }
    }

    /// Worker `id` left PHP for good (after `thread_shutdown`); the supervisor joins
    /// it and, while the pool runs, replaces it.
    pub fn worker_exited(&self, id: WorkerId, recycled: bool) {
        let mut st = self.lock();
        let handle = match st.workers.remove(&id) {
            Some(w) => Some(w.handle),
            None => st.abandoned.remove(&id),
        };
        st.exited.extend(handle);
        if recycled {
            st.recycled_total += 1;
        }
        self.publish_health(&st);
        self.supervisor.notify_one();
    }
}

/// Answers a job that will never run.
pub(crate) fn reject(job: Job, why: &str) {
    let _ = job
        .reply
        .send(Err(ExecutionError::Bootstrap(why.to_owned())));
}
