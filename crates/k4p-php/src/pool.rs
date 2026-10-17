//! [`PhpPool`]: the public handle, implementing [`Executor`].

use std::panic::resume_unwind;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use k4p_core::{BoxFuture, ExecutionError, Executor, InboundMessage, Outcome};
use tokio::sync::watch;

use crate::state::{PoolHealth, Shared};
use crate::supervisor::{self, ShutdownReport};
use crate::{LogSink, PhpConfig, StartError, log};

/// PHP starts once per process.
static STARTED: AtomicBool = AtomicBool::new(false);

/// The PHP runtime and its fixed pool of PHP threads (DESIGN.md §5.5).
///
/// `execute` queues the message; the next free PHP thread runs it. Calls beyond
/// [`capacity`](Executor::capacity) wait in the queue (FIFO) — the hard timeout starts
/// when a thread picks the message up. Dropping an execute future does not stop a
/// running request; a message whose future was dropped before it started is skipped.
///
/// Dropping the pool does not stop it: call [`shutdown`](Self::shutdown).
#[derive(Debug)]
pub struct PhpPool {
    shared: Arc<Shared>,
    runtime: Mutex<Runtime>,
}

/// The runtime thread, until `shutdown` joined it.
#[derive(Debug)]
enum Runtime {
    Running(JoinHandle<ShutdownReport>),
    Stopped(ShutdownReport),
}

impl PhpPool {
    /// Starts PHP on a dedicated runtime thread and spawns `config.threads` PHP
    /// threads. Can succeed once per process; `sink` receives all PHP output.
    pub fn start(config: PhpConfig, sink: impl LogSink) -> Result<Self, StartError> {
        config.validate()?;
        if STARTED.swap(true, Ordering::SeqCst) {
            return Err(StartError::AlreadyStarted);
        }
        log::install(Box::new(sink));

        let shared = Arc::new(Shared::new(&config));
        let (init_tx, init_rx) = mpsc::channel();
        let runtime_config = config.runtime();
        let supervisor_shared = Arc::clone(&shared);
        let runtime = thread::Builder::new()
            .name("php-runtime".into())
            .spawn(move || supervisor::run(supervisor_shared, runtime_config, init_tx))
            .map_err(StartError::Spawn)?;
        match init_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                shared,
                runtime: Mutex::new(Runtime::Running(runtime)),
            }),
            Ok(Err(e)) => {
                let _ = runtime.join();
                Err(StartError::Init(e))
            }
            // The runtime thread panicked before reporting.
            Err(_) => resume_unwind(runtime.join().expect_err("no init result")),
        }
    }

    /// Current health.
    pub fn health(&self) -> PoolHealth {
        self.shared.health()
    }

    /// Notified on every health change, e.g. to await `unhealthy`
    /// (`rx.wait_for(|h| h.unhealthy)`).
    pub fn health_watch(&self) -> watch::Receiver<PoolHealth> {
        self.shared.health_watch()
    }

    /// Graceful shutdown; blocks (use `spawn_blocking` from async code). Stops
    /// accepting messages (`execute` then answers `Bootstrap`), lets queued and
    /// in-flight requests finish within `grace`, abandons what is still running after
    /// it (`HardTimeout`), joins the threads and shuts PHP down — unless an abandoned
    /// thread may still be running PHP; then PHP is left as is for the exiting process.
    /// Later calls return the first call's report.
    pub fn shutdown(&self, grace: Duration) -> ShutdownReport {
        let mut runtime = self.runtime.lock().unwrap_or_else(PoisonError::into_inner);
        let report =
            match std::mem::replace(&mut *runtime, Runtime::Stopped(ShutdownReport::default())) {
                Runtime::Stopped(report) => report,
                Runtime::Running(handle) => {
                    self.shared.close(grace);
                    handle.join().unwrap_or_else(|panic| resume_unwind(panic))
                }
            };
        *runtime = Runtime::Stopped(report.clone());
        report
    }
}

impl Executor for PhpPool {
    fn execute(&self, msg: InboundMessage) -> BoxFuture<'static, Outcome> {
        let reply = self.shared.submit(msg);
        Box::pin(async move {
            let Some(reply) = reply else {
                return Err(ExecutionError::Bootstrap("PHP pool is shut down".into()));
            };
            reply.await.unwrap_or_else(|_| {
                Err(ExecutionError::Bootstrap(
                    "PHP thread exited without a result".into(),
                ))
            })
        })
    }

    fn capacity(&self) -> usize {
        self.shared.settings.threads
    }
}
