//! A PHP worker thread: `thread_init`, requests from the shared queue one at a time,
//! `thread_shutdown`, exit. It leaves after its drawn request limit
//! (recycling), after its request was abandoned by the watchdog, after a request
//! could not start (the thread is unusable), or when the pool closes.

use std::sync::Arc;

use k4p_core::ExecutionError;

use crate::log;
use crate::state::{Shared, WorkerId};

pub(crate) fn run(shared: Arc<Shared>, id: WorkerId) {
    // Reports the exit even if this thread panics, so the supervisor never waits on it.
    let mut exit = ExitGuard {
        shared: &shared,
        id,
        recycled: false,
    };
    if !k4p_php_sys::thread_init() {
        // Workers only exist between runtime init and shutdown.
        tracing::error!(thread = id, "PHP thread init failed: runtime not running");
        return;
    }
    let limit = shared.settings.draw_request_limit();
    let mut served = 0;
    while let Some(msg) = shared.next_job(id) {
        let outcome = log::with_context(&msg, || k4p_php_sys::execute(&msg));
        served += 1;
        let unusable = match &outcome {
            Err(ExecutionError::Bootstrap(why)) => Some(why.clone()),
            _ => None,
        };
        if !shared.finish_job(id, outcome) {
            tracing::warn!(
                thread = id,
                "abandoned PHP thread finished its request; exiting"
            );
            break;
        }
        if let Some(why) = unusable {
            tracing::warn!(thread = id, "PHP thread unusable ({why}); replacing it");
            break;
        }
        if served >= limit {
            exit.recycled = true;
            break;
        }
    }
    k4p_php_sys::thread_shutdown();
}

struct ExitGuard<'a> {
    shared: &'a Shared,
    id: WorkerId,
    recycled: bool,
}

impl Drop for ExitGuard<'_> {
    fn drop(&mut self) {
        self.shared.worker_exited(self.id, self.recycled);
    }
}
