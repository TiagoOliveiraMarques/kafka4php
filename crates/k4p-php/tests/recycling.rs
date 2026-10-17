//! Recycling (PLAN.md S3): 4 threads replaced after 9 or 10 requests each (a limit of
//! 10 with jitter); 1,000 requests all succeed and the number of replacements matches.

mod common;

use std::time::Duration;

use common::{Case, content_value, execute_all, msg, wait_until};
use k4p_php::{PhpPool, ShutdownReport};

const THREADS: usize = 4;
const MAX_REQUESTS: u64 = 10;
/// The lowest limit a thread may draw (`max - max/10`).
const MIN_REQUESTS: u64 = 9;
const REQUESTS: u64 = 1_000;

fn recycles_every_max_requests(pool: &PhpPool) {
    let msgs = (0..REQUESTS as i64)
        .map(|i| msg("echo", i, i.to_string()))
        .collect();
    let outcomes = execute_all(pool, msgs);
    for (i, (o, _)) in outcomes.iter().enumerate() {
        assert_eq!(content_value(o), i.to_string().as_bytes(), "request {i}");
    }

    // Every finished thread served its limit, MIN_REQUESTS..=MAX_REQUESTS; the live
    // ones fewer than theirs (< MAX_REQUESTS). So, with r = recycled:
    //   MIN_REQUESTS * r <= REQUESTS < MAX_REQUESTS * r + THREADS * MAX_REQUESTS.
    let settled = wait_until(Duration::from_secs(5), || {
        let h = pool.health();
        h.live_threads == THREADS
            && REQUESTS < MAX_REQUESTS * h.recycled_total + THREADS as u64 * MAX_REQUESTS
    });
    let health = pool.health();
    println!("  {REQUESTS} requests: {health:?}");
    assert!(settled, "{health:?}");
    assert!(
        MIN_REQUESTS * health.recycled_total <= REQUESTS,
        "{health:?}"
    );
    assert_eq!((health.abandoned_total, health.unhealthy), (0, false));
}

/// Replacement threads shut down cleanly too.
fn shutdown_after_recycling(pool: &PhpPool) {
    assert_eq!(
        pool.shutdown(Duration::from_secs(5)),
        ShutdownReport {
            abandoned_at_shutdown: 0,
            abandoned_threads: 0,
            runtime_shut_down: true,
        }
    );
}

fn main() {
    let cases: &[Case] = &[
        ("recycles_every_max_requests", recycles_every_max_requests),
        ("shutdown_after_recycling", shutdown_after_recycling),
    ];
    common::run(
        common::config(THREADS, MAX_REQUESTS, Duration::from_secs(10), 1),
        cases,
    )
}
