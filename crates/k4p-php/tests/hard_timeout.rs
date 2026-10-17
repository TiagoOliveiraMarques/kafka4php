//! Hard timeout (PLAN.md S3): 2 threads, `hard_timeout` 1 s, unhealthy after 2
//! abandoned threads. A request blocked in C (`usleep()` with no soft timeout) is
//! answered `HardTimeout`, its thread abandoned and replaced; the cases build on each
//! other and the last one shuts down with a request still stuck.

mod common;

use std::time::{Duration, Instant};

use common::{Case, content_value, execute_all, msg, spawn, wait_until};
use k4p_core::ExecutionError;
use k4p_php::{Executor, PhpPool, ShutdownReport};

const THREADS: usize = 2;
const HARD_TIMEOUT: Duration = Duration::from_secs(1);

fn block(ms: u32) -> k4p_core::InboundMessage {
    msg("block", 0, ms.to_string())
}

fn abandons_and_replaces_the_thread(pool: &PhpPool) {
    let start = Instant::now();
    let blocked = spawn(pool.execute(block(3000)));

    // The other thread keeps serving meanwhile.
    let mut served = 0;
    while !blocked.is_finished() {
        let (o, _) = execute_all(pool, vec![msg("echo", served, "x")]).remove(0);
        assert_eq!(content_value(&o), b"x");
        served += 1;
    }
    let outcome = common::block_on(blocked).unwrap();
    let took = start.elapsed();
    println!("  HardTimeout after {took:?}; {served} requests served meanwhile");
    assert_eq!(outcome, Err(ExecutionError::HardTimeout));
    assert!(
        took >= HARD_TIMEOUT && took < Duration::from_millis(1600),
        "{took:?}"
    );
    assert!(served > 10, "{served}");

    let health = pool.health();
    assert_eq!(
        (
            health.live_threads,
            health.abandoned,
            health.abandoned_total
        ),
        (2, 1, 1)
    );
    assert!(!health.unhealthy);

    // Capacity is back while the abandoned thread is still stuck.
    let outcomes = execute_all(pool, (0..THREADS).map(|_| msg("sleep", 0, "500")).collect());
    let slowest = outcomes.iter().map(|(_, t)| *t).max().unwrap();
    assert!(slowest < Duration::from_millis(900), "{slowest:?}");
    assert_eq!(pool.health().abandoned, 1);

    // It finishes its request at ~3 s, discards the result and exits.
    assert!(
        wait_until(Duration::from_secs(5), || pool.health().abandoned == 0),
        "{:?}",
        pool.health()
    );
    assert_eq!(pool.health().live_threads, THREADS);
}

fn unhealthy_after_max_abandoned_threads(pool: &PhpPool) {
    let mut health = pool.health_watch();
    // Stuck for longer than the rest of this test binary runs.
    let outcome = common::block_on(pool.execute(block(60_000)));
    assert_eq!(outcome, Err(ExecutionError::HardTimeout));
    common::block_on(async {
        tokio::time::timeout(Duration::from_secs(1), health.wait_for(|h| h.unhealthy))
            .await
            .expect("became unhealthy")
            .expect("watch open");
    });
    let health = pool.health();
    assert_eq!(
        (
            health.abandoned,
            health.abandoned_total,
            health.live_threads
        ),
        (1, 2, 2)
    );
    // Unhealthy, but still serving (the binary decides to restart).
    let outcomes = execute_all(pool, (0..4).map(|i| msg("echo", i, "y")).collect());
    assert!(outcomes.iter().all(|(o, _)| content_value(o) == b"y"));
}

/// Last case: a request still running when the grace period ends is abandoned, and
/// PHP is left running because abandoned threads may still use it.
fn shutdown_abandons_stuck_requests(pool: &PhpPool) {
    let stuck = spawn(pool.execute(block(60_000)));
    std::thread::sleep(Duration::from_millis(200));
    let start = Instant::now();
    let report = pool.shutdown(Duration::from_millis(300));
    let took = start.elapsed();
    println!("  shutdown took {took:?}: {report:?}");
    assert!(
        took < HARD_TIMEOUT,
        "grace, not the hard timeout, ended it: {took:?}"
    );
    assert_eq!(
        report,
        ShutdownReport {
            abandoned_at_shutdown: 1,
            abandoned_threads: 2,
            runtime_shut_down: false,
        }
    );
    assert_eq!(
        common::block_on(stuck).unwrap(),
        Err(ExecutionError::HardTimeout)
    );
}

fn main() {
    let cases: &[Case] = &[
        (
            "abandons_and_replaces_the_thread",
            abandons_and_replaces_the_thread,
        ),
        (
            "unhealthy_after_max_abandoned_threads",
            unhealthy_after_max_abandoned_threads,
        ),
        (
            "shutdown_abandons_stuck_requests",
            shutdown_abandons_stuck_requests,
        ),
    ];
    common::run(common::config(THREADS, 1_000_000, HARD_TIMEOUT, 2), cases)
}
