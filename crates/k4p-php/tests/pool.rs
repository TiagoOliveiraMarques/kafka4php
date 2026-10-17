//! PhpPool with 8 threads (PLAN.md S3): concurrent correctness, real parallelism,
//! queueing beyond capacity, soft timeouts under concurrency, log context, health,
//! and graceful shutdown with in-flight requests (last: it stops the pool).

mod common;

use std::time::{Duration, Instant};

use common::{Case, content_value, execute_all, msg, spawn, take_logs};
use k4p_core::{ExecutionError, HandlerResult, InboundMessage, OutboundMessage};
use k4p_php::{Executor, LogLevel, LogSource, PhpPool, PoolHealth, ShutdownReport, StartError};

const THREADS: usize = 8;

fn echo_message(i: i64) -> InboundMessage {
    let mut m = msg(
        "echo",
        i,
        [b"value\0\xff-".as_slice(), &i.to_be_bytes()].concat(),
    );
    m.partition = (i % 7) as i32;
    m.attempt = (i % 3) as u32 + 1;
    m.key = (i % 5 != 0).then(|| format!("key-{i}\0").into_bytes());
    m.headers = vec![
        ("h".into(), Some(i.to_string().into_bytes())),
        ("dup".into(), None),
        ("dup".into(), Some(vec![0xfe, (i % 256) as u8])),
    ];
    m
}

fn expected_echo(m: &InboundMessage) -> OutboundMessage {
    let mut headers = m.headers.clone();
    let coordinates = format!("{}:{}:{}", m.partition, m.offset, m.attempt);
    headers.push(("coordinates".into(), Some(coordinates.into_bytes())));
    OutboundMessage {
        topic: "echo".into(),
        partition: None,
        key: m.key.clone(),
        value: m.value.clone(),
        headers,
    }
}

fn echo_10k_concurrent(pool: &PhpPool) {
    let msgs: Vec<_> = (0..10_000).map(echo_message).collect();
    let start = Instant::now();
    let outcomes = execute_all(pool, msgs.clone());
    println!(
        "  10,000 echo requests on {THREADS} threads in {:?}",
        start.elapsed()
    );
    for (m, (outcome, _)) in msgs.iter().zip(&outcomes) {
        assert_eq!(
            outcome,
            &Ok(HandlerResult::Content(vec![expected_echo(m)])),
            "offset {}",
            m.offset
        );
    }
}

fn sleeps(n: usize, ms: u32) -> Vec<InboundMessage> {
    (0..n)
        .map(|i| msg("sleep", i as i64, ms.to_string()))
        .collect()
}

fn concurrency_is_real(pool: &PhpPool) {
    let outcomes = execute_all(pool, sleeps(THREADS, 1000));
    let slowest = outcomes.iter().map(|(_, t)| *t).max().unwrap();
    for (o, _) in &outcomes {
        assert_eq!(content_value(o), b"1000");
    }
    println!("  {THREADS} x 1 s sleeps took {slowest:?}");
    assert!(slowest >= Duration::from_secs(1), "{slowest:?}");
    assert!(
        slowest < Duration::from_millis(1800),
        "not parallel: {slowest:?}"
    );
}

/// More calls than `capacity()` queue and run as threads free up.
fn beyond_capacity_queues(pool: &PhpPool) {
    assert_eq!(pool.capacity(), THREADS);
    let outcomes = execute_all(pool, sleeps(2 * THREADS, 500));
    let mut times: Vec<_> = outcomes.iter().map(|(_, t)| *t).collect();
    times.sort();
    for (o, _) in &outcomes {
        assert_eq!(content_value(o), b"500");
    }
    println!(
        "  {} x 0.5 s sleeps: first {:?}, last {:?}",
        2 * THREADS,
        times[0],
        times[2 * THREADS - 1]
    );
    assert!(times[THREADS - 1] < Duration::from_millis(900), "{times:?}");
    assert!(times[THREADS] >= Duration::from_secs(1), "{times:?}");
    assert!(
        times[2 * THREADS - 1] < Duration::from_millis(1800),
        "{times:?}"
    );
}

/// Per-thread `max_execution_time` timers: half the threads time out, the other half
/// (sleeping past the 1 s limit) are unaffected.
fn soft_timeout_under_concurrency(pool: &PhpPool) {
    let half = THREADS / 2;
    let msgs: Vec<_> = (0..half)
        .map(|i| msg("busy", i as i64, "1"))
        .chain(sleeps(half, 1500))
        .collect();
    let outcomes = execute_all(pool, msgs);
    for (o, t) in &outcomes[..half] {
        assert_eq!(o, &Err(ExecutionError::SoftTimeout));
        assert!(
            *t >= Duration::from_secs(1) && *t < Duration::from_secs(3),
            "{t:?}"
        );
    }
    for (o, _) in &outcomes[half..] {
        assert_eq!(content_value(o), b"1500");
    }
    // The timed-out threads keep serving.
    let outcomes = execute_all(pool, (0..2 * THREADS as i64).map(echo_message).collect());
    assert!(outcomes.iter().all(|(o, _)| o.is_ok()), "{outcomes:?}");
    assert_eq!(pool.health().abandoned_total, 0);
}

fn log_lines_carry_message_context(pool: &PhpPool) {
    let mut m = msg("log", 42, "");
    m.partition = 3;
    let (outcome, _) = execute_all(pool, vec![m]).remove(0);
    assert_eq!(outcome, Ok(HandlerResult::NoContent));
    let logs = take_logs();
    let stdout = logs
        .iter()
        .find(|l| l.source == LogSource::Stdout)
        .expect("stdout line");
    assert_eq!(stdout.text, "hello from offset 42");
    assert_eq!(stdout.level, LogLevel::Info);
    let warning = logs
        .iter()
        .find(|l| l.source == LogSource::Error)
        .expect("warning");
    assert!(warning.text.contains("careful"), "{warning:?}");
    assert_eq!(warning.level, LogLevel::Warn);
    for l in [stdout, warning] {
        let c = l.context.as_ref().expect("context");
        assert_eq!(
            (c.topic.as_str(), c.partition, c.offset, c.attempt),
            ("log", 3, 42, 1)
        );
    }
}

fn second_start_is_refused(_pool: &PhpPool) {
    let mut config = common::config(1, 1, Duration::ZERO, 1);
    assert!(matches!(
        PhpPool::start(config.clone(), k4p_php::TracingLogSink),
        Err(StartError::InvalidConfig(_))
    ));
    config.hard_timeout = Duration::from_secs(1);
    assert!(matches!(
        PhpPool::start(config, k4p_php::TracingLogSink),
        Err(StartError::AlreadyStarted)
    ));
}

fn healthy(pool: &PhpPool) {
    assert_eq!(
        pool.health(),
        PoolHealth {
            live_threads: THREADS,
            abandoned: 0,
            abandoned_total: 0,
            recycled_total: 0,
            unhealthy: false,
        }
    );
}

/// Last case: shutdown lets in-flight requests finish within the grace period.
fn graceful_shutdown_completes_in_flight(pool: &PhpPool) {
    let start = Instant::now();
    let in_flight: Vec<_> = sleeps(THREADS, 1000)
        .into_iter()
        .map(|m| spawn(pool.execute(m)))
        .collect();
    std::thread::sleep(Duration::from_millis(200));
    let report = pool.shutdown(Duration::from_secs(5));
    let took = start.elapsed();
    println!("  shutdown with {THREADS} in-flight 1 s requests took {took:?}: {report:?}");
    assert_eq!(
        report,
        ShutdownReport {
            abandoned_at_shutdown: 0,
            abandoned_threads: 0,
            runtime_shut_down: true,
        }
    );
    assert!(took < Duration::from_secs(2), "{took:?}");
    for task in in_flight {
        assert_eq!(content_value(&common::block_on(task).unwrap()), b"1000");
    }
    let after = common::block_on(pool.execute(echo_message(0)));
    assert!(
        matches!(after, Err(ExecutionError::Bootstrap(_))),
        "{after:?}"
    );
    assert_eq!(pool.shutdown(Duration::ZERO), report, "idempotent");
}

fn main() {
    let cases: &[Case] = &[
        ("healthy_at_start", healthy),
        ("echo_10k_concurrent", echo_10k_concurrent),
        ("concurrency_is_real", concurrency_is_real),
        ("beyond_capacity_queues", beyond_capacity_queues),
        (
            "soft_timeout_under_concurrency",
            soft_timeout_under_concurrency,
        ),
        (
            "log_lines_carry_message_context",
            log_lines_carry_message_context,
        ),
        ("second_start_is_refused", second_start_is_refused),
        ("healthy_at_end", healthy),
        (
            "graceful_shutdown_completes_in_flight",
            graceful_shutdown_completes_in_flight,
        ),
    ];
    common::run(
        common::config(THREADS, 1_000_000, Duration::from_secs(10), 1),
        cases,
    )
}
