//! Shared harness for the k4p-php integration tests.
//!
//! PHP can be started only once per process, so each test binary has its own `main`
//! (`harness = false`): it starts one [`PhpPool`] with its config, runs its cases in
//! order (each may rely on the pool state the previous ones left), shuts the pool
//! down and exits non-zero if any case failed.
//!
//! Usage: `cargo test -p k4p-php --test pool [-- <substring filter>]`.
//! `K4P_TEST_ECHO_LOG=1` echoes PHP log lines to stderr.

#![allow(dead_code)]

use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use k4p_core::{HandlerResult, InboundMessage, Outcome};
use k4p_php::{LogLevel, LogRecord, LogSink, LogSource, MessageContext, PhpConfig, PhpPool};

/// One captured log record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Logged {
    pub level: LogLevel,
    pub source: LogSource,
    pub text: String,
    pub context: Option<MessageContext>,
}

static LOGS: Mutex<Vec<Logged>> = Mutex::new(Vec::new());

struct CaptureSink {
    echo: bool,
}

impl LogSink for CaptureSink {
    fn log(&self, r: &LogRecord<'_>) {
        let logged = Logged {
            level: r.level,
            source: r.source,
            text: String::from_utf8_lossy(r.message).into_owned(),
            context: r.context.cloned(),
        };
        if self.echo {
            eprintln!("[php] {logged:?}");
        }
        LOGS.lock().unwrap().push(logged);
    }
}

/// Takes (and clears) all log records captured so far.
pub fn take_logs() -> Vec<Logged> {
    std::mem::take(&mut *LOGS.lock().unwrap())
}

/// A copy of the fixtures outside the repo: on Docker Desktop for Windows the repo
/// is a bind mount where every open()/stat() costs milliseconds.
fn fixtures() -> PathBuf {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let dir = std::env::temp_dir().join(format!("k4p-php-fixtures-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    for entry in std::fs::read_dir(&src).expect("fixtures dir") {
        let path = entry.expect("fixture").path();
        std::fs::copy(&path, dir.join(path.file_name().unwrap())).expect("copy fixture");
    }
    dir
}

/// Pool config for the fixtures, with the given sizing and timeouts.
pub fn config(
    threads: usize,
    max_requests_per_thread: u64,
    hard_timeout: Duration,
    max_abandoned_threads: usize,
) -> PhpConfig {
    PhpConfig {
        entrypoint: fixtures().join("entry.php"),
        ini_file: None,
        // The fixture copies are brand new: let OPcache (8.5) cache them anyway.
        ini_entries: "opcache.file_update_protection=0\n".into(),
        threads: NonZeroUsize::new(threads).unwrap(),
        max_requests_per_thread: NonZeroU64::new(max_requests_per_thread).unwrap(),
        hard_timeout,
        max_abandoned_threads: NonZeroUsize::new(max_abandoned_threads).unwrap(),
    }
}

fn tokio() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

pub fn block_on<F: Future>(f: F) -> F::Output {
    tokio().block_on(f)
}

/// Spawns `fut` on the test runtime (so it progresses while the case blocks).
pub fn spawn<F>(fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio().spawn(fut)
}

/// Executes all `msgs` concurrently; outcomes in input order, with each one's
/// completion time since the start.
pub fn execute_all(pool: &PhpPool, msgs: Vec<InboundMessage>) -> Vec<(Outcome, Duration)> {
    use k4p_php::Executor;
    let start = Instant::now();
    let tasks: Vec<_> = msgs
        .into_iter()
        .map(|m| {
            let fut = pool.execute(m);
            spawn(async move {
                let outcome = fut.await;
                (outcome, start.elapsed())
            })
        })
        .collect();
    block_on(async {
        let mut out = Vec::with_capacity(tasks.len());
        for t in tasks {
            out.push(t.await.expect("task"));
        }
        out
    })
}

/// A message for fixture `fixture` with `value`.
pub fn msg(fixture: &str, offset: i64, value: impl Into<Vec<u8>>) -> InboundMessage {
    InboundMessage {
        value: Some(value.into()),
        ..k4p_core::testing::inbound(fixture, 0, offset)
    }
}

/// The value of the single produced message of a `Content` outcome.
pub fn content_value(outcome: &Outcome) -> Vec<u8> {
    match outcome {
        Ok(HandlerResult::Content(m)) if m.len() == 1 => m[0].value.clone().expect("value"),
        other => panic!("expected Content with one message, got {other:?}"),
    }
}

/// Polls `cond` until it holds or `timeout` passes.
pub fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    cond()
}

pub type Case = (&'static str, fn(&PhpPool));

/// Starts the pool, runs `cases` in order, shuts the pool down (if no case did) and
/// exits the process (status 1 if any case failed).
pub fn run(config: PhpConfig, cases: &[Case]) -> ! {
    let filter: Option<String> = std::env::args().skip(1).find(|a| !a.starts_with('-'));
    let echo = std::env::var_os("K4P_TEST_ECHO_LOG").is_some();
    let pool = PhpPool::start(config, CaptureSink { echo }).expect("PhpPool::start");
    for l in take_logs() {
        eprintln!("startup log: {l:?}");
    }

    let mut failed = Vec::new();
    let cases: Vec<&Case> = cases
        .iter()
        .filter(|(name, _)| filter.as_deref().is_none_or(|f| name.contains(f)))
        .collect();
    println!("\nrunning {} cases", cases.len());
    for (name, case) in cases {
        take_logs();
        let start = Instant::now();
        let result = catch_unwind(AssertUnwindSafe(|| case(&pool)));
        let ms = start.elapsed().as_millis();
        match result {
            Ok(()) => println!("case {name} ... ok ({ms} ms)"),
            Err(_) => {
                println!("case {name} ... FAILED ({ms} ms)");
                failed.push(*name);
            }
        }
    }
    let report = pool.shutdown(Duration::from_secs(10));
    println!("shutdown: {report:?}");

    if failed.is_empty() {
        println!("\ntest result: ok\n");
        std::process::exit(0);
    }
    println!("\ntest result: FAILED. failed cases: {failed:?}\n");
    std::process::exit(1);
}
