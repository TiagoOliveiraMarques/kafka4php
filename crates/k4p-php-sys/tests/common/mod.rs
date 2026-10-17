//! Shared harness for the k4p-php-sys integration tests (and the `request_cost` bench).
//!
//! PHP can be started only once per process, on a thread that outlives all PHP use.
//! Each test binary therefore has its own `main` (`harness = false`): it starts the
//! runtime on the main thread, runs every case sequentially on one PHP worker thread
//! (as `k4p-php` will), shuts PHP down and exits non-zero if any case failed.
//!
//! Usage: `cargo test -p k4p-php-sys --test sapi [-- <substring filter>]`.

#![allow(dead_code)]

use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Instant;

use k4p_php_sys::{RuntimeConfig, ffi};

/// One call of the log callback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    pub level: ffi::k4p_log_level,
    pub source: String,
    pub msg: Vec<u8>,
}

impl LogRecord {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.msg).into_owned()
    }
}

static LOGS: Mutex<Vec<LogRecord>> = Mutex::new(Vec::new());

/// Echo every PHP log line to stderr (set `K4P_TEST_ECHO_LOG=1`).
static ECHO: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

unsafe extern "C" fn log_cb(
    _user_data: *mut c_void,
    level: ffi::k4p_log_level,
    source: *const std::ffi::c_char,
    msg: *const u8,
    len: usize,
) {
    // SAFETY: the C side passes a static NUL-terminated source and `len` bytes.
    let (source, msg) = unsafe {
        (
            std::ffi::CStr::from_ptr(source)
                .to_string_lossy()
                .into_owned(),
            std::slice::from_raw_parts(msg, len).to_vec(),
        )
    };
    if *ECHO.get_or_init(|| std::env::var_os("K4P_TEST_ECHO_LOG").is_some()) {
        eprintln!("[php {level:?} {source}] {}", String::from_utf8_lossy(&msg));
    }
    if let Ok(mut logs) = LOGS.lock() {
        logs.push(LogRecord { level, source, msg });
    }
}

/// Takes (and clears) all log records captured so far.
pub fn take_logs() -> Vec<LogRecord> {
    std::mem::take(&mut *LOGS.lock().unwrap())
}

pub fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

pub type Case = (&'static str, fn());

/// Starts PHP with `config`, runs `cases` on one PHP worker thread, shuts PHP down,
/// and exits the process (status 1 if any case failed).
pub fn run(config: RuntimeConfig, cases: &[Case]) -> ! {
    let filter: Option<String> = std::env::args().skip(1).find(|a| !a.starts_with('-'));

    // SAFETY: first and only init in this process, on the main thread, which outlives
    // the worker; the callback is thread-safe and `log_user_data` is unused.
    unsafe { k4p_php_sys::runtime_init(&config, Some(log_cb), std::ptr::null_mut()) }
        .expect("runtime_init");
    let startup_logs = take_logs();
    for l in &startup_logs {
        eprintln!("startup log: {l:?} {}", l.text());
    }
    // No test configures PHP to warn at startup (e.g. a broken preload script).
    assert!(
        startup_logs.iter().all(|l| l.source != "error"),
        "PHP reported errors during runtime init"
    );

    let cases: Vec<Case> = cases
        .iter()
        .copied()
        .filter(|(name, _)| filter.as_deref().is_none_or(|f| name.contains(f)))
        .collect();

    let failed = std::thread::Builder::new()
        .name("php-worker".into())
        .spawn(move || {
            assert!(k4p_php_sys::thread_init(), "thread_init");
            let mut failed = Vec::new();
            println!("\nrunning {} cases", cases.len());
            for (name, case) in cases {
                take_logs();
                let start = Instant::now();
                let result = catch_unwind(AssertUnwindSafe(case));
                let ms = start.elapsed().as_millis();
                match result {
                    Ok(()) => println!("case {name} ... ok ({ms} ms)"),
                    Err(_) => {
                        println!("case {name} ... FAILED ({ms} ms)");
                        failed.push(name);
                    }
                }
            }
            assert!(k4p_php_sys::thread_shutdown(), "thread_shutdown");
            failed
        })
        .expect("spawn worker")
        .join()
        .expect("worker thread panicked outside a case");

    // SAFETY: the only PHP worker thread has finished and called thread_shutdown().
    assert!(
        unsafe { k4p_php_sys::runtime_shutdown() },
        "runtime_shutdown"
    );

    if failed.is_empty() {
        println!("\ntest result: ok\n");
        std::process::exit(0);
    }
    println!("\ntest result: FAILED. failed cases: {failed:?}\n");
    std::process::exit(1);
}
