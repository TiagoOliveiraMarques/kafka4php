//! OPcache, `opcache.preload` and the tracing JIT under the `kafka4php` SAPI (PLAN.md
//! S4, DESIGN.md §5.1 and §5.7).
//!
//! PHP 8.5: OPcache is active (hits increase across requests), the preloaded class and
//! function are visible in every request without autoloading, and the JIT is on and
//! survives ≥ 100,000 requests on 4 threads at once. PHP 8.4: OPcache is loaded but
//! inactive under our SAPI name (accepted limitation), so nothing is preloaded; the
//! multi-threaded run still has to pass.
//!
//! `K4P_JIT_REQUESTS` overrides the multi-threaded request count (default 100,000).

mod common;

use common::{Case, fixtures, take_logs};
use k4p_core::testing::inbound;
use k4p_core::{HandlerResult, Outcome};
use k4p_php_sys::{RuntimeConfig, execute};
use serde_json::Value;

const JIT_THREADS: usize = 4;

fn value(outcome: Outcome) -> Vec<u8> {
    match outcome {
        Ok(HandlerResult::Content(mut msgs)) if msgs.len() == 1 => {
            msgs.remove(0).value.expect("value")
        }
        other => panic!("expected Content with one message, got {other:?}"),
    }
}

fn run(key: &str, offset: i64) -> Vec<u8> {
    let mut m = inbound("opcache", 0, offset);
    m.key = Some(key.as_bytes().to_vec());
    value(execute(&m))
}

fn status() -> Value {
    serde_json::from_slice(&run("status", 0)).expect("status JSON")
}

/// Whether OPcache can be active under our SAPI (PHP 8.5+), from a [`status`] report.
fn opcache_active(status: &Value) -> bool {
    status["php_version_id"].as_i64().expect("PHP_VERSION_ID") >= 80500
}

/// What the fixture's `jit` loop computes.
fn expected_jit(offset: i64) -> i64 {
    (0..2000).fold(0, |acc, i| (acc * 31 + i * offset) % 1_000_003)
}

fn opcache_status() {
    let first = status();
    println!("opcache: status = {first}");
    assert_eq!(first["opcache_loaded"], true, "{first}");
    if !opcache_active(&first) {
        // PHP 8.4: loaded but inactive under SAPI name `kafka4php` (DESIGN.md §5.1). If
        // this fails, PHP changed: revisit the limitation.
        assert_eq!(first["status"], false, "{first}");
        return;
    }
    let second = status();
    let (s1, s2) = (&first["status"], &second["status"]);
    assert_eq!(s2["enabled"], true, "{second}");
    assert_eq!(s2["entry_cached"], true, "{second}");
    assert!(
        s2["hits"].as_u64() > s1["hits"].as_u64(),
        "OPcache hits did not increase: {first} -> {second}"
    );
}

fn preload() {
    let s = status();
    if !opcache_active(&s) {
        assert_eq!(s["preloaded_class"], Value::Null, "{s}");
        assert_eq!(s["preloaded_function"], Value::Null, "{s}");
        return;
    }
    assert_eq!(s["status"]["preload"], true, "{s}");
    assert_eq!(s["preloaded_class"], "hello kafka4php", "{s}");
    assert_eq!(s["preloaded_function"], "preloaded", "{s}");
}

fn jit_multithread() {
    let requests: usize = std::env::var("K4P_JIT_REQUESTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);
    let before = status();
    let active = opcache_active(&before);
    if active {
        assert_eq!(before["status"]["jit_enabled"], true, "{before}");
        assert_eq!(before["status"]["jit_on"], true, "{before}");
    }

    let per_thread = requests.div_ceil(JIT_THREADS);
    let started = std::time::Instant::now();
    let threads: Vec<_> = (0..JIT_THREADS)
        .map(|t| {
            std::thread::spawn(move || {
                assert!(k4p_php_sys::thread_init(), "thread_init");
                for i in 0..per_thread {
                    // Distinct offsets per thread, so a mixed-up result is noticed.
                    let offset = (t * per_thread + i) as i64;
                    let got = run("jit", offset);
                    assert_eq!(
                        got,
                        expected_jit(offset).to_string().into_bytes(),
                        "thread {t}, offset {offset}"
                    );
                }
                assert!(k4p_php_sys::thread_shutdown(), "thread_shutdown");
            })
        })
        .collect();
    for t in threads {
        t.join().expect("JIT worker thread failed");
    }
    let elapsed = started.elapsed();
    println!(
        "opcache: {} JIT requests on {JIT_THREADS} threads in {:.1} s ({:.0} req/s)",
        per_thread * JIT_THREADS,
        elapsed.as_secs_f64(),
        (per_thread * JIT_THREADS) as f64 / elapsed.as_secs_f64()
    );

    if active {
        let after = status();
        println!("opcache: status after the JIT run = {after}");
        // The hot loop's traces were compiled into the (shared) JIT buffer.
        assert!(
            after["status"]["jit_buffer_used"].as_u64()
                > before["status"]["jit_buffer_used"].as_u64(),
            "the JIT compiled nothing: {before} -> {after}"
        );
    }
    let errors: Vec<_> = take_logs()
        .into_iter()
        .filter(|l| l.source != "stdout")
        .collect();
    assert!(errors.is_empty(), "{errors:?}");
}

fn main() {
    // A local copy: off the slow /work bind mount (see memory.rs).
    let dir = std::env::temp_dir().join(format!("k4p-opcache-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    for f in ["entry.php", "preload.php", "preloaded.php"] {
        std::fs::copy(fixtures().join("opcache").join(f), dir.join(f)).expect("copy fixture");
    }

    let config = RuntimeConfig {
        entrypoint: dir.join("entry.php"),
        ini_file: None,
        // file_update_protection: the copies above are brand new (see memory.rs).
        // preload_user: required when preloading as root (the toolchain container);
        // with it set to the current user, OPcache preloads in-process, no fork().
        ini_entries: format!(
            "opcache.file_update_protection=0\n\
             opcache.preload={}\n\
             opcache.preload_user=root\n\
             opcache.jit=tracing\n\
             opcache.jit_buffer_size=32M\n",
            dir.join("preload.php").display()
        ),
    };
    let cases: &[Case] = &[
        ("opcache_status", opcache_status),
        ("preload", preload),
        ("jit_multithread", jit_multithread),
    ];
    common::run(config, cases)
}
