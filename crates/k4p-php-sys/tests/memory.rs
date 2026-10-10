//! Memory stability (PLAN.md S2): 100,000 sequential requests of a fixture that
//! builds ~1 MB of userland data each. Process RSS after warm-up must stay within ±5%,
//! and `memory_get_usage()` at script start must be identical in every request (from
//! the second one on: with OPcache active the first request compiles the script).
//!
//! `K4P_MEMORY_REQUESTS` overrides the request count (default 100,000).

mod common;

use std::collections::BTreeMap;

use common::{Case, fixtures, msg};
use k4p_core::HandlerResult;
use k4p_php_sys::{RuntimeConfig, execute};

const WARMUP: usize = 1_000;
const SAMPLE_EVERY: usize = 10_000;

fn rss_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("/proc/self/status");
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
        .expect("VmRSS")
}

fn memory_stability() {
    let requests: usize = std::env::var("K4P_MEMORY_REQUESTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);
    assert!(requests > WARMUP);

    // memory_get_usage() at script start -> (first request, last request, count)
    let mut start_usages: BTreeMap<u64, (usize, usize, usize)> = BTreeMap::new();
    let mut min_userland = u64::MAX;
    let mut baseline = 0;
    let mut samples = Vec::new();
    let started = std::time::Instant::now();

    for i in 1..=requests {
        let value = match execute(&msg("memory")) {
            Ok(HandlerResult::Content(m)) => m[0].value.clone().expect("value"),
            other => panic!("request {i}: {other:?}"),
        };
        let value = String::from_utf8(value).unwrap();
        let (start, peak) = value.split_once(':').unwrap();
        let (start, peak): (u64, u64) = (start.parse().unwrap(), peak.parse().unwrap());
        let e = start_usages.entry(start).or_insert((i, i, 0));
        e.1 = i;
        e.2 += 1;
        min_userland = min_userland.min(peak - start);

        if i == WARMUP {
            baseline = rss_kib();
            samples.push((i, baseline));
        } else if i % SAMPLE_EVERY == 0 || i == requests {
            samples.push((i, rss_kib()));
        }
    }

    let elapsed = started.elapsed();
    println!(
        "memory: {requests} requests in {:.1} s ({:.0} req/s); userland data per request >= {} KiB",
        elapsed.as_secs_f64(),
        requests as f64 / elapsed.as_secs_f64(),
        min_userland / 1024
    );
    println!("memory: memory_get_usage() at script start: {start_usages:?}");
    for (i, rss) in &samples {
        let delta = (*rss as f64 - baseline as f64) / baseline as f64 * 100.0;
        println!("memory: after {i:>7} requests RSS = {rss:>7} KiB ({delta:+.2}% vs warm-up)");
    }

    assert!(
        min_userland >= 1024 * 1024,
        "fixture allocates < 1 MB: {min_userland}"
    );
    // With OPcache active, request 1 compiles and caches the script (its op_array is
    // then in request memory); every later request must start from the same usage.
    let after_first: Vec<_> = start_usages
        .iter()
        .filter(|(_, (_, last, _))| *last >= 2)
        .collect();
    assert_eq!(
        after_first.len(),
        1,
        "memory_get_usage() at script start differs between requests: {start_usages:?}"
    );
    let (first_seen, _, _) = after_first[0].1;
    assert!(*first_seen <= 2, "{start_usages:?}");
    for (i, rss) in &samples {
        let ratio = *rss as f64 / baseline as f64;
        assert!(
            (0.95..=1.05).contains(&ratio),
            "RSS after {i} requests is {rss} KiB, baseline {baseline} KiB"
        );
    }
}

fn main() {
    // Run the fixture from a local copy: on Docker Desktop for Windows the repo is a
    // bind mount where every open()/stat() costs milliseconds, which would dominate the
    // request time (~5 ms instead of ~1.7 ms) and test nothing about PHP.
    let dir = std::env::temp_dir().join(format!("k4p-memory-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let entrypoint = dir.join("memory.php");
    std::fs::copy(fixtures().join("memory.php"), &entrypoint).expect("copy fixture");

    let config = RuntimeConfig {
        entrypoint,
        ini_file: None,
        // Where OPcache is active (8.5; see S2's handoff notes for 8.4), it refuses to
        // cache a file modified < 2 s ago, and the copy above is brand new: requests
        // would compile the script themselves (more request memory) until then.
        ini_entries: "opcache.file_update_protection=0\n".into(),
    };
    let cases: &[Case] = &[("memory_stability", memory_stability)];
    common::run(config, cases)
}
