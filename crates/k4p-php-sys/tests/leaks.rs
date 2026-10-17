//! 1,000 requests cycling through every fixture (every outcome kind, including fatal
//! errors, timeouts and bailouts). In a ZTS *debug* PHP build, PHP itself reports any
//! request-memory leak on stderr at request shutdown; `tests/leak-check.sh` runs this
//! binary in that build and fails on any report (PLAN.md S2). In a normal build it is
//! a plain smoke test.
//!
//! `K4P_LEAK_CONTROL=1` instead runs the positive control (`zend_leak_bytes()`, debug
//! image only), proving the leak reports are seen.

mod common;

use common::{Case, fixtures};
use k4p_core::testing::inbound;
use k4p_core::{ExecutionError, HandlerResult};
use k4p_php_sys::{RuntimeConfig, execute};

const REQUESTS: usize = 1_000;

/// (fixture, message value)
const MIX: &[(&str, &str)] = &[
    ("no_content", ""),
    ("content", ""),
    ("fail", "with_class"),
    ("fail", ""),
    ("retry_later", ""),
    ("no_result", ""),
    ("invalid_result", "headers_not_list"),
    ("invalid_result", "header_name_not_utf8"),
    ("invalid_result", "partition_negative:caught"),
    ("invalid_result", "message_extra_key:then_valid"),
    ("uncaught", ""),
    ("uncaught_error", ""),
    ("exception_handler", ""),
    ("fatal_memory", ""),
    ("fatal_redeclare", ""),
    ("fatal_parse", ""),
    ("result_then_exit", ""),
    ("exit_only", ""),
    ("shutdown_result", ""),
    ("destructor_result", ""),
    ("echo_back", "\0\u{ff}binary"),
    ("logging", ""),
    ("sapi_name", ""),
    ("isolation", ""),
    ("headers", ""),
];

fn mixed_requests() {
    let mut kinds = std::collections::BTreeMap::<&str, usize>::new();
    for i in 0..REQUESTS {
        // A soft timeout costs 1 s: only a few of them.
        let (fixture, value) = if i % 250 == 249 {
            ("soft_timeout", "")
        } else {
            MIX[i % MIX.len()]
        };
        let mut m = inbound(fixture, 0, 0);
        m.value = Some(value.as_bytes().to_vec());
        m.headers = vec![("h".into(), Some(b"\0v".to_vec())), ("h".into(), None)];
        let kind = match execute(&m) {
            Ok(HandlerResult::NoContent) => "NoContent",
            Ok(HandlerResult::Content(_)) => "Content",
            Ok(HandlerResult::Fail { .. }) => "Fail",
            Ok(HandlerResult::RetryLater { .. }) => "RetryLater",
            Err(ExecutionError::NoResult) => "NoResult",
            Err(ExecutionError::InvalidResult(_)) => "InvalidResult",
            Err(ExecutionError::UncaughtException { .. }) => "UncaughtException",
            Err(ExecutionError::FatalError(_)) => "FatalError",
            Err(ExecutionError::SoftTimeout) => "SoftTimeout",
            Err(ExecutionError::HardTimeout) => "HardTimeout",
            Err(ExecutionError::Bootstrap(e)) => {
                panic!("request {i} ({fixture}): bootstrap error {e}")
            }
        };
        *kinds.entry(kind).or_default() += 1;
        common::take_logs();
    }
    println!("leaks: {REQUESTS} requests, outcome kinds: {kinds:?}");
}

fn leak_control() {
    for _ in 0..10 {
        execute(&inbound("leak_control", 0, 0)).expect("leak_control");
    }
}

fn main() {
    let config = RuntimeConfig {
        entrypoint: fixtures().join("entry.php"),
        ini_file: None,
        ini_entries: "max_execution_time=1\n".into(),
    };
    let cases: &[Case] = if std::env::var_os("K4P_LEAK_CONTROL").is_some() {
        &[("leak_control", leak_control)]
    } else {
        &[("mixed_requests", mixed_requests)]
    };
    common::run(config, cases)
}
