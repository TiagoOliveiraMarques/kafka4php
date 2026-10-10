//! The kafka4php SAPI on a single PHP thread: every outcome kind, binary safety,
//! logging, isolation (PLAN.md S2 acceptance criteria).

mod common;

use std::time::{Duration, Instant};

use common::{Case, LogRecord, fixtures, msg, take_logs};
use k4p_core::{ExecutionError, HandlerResult, InboundMessage, OutboundMessage, Outcome};
use k4p_php_sys::{RuntimeConfig, execute, ffi::k4p_log_level};
use serde_json::{Value, json};

fn run_with_value(fixture: &str, value: &str) -> Outcome {
    let mut m = msg(fixture);
    m.value = Some(value.as_bytes().to_vec());
    execute(&m)
}

fn content(outcome: Outcome) -> Vec<OutboundMessage> {
    match outcome {
        Ok(HandlerResult::Content(msgs)) => msgs,
        other => panic!("expected Content, got {other:?}"),
    }
}

/// The value of the single message of a Content outcome, parsed as JSON.
fn json_value(outcome: Outcome) -> Value {
    let msgs = content(outcome);
    assert_eq!(msgs.len(), 1, "{msgs:?}");
    serde_json::from_slice(msgs[0].value.as_deref().expect("value")).expect("JSON")
}

fn error_logs(logs: &[LogRecord]) -> Vec<String> {
    logs.iter()
        .filter(|l| l.source == "error")
        .map(LogRecord::text)
        .collect()
}

fn no_content() {
    assert_eq!(execute(&msg("no_content")), Ok(HandlerResult::NoContent));
}

fn content_multiple_messages() {
    let h = |k: &str, v: Option<&str>| (k.to_owned(), v.map(|v| v.as_bytes().to_vec()));
    assert_eq!(
        content(execute(&msg("content"))),
        vec![
            OutboundMessage {
                topic: "out-a".into(),
                partition: Some(3),
                key: Some(b"k1".to_vec()),
                value: Some(b"v1".to_vec()),
                headers: vec![
                    h("h1", Some("x")),
                    h("h1", Some("y")),
                    h("h2", None),
                    h("h3", Some(""))
                ],
            },
            OutboundMessage {
                topic: "out-b".into(),
                partition: None,
                key: None,
                value: None,
                headers: vec![],
            },
            OutboundMessage {
                topic: "out-a".into(),
                partition: None,
                key: None,
                value: Some(vec![]),
                headers: vec![],
            },
        ]
    );
}

fn fail_with_and_without_class() {
    let fail = |class: Option<&str>| {
        Ok(HandlerResult::Fail {
            reason: "bad order".into(),
            error_class: class.map(Into::into),
        })
    };
    assert_eq!(
        run_with_value("fail", "with_class"),
        fail(Some("App\\InvalidOrder"))
    );
    assert_eq!(run_with_value("fail", "null_class"), fail(None));
    assert_eq!(run_with_value("fail", "omitted"), fail(None));
}

fn retry_later() {
    assert_eq!(
        execute(&msg("retry_later")),
        Ok(HandlerResult::RetryLater {
            reason: "downstream unavailable".into()
        })
    );
}

fn no_result() {
    assert_eq!(execute(&msg("no_result")), Err(ExecutionError::NoResult));
}

const INVALID_CASES: &[(&str, &str)] = &[
    ("missing_type", "type must be one of"),
    ("unknown_type", "type must be one of"),
    ("type_not_string", "type must be one of"),
    ("extra_key", "unexpected key \"oops\""),
    ("int_key", "unexpected key 0"),
    ("fail_no_reason", "reason must be a string"),
    ("fail_reason_int", "reason must be a string"),
    ("fail_class_int", "error_class must be a string or null"),
    ("retry_no_reason", "reason must be a string"),
    ("messages_missing", "messages must be a list"),
    ("messages_not_list", "messages must be a list"),
    ("message_not_array", "messages[0] must be an array"),
    (
        "topic_missing",
        "messages[0].topic must be a non-empty valid UTF-8 string",
    ),
    (
        "topic_empty",
        "messages[0].topic must be a non-empty valid UTF-8 string",
    ),
    (
        "topic_not_utf8",
        "messages[0].topic must be a non-empty valid UTF-8 string",
    ),
    ("value_missing", "messages[0].value is required"),
    ("value_int", "messages[0].value must be a string or null"),
    ("key_int", "messages[0].key must be a string or null"),
    (
        "partition_negative",
        "messages[0].partition must be null or an int",
    ),
    (
        "partition_too_big",
        "messages[0].partition must be null or an int",
    ),
    (
        "partition_string",
        "messages[0].partition must be null or an int",
    ),
    ("headers_not_list", "messages[0].headers must be a list"),
    (
        "header_not_pair",
        "messages[0].headers[0] must be a [name, value] pair",
    ),
    (
        "header_name_int",
        "messages[0].headers[0][0] (name) must be a valid UTF-8",
    ),
    (
        "header_name_not_utf8",
        "messages[0].headers[0][0] (name) must be a valid UTF-8",
    ),
    (
        "header_value_int",
        "messages[0].headers[0][1] (value) must be a string or null",
    ),
    (
        "message_extra_key",
        "messages[0].unexpected key \"timestamp\"",
    ),
];

fn invalid_result_uncaught_value_error() {
    for (case, expected) in INVALID_CASES {
        take_logs();
        match run_with_value("invalid_result", case) {
            Err(ExecutionError::InvalidResult(m)) => {
                assert!(m.contains(expected), "{case}: {m:?} lacks {expected:?}")
            }
            other => panic!("{case}: expected InvalidResult, got {other:?}"),
        }
        // The \ValueError is visible to the developer in the log.
        let errors = error_logs(&take_logs());
        assert!(
            errors.iter().any(|e| e.contains("Uncaught ValueError")
                && e.contains(
                    "Kafka4php\\Native\\result(): Argument #1 ($result) is not a valid result"
                )
                && e.contains(expected)),
            "{case}: {errors:?}"
        );
    }
}

fn invalid_result_caught() {
    // Caught \ValueError and nothing else: still InvalidResult.
    match run_with_value("invalid_result", "unknown_type:caught") {
        Err(ExecutionError::InvalidResult(m)) => assert!(m.contains("type must be one of")),
        other => panic!("expected InvalidResult, got {other:?}"),
    }
    let logs = take_logs();
    assert!(
        logs.iter()
            .any(|l| l.source == "stdout" && l.text().starts_with("caught: ")),
        "{logs:?}"
    );
    // A later valid result() replaces the invalid one.
    assert_eq!(
        run_with_value("invalid_result", "topic_empty:then_valid"),
        Ok(HandlerResult::NoContent)
    );
}

fn uncaught_exception() {
    assert_eq!(
        execute(&msg("uncaught")),
        Err(ExecutionError::UncaughtException {
            class: "App\\OrderFailed".into(),
            message: "db down: café".into(),
        })
    );
    let errors = error_logs(&take_logs());
    assert!(
        errors
            .iter()
            .any(|e| e.contains("PHP Fatal error:  Uncaught App\\OrderFailed")),
        "{errors:?}"
    );

    match execute(&msg("uncaught_error")) {
        Err(ExecutionError::UncaughtException { class, message }) => {
            assert_eq!(class, "Error");
            assert!(
                message.contains("k4p_this_function_does_not_exist"),
                "{message}"
            );
        }
        other => panic!("expected UncaughtException, got {other:?}"),
    }
}

fn user_exception_handler_handles() {
    assert_eq!(
        execute(&msg("exception_handler")),
        Ok(HandlerResult::Fail {
            reason: "handled: boom".into(),
            error_class: Some("DomainException".into()),
        })
    );
}

fn fatal_error_memory_limit() {
    match execute(&msg("fatal_memory")) {
        Err(ExecutionError::FatalError(m)) => {
            assert!(
                m.contains("Allowed memory size of 16777216 bytes exhausted"),
                "{m}"
            );
            assert!(m.contains("fatal_memory.php on line"), "{m}");
        }
        other => panic!("expected FatalError, got {other:?}"),
    }
    let logs = take_logs();
    assert!(
        logs.iter().any(|l| l.source == "error"
            && l.level == k4p_log_level::K4P_LOG_ERROR
            && l.text().contains("PHP Fatal error:  Allowed memory size")),
        "{logs:?}"
    );
    // memory_limit is per request: the next request has the default again.
    assert_eq!(execute(&msg("no_content")), Ok(HandlerResult::NoContent));
}

fn fatal_error_compile_and_parse() {
    match execute(&msg("fatal_redeclare")) {
        Err(ExecutionError::FatalError(m)) => {
            assert!(m.contains("Cannot redeclare"), "{m}")
        }
        other => panic!("expected FatalError, got {other:?}"),
    }
    match execute(&msg("fatal_parse")) {
        Err(ExecutionError::FatalError(m)) => {
            assert!(m.contains("syntax error"), "{m}");
            assert!(m.contains("parse_error.inc"), "{m}");
        }
        other => panic!("expected FatalError, got {other:?}"),
    }
}

fn soft_timeout() {
    let start = Instant::now();
    assert_eq!(
        execute(&msg("soft_timeout")),
        Err(ExecutionError::SoftTimeout)
    );
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(900) && elapsed < Duration::from_secs(5),
        "{elapsed:?}"
    );
    let errors = error_logs(&take_logs());
    assert!(
        errors
            .iter()
            .any(|e| e.contains("Maximum execution time of 1 second exceeded")),
        "{errors:?}"
    );
    // The thread is fine afterwards, and the timer was reset.
    assert_eq!(execute(&msg("no_content")), Ok(HandlerResult::NoContent));
    assert_eq!(
        execute(&msg("soft_timeout")),
        Err(ExecutionError::SoftTimeout)
    );
    assert!(execute(&msg("retry_later")).is_ok());
}

fn result_then_exit() {
    assert_eq!(
        execute(&msg("result_then_exit")),
        Ok(HandlerResult::RetryLater {
            reason: "set before exit".into()
        })
    );
    assert!(!take_logs().iter().any(|l| l.text().contains("unreachable")));
    assert_eq!(execute(&msg("exit_only")), Err(ExecutionError::NoResult));
}

fn shutdown_function_result_counts() {
    assert_eq!(
        execute(&msg("shutdown_result")),
        Ok(HandlerResult::RetryLater {
            reason: "from shutdown function for shutdown_result".into()
        })
    );
}

fn destructor_result_after_outcome() {
    assert_eq!(
        execute(&msg("destructor_result")),
        Ok(HandlerResult::RetryLater {
            reason: "before destructors".into()
        })
    );
    let errors = error_logs(&take_logs());
    assert!(
        errors.iter().any(|e| e.contains("LogicException")
            && e.contains("called after the request's outcome was determined")),
        "{errors:?}"
    );
}

fn binary_round_trip() {
    let nasty: Vec<u8> = vec![0x00, b'a', 0xff, 0xfe, 0x00, 0xc3, 0x28, b'\n', 0x80, 0x00];
    let m = InboundMessage {
        topic: "echo_back".into(),
        delivery_topic: "echo_back.retry.10s".into(),
        partition: 7,
        offset: 9_007_199_254_740_993, // > 2^53: must not go through a float
        timestamp_ms: Some(1_760_000_000_123),
        key: Some(nasty.clone()),
        value: Some([nasty.as_slice(), b"tail"].concat()),
        headers: vec![
            ("dup".into(), Some(b"first\0".to_vec())),
            ("other".into(), None),
            ("dup".into(), Some(vec![0xff, 0x00])),
            ("empty".into(), Some(vec![])),
            ("dup".into(), Some(b"third".to_vec())),
            ("caf\u{e9}".into(), Some(nasty.clone())),
        ],
        attempt: 3,
    };
    let msgs = content(execute(&m));
    assert_eq!(msgs.len(), 2);

    // Way back: byte-identical, header order and duplicates preserved.
    assert_eq!(msgs[0].topic, "echo");
    assert_eq!(msgs[0].key, m.key);
    assert_eq!(msgs[0].value, m.value);
    assert_eq!(msgs[0].headers, m.headers);

    // Way in: what PHP saw, hex-encoded by PHP.
    let meta: Value = serde_json::from_slice(msgs[1].value.as_deref().unwrap()).unwrap();
    let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    let headers_hex: Vec<Value> = m
        .headers
        .iter()
        .map(|(k, v)| json!([hex(k.as_bytes()), v.as_deref().map(hex)]))
        .collect();
    assert_eq!(
        meta,
        json!({
            "keys": ["topic", "delivery_topic", "partition", "offset", "timestamp", "key",
                     "value", "headers", "attempt"],
            "topic": "echo_back",
            "delivery_topic": "echo_back.retry.10s",
            "partition": 7,
            "offset": 9_007_199_254_740_993_i64,
            "timestamp": 1_760_000_000_123_i64,
            "attempt": 3,
            "key_hex": hex(&nasty),
            "value_hex": hex(m.value.as_deref().unwrap()),
            "headers_hex": headers_hex,
            "headers_is_list": true,
            "same_on_second_call": true,
        })
    );
}

fn null_and_empty_round_trip() {
    let mut m = msg("echo_back");
    m.key = None;
    m.value = Some(vec![]);
    let msgs = content(execute(&m));
    assert_eq!(msgs[0].key, None);
    assert_eq!(msgs[0].value, Some(vec![]));
    assert_eq!(msgs[0].headers, vec![]);
    let meta: Value = serde_json::from_slice(msgs[1].value.as_deref().unwrap()).unwrap();
    assert_eq!(meta["key_hex"], Value::Null);
    assert_eq!(meta["value_hex"], json!(""));
    assert_eq!(meta["timestamp"], Value::Null);
    assert_eq!(meta["headers_hex"], json!([]));
}

fn output_and_errors_reach_log() {
    assert_eq!(execute(&msg("logging")), Ok(HandlerResult::NoContent));
    let logs = take_logs();
    let stdout: Vec<(k4p_log_level, String)> = logs
        .iter()
        .filter(|l| l.source == "stdout")
        .map(|l| (l.level, l.text()))
        .collect();
    let info = k4p_log_level::K4P_LOG_INFO;
    assert_eq!(
        stdout,
        vec![
            (info, "hello from php".to_owned()),
            (info, "two".to_owned()),
            (info, "lines joined".to_owned()),
            (info, "no trailing newline".to_owned()),
        ],
        "{logs:?}"
    );

    let errors: Vec<(k4p_log_level, String)> = logs
        .iter()
        .filter(|l| l.source == "error")
        .map(|l| (l.level, l.text()))
        .collect();
    let warn = k4p_log_level::K4P_LOG_WARN;
    let has = |level: k4p_log_level, needle: &str| {
        errors
            .iter()
            .any(|(l, m)| *l == level && m.contains(needle))
    };
    assert!(
        has(
            warn,
            "PHP Warning:  Undefined variable $k4p_undefined_variable"
        ),
        "{errors:?}"
    );
    assert!(has(warn, "PHP Notice:  a user notice"), "{errors:?}");
    assert!(has(warn, "PHP Warning:  a user warning"), "{errors:?}");
    assert!(has(warn, "an error_log() line"), "{errors:?}");
    assert!(
        !errors.iter().any(|(_, m)| m.contains("silenced")),
        "{errors:?}"
    );
    assert_eq!(errors.len(), 4, "{errors:?}");
    assert!(
        logs.iter()
            .all(|l| l.source == "stdout" || l.source == "error")
    );
}

fn php_sapi_is_kafka4php() {
    assert_eq!(
        json_value(execute(&msg("sapi_name"))),
        json!({
            "PHP_SAPI": "kafka4php",
            "php_sapi_name": "kafka4php",
            "is_kafka4php": true,
            "extension_loaded": true,
            "zts": true,
            "script_filename": fixtures().join("entry.php"),
            // SAPI defaults
            "display_errors": "0",
            "log_errors": "1",
            // from the runtime's INI overrides
            "max_execution_time": "1",
            "k4p_test_ini": "13",
        })
    );
}

fn request_isolation() {
    let expected = json!({
        "global": false,
        "const": false,
        "static_var": 1,
        "static_prop": 1,
        "static_cache": 0,
        "ini": "13",
        "included": 2,
        "output_level": 0,
        "error_handler_set": false,
    });
    for _ in 0..4 {
        assert_eq!(json_value(execute(&msg("isolation"))), expected);
    }
}

fn headers_are_noops() {
    assert_eq!(json_value(execute(&msg("headers"))), json!([]));
    let logs = take_logs();
    assert!(error_logs(&logs).is_empty(), "{logs:?}");
}

fn execute_without_thread_init_is_bootstrap_error() {
    let outcome = std::thread::spawn(|| execute(&msg("no_content")))
        .join()
        .unwrap();
    match outcome {
        Err(ExecutionError::Bootstrap(m)) => assert!(m.contains("k4p_thread_init"), "{m}"),
        other => panic!("expected Bootstrap, got {other:?}"),
    }
}

fn second_thread_init_and_shutdown() {
    // A fresh PHP thread works and can be torn down (S3 recycles threads this way).
    let outcome = std::thread::spawn(|| {
        assert!(k4p_php_sys::thread_init());
        let a = execute(&msg("retry_later"));
        let b = execute(&msg("no_content"));
        assert!(k4p_php_sys::thread_shutdown());
        (a, b)
    })
    .join()
    .unwrap();
    assert!(outcome.0.is_ok() && outcome.1 == Ok(HandlerResult::NoContent));
}

fn main() {
    let config = RuntimeConfig {
        entrypoint: fixtures().join("entry.php"),
        ini_file: None,
        ini_entries: "max_execution_time=1\nprecision=13\n".into(),
    };
    let cases: &[Case] = &[
        ("no_content", no_content),
        ("content_multiple_messages", content_multiple_messages),
        ("fail_with_and_without_class", fail_with_and_without_class),
        ("retry_later", retry_later),
        ("no_result", no_result),
        (
            "invalid_result_uncaught_value_error",
            invalid_result_uncaught_value_error,
        ),
        ("invalid_result_caught", invalid_result_caught),
        ("uncaught_exception", uncaught_exception),
        (
            "user_exception_handler_handles",
            user_exception_handler_handles,
        ),
        ("fatal_error_memory_limit", fatal_error_memory_limit),
        (
            "fatal_error_compile_and_parse",
            fatal_error_compile_and_parse,
        ),
        ("soft_timeout", soft_timeout),
        ("result_then_exit", result_then_exit),
        (
            "shutdown_function_result_counts",
            shutdown_function_result_counts,
        ),
        (
            "destructor_result_after_outcome",
            destructor_result_after_outcome,
        ),
        ("binary_round_trip", binary_round_trip),
        ("null_and_empty_round_trip", null_and_empty_round_trip),
        ("output_and_errors_reach_log", output_and_errors_reach_log),
        ("php_sapi_is_kafka4php", php_sapi_is_kafka4php),
        ("request_isolation", request_isolation),
        ("headers_are_noops", headers_are_noops),
        (
            "execute_without_thread_init_is_bootstrap_error",
            execute_without_thread_init_is_bootstrap_error,
        ),
        (
            "second_thread_init_and_shutdown",
            second_thread_init_and_shutdown,
        ),
    ];
    common::run(config, cases)
}
