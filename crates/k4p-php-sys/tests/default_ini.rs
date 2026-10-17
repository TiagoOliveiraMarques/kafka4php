//! The host's default INI (PLAN.md S4, DESIGN.md §5.1.2): applied below the user's
//! ini file, the scan dir and the INI overrides.

mod common;

use common::{Case, fixtures};
use k4p_core::HandlerResult;
use k4p_core::testing::inbound;
use k4p_php_sys::{RuntimeConfig, execute};
use serde_json::{Value, json};

fn effective() -> Value {
    match execute(&inbound("default_ini", 0, 0)) {
        Ok(HandlerResult::Content(msgs)) => {
            serde_json::from_slice(msgs[0].value.as_deref().expect("value")).expect("JSON")
        }
        other => panic!("expected Content, got {other:?}"),
    }
}

fn defaults_and_precedence() {
    let e = effective();
    let ini = &e["ini"];
    // The host's defaults.
    assert_eq!(ini["display_errors"], "0", "{e}");
    assert_eq!(ini["log_errors"], "1", "{e}");
    assert_eq!(ini["html_errors"], "0", "{e}");
    assert_eq!(ini["register_argc_argv"], "0", "{e}");
    assert_eq!(ini["output_buffering"], "0", "{e}");
    assert_eq!(ini["opcache.enable"], "1", "{e}");
    let disabled: Vec<&str> = ini["disable_functions"]
        .as_str()
        .expect("disable_functions")
        .split(',')
        .collect();
    for f in [
        "pcntl_fork",
        "pcntl_exec",
        "pcntl_signal",
        "pcntl_async_signals",
        "pcntl_alarm",
    ] {
        assert!(disabled.contains(&f), "{f} not disabled: {e}");
    }
    assert!(disabled.iter().all(|f| f.starts_with("pcntl_")), "{e}");
    // ini_file beats the defaults (memory_limit 128M), ini_entries beat both
    // (max_execution_time 30), and the scan dir is still read.
    assert_eq!(ini["memory_limit"], "64M", "{e}");
    assert_eq!(ini["max_execution_time"], "5", "{e}");
    assert_eq!(e["sodium_loaded"], json!(true), "{e}");
}

fn main() {
    let dir = fixtures().join("default_ini");
    let config = RuntimeConfig {
        entrypoint: dir.join("entry.php"),
        ini_file: Some(dir.join("user.ini")),
        ini_entries: "max_execution_time=5\n".into(),
    };
    let cases: &[Case] = &[("defaults_and_precedence", defaults_and_precedence)];
    common::run(config, cases)
}
