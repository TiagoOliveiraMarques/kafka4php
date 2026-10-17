//! Per-request cost of the kafka4php SAPI on one PHP thread (PLAN.md S4; results in
//! DESIGN.md §5.7).
//!
//! Scenarios (fixture app in `benches/app/`):
//! - `empty`: an entrypoint that only calls `result()`;
//! - `kernel`: a typical entrypoint: the composer autoloader (optimized), the real
//!   `kafka4php/framework` (`php/`) `Kernel::run` and `Router`, and a trivial handler
//!   (decode JSON, return `NoContent`); ~15 classes loaded per request;
//! - `preload`: `kernel` with every app and framework class in `opcache.preload`.
//!
//! each with OPcache `off`, `on` (PHP defaults) and `on-vt0` (`validate_timestamps=0`):
//! see [`ROWS`]. PHP 8.4 runs only the `off` rows (OPcache is inactive under our SAPI).
//!
//!   scripts/dev.sh cargo bench -p k4p-php-sys --bench request_cost
//!
//! PHP can start once per process, so the driver re-runs this binary [`RUNS`] times per
//! row (`K4P_BENCH_ROW=<scenario>/<opcache>`), each child running the measurement as
//! the single case of the shared test harness, and prints a Markdown table of the
//! median runs, preceded by the system it ran on. The driver first copies the app to a
//! temp dir (off the slow `/work` bind mount of Docker Desktop) and `composer update`s
//! it there, offline, once for all children (`K4P_BENCH_APP`). Env: `K4P_BENCH_REQUESTS`
//! (measured requests per row, default 20,000).

#[path = "../tests/common/mod.rs"]
mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use k4p_core::HandlerResult;
use k4p_core::testing::inbound;
use k4p_php_sys::{RuntimeConfig, execute};

const WARMUP: usize = 1_000;
/// Runs per row; the table shows the median run (by req/s).
const RUNS: usize = 3;
const RESULT_PREFIX: &str = "k4p-bench-result ";

/// One table row: a scenario with an OPcache configuration.
struct Row {
    scenario: &'static str,
    opcache: &'static str,
    /// INI lines; `{app}` is replaced by the app's directory.
    ini: &'static str,
}

/// `preload` rows' INI (`preload_user`: required when preloading as root, as in the
/// toolchain container), plus `$extra`.
macro_rules! preload_ini {
    ($extra:literal) => {
        concat!(
            "opcache.preload={app}/preload.php\nopcache.preload_user=root\n",
            $extra
        )
    };
}

const ROWS: &[Row] = &[
    Row {
        scenario: "empty",
        opcache: "off",
        ini: "opcache.enable=0\n",
    },
    Row {
        scenario: "empty",
        opcache: "on",
        ini: "",
    },
    Row {
        scenario: "empty",
        opcache: "on-vt0",
        ini: "opcache.validate_timestamps=0\n",
    },
    Row {
        scenario: "kernel",
        opcache: "off",
        ini: "opcache.enable=0\n",
    },
    Row {
        scenario: "kernel",
        opcache: "on",
        ini: "",
    },
    Row {
        scenario: "kernel",
        opcache: "on-vt0",
        ini: "opcache.validate_timestamps=0\n",
    },
    Row {
        scenario: "preload",
        opcache: "on",
        ini: preload_ini!(""),
    },
    Row {
        scenario: "preload",
        opcache: "on-vt0",
        ini: preload_ini!("opcache.validate_timestamps=0\n"),
    },
];

impl Row {
    fn id(&self) -> String {
        format!("{}/{}", self.scenario, self.opcache)
    }

    fn entrypoint(&self) -> &'static str {
        if self.scenario == "empty" {
            "empty.php"
        } else {
            "consumer.php"
        }
    }
}

/// What one run measured.
struct Stats {
    req_per_s: f64,
    mean_us: f64,
    p50_us: f64,
    p99_us: f64,
}

impl Stats {
    fn to_line(&self) -> String {
        format!(
            "{RESULT_PREFIX}{} {} {} {}",
            self.req_per_s, self.mean_us, self.p50_us, self.p99_us
        )
    }

    fn from_line(line: &str) -> Option<Self> {
        let v: Vec<f64> = line
            .strip_prefix(RESULT_PREFIX)?
            .split(' ')
            .map(|x| x.parse().ok())
            .collect::<Option<_>>()?;
        let [req_per_s, mean_us, p50_us, p99_us] = v[..] else {
            return None;
        };
        Some(Self {
            req_per_s,
            mean_us,
            p50_us,
            p99_us,
        })
    }
}

// ---------------------------------------------------------------------------------
// Driver: RUNS child processes per row.

fn php_version_id() -> u32 {
    let out = Command::new(std::env::var("PHP_CONFIG").unwrap_or_else(|_| "php-config".into()))
        .arg("--vernum")
        .output()
        .expect("php-config --vernum");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .expect("PHP version number")
}

/// Runs `row` once in a child process on `app`; `None` (and the child's output) if it
/// failed.
fn run_child(exe: &Path, app: &Path, row: &Row) -> Option<Stats> {
    let out = Command::new(exe)
        .env("K4P_BENCH_ROW", row.id())
        .env("K4P_BENCH_APP", app)
        .output()
        .expect("run benchmark child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stats = stdout
        .lines()
        .find_map(Stats::from_line)
        .filter(|_| out.status.success());
    if stats.is_none() {
        eprintln!(
            "benchmark {} failed ({}):\n{stdout}\n{}",
            row.id(),
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
    }
    stats
}

/// The first `key: value` line of a `/proc`-style file whose key is `key`.
fn proc_field(path: &str, key: &str) -> String {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| {
            s.lines()
                .filter_map(|l| l.split_once(':'))
                .find(|(k, _)| k.trim() == key)
                .map(|(_, v)| v.trim().to_owned())
        })
        .unwrap_or_else(|| "?".into())
}

fn first_line(path: &str) -> String {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.lines().next().map(str::to_owned))
        .unwrap_or_else(|| "?".into())
}

/// The system the numbers come from, as seen from inside the container.
fn print_system() {
    let kib = |v: String| {
        v.trim_end_matches("kB")
            .trim()
            .parse::<f64>()
            .map_or("?".into(), |k| format!("{:.1} GiB", k / 1024.0 / 1024.0))
    };
    let cpus = std::thread::available_parallelism().map_or(0, |n| n.get());
    println!("System:");
    println!(
        "- CPU: {}, {cpus} CPUs available",
        proc_field("/proc/cpuinfo", "model name")
    );
    println!(
        "- Memory: {} total, {} available",
        kib(proc_field("/proc/meminfo", "MemTotal")),
        kib(proc_field("/proc/meminfo", "MemAvailable"))
    );
    println!("- Kernel: {}", first_line("/proc/sys/kernel/osrelease"));
    println!(
        "- Transparent huge pages: {}",
        first_line("/sys/kernel/mm/transparent_hugepage/enabled")
    );
}

fn driver() {
    let version = php_version_id();
    println!(
        "\nPer-request cost, one PHP thread, PHP {}.{}.{} (sequential requests, Rust release build)\n",
        version / 10_000,
        version / 100 % 100,
        version % 100
    );
    print_system();
    println!();
    println!("| scenario | OPcache | req/s | mean µs | p50 µs | p99 µs |");
    println!("|---|---|---:|---:|---:|---:|");
    let exe = std::env::current_exe().expect("current_exe");
    let app = prepare_app();
    let mut failed = false;
    for row in ROWS
        .iter()
        .filter(|r| version >= 80500 || r.opcache == "off")
    {
        let runs: Option<Vec<Stats>> = (0..RUNS).map(|_| run_child(&exe, &app, row)).collect();
        let Some(mut runs) = runs else {
            failed = true;
            continue;
        };
        runs.sort_by(|a, b| a.req_per_s.total_cmp(&b.req_per_s));
        let s = &runs[RUNS / 2];
        println!(
            "| {} | {} | {:.0} | {:.1} | {:.1} | {:.1} |",
            row.scenario, row.opcache, s.req_per_s, s.mean_us, s.p50_us, s.p99_us
        );
    }
    println!();
    let _ = std::fs::remove_dir_all(app.parent().expect("temp root"));
    if failed {
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------------
// Child: one run of one row.

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create dir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("dir entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).expect("copy file");
        }
    }
}

/// Runs `composer <args>` in `dir`.
fn composer(dir: &Path, args: &[&str]) {
    let status = Command::new("composer")
        .args(args)
        .args(["--no-interaction", "--quiet", "--working-dir"])
        .arg(dir)
        .env("COMPOSER_ALLOW_SUPERUSER", "1")
        .status()
        .expect("run composer (in the toolchain container)");
    assert!(status.success(), "composer {args:?} failed");
}

/// `<temp>/app`: a copy of `benches/app` with its dependencies installed and an
/// optimized autoloader. Its only dependency, the real `kafka4php/framework`, comes
/// from `<temp>/framework` (the app's composer.json has that path repository and no
/// Packagist, so this is offline): `php/composer.json` and `php/src/`, all a dependency
/// needs — not `php/vendor/`, slow to copy from the bind mount.
fn prepare_app() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = std::env::temp_dir().join(format!("k4p-bench-{}", std::process::id()));
    let (php, framework, app) = (
        manifest.join("../../php"),
        root.join("framework"),
        root.join("app"),
    );
    copy_dir(&php.join("src"), &framework.join("src"));
    std::fs::copy(php.join("composer.json"), framework.join("composer.json"))
        .expect("copy php/composer.json");
    copy_dir(&manifest.join("benches/app"), &app);
    composer(&app, &["update", "--optimize-autoloader", "--no-dev"]);
    app
}

fn micros(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

/// The harness case: warm up, measure, print the [`Stats`] line.
fn measure() {
    let requests: usize = std::env::var("K4P_BENCH_REQUESTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);
    let mut msg = inbound("orders", 0, 0);
    let mut one = |i: usize| {
        msg.offset = i as i64;
        msg.value = Some(format!(r#"{{"order_id":{i},"amount":12.5,"currency":"EUR"}}"#).into());
        let started = Instant::now();
        let outcome = execute(&msg);
        let took = started.elapsed();
        assert_eq!(outcome, Ok(HandlerResult::NoContent), "request {i}");
        took
    };
    for i in 0..WARMUP {
        one(i);
    }
    let started = Instant::now();
    let mut times: Vec<Duration> = (0..requests).map(|i| one(WARMUP + i)).collect();
    let total = started.elapsed();

    let logs = common::take_logs();
    assert!(logs.is_empty(), "PHP logged: {logs:?}");
    times.sort_unstable();
    let pct = |p: f64| micros(times[((times.len() - 1) as f64 * p) as usize]);
    let stats = Stats {
        req_per_s: requests as f64 / total.as_secs_f64(),
        mean_us: micros(total) / requests as f64,
        p50_us: pct(0.50),
        p99_us: pct(0.99),
    };
    println!("{}", stats.to_line());
}

/// One run of row `id` on the app the driver prepared.
fn child(id: &str, app: &Path) -> ! {
    let row = ROWS
        .iter()
        .find(|r| r.id() == id)
        .unwrap_or_else(|| panic!("unknown K4P_BENCH_ROW {id}"));
    let app_dir = app.to_str().expect("UTF-8 temp dir");
    let config = RuntimeConfig {
        entrypoint: app.join(row.entrypoint()),
        ini_file: None,
        // The copied app is brand new: without this OPcache would not cache it for 2 s.
        ini_entries: format!(
            "opcache.file_update_protection=0\n{}",
            row.ini.replace("{app}", app_dir)
        ),
    };
    common::run(config, &[("measure", measure)])
}

fn main() {
    match std::env::var("K4P_BENCH_ROW") {
        Ok(id) => {
            let app = std::env::var_os("K4P_BENCH_APP").expect("K4P_BENCH_APP");
            child(&id, Path::new(&app))
        }
        Err(_) => driver(),
    }
}
