# kafka4php — Design

> **Status:** v1 design, authoritative. This document is the source of truth for how
> kafka4php works. If you change behaviour, change this document in the same commit.
> If the code and this document disagree, one of them is a bug — find out which.

---

## 1. What kafka4php is

kafka4php is a **native (Rust + C) Kafka consumer host that runs PHP handlers
in-process, one isolated PHP request per Kafka message** — the same lifecycle
PHP-FPM gives an HTTP request, applied to Kafka messages.

### 1.1 The problem

Today, consuming Kafka from PHP means a long-running `php-cli` process that owns the
Kafka consumer and loops forever. This is fragile:

- Userland state (static caches, ORM identity maps, container singletons) and
  extension leaks accumulate across messages. Long-running PHP is not something the
  ecosystem is built for.
- Restarting the process to contain leaks makes the consumer leave and rejoin its
  group, triggering a rebalance.
- A slow handler can exceed `max.poll.interval.ms` and get the consumer kicked out of
  the group.

### 1.2 The approach

- A **Rust host** owns everything Kafka: group membership, polling, offsets,
  producing, retries, dead-lettering.
- **PHP is embedded** (libphp, ZTS build) in the same process via a **custom SAPI**
  written in C. A fixed pool of OS threads each run a PHP interpreter context.
- Each Kafka message is executed as **one PHP request**:
  `php_request_startup()` → run entry script → `php_request_shutdown()`. The Zend
  memory manager frees all request memory afterwards, so userland state cannot leak
  between messages — exactly like FPM.
- The PHP handler returns a **result object** (`NoContent`, `Content`, `Fail`,
  `RetryLater`) and the host performs the corresponding Kafka actions.

### 1.3 What is novel

Embedding PHP with a per-request lifecycle is proven (mod_php, NGINX Unit,
FrankenPHP). Using it as the execution model for a Kafka consumer is not done by any
existing tool: RoadRunner and php-cli consumers use long-running PHP workers.
**FrankenPHP's C glue (`frankenphp.c`, MIT) is the primary reference implementation**
for embedding ZTS PHP in a threaded native host — read it before writing SAPI code.

### 1.4 Non-goals for v1

- Ordering modes other than per-partition (per-key, unordered) — config has the
  `ordering` field so they can be added later.
- Exactly-once / Kafka transactions.
- Deserialization (Avro, Protobuf, schema registry). PHP receives raw bytes.
- OpenTelemetry / baggage propagation. Headers pass through untouched, so this can be
  added later without protocol changes.
- Metrics endpoint (Prometheus). Logs + health endpoint only.
- Batch delivery (N messages per PHP request).
- Non-Linux platforms. Distribution is a Docker image.

---

## 2. Core decisions (and why)

| # | Decision | Rationale |
|---|---|---|
| D1 | Rust host owns the consumer; PHP never touches Kafka | PHP can crash, time out, or leak without affecting group membership. |
| D2 | Embed libphp in-process with a custom SAPI (not FastCGI, not php-cli workers) | Per-message isolation like FPM, without FPM. php-cli workers and FPM bridges exist already; this is the novel part. |
| D3 | **Threads, not processes** (ZTS PHP) | Target deployment is one process per one-or-two consumers; a process tree per consumer is too heavy. Opcache is naturally shared within the process. |
| D4 | Per-partition ordering only (v1) | Simple and correct. At most one message in flight per partition, so committed offsets are always contiguous — no out-of-order offset tracking needed. |
| D5 | At-least-once delivery throughout | Duplicates are the handler's problem. Handlers must be idempotent. Simplifies rebalances and crashes. |
| D6 | Static group membership (`group.instance.id`) is a first-class config option | Thread mode means a crash kills the consumer. Static membership lets a restarted container reclaim its partitions without a rebalance. |
| D7 | Raw bytes to PHP; no deserialization in the host | Keep the host format-agnostic. |
| D8 | Distribution is a Docker image built on official `php:*-zts` images | Users keep `docker-php-ext-install` / `pecl install` for extensions, all built ZTS against the same libphp. |
| D9 | C for SAPI glue, Rust for everything else; FFI boundary is our own small C header | PHP headers are macro-heavy and hostile to bindgen. We bindgen only our own header. |

---

## 3. Architecture

```
┌───────────────────────────────── kafka4php process ───────────────────────────────────┐
│                                                                                        │
│  tokio runtime (k4p-host)                         PHP thread pool (k4p-php)           │
│  ┌──────────────────────────────┐                 ┌────────────────────────────────┐  │
│  │ Consumer(s) (rust-rdkafka)   │                 │ thread 1: ts_resource          │  │
│  │  - group membership          │  InboundMessage │   loop:                        │  │
│  │  - pause/resume partitions   │ ──────────────► │     php_request_startup        │  │
│  │ Dispatcher                   │                 │     run entrypoint.php         │  │
│  │  - per-partition queues      │ ◄────────────── │     php_request_shutdown       │  │
│  │  - 1 in-flight per partition │  Outcome        │ thread 2 ...                   │  │
│  │ Result actions               │                 │ thread N ...                   │  │
│  │  - produce Content/DLQ/retry │                 │ watchdog: hard timeouts,       │  │
│  │  - store offsets             │                 │   abandon + replace threads    │  │
│  │ Producer (shared)            │                 └───────────────┬────────────────┘  │
│  └──────────────────────────────┘                                 │ C ABI (k4p_sapi.h) │
│                                                    ┌──────────────▼────────────────┐  │
│  kafka4php binary: config, logging, signals,       │ k4p-php-sys: custom SAPI in C │  │
│  health endpoint, wiring                           │ "kafka4php" + native functions│  │
│                                                    │ linked against libphp (ZTS)   │  │
│                                                    └───────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.1 Repository layout

```
kafka4php/
├── AGENTS.md / CLAUDE.md        # agent entry points → this doc + PLAN.md
├── Cargo.toml                   # workspace
├── crates/
│   ├── k4p-core/                # shared types + Executor trait. No PHP, no Kafka deps.
│   ├── k4p-php-sys/             # C SAPI (csrc/), our C header, build.rs, raw FFI
│   ├── k4p-php/                 # safe wrapper: runtime init, thread pool, watchdog. impl Executor
│   ├── k4p-host/                # Kafka engine: consumers, dispatcher, result actions. Uses Executor
│   └── kafka4php/               # binary: config, CLI, logging, signals, health, wiring
├── php/                         # composer package `kafka4php/framework` (userland)
│   ├── src/  tests/  stubs/
├── docker/
│   ├── dev.Dockerfile           # toolchain image: ZTS+embed PHP, Rust, cmake, php-config
│   ├── Dockerfile               # production image
│   └── compose.test.yml         # Kafka (KRaft) for integration/E2E tests
├── examples/basic/              # example PHP app + kafka4php.toml
├── tests/e2e/                   # end-to-end scenarios against the production image
└── docs/
    ├── DESIGN.md                # this file
    └── PLAN.md                  # implementation plan & status
```

The crate split exists so **`k4p-host` and `k4p-php` never depend on each other**.
Both depend only on `k4p-core`. The host is tested with a fake `Executor`; the PHP
pool is tested without Kafka. Only the binary wires them together.

---

## 4. Shared contract: `k4p-core`

These types are the contract between the Kafka side and the PHP side. Changing them
affects every crate — do so deliberately and update this section.

```rust
pub type Headers = Vec<(String, Option<Vec<u8>>)>; // ordered; duplicate keys allowed (Kafka semantics)

/// A message handed to PHP.
pub struct InboundMessage {
    pub topic: String,            // logical topic: original topic, even when delivered via a retry topic
    pub delivery_topic: String,   // topic it was actually consumed from (== topic unless retry)
    pub partition: i32,           // partition of delivery_topic
    pub offset: i64,              // offset in delivery_topic
    pub timestamp_ms: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Headers,         // user headers only; `k4p-*` headers are stripped
    pub attempt: u32,             // 1 on first delivery, incremented on each retry
}

pub struct OutboundMessage {
    pub topic: String,
    pub partition: Option<i32>,   // None = default partitioner (by key)
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Headers,
}

/// What the handler decided.
pub enum HandlerResult {
    NoContent,                                       // commit, produce nothing
    Content(Vec<OutboundMessage>),                   // produce all (acks=all), then commit. Empty vec ≡ NoContent
    Fail { reason: String, error_class: Option<String> }, // send original to DLQ, then commit
    RetryLater { reason: String },                   // send original to next retry tier, then commit
}

/// Why no HandlerResult could be obtained.
pub enum ExecutionError {
    NoResult,                          // script finished without calling result()
    InvalidResult(String),             // result() called with a malformed array
    UncaughtException { class: String, message: String },
    FatalError(String),                // E_ERROR & friends, incl. memory_limit exhaustion
    SoftTimeout,                       // max_execution_time hit (PHP-level timer)
    HardTimeout,                       // watchdog fired; thread abandoned
    Bootstrap(String),                 // runtime/thread could not execute at all
}

pub type Outcome = Result<HandlerResult, ExecutionError>;

pub trait Executor: Send + Sync + 'static {
    /// Execute one message as one PHP request. Resolves when the request finishes
    /// or the hard timeout fires. Must not be called more than `capacity()` times
    /// concurrently; the caller (dispatcher) enforces this.
    fn execute(&self, msg: InboundMessage) -> BoxFuture<'static, Outcome>;
    /// Number of messages that may execute concurrently (= live PHP threads).
    fn capacity(&self) -> usize;
}
```

Implementation notes (`crates/k4p-core`):

- All types derive `Debug, Clone, PartialEq, Eq`. `ExecutionError` implements
  `std::error::Error`; its `Display` string (e.g. `uncaught exception
  RuntimeException: db down`) is used as the retry reason.
- `BoxFuture<'a, T>` is defined in `k4p-core` itself
  (`Pin<Box<dyn Future<Output = T> + Send + 'a>>`, same shape as `futures`'),
  so the crate has no runtime dependencies.
- `Executor` is implemented for `Arc<E>` (including `Arc<dyn Executor>`).
- Feature `testing` (test-only, pulls in `tokio` for timers) provides
  `k4p_core::testing::FakeExecutor`: outcomes scripted per record
  (`delivery_topic`, `partition`, `offset`), by closure, or a default; optional
  per-message delay; records received messages in call order; reports a fixed
  `capacity` and tracks current / maximum in-flight executions. Also
  `testing::inbound(topic, partition, offset)` to build test messages.

**Mapping `ExecutionError` → action (host policy, v1):** every `ExecutionError` is
treated as `RetryLater { reason: <error description> }`. A message that repeatedly
crashes or times out therefore reaches `max_attempts` and lands in the DLQ with
`k4p-failure-kind: max_attempts_exceeded`. This is what keeps poison messages from
blocking a partition forever.

---

## 5. The PHP side

### 5.1 Runtime lifecycle (C SAPI, `k4p-php-sys`)

PHP is built **ZTS** with `--enable-embed --enable-zend-max-execution-timers`.

| Phase | Where | What happens |
|---|---|---|
| Runtime init | the runtime thread (`k4p-php`: `php-runtime`, §5.5), once, **before** worker threads | `php_tsrm_startup()`, `zend_signal_startup()` (if applicable), `sapi_startup(&k4p_sapi_module)`, set INI (file + overrides), `php_module_startup(&k4p_sapi_module, &k4p_native_module)`. Opcache initialises here; its shared memory is shared by all threads. |
| Thread init | each worker thread | `ts_resource(0)` — allocates this thread's interpreter globals. |
| Request | worker thread, per message | Set thread-local current message → `php_request_startup()` → `zend_try { execute entrypoint } zend_catch {} zend_end_try()` → run `register_shutdown_function()` callbacks → classify outcome, copy result out of request memory → `php_request_shutdown()`. |
| Thread shutdown | same worker thread | `ts_free_thread()`. |
| Runtime shutdown | the runtime thread, after all threads joined | `php_module_shutdown()`, `sapi_shutdown()`, `tsrm_shutdown()`. |

The same calls work unchanged on PHP 8.4 and 8.5 (verified against both headers).

**Executing the entrypoint.** The SAPI does not call `php_execute_script()`: it
mirrors it (honouring `auto_prepend_file` / `auto_append_file`, no `chdir`, like the
CLI and embed SAPIs) and mirrors `zend_execute_script()`, so that it can record the
class and message of an uncaught exception *before* PHP reports and discards it.
User exception handlers (`set_exception_handler`) run first, exactly as in PHP; an
exception they handle is not "uncaught". Then, still inside the request,
`register_shutdown_function()` callbacks run (they would be the first thing
`php_request_shutdown()` does): they are part of the script, so their `result()`
counts. Destructors of objects alive at request end run in `php_request_shutdown()`,
after classification. Because these two functions are copies of PHP internals,
**re-check them against `main/main.c` and `Zend/zend.c` on every new PHP minor
version** (the sapi test suite covers their observable behaviour).

**Request info.** `SG(server_context)` points at the thread's state;
`SG(request_info)`: no method/query/URI, `path_translated` = entrypoint,
`no_headers = 1` (set after `php_request_startup()`, which resets it);
`SAPI_OPTION_NO_CHDIR`. If `php_request_startup()` fails, the outcome is
`Bootstrap` and the thread refuses all further work (like FPM/FrankenPHP treat it);
the pool should replace it.

#### 5.1.1 The C ABI (`crates/k4p-php-sys/csrc/k4p_sapi.h`)

Plain C types only; bindgen sees only this header. All functions are C-callable from
Rust through `k4p_php_sys::ffi`. The safe public API of `k4p_php_sys` is
`RuntimeConfig`, `InitError`, `runtime_init` / `runtime_shutdown` (unsafe),
`thread_init` / `thread_shutdown` and `execute(&InboundMessage) -> Outcome`; the C
conversions behind `execute` are private. The C side is the authoritative validator
of handler results: Rust trusts a filled outcome (non-UTF-8 topics or header names
are only `debug_assert`ed) and treats an unknown outcome kind as a bug (`unreachable!`).

Sources (`csrc/`): `k4p_sapi.h` (the ABI), `k4p_internal.h` (private declarations),
`k4p_sapi.c` (SAPI + native module, request execution, classification, lifecycle),
`k4p_result.c` (`result()` arrays → `k4p_outcome`, the only C code that knows the
§5.2 shapes), `k4p_output.c` (log sink, stdout line buffering), `k4p_buf.c` (malloc'd
buffers/outcomes, UTF-8 validation).

```c
typedef struct { const uint8_t *data; size_t len; uint8_t is_null; } k4p_buf; // bytes, NUL-safe
typedef struct { k4p_buf key; k4p_buf value; } k4p_header;                   // key never null
typedef struct { k4p_buf topic, delivery_topic; int32_t partition; int64_t offset;
                 uint8_t has_timestamp; int64_t timestamp_ms; k4p_buf key, value;
                 const k4p_header *headers; size_t headers_len; uint32_t attempt; } k4p_message;
typedef struct { k4p_buf topic; uint8_t has_partition; int32_t partition; k4p_buf key, value;
                 k4p_header *headers; size_t headers_len; } k4p_outbound;
typedef enum { K4P_OUTCOME_NO_CONTENT = 0, K4P_OUTCOME_CONTENT = 1, K4P_OUTCOME_FAIL = 2,
               K4P_OUTCOME_RETRY_LATER = 3, K4P_OUTCOME_NO_RESULT = 10,
               K4P_OUTCOME_INVALID_RESULT = 11, K4P_OUTCOME_UNCAUGHT_EXCEPTION = 12,
               K4P_OUTCOME_FATAL_ERROR = 13, K4P_OUTCOME_SOFT_TIMEOUT = 14,
               K4P_OUTCOME_BOOTSTRAP = 15 } k4p_outcome_kind;
typedef struct { k4p_outcome_kind kind; k4p_outbound *messages; size_t messages_len;
                 k4p_buf text;          // reason / error message
                 k4p_buf class_name;    // Fail.error_class / exception class
               } k4p_outcome;
typedef enum { K4P_LOG_ERROR = 1, K4P_LOG_WARN = 2, K4P_LOG_INFO = 3 } k4p_log_level;
typedef void (*k4p_log_fn)(void *user_data, k4p_log_level level, const char *source,
                           const uint8_t *msg, size_t len);
typedef struct { const char *ini_file; const char *ini_entries; const char *entrypoint;
                 k4p_log_fn log; void *log_user_data; } k4p_runtime_config;

int  k4p_runtime_init(const k4p_runtime_config *);  // K4P_OK / _ERR_INVALID_ARGUMENT / _ERR_STATE / _ERR_STARTUP
int  k4p_runtime_shutdown(void);
int  k4p_thread_init(void);
int  k4p_thread_shutdown(void);
void k4p_execute(const k4p_message *msg, k4p_outcome *out); // always fills *out
void k4p_outcome_free(k4p_outcome *out);
```

Contract:

- `k4p_runtime_init` once per process; its thread becomes the **runtime thread**,
  which may itself execute messages without `k4p_thread_init`. PHP cannot be
  restarted: after `k4p_runtime_shutdown` (runtime thread only, after all PHP
  threads called `k4p_thread_shutdown`), init returns `K4P_ERR_STATE`. All config
  strings are copied.
- Other threads: `k4p_thread_init` (idempotent) → any number of `k4p_execute`, one at
  a time → `k4p_thread_shutdown` (refused during a request; no-op on the runtime
  thread). `k4p_execute` on a thread without init, before runtime init, re-entrantly,
  or on a broken thread yields `K4P_OUTCOME_BOOTSTRAP` instead of crashing.
- `k4p_message` is borrowed for the duration of `k4p_execute` only. Every byte in
  `k4p_outcome` is `malloc`ed by C and owned by the outcome until
  `k4p_outcome_free` (idempotent). A non-null `k4p_buf` in an outcome never has a
  NULL `data`. Topics and header names in `Content` are validated UTF-8; `text` and
  `class_name` are not guaranteed UTF-8 (Rust converts them lossily).
- **Thread-local state** (C `__thread`, never zvals): the thread's **phase**
  (`NONE` → `IDLE` → `SCRIPT` (result() accepted) → `FINISHING` → `IDLE`, or
  `BROKEN` after a failed request startup), which every entry point checks; the
  current message; and two outcome slots, each resolved when it happens:
  `result` (the converted `result()` array, or `INVALID_RESULT`) and `uncaught`
  (`UNCAUGHT_EXCEPTION`, or `INVALID_RESULT` when the uncaught exception is the
  `\ValueError` that `result()` threw). Classification is then a fixed order:
  soft timeout → `uncaught` → fatal error → `result` (`NO_RESULT` if never set). The
  runtime thread is identified by its `pthread_t`. The stdout line buffer is a
  separate `__thread` in `k4p_output.c`. Sources are built with
  `ZEND_ENABLE_STATIC_TSRMLS_CACHE`; `k4p_runtime_init` / `k4p_thread_init` run
  `ZEND_TSRMLS_CACHE_UPDATE()`.

**Log callback.** Called synchronously on the PHP thread that produced the output
(during `k4p_execute`, or during init/shutdown on the runtime thread); it must be
thread-safe, must not unwind and must not call back into the API. `msg` is not
NUL-terminated, may be invalid UTF-8, and is only valid during the call. `source` is
a static string:

| source | what | level |
|---|---|---|
| `stdout` | script output (`echo`, `print`, `var_dump`, flushed output buffers): one call per line, line terminator (`\n` / `\r\n`) stripped, empty lines dropped, lines longer than 8 KiB split in 8 KiB chunks; a trailing partial line is flushed at request end | `INFO` |
| `error` | PHP's error log via `log_message`: warnings, notices, deprecations, fatal errors, uncaught exceptions, `error_log()` — the PHP-formatted line, e.g. `PHP Warning:  … in /app/x.php on line 3` | `ERROR` for syslog level `LOG_ERR` or worse (fatal errors, uncaught exceptions, parse errors), else `WARN` |
| `sapi` | diagnostics of the SAPI itself (e.g. module startup failed) | as given |

If the user sets the `error_log` INI setting, PHP writes there instead and the
callback does not see errors.

**INI.** Precedence, lowest first: the host's default INI (§5.1.2, the SAPI's
`ini_defaults` hook) → `ini_file` (`php_ini_path_override`; NULL = PHP's normal search in
`PHP_CONFIG_FILE_PATH`, never the cwd) → the scan dir (`conf.d`, always read: that's
where `docker-php-ext-install` enables extensions, and where PHP 8.4 images load
OPcache) → `ini_entries` (`key=value` lines, raw INI syntax — not quoted by the SAPI, highest).
Anything none of these set has PHP's built-in default.

#### 5.1.2 Default INI

Compiled into the SAPI (`k4p_sapi_ini_defaults()` in `csrc/k4p_sapi.c`), applied
below every ini file and override. A user setting **replaces** a default (e.g. a
`disable_functions` in php.ini replaces the whole list below, re-enabling `pcntl_*`).

| Setting | Default | Why |
|---|---|---|
| `display_errors` | `0` | Output is not a response; errors go to the log (`log_errors`). |
| `log_errors` | `1` | Errors reach the host log through `log_message` (§5.1.1). |
| `html_errors` | `0` | Plain-text log lines. |
| `register_argc_argv` | `0` | No argv: a request is not a CLI invocation. |
| `output_buffering` | `0` | Output streams to the log line by line. |
| `memory_limit` | `128M` | Per request (Zend MM). Worst-case PHP heap ≈ `threads` × `memory_limit`, plus OPcache shared memory. |
| `max_execution_time` | `30` | The soft timeout (§5.5); keep it below `timeouts.hard`. |
| `disable_functions` | every `pcntl_*` function (34, the 8.4/8.5 list) | Signals, fork and exec belong to the host (§5.6). Names of functions that aren't installed are ignored. |
| `opcache.enable` | `1` | OPcache on (PHP 8.5; no effect on 8.4, see §5.1). |

The official images ship no `php.ini` (only `php.ini-production` /
`php.ini-development` templates, usable via `ini_file`), so everything else is PHP's
built-in default. Notable OPcache defaults, deliberately left alone:

- `opcache.validate_timestamps=1`, `opcache.revalidate_freq=2`: edited files are
  picked up within 2 s, which keeps development with a mounted code dir working.
  **Production recommendation: `opcache.validate_timestamps=0`** — the code in an
  image never changes, and it skips the periodic `stat()` of every included file (a
  small gain on a local filesystem, §5.7; larger on slow ones).
- `opcache.jit=disable`. Opt in with `opcache.jit=tracing` (PHP 8.5; verified under
  concurrent execution, §9). Handlers are usually I/O-bound, so the JIT rarely pays.
- `opcache.memory_consumption=128` (MB, shared by all threads),
  `opcache.file_update_protection=2` (files modified < 2 s ago are not cached).

**Preloading** (`opcache.preload`, PHP 8.5): the preload script runs once, inside
`php_module_startup()` on the runtime thread, before any worker thread starts; what it
declares is visible in every request without autoloading. It runs outside a kafka4php
request (`Kafka4php\Native\message()` throws `\LogicException`), its errors reach the
host log, but its *output* goes straight to the process's stdout (OPcache swaps the
SAPI's output callbacks while preloading). A failing preload script makes runtime init
fail. When the process runs as **root**, OPcache requires `opcache.preload_user`: set it
to `root`, which preloads in-process; any other user makes OPcache `fork()` a child to
preload as that user, which is not supported in this multi-threaded host. As a
non-root user (the production image), leave `opcache.preload_user` unset (OPcache warns
that it is ignored).

SAPI module properties:

- **name:** `"kafka4php"` (this is `PHP_SAPI`). Some libraries branch on
  `PHP_SAPI === 'cli'`; this is documented user-facing. Opcache reads
  `opcache.enable` (not `opcache.enable_cli`) for non-cli SAPIs.
  **Limitation: no OPcache on PHP 8.4 (accepted, by decision).** PHP 8.4's OPcache
  only activates for a hard-coded list of SAPI names (`accel_find_sapi()`:
  apache2handler, fpm-fcgi, frankenphp, …), so under `kafka4php` it is loaded but
  inactive (`opcache_get_status()` returns false): every message recompiles the
  scripts it runs, and `opcache.preload` / the JIT are unavailable. We do **not**
  work around it (e.g. by presenting another SAPI name to OPcache). PHP 8.5 dropped
  the list, so OPcache, preload and the JIT work there (§5.1.2, tested). Without
  OPcache a typical entrypoint costs ~25× more per message (§5.7). Users who need
  OPcache run the PHP 8.5 image; this is documented user-facing.
- **ub_write** (output: `echo`, `print`, `var_dump`): forwarded to the host log at
  `info` with `stream=stdout` (log callback source `stdout`, line by line, §5.1.1).
  Never part of the result.
- **log_message**: forwarded to the host log (`warn` / `error` by PHP error level;
  source `error`).
- **send_headers / header_handler**: no-ops; headers are discarded (`headers_list()`
  is always empty) and `header()` never warns about "headers already sent".
- **SG(server_context)**: set to a non-NULL per-request pointer (some PHP code paths
  treat NULL as "no request").
- `$_SERVER`: the process environment plus `SCRIPT_FILENAME`, `SCRIPT_NAME`,
  `PHP_SELF`, `PATH_TRANSLATED` (= entrypoint) and PHP's `REQUEST_TIME[_FLOAT]`.
  Message data is **not** passed via superglobals — use the native functions.
- The native module is registered as extension **`kafka4php`**
  (`extension_loaded('kafka4php') === true`).

### 5.2 Native PHP API (registered by the SAPI)

Deliberately minimal and array-based. Users should use the framework (§5.4), which
wraps these.

```php
namespace Kafka4php\Native;

/** Current message. Shape below. Throws \LogicException outside a kafka4php request. */
function message(): array;

/** Set the result for the current request. Calling it again replaces the previous result. */
function result(array $result): void;
```

`message()` works anywhere during the request, including shutdown functions and
destructors run at request end; it builds a fresh array on each call. `result()`
works in the script and in shutdown functions; called after the outcome was
determined (from a destructor run at request shutdown) or outside a request it
throws `\LogicException`. A non-array argument is a `\TypeError` (an uncaught
exception like any other).

`message()` shape:

```php
[
  'topic'          => string,      // logical topic
  'delivery_topic' => string,
  'partition'      => int,
  'offset'         => int,
  'timestamp'      => ?int,        // unix ms
  'key'            => ?string,     // binary-safe
  'value'          => ?string,     // binary-safe
  'headers'        => list<array{0: string, 1: ?string}>, // ordered pairs, duplicates allowed
  'attempt'        => int,
]
```

`result()` accepted shapes (anything else → `ExecutionError::InvalidResult`; the C
side validates and throws `\ValueError` immediately so the developer sees it):

```php
['type' => 'no_content']
['type' => 'content', 'messages' => list<[
    'topic' => string, 'value' => ?string,
    'key' => ?string,  'headers' => list<array{0:string,1:?string}>, // optional, default []
    'partition' => ?int,                                              // optional, default null
]>]
['type' => 'fail',        'reason' => string, 'error_class' => ?string]
['type' => 'retry_later', 'reason' => string]
```

Validation is strict: unknown keys (at the top level and in each message) are
rejected; `messages` and `headers` must be lists (`array_is_list`), each header an
exact `[name, value]` pair; `topic` must be a non-empty valid UTF-8 string and header
names valid UTF-8 (Rust `String`s); `value` is required (string or null), `key`,
`headers`, `partition` and `error_class` are optional; `partition` must be an int in
`0..=2^31-1` or null. The `\ValueError` message is
`Kafka4php\Native\result(): Argument #1 ($result) is not a valid result: <what>`,
and `<what>` (e.g. `messages[0].topic must be a non-empty valid UTF-8 string`) is the
`InvalidResult` text.

The C side converts the result into C structs allocated with `malloc` (not
`emalloc`) at the moment `result()` is called — so it survives the request; Rust takes
ownership and frees it via `k4p_outcome_free()`.

### 5.3 Outcome classification (C side)

After the script and the shutdown functions ran, still inside the request:

1. If the soft timeout fired → `SoftTimeout`. Detected with
   `PG(connection_status) & PHP_CONNECTION_TIMEOUT` (set by PHP's timeout handler),
   because `EG(timed_out)` is already reset when the "Maximum execution time" fatal
   error is raised (PHP 8.4 and 8.5).
2. Else if an exception reached the top uncaught (and no user exception handler
   handled it): if it is the `\ValueError` thrown by `result()` itself →
   `InvalidResult(what)`; else `UncaughtException{class, message}` (class name and
   `getMessage()` of the outermost exception). `exit()` is not an exception here;
   `\ParseError` / `\CompileError` are reported by PHP as `E_PARSE` /
   `E_COMPILE_ERROR`, so they land in 3.
3. Else if a fatal error occurred (`PG(last_error_type) & E_FATAL_ERRORS`, incl.
   `memory_limit` exhaustion, compile and parse errors) →
   `FatalError("<message> in <file> on line <n>")`.
4. Else if `result()` was called → the last result (even if `exit()` was called after
   it). If the last call was invalid (the `\ValueError` was caught and no valid call
   followed) → `InvalidResult(what)`.
5. Else → `NoResult`.

### 5.4 Userland framework: `kafka4php/framework` (composer, in `php/`)

Namespace `Kafka4php\`. Pure PHP ≥ 8.4, no runtime dependency (composer `require`
is only `php`) — not even on the native functions: they're reached through a
`NativeBridge` interface so everything is unit-testable with plain PHPUnit.

- `NativeBridge` — `message(): array`, `result(array): void`. Its PHPStan type aliases
  (`NativeMessage`, `NativeOutbound`, `NativeHeaders`, `NativeResult`) are the §5.2
  shapes, and the only PHP that knows them is `Message::fromNative()` (reading),
  the `toNative()` methods of the results and `OutboundMessage` (writing) and
  `Testing\MessageBuilder::toNative()` (playing the C side). The framework trusts
  `message()` to match its shape and does not re-validate results — the C side is the
  validator (`\ValueError`, §5.2). `NativeFunctionsBridge` calls `\Kafka4php\Native\*`.
- `Message` — readonly: `topic`, `deliveryTopic`, `partition`, `offset`,
  `timestampMs`, `key`, `value`, `headers`, `attempt`. `Headers` (readonly, ordered,
  duplicates allowed, exact-case names): `get(name): ?string` first match,
  `has(name)`, `all(name): list<?string>`, `toList()` (`[name, value]` pairs, the
  shape `OutboundMessage` takes, so inbound headers can be forwarded).
- `Result\Result` (interface, `toNative()` is `@internal`; not meant to be
  implemented by users) with `Result\NoContent`, `Result\Content(OutboundMessage ...$messages)`,
  `Result\Fail(string $reason, ?string $errorClass = null)`, `Result\RetryLater(string $reason)`.
  All final readonly classes; `toNative()` always emits the optional keys
  (`key`, `headers`, `partition`, `error_class`), null/`[]` when unset.
- `OutboundMessage(string $topic, ?string $value, ?string $key = null, array $headers = [], ?int $partition = null)`.
- A **handler** is any `callable(Message): Result`. `Handler` is the interface for
  handler classes: `__invoke(Message $message): Result`, so the framework never
  adapts anything — it just calls handlers.
- `Router implements Handler` — `on(topic, callable): self` maps a logical topic
  (`Message::$topic`, so retries reach the same handler) to a handler; a second
  handler for a topic is an `\InvalidArgumentException`. Unknown topic →
  `Fail('no handler for topic "X"')`. Being a handler, a router can be nested.
- `Kernel` (final readonly) — configured by its constructor:
  `new Kernel(NativeBridge $bridge = new NativeFunctionsBridge(), ExceptionPolicy $onException = ExceptionPolicy::RetryLater)`.
  `run(callable $handler): void` reads the message, calls the handler, catches any
  `Throwable` from it and maps it with the `ExceptionPolicy` enum, then calls
  `result()` exactly once. `ExceptionPolicy::RetryLater` (default) →
  `RetryLater("<class>: <message>")`; `ExceptionPolicy::Fail` →
  `Fail("<class>: <message>", errorClass: <class>)` (`<class>` fully qualified).
  A handler returning anything but a `Result` is caught by PHP's own return type
  checks — the `Result` return type of `Handler::__invoke` / `Router::__invoke`, or
  for an untyped callable that of the `Kernel` method calling it — as a `\TypeError`,
  which goes through the policy too. Exceptions outside the handler (e.g. `message()`
  outside a request) are not caught.
- `Testing\MessageBuilder` — mutable fluent builder (each `with*()` sets a field and
  returns it): `MessageBuilder::forTopic('orders')->withValue(..)->withHeader(..)->build()`;
  defaults partition 0, offset 0, no timestamp/key/value/headers, attempt 1,
  `delivery_topic` = topic. `Testing\FakeNativeBridge(MessageBuilder)` serves the
  message as built at construction and records every reported result array
  (`results()`), unvalidated.
- `stubs/native.php` — stubs for `Kafka4php\Native\*` for IDEs / PHPStan
  (`scanFiles`, never loaded at runtime).
- Checks: `composer test` (PHPUnit 12, fails on warnings/risky/skipped) and
  `composer stan` (PHPStan 2, level max, `src` + `tests`), run on stock
  `php:<ver>-cli` by `scripts/php-test.sh` (`PHP_VERSION`, default 8.4): `composer
  install` in the `composer` image (`config.platform.php` = 8.4.0, so one `vendor/`
  serves every version), then both scripts with the PHP under test.
  `composer.lock` is not committed (library).

Typical entrypoint (executed once per message):

```php
<?php
require __DIR__ . '/vendor/autoload.php';

use Kafka4php\Kernel;
use Kafka4php\Router;

(new Kernel())->run((new Router())
    ->on('orders', new App\OrderPlacedHandler())
    ->on('payments', new App\PaymentHandler()));
```

The entrypoint's bootstrap cost is paid on every message. Users keep it small. OPcache
(PHP 8.5) is what makes it cheap; `opcache.preload` is the further lever, and pays more
the more classes a message loads (§5.7).

### 5.5 Thread pool, timeouts, recycling (`k4p-php`)

API: `PhpPool::start(PhpConfig, impl LogSink) -> Result<PhpPool, StartError>`
(once per process; a second call is `AlreadyStarted`), `impl Executor for PhpPool`,
`health() -> PoolHealth`, `health_watch() -> tokio::sync::watch::Receiver<PoolHealth>`,
`shutdown(grace) -> ShutdownReport`. `PhpConfig` = entrypoint, ini file, INI entries
(`key=value` lines), `threads`, `max_requests_per_thread`, `hard_timeout`,
`max_abandoned_threads` (counts are `NonZero*`, `hard_timeout` must be > 0).

- **Threads.** A dedicated **runtime thread** (`php-runtime`) runs
  `k4p_runtime_init`, spawns the PHP threads, then **supervises** (watchdog,
  replacements, joining exited threads) until shutdown, and finally runs
  `k4p_runtime_shutdown`. It never executes a message; `start` and `shutdown` may be
  called from any thread. `php.threads` dedicated **PHP threads** (`php-<n>`, never
  tokio threads) each `k4p_thread_init`, run requests one at a time, then
  `k4p_thread_shutdown` and exit. **No zval ever crosses threads:** messages go in as
  owned bytes, results come out as owned Rust structs (a oneshot per message).
- **Queue.** `execute` appends the message to one FIFO queue shared by all PHP
  threads; it never fails while the pool runs. **Calling `execute` more than
  `capacity()` times concurrently is allowed: the extra messages wait in the queue**
  (the dispatcher normally never does it). The hard timeout starts when a thread
  picks the message up. Dropping an execute future does not stop a running request;
  a queued message whose future was dropped is skipped. `capacity()` = `php.threads`,
  constant across replacements.
- **Soft timeout:** PHP's `max_execution_time` (per-thread timers, requires
  `--enable-zend-max-execution-timers`). Produces `SoftTimeout`; the thread stays.
- **Hard timeout:** the runtime thread's watchdog. If a request exceeds
  `timeouts.hard`, its execute future resolves with `HardTimeout` immediately, the
  thread is marked **abandoned** (a thread cannot be safely killed), and a
  replacement thread is spawned so capacity is preserved. The future resolves only
  after the replacement was spawned and `health()` reflects the abandonment. This
  does not depend on the future being polled. If the abandoned thread ever returns, its result is discarded
  and it calls `k4p_thread_shutdown` and exits.
- **Health** (`PoolHealth`): `live_threads`, `abandoned` (abandoned threads still
  stuck), `abandoned_total`, `recycled_total`, `unhealthy`. The pool is **unhealthy**
  once `abandoned_total ≥ timeouts.max_abandoned_threads` (over the pool's lifetime,
  whether or not those threads came back) or a replacement thread could not be
  spawned. Unhealthy is sticky; the pool keeps executing, and the binary performs a
  graceful process restart (§7.3), e.g. awaiting `health_watch().wait_for(|h| h.unhealthy)`.
- **Recycling:** after its request limit, a thread answers its last request, calls
  `k4p_thread_shutdown()` (`ts_free_thread()`), exits, and is replaced by a fresh
  thread; queued messages wait for it, none is dropped. Each thread draws its limit
  uniformly from `[max - max/10, max]` (`max` = `php.max_requests_per_thread`, so it
  stays an upper bound): under the steady load of a consumer, threads started
  together would otherwise all retire at once. Handles per-thread leaks. Process-wide extension leaks are handled by process restarts
  (cheap thanks to static membership). A thread whose request could not start
  (`Bootstrap`, §5.1) is replaced the same way.
- **Shutdown** (`shutdown(grace)`, blocking; idempotent, later calls return the first
  report): `execute` answers `Bootstrap("PHP pool is shut down")` from then on;
  queued and in-flight messages may finish within `grace`; then still-running
  requests are answered `HardTimeout` and their threads abandoned, and still-queued
  messages are answered `Bootstrap`. The runtime thread joins every exited PHP thread
  and shuts PHP down — **unless an abandoned thread may still be running PHP: then
  PHP is left running** (`ShutdownReport::runtime_shut_down == false`), since the
  process is about to exit anyway. Dropping a `PhpPool` without `shutdown` leaves
  its threads running.
- **Logging.** The `LogSink` (one per process) receives every log callback line
  (§5.1.1) as a `LogRecord { level, source, message, context }`; `context` is the
  message being executed on that PHP thread (`topic`, `delivery_topic`, `partition`,
  `offset`, `attempt`; `None` during runtime init/shutdown), from a thread-local set
  around each request. `TracingLogSink` forwards to `tracing` (target `php`, fields
  `source = "php"`, `stream`, and the context). Sink panics are swallowed. Pool
  events (abandoned thread with its message's coordinates, replacements, spawn
  failures) are logged with `tracing` directly.
- `memory_limit` is enforced per request by the Zend memory manager.

### 5.6 Process-wide state hazards (document for users)

Under ZTS these remain process-global and affect all threads: `setlocale()`,
`putenv()`, `umask()`, signal handlers. `pcntl_*` functions are unsupported (the
default INI disables `pcntl_signal`, `pcntl_fork`, `pcntl_exec` etc. via
`disable_functions`). The Rust host owns all signal handling. Some PECL extensions
wrap non-thread-safe C libraries; that's on the user.

### 5.7 Performance characteristics

Per-request cost of the SAPI on **one PHP thread**, sequential requests, measured by
`cargo bench -p k4p-php-sys --bench request_cost` (median of 3 runs × 50,000 requests
per row, after 1,000 warm-up requests; scripts on the container's own filesystem).
Scenarios: `empty` = an entrypoint that only calls `result()` (the host + request
lifecycle floor); `kernel` = a typical entrypoint: composer autoloader (optimized),
the real `kafka4php/framework` `Kernel::run` + `Router` (§5.4) and a trivial handler
(decode a small JSON value, return `NoContent`), ~14 classes per request (10 of the
framework); `preload` = `kernel` with all app and framework classes in
`opcache.preload`. OPcache `on` = PHP defaults, `on-vt0` = plus
`opcache.validate_timestamps=0`.

| scenario | OPcache | req/s | mean µs | p50 µs | p99 µs |
|---|---|---:|---:|---:|---:|
| empty | off (8.4) | 44,955 | 22.2 | 19.3 | 63.2 |
| empty | off (8.5) | 43,524 | 23.0 | 20.5 | 60.3 |
| empty | on | 88,480 | 11.3 | 10.4 | 25.8 |
| empty | on-vt0 | 89,217 | 11.2 | 10.0 | 32.3 |
| kernel | off (8.4) | 1,137 | 879.2 | 845.6 | 1,479.8 |
| kernel | off (8.5) | 1,133 | 882.7 | 857.7 | 1,378.6 |
| kernel | on | 30,238 | 33.1 | 30.1 | 77.6 |
| kernel | on-vt0 | 30,271 | 33.0 | 30.2 | 83.1 |
| preload | on | 43,152 | 23.2 | 21.0 | 64.5 |
| preload | on-vt0 | 41,665 | 24.0 | 21.4 | 68.6 |

`on` rows are PHP 8.5.11 (8.4.26 has no OPcache under our SAPI, §5.1). The first
recording (S4) ran `kernel` on a smaller stand-in framework (9 classes, half the
source): 27.2 µs with OPcache, 673 µs without, 20.6 µs preloaded, on an `empty`
floor of 10.8 / 20.2 µs. This machine's floor varies by ~5–10% between sessions
(here 11.3 / 22.2 µs, reproduced by a second 8.5 run), so compare `kernel` above the
floor: the real framework (one more class, the `ExceptionPolicy` enum loaded for the
`Kernel` constructor's default; twice the source, mostly docblocks) costs ~5 µs more
with OPcache (22 vs 16 µs above the floor) and ~200 µs more without.

**System** (all recordings 2026-10-17; the bench prints the container-side lines itself):

- Host: ASUS ROG Zephyrus G14 (GA401IU) laptop, **on AC power**, Windows 11 Home
  (10.0.26200), power plan "High performance".
- CPU: AMD Ryzen 7 4800HS (8 cores / 16 threads, 2.9 GHz base); 16 CPUs visible in
  the container.
- Memory: 31.4 GiB host; 15.3 GiB in the Docker Desktop VM (13.8 GiB available).
- Docker Desktop (engine 28.3.3), WSL2 kernel 5.15.167.4-microsoft-standard-WSL2,
  transparent huge pages `always`; toolchain image (Debian bookworm), no other
  containers running.

**Run-to-run variance:** the same machine on battery power measured every row ~3×
slower (S4 stand-in: `kernel`/on 82 µs, `kernel`/off 2.1 ms) with wider tails; the ratios
below held in every run and are the claims to rely on:

- **OPcache is the lever that matters.** Without it a typical entrypoint recompiles
  every file it includes on every message: `kernel` is **~25× slower** than with
  OPcache (≈ 1.1k msg/s per thread vs ≈ 30k here). This is the cost of PHP 8.4 (§5.1).
- The host + request lifecycle floor (`empty`, OPcache on) is ~11 µs per message
  (≈ 90k msg/s per thread); OPcache halves even that.
- **Preload** saves ~30% over `kernel` with ~15 classes per message (33 → 23 µs); it
  pays more the more classes a message loads.
- `validate_timestamps=0` is within run-to-run noise from a local filesystem (it saves
  `stat()` calls, which matter on slow filesystems: on the Docker Desktop bind mount
  an `empty` request costs ~5 ms, so never run code from a bind mount in production).
- Real handlers are dominated by their own I/O (DB, HTTP); these numbers bound the
  overhead kafka4php adds, per PHP thread. Throughput scales with `php.threads` up to
  the cores available (§5.5; the JIT smoke test, a debug Rust build, ran 100k
  requests on 4 threads at ~35k req/s on PHP 8.5).

---

## 6. The Kafka side (`k4p-host`)

### 6.1 Consumers

- A **consumer** = one rust-rdkafka consumer with its own group id, topic list,
  retry policy and DLQ. A process typically runs one or two. All consumers share one
  `Executor` (PHP pool) and one producer.
- Each consumer subscribes to its source topics **and** all of their retry topics.
- librdkafka settings forced by the host (user cannot override):
  `enable.auto.commit=true`, `enable.auto.offset.store=false` (we store offsets
  explicitly after a message is fully handled; librdkafka commits stored offsets
  periodically), `partition.assignment.strategy=cooperative-sticky`.
- `group.instance.id` from config (supports env expansion, e.g. `${HOSTNAME}`).
  Strongly recommended; the host logs a warning if unset.
- Arbitrary other librdkafka properties pass through from config.
- **Startup validation:** fetch metadata; fail fast (exit code 78) if any source,
  retry or DLQ topic is missing. The host never auto-creates topics.

### 6.2 Dispatcher

- Fetched messages go into a **per-partition FIFO queue**.
- A partition is *ready* when its queue is non-empty, nothing from it is in flight,
  and its head message is due (§6.4). The dispatcher hands ready heads to the
  executor while `in_flight < executor.capacity()`, choosing partitions round-robin
  for fairness.
- **Backpressure:** when a partition queue exceeds `dispatch.max_buffered_per_partition`,
  `pause()` that partition; `resume()` when it drops below half. The host keeps
  polling regardless, so `max.poll.interval.ms` is never caused by slow PHP.
- Because there is at most one in-flight message per partition, completed offsets
  per partition are contiguous: after a message is fully handled, store
  `offset + 1` for its partition.

### 6.3 Result actions

The invariant: **store the offset only after every side effect of the result has been
acknowledged by the broker.**

| Outcome | Actions, in order |
|---|---|
| `NoContent` / `Content([])` | store offset |
| `Content(msgs)` | produce all `msgs` (idempotent producer, `acks=all`), await all acks, store offset |
| `Fail` | produce original message to DLQ (§6.5 headers, `k4p-failure-kind: handler_fail`), await ack, store offset |
| `RetryLater` | if `attempt >= max_attempts` → as `Fail` with `k4p-failure-kind: max_attempts_exceeded`; else produce original to retry tier (§6.4), await ack, store offset |
| `Err(ExecutionError)` | treated as `RetryLater { reason }` (§4) |

**Produce failures** (after librdkafka's own retries are exhausted): do not store the
offset. Retry the produce with exponential backoff (cap 30s), keeping the partition
blocked. Never skip a message. Log every retry at `warn`.

### 6.4 Retry topics

- Configured as tiers, e.g. `tiers = ["10s", "1m", "10m"]`, `max_attempts = 5`.
- Topic naming (fixed in v1): `<source_topic>.retry.<tier>` → `orders.retry.10s`.
- On retry of a message with attempt `n`, target tier = `tiers[min(n-1, len-1)]`
  (the last tier repeats).
- The retried record keeps the original **key, value and user headers**, plus
  `k4p-*` headers (§6.5) including `k4p-attempt = n+1` and `k4p-due-at`.
- **Delay:** when a retry topic partition's head message has `k4p-due-at` in the
  future, the partition is not ready; it is paused and a timer resumes/re-evaluates
  it at the due time. Since a tier has a single fixed delay, records are roughly
  ordered by due time, so waiting on the head doesn't stall later due messages.
- When consumed from a retry topic, the `InboundMessage` has
  `topic = k4p-original-topic`, `delivery_topic` = the retry topic, `attempt` from
  `k4p-attempt`, and `k4p-*` headers stripped.
- Retried messages lose ordering relative to later messages with the same key.
  Documented, expected.

### 6.5 Header conventions

All host-owned headers are prefixed `k4p-`. Values are UTF-8 strings; integers are
decimal ASCII.

| Header | On retry | On DLQ | Meaning |
|---|---|---|---|
| `k4p-original-topic` | ✓ | ✓ | First topic the message was consumed from |
| `k4p-original-partition` | ✓ | ✓ | |
| `k4p-original-offset` | ✓ | ✓ | |
| `k4p-original-timestamp` | ✓ | ✓ | unix ms, if known |
| `k4p-attempt` | ✓ | ✓ | Attempt number of the *next* delivery (retry) / of the failed delivery (DLQ) |
| `k4p-due-at` | ✓ | | unix ms when the retry may be processed |
| `k4p-retry-reason` | ✓ | | Reason from `RetryLater` / error description |
| `k4p-failure-kind` | | ✓ | `handler_fail` \| `max_attempts_exceeded` |
| `k4p-failure-reason` | | ✓ | |
| `k4p-failure-class` | | ✓ | `error_class` / exception class, if any |
| `k4p-failed-at` | | ✓ | unix ms |
| `k4p-host` | ✓ | ✓ | `group.instance.id` or hostname |

`k4p-original-*` are set once (first hop) and copied unchanged on subsequent hops.

### 6.6 Rebalances

- On partitions revoked: drop queued (not in-flight) messages for those partitions;
  synchronously commit stored offsets for them; let in-flight messages finish. A
  finished message whose partition is no longer assigned is **not** acted upon —
  no produce, no offset store. It will be redelivered to the new owner (at-least-once).
- Cooperative-sticky means only moved partitions are revoked.
- On assignment: create queues; resume normally.

---

## 7. The binary (`kafka4php`)

### 7.1 Configuration

TOML file, path from `--config` (default `/etc/kafka4php/kafka4php.toml`).
`${VAR}` and `${VAR:-default}` are expanded from the environment in string values.

```toml
[php]
entrypoint = "/app/consumer.php"          # required
ini_file = "/usr/local/etc/php/php.ini"   # optional
threads = 4                               # PHP threads (= max concurrency)
max_requests_per_thread = 1000

[php.ini]                                 # INI overrides, applied after ini_file
memory_limit = "256M"
max_execution_time = "30"                 # soft timeout (seconds)

[timeouts]
hard = "60s"                              # watchdog; must be > max_execution_time
max_abandoned_threads = 2
shutdown_grace = "30s"

[dispatch]
max_buffered_per_partition = 100

[producer.properties]                     # librdkafka passthrough for the shared producer
"compression.type" = "zstd"

[health]
listen = "0.0.0.0:8080"                   # optional; GET /healthz

[log]
format = "json"                           # json | text
level = "info"

[[consumers]]
name = "orders"
bootstrap_servers = "${KAFKA_BROKERS}"
group_id = "orders-service"
group_instance_id = "${HOSTNAME}"
topics = ["orders"]
ordering = "partition"                    # only value in v1
dlq_topic = "orders.dlq"

[consumers.retry]
tiers = ["10s", "1m", "10m"]
max_attempts = 5

[consumers.properties]                    # librdkafka passthrough
"security.protocol" = "PLAINTEXT"
```

Validation errors exit with code 78 (`EX_CONFIG`) and a precise message.

### 7.2 Health

`GET /healthz` → `200` when: PHP runtime is up, every consumer has joined its group
(or is rebalancing for < 60s), and the pool is not unhealthy. Otherwise `503` with a
JSON body listing failing checks. Used as the Docker `HEALTHCHECK`.

### 7.3 Signals, shutdown, exit codes

- `SIGTERM` / `SIGINT` → graceful shutdown: stop dispatching, wait for in-flight
  messages up to `timeouts.shutdown_grace` (complete their result actions), commit
  stored offsets synchronously, close consumers, shut down the PHP pool, exit 0.
  With static membership, closing does not trigger a rebalance; the restarted
  container reclaims its partitions.
- Pool unhealthy (too many abandoned threads) → same graceful shutdown, exit 75
  (`EX_TEMPFAIL`); the container restart policy brings it back.
- Startup failures: 78 for config/topic validation, 70 (`EX_SOFTWARE`) for PHP
  runtime init failure.
- A segfault in PHP/an extension kills the process. Correctness is preserved by
  at-least-once; recovery relies on the container restart policy + static
  membership.

### 7.4 Logging

`tracing` with JSON or text output. Every log line about a message carries
`consumer`, `topic`, `partition`, `offset`, `attempt`. PHP output and PHP errors are
logged with `source=php`.

---

## 8. Build & distribution

- **Toolchain image** (`docker/dev.Dockerfile`, `ARG PHP_VERSION`, default 8.4):
  based on `php:<ver>-zts-bookworm`. PHP must be ZTS + embed (`libphp.so`) + Zend
  max execution timers, with OPcache loaded. The official ZTS images ship exactly
  that (`--enable-embed --enable-zts`; timers default on with ZTS), so PHP is used
  **as is** — never rebuilt to fix a missing feature. `docker/php-check.sh` runs
  first and **fails the build** if any of these is missing. The one explicit,
  test-only exception: `--build-arg PHP_DEBUG=1` (last layer,
  `docker/php-debug-build.sh`) rebuilds the same PHP source with the same flags plus
  `--enable-debug --enable-zend-test` for the leak check; `scripts/dev.sh` selects
  it with `PHP_DEBUG=1` (image `kafka4php-dev:php<ver>-debug`, own `target/`
  volume). Plus the Rust toolchain pinned by
  `rust-toolchain.toml`, cmake,
  clang/libclang (bindgen), pkg-config, OpenSSL/zstd/SASL headers, Composer.
  All builds and tests run inside it via `scripts/dev.sh` (Git Bash/Linux) or
  `scripts/dev.ps1` (PowerShell): repo mounted at `/work`, cargo registry/git in
  named volumes, `target/` in a named volume per PHP version
  (`k4p-target-php<ver>`), env `PHP_VERSION` selects the image.
- **Test Kafka** (`docker/compose.test.yml`): single-node KRaft `apache/kafka`
  (pinned), network `kafka4php-test`, broker `kafka:9092` inside the network and
  `localhost:19092` from the host, topic auto-creation **off**.
  `scripts/dev.sh --with-kafka …` starts it, waits for health and attaches the
  container (env `K4P_KAFKA_BOOTSTRAP=kafka:9092`). `scripts/kafka-check.sh`
  smoke-tests it (create topic, produce, consume).
- **`k4p-php-sys` build.rs**: runs `php-config --includes --prefix` (`$PHP_CONFIG`
  overrides the binary), compiles every `csrc/*.c` with the `cc` crate (C11,
  `-Wall -Wextra -Werror`, `ZEND_ENABLE_STATIC_TSRMLS_CACHE=1`), links
  `<prefix>/lib/libphp.so` dynamically (its own dependencies come with it; no
  `--libs`), bindgens only `csrc/k4p_sapi.h`. No rpath: the loader finds
  `/usr/local/lib/libphp.so` via the ld cache of the `php` images. The crate has
  `links = "php"` and exports `DEP_PHP_LIB_DIR` to dependents' build scripts.
- **rust-rdkafka** with the `cmake-build`, `ssl`, `zstd` features (librdkafka built
  statically from bundled source).
- **Production image** (`docker/Dockerfile`), multi-stage: toolchain stage builds
  the release binary; runtime stage is the ZTS+embed PHP image (with `libphp.so`)
  plus the binary. `ENTRYPOINT ["kafka4php"]`, `HEALTHCHECK` on `/healthz`.
  Published per PHP minor version: `kafka4php:<version>-php8.4`, `-php8.5`.
- Users build on top:

  ```dockerfile
  FROM kafka4php:1-php8.4
  RUN docker-php-ext-install pdo_mysql
  COPY . /app
  RUN composer install --no-dev -d /app
  COPY kafka4php.toml /etc/kafka4php/kafka4php.toml
  ```

- Supported PHP: 8.4 and 8.5 (CI matrix); 8.4 runs without OPcache (§5.1). Linux amd64 + arm64.

---

## 9. Testing strategy

| Layer | How |
|---|---|
| `k4p-core` | Unit tests. |
| `k4p-php-sys` / `k4p-php` | Rust tests in the toolchain container running real PHP scripts (`crates/k4p-php-sys/tests/fixtures/`, `crates/k4p-php/tests/fixtures/`): every outcome kind, binary-safe data round-trip, memory stability over ≥100k requests (RSS and `memory_get_usage` baseline stable), N-thread concurrency, recycling, soft and hard timeouts, abandoned-thread replacement. PHP can be started once per process, so these test binaries use `harness = false` with their own runner (init on the main thread, cases on a PHP worker thread). `crates/k4p-php-sys/tests/leak-check.sh` re-runs the SAPI tests on a ZTS **debug** PHP (the `PHP_DEBUG=1` variant of the toolchain image) and fails on any reported request-memory leak, with a positive control. `k4p-php-sys` tests `opcache` (8.5: OPcache active, preloaded class/function visible without autoload, tracing JIT on and ≥ 100k requests on 4 threads; 8.4: OPcache loaded but inactive, so a PHP change is noticed) and `default_ini` (§5.1.2 values and precedence). The benchmark `cargo bench -p k4p-php-sys --bench request_cost` produces the §5.7 table. |
| `k4p-host` | Unit tests with a `FakeExecutor` (scripted outcomes, delays). Integration tests against a real Kafka from `docker/compose.test.yml`: offsets, retry tiers & due-time, DLQ, max attempts, rebalance with in-flight messages, produce-failure blocking, backpressure. |
| `php/` | PHPUnit + PHPStan (max level) using `FakeNativeBridge`. Runs on stock `php:8.x-cli` via `scripts/php-test.sh` (§5.4). |
| CI | `.github/workflows/build.yml`, every PR and push to `main`, PHP 8.4 and 8.5: builds the toolchain image, runs `cargo fmt --check`, clippy `-D warnings`, `cargo test --workspace --all-features` in it, the debug-PHP leak check, and `scripts/php-test.sh` (no toolchain image). Every test suite must run there. |
| E2E | `tests/e2e/`: production image + Kafka + `examples/basic`. Scenarios: happy path, `Content` produce, `Fail`→DLQ, retry→eventual success, max attempts→DLQ, uncaught exception, fatal error, soft/hard timeout, process crash (`posix_kill(getmypid(), SIGSEGV)`) then restart with static membership (no rebalance observed), graceful SIGTERM (no message lost or double-produced beyond at-least-once). |

---

## 10. Future work (explicitly out of v1)

Per-key and unordered ordering modes (unordered needs out-of-order offset tracking:
commit only up to the lowest unfinished offset); batch delivery; transactional
exactly-once using `Content` results; OTel/baggage propagation; Prometheus metrics;
configurable retry topic naming; dynamic thread pool sizing.

---

## 11. Glossary

- **Consumer** — one Kafka consumer-group membership configured under `[[consumers]]`.
- **Logical topic** — the original topic of a message, even when delivered via a retry topic.
- **Request** — one PHP request lifecycle executing the entrypoint for one message.
- **Abandoned thread** — a PHP thread past its hard timeout; left to finish or hang, never reused.
- **Result actions** — the Kafka side effects (produce, DLQ, retry, offset store) of a handler result.
