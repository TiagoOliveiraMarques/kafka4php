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
| Runtime init | main thread, once, **before** worker threads | `php_tsrm_startup()`, `zend_signal_startup()` (if applicable), `sapi_startup(&k4p_sapi_module)`, set INI (file + overrides), `php_module_startup(&k4p_sapi_module, &k4p_native_module)`. Opcache initialises here; its shared memory is shared by all threads. |
| Thread init | each worker thread | `ts_resource(0)` — allocates this thread's interpreter globals. |
| Request | worker thread, per message | Set thread-local current message → `php_request_startup()` → `zend_try { php_execute_script(entrypoint) } zend_catch {} zend_end_try()` → classify outcome, copy result out of request memory → `php_request_shutdown()`. |
| Thread shutdown | same worker thread | `ts_free_thread()`. |
| Runtime shutdown | main thread, after all threads joined | `php_module_shutdown()`, `sapi_shutdown()`, `tsrm_shutdown()`. |

Exact function names/signatures vary slightly across PHP versions — check the
headers of the PHP version in `docker/dev.Dockerfile` and FrankenPHP's source.

SAPI module properties:

- **name:** `"kafka4php"` (this is `PHP_SAPI`). Some libraries branch on
  `PHP_SAPI === 'cli'`; this is documented user-facing. Opcache reads
  `opcache.enable` (not `opcache.enable_cli`) for non-cli SAPIs.
- **ub_write** (output: `echo`, `print`, `var_dump`): forwarded to the host log at
  `info` with `stream=stdout`. Never part of the result.
- **log_message**: forwarded to the host log (`warn` / `error` by PHP error level).
- **send_headers / header_handler**: no-ops.
- **SG(server_context)**: set to a non-NULL per-request pointer (some PHP code paths
  treat NULL as "no request").
- `$_SERVER`: standard environment only. Message data is **not** passed via
  superglobals — use the native functions.

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

The C side converts the result into C structs allocated with `malloc` (not
`emalloc`) **before** request shutdown, so it survives the request; Rust takes
ownership and frees it via `k4p_outcome_free()`.

### 5.3 Outcome classification (C side)

After the script runs, still inside the request:

1. If `EG(timed_out)` (or equivalent for the PHP version) → `SoftTimeout`.
2. Else if a fatal error occurred (`PG(last_error_type) & E_FATAL_ERRORS`): if it is
   an uncaught exception → `UncaughtException{class,message}`; else
   `FatalError(message)`.
3. Else if `result()` was called → that result (even if `exit()` was called after it).
4. Else → `NoResult`.

### 5.4 Userland framework: `kafka4php/framework` (composer, in `php/`)

Namespace `Kafka4php\`. Pure PHP, no runtime dependency on the native functions
(they're reached through a `NativeBridge` interface so everything is unit-testable
with plain PHPUnit).

- `Message` — readonly: `topic`, `deliveryTopic`, `partition`, `offset`,
  `timestampMs`, `key`, `value`, `headers` (`Headers` object: `get(name): ?string`
  first match, `all(name): list`, `toList()`), `attempt`.
- `Result\Result` (interface) with `Result\NoContent`, `Result\Content(OutboundMessage ...$messages)`,
  `Result\Fail(string $reason, ?string $errorClass = null)`, `Result\RetryLater(string $reason)`.
- `OutboundMessage(string $topic, ?string $value, ?string $key = null, array $headers = [], ?int $partition = null)`.
- `Handler` interface: `handle(Message $message): Result`.
- `Router` — maps logical topic → `Handler`. Unknown topic → `Fail("no handler for topic X")`.
- `Kernel::run(Handler|Router|callable $handler, ?NativeBridge $bridge = null): void` —
  reads the message, invokes the handler, catches `Throwable` (default:
  `RetryLater` with the exception class/message; configurable to `Fail`), calls
  `result()`.
- `Testing\` — `FakeNativeBridge`, `MessageBuilder` for handler unit tests.
- `stubs/native.php` — stubs for `Kafka4php\Native\*` for IDEs / PHPStan.

Typical entrypoint (executed once per message):

```php
<?php
require __DIR__ . '/vendor/autoload.php';

use Kafka4php\Kernel;
use Kafka4php\Router;

Kernel::run((new Router())
    ->on('orders', new App\OrderPlacedHandler())
    ->on('payments', new App\PaymentHandler()));
```

The entrypoint's bootstrap cost is paid on every message. Users keep it small;
`opcache.preload` is the recommended lever.

### 5.5 Thread pool, timeouts, recycling (`k4p-php`)

- Fixed pool of `php.threads` dedicated `std::thread`s (never tokio threads). Each
  thread receives work through a channel and replies via a oneshot. **No zval ever
  crosses threads:** messages go in as owned bytes, results come out as owned Rust
  structs.
- **Soft timeout:** PHP's `max_execution_time` (per-thread timers, requires
  `--enable-zend-max-execution-timers`). Produces `SoftTimeout`.
- **Hard timeout:** a watchdog in the pool. If a request exceeds `timeouts.hard`,
  the execute future resolves with `HardTimeout` immediately, the thread is marked
  **abandoned** (a thread cannot be safely killed), and a replacement thread is
  spawned so capacity is preserved. If the abandoned thread ever returns, its result
  is discarded and the thread exits.
- When abandoned threads ≥ `timeouts.max_abandoned_threads`, the pool reports
  `unhealthy`, and the binary performs a graceful process restart (§7.3).
- **Recycling:** after `php.max_requests_per_thread` requests, a thread finishes its
  current request, calls `ts_free_thread()`, exits, and is replaced by a fresh
  thread. Handles per-thread leaks. Process-wide extension leaks are handled by
  process restarts (cheap thanks to static membership).
- `memory_limit` is enforced per request by the Zend memory manager.

### 5.6 Process-wide state hazards (document for users)

Under ZTS these remain process-global and affect all threads: `setlocale()`,
`putenv()`, `umask()`, signal handlers. `pcntl_*` functions are unsupported (the
default INI disables `pcntl_signal`, `pcntl_fork`, `pcntl_exec` etc. via
`disable_functions`). The Rust host owns all signal handling. Some PECL extensions
wrap non-thread-safe C libraries; that's on the user.

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

- **Toolchain image** (`docker/dev.Dockerfile`): based on `php:<ver>-zts-bookworm`,
  PHP rebuilt from the matching source with `--enable-zts --enable-embed
  --enable-zend-max-execution-timers` (installed into `/usr/local` so
  `php-config`, `docker-php-ext-install` and `pecl` keep working), plus the Rust
  toolchain, cmake, clang (for bindgen). All builds and tests run inside it.
- **`k4p-php-sys` build.rs**: runs `php-config --includes --prefix --libs`, compiles
  `csrc/*.c` with the `cc` crate, links `libphp` dynamically, bindgens only
  `csrc/k4p_sapi.h`.
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

- Supported PHP: 8.4 and 8.5 (CI matrix). Linux amd64 + arm64.

---

## 9. Testing strategy

| Layer | How |
|---|---|
| `k4p-core` | Unit tests. |
| `k4p-php-sys` / `k4p-php` | Rust tests in the toolchain container running real PHP scripts from `crates/k4p-php/tests/fixtures/`: every outcome kind, binary-safe data round-trip, memory stability over ≥100k requests (RSS and `memory_get_usage` baseline stable), N-thread concurrency, recycling, soft and hard timeouts, abandoned-thread replacement. |
| `k4p-host` | Unit tests with a `FakeExecutor` (scripted outcomes, delays). Integration tests against a real Kafka from `docker/compose.test.yml`: offsets, retry tiers & due-time, DLQ, max attempts, rebalance with in-flight messages, produce-failure blocking, backpressure. |
| `php/` | PHPUnit + PHPStan (max level) using `FakeNativeBridge`. Runs on stock `php:8.x-cli`. |
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
