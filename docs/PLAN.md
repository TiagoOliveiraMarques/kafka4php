# kafka4php — Implementation Plan (v1)

Read [DESIGN.md](DESIGN.md) in full before picking a step. This plan says **what to
build in which order**; DESIGN.md says **how the system works**.

## How agents use this plan

1. **Pick a step** whose status is `todo` and whose dependencies are **all `done`**.
   Never start a step with a dependency that is `todo`, `in-progress` or `blocked`.
2. **Claim it** before writing code: set its status in the table below to
   `in-progress` and fill in the *Owner* column (session/agent name + date). Commit
   that change on its own (`plan: claim S3`) so other agents see it.
3. **Stay in scope.** Do only your step. If you find work that belongs to another
   step, write it in that step's *Notes from other steps* list instead of doing it.
4. **Meet every acceptance criterion.** Run the tests inside the toolchain container
   (see AGENTS.md). A step is not done if anything is failing or skipped.
5. **Keep DESIGN.md true.** If you had to deviate from the design (an API name, a
   header, a behaviour), update DESIGN.md in the same change and say so in your
   handoff notes.
6. **Finish:** set status to `done`, write *Handoff notes* for your step (what you
   built, where it lives, deviations, gotchas for dependent steps), commit.
7. **Blocked?** Set status to `blocked`, explain why in the handoff notes, and stop.

## Status

| Step | Title | Depends on | Status | Owner |
|---|---|---|---|---|
| S0 | Workspace scaffold + `k4p-core` contract | — | done | Claude Code (Opus 5.5), 2026-10-10 |
| S1 | Toolchain Docker image (ZTS + embed PHP, Rust) | — | in-progress | Claude Code (Opus 5.5), 2026-10-10 |
| S2 | C SAPI, single thread (`k4p-php-sys`) | S0, S1 | todo | |
| S3 | PHP thread pool + timeouts + recycling (`k4p-php`) | S2 | todo | |
| S4 | Opcache, preload & per-request cost under ZTS | S2 | todo | |
| S5 | PHP userland framework (`php/`) | S0 | todo | |
| S6 | Kafka consumer engine + dispatcher (`k4p-host`) | S0, S1 | todo | |
| S7 | Result actions: produce, retry tiers, DLQ (`k4p-host`) | S6 | todo | |
| S8 | Binary: config, wiring, signals, health (`kafka4php`) | S3, S7 | todo | |
| S9 | Production image, example app, E2E suite | S4, S5, S8 | todo | |
| S10 | User documentation | S9 | todo | |

```
S0 ──┬──────────────► S5 ───────────────────────┐
     │                                          │
     ├──► S2 ──┬──► S3 ──────────┐              │
S1 ──┤         └──► S4 ──────────┼──────────────┼──► S9 ──► S10
     │                           │              │
     └──► S6 ──► S7 ─────────────┴──► S8 ───────┘
```

Parallelism: S0 and S1 can start immediately. After them, S2, S5 and S6 run in
parallel; after S2, S3 and S4 run in parallel.

---

## S0 — Workspace scaffold + `k4p-core` contract

**Depends on:** —

**Goal:** a buildable Cargo workspace with all crates stubbed, and the shared
contract (DESIGN.md §4) implemented in `k4p-core`, so S2/S5/S6 can start in parallel.

**Tasks**
- `git init`, `.gitignore` (Rust `target/`, `php/vendor/`, IDE files),
  `.gitattributes` (`* text=auto eol=lf` — the dev machine is Windows, everything
  runs in Linux containers; shell scripts and C files must be LF).
- Cargo workspace per DESIGN.md §3.1 with crates `k4p-core`, `k4p-php-sys`,
  `k4p-php`, `k4p-host`, `kafka4php` (bin). Non-core crates are empty stubs that
  compile. Pin `rust-toolchain.toml` to a recent stable.
- `k4p-core`: `Headers`, `InboundMessage`, `OutboundMessage`, `HandlerResult`,
  `ExecutionError`, `Outcome`, `Executor` exactly as DESIGN.md §4. Derive
  `Debug, Clone, PartialEq` where sensible; `ExecutionError` implements
  `std::error::Error` + `Display` (the `Display` string is used as the retry reason).
- `k4p-core::testing::FakeExecutor` (behind a `testing` feature): scripted outcomes
  per message (by topic/offset or a closure), optional artificial delay, records
  received messages, configurable `capacity`. S6/S7 depend on it.
- Empty `php/`, `docker/`, `examples/`, `tests/e2e/` dirs with `.gitkeep`.
- Formatting/lint config: `rustfmt.toml`, workspace clippy lints (`-D warnings` in CI scripts).

**Acceptance criteria**
- `cargo build --workspace` and `cargo test -p k4p-core --all-features` pass
  (on any machine with Rust — this step needs no PHP; run in `rust:1-bookworm` if
  S1 isn't done yet).
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- `FakeExecutor` has tests for scripted outcomes, delays and the capacity value.

**Handoff notes:**
- **Built:** Cargo workspace (`Cargo.toml`, resolver 3, edition 2024) with crates
  `k4p-core`, `k4p-php-sys`, `k4p-php`, `k4p-host`, `kafka4php` (bin). Non-core
  crates are stubs; `k4p-php`, `k4p-host`, `kafka4php` already depend on `k4p-core`
  (`k4p-php-sys` depends on nothing). Shared deps go in `[workspace.dependencies]`
  (`k4p-core`, `tokio` with `default-features = false` — add features per crate).
- **Toolchain:** `rust-toolchain.toml` pins **1.99.0** (+ rustfmt, clippy). S1:
  install that toolchain in the dev image. Lints in `[workspace.lints]`; every crate
  has `[lints] workspace = true`. `rustfmt.toml` sets `max_width = 100`, LF.
- **Contract:** `crates/k4p-core/src/lib.rs`, exactly DESIGN.md §4. Deviations
  (documented in §4): `BoxFuture` is our own alias, not `futures`'; extra
  `impl Executor for Arc<E>`; all types also derive `Eq`.
- **FakeExecutor:** `crates/k4p-core/src/testing.rs`, feature `testing`. Use as a
  dev-dependency: `k4p-core = { workspace = true, features = ["testing"] }`.
  Builder: `FakeExecutor::new(capacity).with_default(..).with_outcome_fn(..)
  .with_delay(..)` / `.with_delay_fn(..)`; builders panic if the fake was already
  cloned. `on_record(delivery_topic, partition, offset, outcome)` works at any time.
  Inspection: `received()`, `received_ids()`, `in_flight()`, `max_in_flight()` (S6:
  assert `max_in_flight() <= capacity()`). Messages are recorded and counted in
  flight when `execute` is **called**, not when first polled. Delays use
  `tokio::time::sleep` — callers need a tokio runtime with time enabled;
  `start_paused = true` tests work.
- **Verified** in `rust:1-bookworm` (S1 not done): `cargo fmt --check`,
  `cargo build --workspace`, `cargo test --workspace --all-features` (12 tests),
  `cargo clippy --workspace --all-targets [--all-features] -- -D warnings`.
- **Repo:** git initialised here (branch `main`, work on `s0-workspace-scaffold`);
  `.gitattributes` forces LF. `core.autocrlf=false` set locally.

**Notes from other steps:** _(none yet)_

---

## S1 — Toolchain Docker image

**Depends on:** —

**Goal:** `docker/dev.Dockerfile` producing an image where libphp (ZTS, embed,
max-execution-timers) and the Rust toolchain are available, plus scripts to run
cargo inside it from a Windows host.

**Tasks**
- `docker/dev.Dockerfile`, `ARG PHP_VERSION=8.4`: start from
  `php:${PHP_VERSION}-zts-bookworm`; rebuild PHP from the image's own source
  (`docker-php-source extract`) with the image's original configure flags **plus**
  `--enable-embed --enable-zend-max-execution-timers`; install to `/usr/local` so
  `php-config`, `docker-php-ext-install` and `pecl` keep working. FrankenPHP's
  Dockerfiles are a good reference.
- Install Rust (rustup, toolchain from `rust-toolchain.toml` if present), `cmake`,
  `clang`/`libclang-dev` (bindgen), `pkg-config`, `libssl-dev`, `libzstd-dev`,
  `libsasl2-dev`, `composer`.
- Enable opcache in the image.
- `scripts/dev.sh` (and a `scripts/dev.ps1` wrapper for Windows) that builds the
  image (cached) and runs a command in it with the repo mounted at `/work`, cargo
  registry and `target/` in named volumes (avoid slow bind-mounted `target/` on
  Windows). Example: `scripts/dev.sh cargo test --workspace`.
- `docker/compose.test.yml`: single-node Kafka in KRaft mode (`apache/kafka`
  image, pinned version), reachable from the dev container on a shared network
  (`kafka:9092`). Add a `scripts/dev.sh --with-kafka …` mode or document how to
  attach.

**Acceptance criteria**
- In the image: `php -v` shows `ZTS`; `php-config --php-sapis` includes `embed`;
  `ls $(php-config --prefix)/lib/libphp.so` exists; `php -i` shows zend max
  execution timers enabled; `php -m` shows `Zend OPcache`.
- `docker-php-ext-install pdo_mysql` works inside the image (proves the extension
  toolchain is intact).
- `scripts/dev.sh cargo --version` works from Git Bash and `scripts/dev.ps1 cargo --version`
  from PowerShell on the Windows host.
- `docker compose -f docker/compose.test.yml up -d` gives a Kafka that a container
  on the network can create a topic on and produce/consume from (document the
  check command).
- Both `PHP_VERSION=8.4` and `8.5` build.

**Handoff notes:** _(fill in when done)_

**Notes from other steps:** _(none yet)_

---

## S2 — C SAPI, single thread (`k4p-php-sys`)

**Depends on:** S0, S1

**Goal:** the custom `kafka4php` SAPI in C, exposed through our own small C header,
able to execute one message per PHP request on a single thread with correct
outcome classification and no per-request memory growth. Highest-risk step.

**Read first:** DESIGN.md §5.1–5.3; FrankenPHP `frankenphp.c`; PHP's
`sapi/embed/php_embed.c` and `main/SAPI.h` for the PHP version in the toolchain image.

**Tasks**
- `crates/k4p-php-sys/csrc/k4p_sapi.h` — our C ABI. Plain C types only (no PHP
  headers). Proposed (refine as needed, document final form in DESIGN.md):
  - `k4p_runtime_init(const k4p_runtime_config*)` / `k4p_runtime_shutdown()`
    (config: ini file path, ini overrides as `key=value\n` string, entrypoint path,
    log callback `void (*)(int level, const char* source, const char* msg, size_t len)`).
  - `k4p_thread_init()` / `k4p_thread_shutdown()`.
  - `k4p_execute(const k4p_message*, k4p_outcome*)` — runs one request.
  - `k4p_outcome_free(k4p_outcome*)`.
  - C structs mirroring `InboundMessage`, `OutboundMessage`, the result variants,
    and the error variants. Byte buffers as `(const uint8_t*, size_t)` + null flag.
- `csrc/k4p_sapi.c` — the SAPI module (§5.1 properties), native module with
  `Kafka4php\Native\message()` / `result()` (§5.2, including validation +
  `\ValueError`), thread-local current message/result, outcome classification
  (§5.3), result copied into `malloc` memory before request shutdown.
- `build.rs`: `php-config` discovery, compile with `cc`, link `libphp`, bindgen
  `k4p_sapi.h` only.
- `src/lib.rs`: raw FFI + minimal safe helpers converting `InboundMessage` → C and
  C outcome → `Outcome` (owned). No threading logic here (that's S3).
- PHP fixture scripts in `crates/k4p-php-sys/tests/fixtures/`.

**Acceptance criteria** (Rust integration tests, single thread, run via `scripts/dev.sh`)
- Each outcome is produced correctly: `NoContent`, `Content` (multiple messages,
  with key/headers/partition), `Fail` (with/without class), `RetryLater`,
  `NoResult`, `InvalidResult` (malformed array → `\ValueError` visible in log),
  `UncaughtException` (class + message), `FatalError` (incl. `memory_limit`
  exhaustion), `SoftTimeout` (`max_execution_time=1` + busy loop), result then
  `exit()` → the result.
- Binary-safe round trip: key/value/header values containing `\0` and invalid UTF-8
  arrive in PHP byte-identical and return byte-identical in `Content`.
- Duplicate header keys preserved in order.
- `echo` output and PHP warnings arrive at the log callback with the right level/source.
- `PHP_SAPI === 'kafka4php'` inside the script.
- **Isolation:** a static/global set in request N is not visible in request N+1.
- **Memory:** 100,000 sequential requests of a fixture that allocates ~1MB of
  userland data each: process RSS after warm-up stays within ±5% (record numbers
  in handoff notes); `memory_get_usage()` at script start is identical across requests.
- No leaks reported by a ZTS **debug** PHP build for a 1,000-request run, or document
  why the check isn't feasible and what was done instead.

**Handoff notes:** _(fill in when done — S3 and S4 depend heavily on these)_

**Notes from other steps:** _(none yet)_

---

## S3 — PHP thread pool, timeouts, recycling (`k4p-php`)

**Depends on:** S2

**Goal:** `PhpPool` implementing `k4p_core::Executor` with N dedicated PHP threads,
hard-timeout watchdog with thread abandonment and replacement, per-thread recycling,
and a health signal. DESIGN.md §5.5.

**Tasks**
- `PhpRuntime::start(PhpConfig, LogSink) -> Result<PhpPool>`: runtime init on the
  calling thread (must be called once per process, before any PHP thread; enforce
  with a guard), then spawn `threads` `std::thread`s each doing `k4p_thread_init`
  and a work loop (channel in, oneshot out).
- `Executor::execute`: send work, await oneshot, race against `timeouts.hard`.
  On hard timeout: resolve `Err(HardTimeout)`, mark thread abandoned, spawn a
  replacement, discard any late result, let the abandoned thread exit if it ever
  returns. `capacity()` = configured thread count (stays constant across replacements).
- Recycling after `max_requests_per_thread`: finish request → `k4p_thread_shutdown`
  → exit → replacement spawned. No message dropped during recycling.
- `PhpPool::health() -> PoolHealth { live_threads, abandoned, unhealthy: bool }`
  and a `watch`/notify mechanism the binary (S8) can await for "became unhealthy".
- `PhpPool::shutdown(grace)`: stop accepting, wait for in-flight up to grace, join
  non-abandoned threads, runtime shutdown (only if no abandoned thread is still
  running PHP — otherwise skip runtime shutdown and document; the process is
  exiting anyway).
- `LogSink` trait bridging the C log callback into `tracing` (S8 decides formatting).

**Acceptance criteria**
- N=8 threads executing 10,000 messages concurrently with correct, uncorrupted
  results (each fixture echoes its input in `Content`; verify every one).
- Concurrency is real: 8 messages each sleeping 1s complete in ~1s, not 8s.
- Soft timeout via `max_execution_time` works under concurrency (proves per-thread
  timers) and doesn't affect other threads' requests.
- Hard timeout: a fixture blocking in C (e.g. `sleep()` with `max_execution_time=0`,
  or a blocking socket read) yields `HardTimeout` at the configured time, capacity
  is restored, other threads unaffected; after `max_abandoned_threads` the pool
  reports unhealthy.
- Recycling: with `max_requests_per_thread=10`, 1,000 requests succeed and thread
  replacement count matches expectations.
- Calling `execute` more than `capacity()` times concurrently is documented
  behaviour (queue or error — pick one, document it in DESIGN.md §5.5).
- Graceful shutdown with in-flight requests completes them within grace.

**Handoff notes:** _(fill in when done)_

**Notes from other steps:** _(none yet)_

---

## S4 — Opcache, preload & per-request cost under ZTS

**Depends on:** S2 (can run in parallel with S3)

**Goal:** prove opcache, the JIT and `opcache.preload` work with the custom SAPI
under ZTS, establish the default INI, and measure per-request overhead so we can
make honest throughput claims.

**Tasks**
- Verify opcache is active for SAPI `kafka4php` (`opcache_get_status()` from a
  fixture; hits increase across requests).
- Verify `opcache.preload` (+ `opcache.preload_user` if needed) works in the
  embedded ZTS runtime: preloaded class available in requests without autoload.
  Fix the SAPI (in `k4p-php-sys`) if needed — coordinate via S2's notes if S3 is
  touching the same files concurrently; keep changes minimal.
- Verify JIT (`opcache.jit=tracing`) doesn't crash under multi-threaded execution
  (requires a minimal multi-threaded harness if S3 isn't done — a test-only loop is fine).
- Define the **default INI** shipped by the host (applied before user ini_file
  and overrides): opcache on, `opcache.validate_timestamps=0` recommended for
  production (documented), `disable_functions` for `pcntl_*` (DESIGN.md §5.6),
  sensible `memory_limit`. Put it in `k4p-php-sys` (or `k4p-php`) as a constant
  and document in DESIGN.md.
- Benchmark (criterion or a simple harness, single thread): per-request overhead
  for (a) empty entrypoint, (b) composer autoload + `Kernel::run` equivalent with a
  trivial handler, (c) the same with preload. Report requests/sec per thread with
  and without opcache.

**Acceptance criteria**
- Tests asserting opcache active + preload working, in the toolchain container.
- JIT multi-thread smoke test: ≥100k requests across ≥4 threads, no crash.
- Benchmark numbers recorded in the handoff notes and in a new
  DESIGN.md subsection "Performance characteristics".
- Default INI documented in DESIGN.md.

**Handoff notes:** _(fill in when done)_

**Notes from other steps:** _(none yet)_

---

## S5 — PHP userland framework (`php/`)

**Depends on:** S0 (only needs the contract in DESIGN.md §5.2 / §5.4 — no native code)

**Goal:** the `kafka4php/framework` composer package as specified in DESIGN.md §5.4.

**Tasks**
- `php/composer.json`: name `kafka4php/framework`, PHP `>=8.4`, PSR-4 `Kafka4php\`,
  dev deps PHPUnit + PHPStan.
- `NativeBridge` interface (`message(): array`, `result(array): void`) with
  `NativeFunctionsBridge` (calls `\Kafka4php\Native\*`) and `Testing\FakeNativeBridge`.
- `Message`, `Headers`, `OutboundMessage`, `Result\*`, `Handler`, `Router`,
  `Kernel`, `Testing\MessageBuilder` — exactly as DESIGN.md §5.4.
- Converting `Result` objects to the native array shapes in §5.2 (the only place
  that knows those shapes, besides the C side).
- `Kernel` exception policy: default `RetryLater("<Class>: <message>")`; option to
  map to `Fail` with `error_class`.
- `stubs/native.php` with the native functions' signatures + docblocks.
- `php/README.md`: short usage for handler authors.

**Acceptance criteria**
- `composer test` (PHPUnit) and `composer stan` (PHPStan max level) pass on
  `php:8.4-cli` and `php:8.5-cli` (run via `docker run`, no toolchain image needed).
- Tests cover: message parsing incl. binary/null key/value and duplicate headers;
  every result → array shape; router dispatch + unknown topic; exception policy
  both modes; handler returning wrong type.
- No dependency other than PHP itself at runtime.

**Handoff notes:** _(fill in when done)_

**Notes from other steps:** _(none yet)_

---

## S6 — Kafka consumer engine + dispatcher (`k4p-host`)

**Depends on:** S0, S1 (S1 only for the container + test Kafka)

**Goal:** the consumer side of DESIGN.md §6.1, §6.2, §6.6: consumers, per-partition
queues, dispatch to an `Executor`, offset storing, backpressure, rebalance handling.
Result actions other than `NoContent` are stubbed (S7) — for now, *every* outcome
just stores the offset, behind a `ResultHandler` trait that S7 will implement.

**Tasks**
- `HostConfig` / `ConsumerConfig` structs (plain Rust; TOML parsing is S8's job).
- Consumer construction with forced librdkafka settings (§6.1), passthrough
  properties, `group.instance.id`.
- Startup topic validation via metadata (returns a typed error; S8 maps to exit code).
- Dispatcher: per-partition FIFO, one in flight per partition, global cap at
  `executor.capacity()`, round-robin fairness, backpressure pause/resume (§6.2).
  Leave a hook for "head not due yet" (S7 implements due-time; here every message
  is due).
- `ResultHandler` trait: `async fn handle(&self, msg: &ConsumedRecord, outcome: Outcome) -> Result<(), ...>`;
  offset is stored only after it returns `Ok`. Default impl: store offset.
- Rebalance handling (§6.6) via a rust-rdkafka `ConsumerContext`: drop queued
  messages of revoked partitions, commit synchronously, ignore late completions
  for partitions no longer owned.
- Graceful stop API: `Host::shutdown(grace)` — stop dispatch, drain in-flight,
  commit, close consumers.
- Status API for health (S8): per consumer `joined | rebalancing(since) | failed`.

**Acceptance criteria** (FakeExecutor + real Kafka from `docker/compose.test.yml`)
- Messages on a 6-partition topic are each executed once; per-partition order
  preserved (verify via recorded execution order); at most one in flight per
  partition; never more than `capacity` in flight overall.
- Offsets: after processing N messages and restarting the host with the same group,
  nothing is reprocessed; after killing it mid-stream (drop without shutdown),
  reprocessing is limited to in-flight/uncommitted messages.
- Slow executor (5s per message, `max.poll.interval.ms=3000`) does **not** get the
  consumer kicked out of the group.
- Backpressure pauses and resumes partitions (observable via a test hook/log).
- Rebalance: starting a second host instance in the same group mid-stream moves
  partitions; no message is lost; completions for revoked partitions are not
  committed by the old owner.
- Static membership: host with `group.instance.id` restarted within
  `session.timeout.ms` gets the same partitions without a rebalance being
  triggered for other members (document how this was observed).

**Handoff notes:** _(fill in when done)_

**Notes from other steps:** _(none yet)_

---

## S7 — Result actions: produce, retry tiers, DLQ (`k4p-host`)

**Depends on:** S6

**Goal:** implement DESIGN.md §6.3–6.5 — the real `ResultHandler`, retry topics
with due-time delays, DLQ, header conventions, `ExecutionError` mapping.

**Tasks**
- Shared idempotent producer (`acks=all`, `enable.idempotence=true`) with
  passthrough properties.
- `Content`: produce all, await all acks, then OK. `Content([])` ≡ `NoContent`.
- `Fail` → DLQ with headers; `RetryLater` → tier selection / max attempts → DLQ;
  `Err(ExecutionError)` → `RetryLater { reason: err.to_string() }`.
- `k4p-*` header building (first hop vs subsequent hops keep `k4p-original-*`),
  and on consume: strip `k4p-*`, populate `InboundMessage.topic` /
  `delivery_topic` / `attempt`.
- Subscribing each consumer to its retry topics (`<topic>.retry.<tier>`) and
  including retry + DLQ topics in startup validation.
- Due-time: implement S6's "head not due" hook — pause the partition, timer to
  re-evaluate at `k4p-due-at`.
- Produce-failure handling: no offset store, exponential backoff (cap 30s),
  partition stays blocked, `warn` logs.

**Acceptance criteria** (FakeExecutor + real Kafka)
- `Content` with 3 messages to 2 topics: all produced with correct key/headers/
  partition before the offset is stored (kill between produce and store → message
  reprocessed, outputs duplicated — expected and asserted as at-least-once).
- `Fail` → exactly one DLQ record with all §6.5 headers and the original
  key/value/user headers byte-identical.
- `RetryLater` with tiers `["2s","5s"]`, `max_attempts=4`: record visits
  `.retry.2s`, `.retry.5s`, `.retry.5s`, then DLQ with `max_attempts_exceeded`;
  each delivery happens no earlier than its due time; `attempt` increments 1→4 as
  seen by the executor; `topic` stays the logical topic.
- A not-yet-due retry record doesn't block other partitions or source topics.
- Each `ExecutionError` variant takes the retry path.
- With the DLQ topic's broker unreachable/unwritable (e.g. ACL-less trick or
  stopping the broker briefly), the partition blocks, no offset is stored, and
  processing resumes correctly once produce succeeds.
- Startup fails with a typed error when a retry or DLQ topic is missing.

**Handoff notes:** _(fill in when done)_

**Notes from other steps:** _(none yet)_

---

## S8 — Binary: config, wiring, signals, health (`kafka4php`)

**Depends on:** S3, S7

**Goal:** the `kafka4php` executable per DESIGN.md §7.

**Tasks**
- CLI (`clap`): `kafka4php [--config PATH]`, `kafka4php check-config [--config PATH]`
  (validate + print resolved config with secrets redacted), `--version`.
- TOML parsing into S6/S3 config structs, `${VAR}` / `${VAR:-default}` expansion,
  durations (`"10s"`), validation with precise errors (e.g. `timeouts.hard` must be
  greater than `php.ini.max_execution_time`; `threads >= 1`; tier list non-empty;
  `ordering == "partition"`).
- Logging (`tracing-subscriber`, json/text, level), PHP `LogSink` → `tracing` with
  `source=php`, per-message fields (§7.4).
- Startup order: config → logging → PHP runtime + pool (on main thread, before
  tokio worker threads start running PHP — design this carefully: PHP runtime
  init must happen once, on a thread that outlives the pool) → host (topic
  validation) → health server → run.
- Signals (§7.3), pool-unhealthy → graceful shutdown with exit 75, exit codes.
- `/healthz` (§7.2) via a tiny HTTP server (`axum` or `hyper`).
- Warn if `group_instance_id` unset.

**Acceptance criteria**
- Unit tests for config parsing, env expansion, validation errors, durations.
- Integration test in the toolchain container with real Kafka + real PHP fixture
  entrypoint: messages in → `Content` out on another topic; SIGTERM mid-stream →
  exit 0, in-flight messages completed and committed; restart processes the rest.
- Hard-timeout fixture repeated past `max_abandoned_threads` → exit 75.
- Bad config → exit 78 with a useful message; missing topic → 78.
- `/healthz` returns 200 when running, 503 during startup/unhealthy.

**Handoff notes:** _(fill in when done)_

**Notes from other steps:** _(none yet)_

---

## S9 — Production image, example app, E2E suite

**Depends on:** S4, S5, S8

**Goal:** shippable Docker image, a realistic example app, and end-to-end tests
covering DESIGN.md §9's E2E scenarios.

**Tasks**
- `docker/Dockerfile` (multi-stage per DESIGN.md §8), `ARG PHP_VERSION`,
  `HEALTHCHECK`, non-root user, default INI from S4, `/etc/kafka4php/kafka4php.toml`
  location, OCI labels. Keep the runtime image free of the Rust toolchain.
- `examples/basic/`: composer app using `kafka4php/framework` (path repository to
  `php/`), handlers demonstrating each result type, a preload script,
  `kafka4php.toml`, `Dockerfile` (`FROM` the production image), `compose.yml`
  (Kafka + topic-creation init container + the app).
- `tests/e2e/`: a test runner (Rust test crate or a script using `kcat` in a
  container — pick one and document) covering every E2E scenario in DESIGN.md §9,
  including crash + restart with static membership and graceful SIGTERM.
- Build + E2E for both PHP 8.4 and 8.5; amd64 required, arm64 build verified via
  buildx (E2E on arm64 optional).
- A `scripts/ci.sh` running everything (fmt, clippy, all tests, PHP tests, image
  build, E2E) so a CI system can be wired later.

**Acceptance criteria**
- `scripts/ci.sh` passes end to end on a clean checkout.
- Every E2E scenario passes; the crash scenario demonstrates no rebalance for other
  members (or documents precisely what was observed).
- Runtime image size recorded in handoff notes.
- `docker run kafka4php:dev --version` and `check-config` work.

**Handoff notes:** _(fill in when done)_

**Notes from other steps:** _(none yet)_

---

## S10 — User documentation

**Depends on:** S9

**Goal:** documentation for people *using* kafka4php (DESIGN.md is for people
building it).

**Tasks**
- `README.md`: what/why (short), quick start with `examples/basic`, link to docs.
- `docs/user/`: getting started; writing handlers (results, idempotency,
  at-least-once, retries & DLQ semantics, ordering caveat for retries); config
  reference (every key, generated or checked against the config structs);
  operations (static membership, health, exit codes, sizing threads vs partitions,
  timeouts, recycling); PHP compatibility (`PHP_SAPI`, process-wide state hazards,
  pcntl, extensions, opcache/preload); performance characteristics (from S4).
- Verify every code/config snippet in the docs actually works (paste into the
  example app or a test).

**Acceptance criteria**
- A person following only the README quick start gets messages flowing through a
  PHP handler.
- Config reference covers every config key that exists in code (add a test or
  script that fails if a key is undocumented).

**Handoff notes:** _(fill in when done)_

**Notes from other steps:** _(none yet)_
