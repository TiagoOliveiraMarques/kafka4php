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
| S1 | Toolchain Docker image (ZTS + embed PHP, Rust) | — | done | Claude Code (Opus 5.5), 2026-10-10 |
| S2 | C SAPI, single thread (`k4p-php-sys`) | S0, S1 | done | Claude Code sub-agent (Opus 5.5), 2026-10-10 |
| S3 | PHP thread pool + timeouts + recycling (`k4p-php`) | S2 | done | Claude Code (Opus 5.5), 2026-10-17 |
| S4 | Opcache, preload & per-request cost under ZTS | S2 | todo | |
| S5 | PHP userland framework (`php/`) | S0 | in-progress | Claude Code (Opus 5.5), 2026-10-17 |
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

**Handoff notes:**
- **Deviation (documented in DESIGN.md §8):** the official `php:8.4/8.5-zts-bookworm`
  images (8.4.26 / 8.5.11 at the time) **already** ship `--enable-embed --enable-zts`,
  `libphp.so`, Zend max execution timers (on by default with ZTS) and OPcache. So PHP
  is **not rebuilt**: `docker/php-check.sh` verifies those and fails the image build
  if any is missing (review decision on PR #2: fail loudly rather than diverge from
  the base image).
- **S2 (updated in S2's review):** a ZTS debug PHP is available as an opt-in variant
  of the same image: `--build-arg PHP_DEBUG=1` / `PHP_DEBUG=1 scripts/dev.sh …`.
  Headers: `php-config --includes`; `sapi/embed/php_embed.h` is installed.
- **CI:** `.github/workflows/build.yml`, on every PR and push to `main`, for PHP 8.4
  and 8.5: `build and test` builds the image (GitHub Actions cache per version) and
  runs `cargo fmt --check`, clippy `-D warnings` and `cargo test --workspace
  --all-features` inside it; `leak check` runs `crates/k4p-php-sys/tests/leak-check.sh`
  (added in S2). **New test suites must run in this workflow** (anything `cargo test`
  picks up does automatically). The full `scripts/ci.sh` is S9.
- **Files:** `docker/dev.Dockerfile` (+ `dev.Dockerfile.dockerignore`: the build
  context is only `rust-toolchain.toml` and `docker/php-check.sh`), `docker/php-check.sh`,
  `docker/compose.test.yml`, `scripts/dev.sh`, `scripts/dev.ps1`, `scripts/kafka-check.sh`,
  `.github/workflows/build.yml`.
- **Image contents:** Rust 1.99.0 from `rust-toolchain.toml` (+ clippy, rustfmt) in
  `/usr/local/{rustup,cargo}`; clang 14 / libclang-dev, cmake 3.25, pkg-config,
  OpenSSL 3.0, zstd 1.5.4, SASL 2.1.28 headers, git, unzip, Composer 2.10.3. It runs
  as root. Image size is about 2.4 GB for each of 8.4 and 8.5.
- **Running things:** `scripts/dev.sh <cmd>` (Git Bash/Linux) or `scripts\dev.ps1 <cmd>`.
  `PHP_VERSION=8.5` selects the image. `target/` is a per-PHP-version named volume
  (`k4p-target-php8.4`), so the host `target/` stays empty. The image is rebuilt (cached)
  on each call. TTY is auto-detected; override it with `K4P_TTY=0|1`. Gotcha: Windows
  PowerShell 5.1 strips embedded `"` from native-command arguments, so pass complex
  `bash -c '...'` commands from Git Bash.
- **Kafka:** `apache/kafka:4.3.1` (KRaft, single node), network `kafka4php-test`,
  `kafka:9092` (in network) / `localhost:19092` (host), **auto topic creation off**,
  group initial rebalance delay 0. `scripts/dev.sh --with-kafka <cmd>` starts it,
  waits for it to be healthy and sets `K4P_KAFKA_BOOTSTRAP=kafka:9092`. Check command:
  `docker compose -f docker/compose.test.yml up -d --wait && scripts/kafka-check.sh`
  (creates a 3-partition topic, then produces and consumes 3 records from a separate container
  on the network). Stop with `docker compose -f docker/compose.test.yml down -v`.
- **Verified:** on 8.4 and 8.5, `php -v` shows ZTS, the SAPIs include `embed`, `libphp.so`
  exists, the timers are enabled, `Zend OPcache` is loaded, `docker-php-ext-install pdo_mysql`
  works, and `cargo build --workspace` passes in the image (8.4 also passes `cargo test`).
  `scripts/dev.sh cargo --version` (Git Bash) and `scripts\dev.ps1 cargo --version`
  (PowerShell) work. The dev container reaches `kafka:9092` via `--with-kafka`, and
  `kafka-check.sh` reports OK.
- Only amd64 was verified; arm64 is for S9 (buildx).

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

**Handoff notes:**
- **Built** (`crates/k4p-php-sys/`): `csrc/k4p_sapi.h` (the C ABI, plain C types);
  `csrc/k4p_sapi.c` (SAPI module `kafka4php`, native module/extension `kafka4php`
  with `Kafka4php\Native\message()` / `result()`, per-thread phase + outcome slots,
  script execution, classification, lifecycle), `csrc/k4p_result.c` (`result()`
  validation/conversion), `csrc/k4p_output.c` (log sink, stdout lines),
  `csrc/k4p_buf.c` (malloc'd buffers/outcomes, UTF-8), `csrc/k4p_internal.h`;
  `build.rs` (php-config → `cc` on every `csrc/*.c` → link `libphp.so` → bindgen of
  our header only); `src/lib.rs` (`ffi` bindings; public API `RuntimeConfig`,
  `InitError`, `runtime_init`/`runtime_shutdown` (unsafe), `thread_init`,
  `thread_shutdown`, `execute`; the C conversions are private), fixtures in
  `tests/fixtures/`, tests `sapi`,
  `memory`, `leaks`, and the debug-PHP leak check (`tests/leak-check.sh`, which runs
  in the `PHP_DEBUG=1` variant of the toolchain image: `docker/php-debug-build.sh`).
- **The final C ABI, thread-local design, INI handling and log callback contract are
  in DESIGN.md §5.1.1**; validation rules in §5.2, classification in §5.3. Read
  those rather than this summary.
- **DESIGN.md changes (deviations/refinements, all in this commit):**
  - §5.1: the entrypoint is not run through `php_execute_script()` but through a
    mirror of it + `zend_execute_script()` (auto_prepend/append honoured, no chdir),
    to capture the uncaught exception's class/message before PHP reports it.
    `register_shutdown_function()` callbacks run before classification (their
    `result()` counts); destructors at request end run after it (`result()` there
    throws `\LogicException`).
  - §5.3: soft timeout is detected with `PG(connection_status) &
    PHP_CONNECTION_TIMEOUT`, because `EG(timed_out)` is reset before the fatal error
    is raised. Uncaught `\ValueError` from `result()` itself → `InvalidResult` (not
    `UncaughtException`). `\ParseError`/`\CompileError` → `FatalError` (that's how
    PHP reports them). An exception handled by a user `set_exception_handler()` is
    not uncaught.
  - §5.2: strict validation (unknown keys rejected; `key` optional; topic and header
    names must be valid UTF-8; partition `0..=2^31-1`).
  - §5.1 SAPI properties: headers discarded, `header()` never warns; `$_SERVER` also
    has `SCRIPT_FILENAME`/`SCRIPT_NAME`/`PHP_SELF`/`PATH_TRANSLATED`.
  - §8 build.rs: `php-config --includes --prefix` only (no `--libs`), `$PHP_CONFIG`,
    `links = "php"`, no rpath (ld cache). §9: fixture path and leak check.
  - §5.1: **PHP 8.4 OPcache is inactive under SAPI name `kafka4php`.** Reviewed
    and accepted as a limitation: no OPcache on 8.4, OPcache on 8.5, no workaround
    (see S4's notes).
- **Gotchas for S3:** see S3's *Notes from other steps*. Also: PHP can't be
  restarted in a process, so every test binary that runs PHP must be its own
  process with `harness = false` (`tests/common/mod.rs` is a reusable runner).
  `k4p_runtime_init` is not thread-safe (guard it). The runtime thread may execute
  messages itself, but don't: it is also the thread that must call
  `runtime_shutdown`. Rust's runtime already ignores `SIGPIPE`; the SAPI doesn't
  touch signal dispositions (no `signal(SIGPIPE, SIG_IGN)` like embed does).
- **Gotchas for S4:** see S4's notes: no OPcache on 8.4 (accepted limitation); benchmark outside the `/work`
  bind mount (an empty request costs ~0.05 ms from `/tmp`, ~5 ms from `/work` on
  Docker Desktop for Windows); `opcache.file_update_protection`.
- **Acceptance evidence** (both PHP 8.4.26 and 8.5.11, toolchain image):
  - `cargo test -p k4p-php-sys --test sapi`: 23 cases pass — every outcome kind
    (incl. 27 malformed-result shapes each → `InvalidResult` with the `\ValueError`
    in the `error` log, caught `\ValueError` → `InvalidResult`, `memory_limit`
    exhaustion / compile / parse errors → `FatalError`, `SoftTimeout` with
    `max_execution_time=1` after ~1 s and the thread usable afterwards, result then
    `exit()` → the result, `exit()` alone → `NoResult`), binary round trip (`\0`,
    `0xff`, invalid UTF-8 in key/value/header values, offsets > 2^53) byte-identical
    both ways (checked independently via PHP-side `bin2hex`), duplicate headers in
    order, `echo`/`print` lines → `stdout`/INFO and warnings/notices/`error_log()` →
    `error`/WARN (fatal → ERROR), `PHP_SAPI === 'kafka4php'`, isolation (globals,
    constants, static vars/props, INI, output buffers, error handlers reset every
    request), `execute` without `thread_init` → `Bootstrap`, a second thread
    init/execute/shutdown.
  - `memory` (100,000 requests, ≥ 1.84 MiB of userland data each — 10,000 strings
    + 1,000 objects): RSS after warm-up (1,000 requests) and at every 10,000:
    **8.4: 31,172 KiB (2nd run: 31,076 KiB) constant (+0.00%) at all 11 samples; 8.5: 34,236 KiB (2nd run: 34,260 KiB) constant
    (+0.00%)**. `memory_get_usage()` at script start: 8.4 386,352 in every request;
    8.5 413,464 in every request from the 2nd on (request 1: 415,744 — OPcache
    compiles the script there). ~570 req/s (8.4, no OPcache) / ~670 req/s (8.5),
    debug Rust build. The test asserts ±5% and identical start usage from request 2.
  - **Leak check: no leaks.** `tests/leak-check.sh` builds a ZTS debug PHP
    (`--enable-debug --enable-zend-test`) on top of the toolchain image and runs, on
    it, the `leaks` test (1,000 requests over every fixture: Content 196, Fail 120,
    FatalError 120, InvalidResult 120, NoContent 120, NoResult 80, RetryLater 160,
    SoftTimeout 4, UncaughtException 80) and the whole `sapi` suite: zero leak
    reports, all ZEND_ASSERTs hold. Positive control (`zend_leak_bytes(64)` from
    `zend_test`) is reported (`… test.c(173) : Freeing 0x… (64 bytes), script=…`),
    proving the reports are captured. Verified on 8.4 and 8.5 (both `leak-check: OK`).
  - `cargo fmt --check`, `cargo clippy --workspace --all-targets --all-features -- -D
    warnings` clean; `cargo test --workspace --all-features` passes on 8.4 and 8.5
    (k4p-core tests included).
- Test run times: `sapi` ~3 s, `leaks` ~10 s (soft timeouts), `memory` ~3 min.
  `K4P_MEMORY_REQUESTS` shortens the memory test locally; `K4P_TEST_ECHO_LOG=1`
  echoes PHP log lines; a substring argument filters cases
  (`cargo test -p k4p-php-sys --test sapi -- soft`).
- The debug image is test-only and S2's: `kafka4php-dev:php<ver>-debug`, built
  `FROM kafka4php-dev:php<ver>` by `leak-check.sh` (5–10 min the first time); it
  does not rely on `docker/dev.Dockerfile` build args.

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

**Handoff notes:**
- **Built** (`crates/k4p-php/`): `PhpPool` (`src/pool.rs`: `start`, `impl Executor`,
  `health`, `health_watch`, `shutdown`), `PhpConfig`/`StartError` (`src/config.rs`),
  `LogSink`/`LogRecord`/`MessageContext`/`TracingLogSink` + the C log callback
  (`src/log.rs`), shared state and job queue (`src/state.rs`), the runtime thread's
  supervisor/watchdog (`src/supervisor.rs`), the PHP worker loop (`src/worker.rs`).
  Tests: `tests/pool.rs`, `tests/hard_timeout.rs`, `tests/recycling.rs` (each its own
  process, `harness = false`, runner in `tests/common/mod.rs`, fixtures in
  `tests/fixtures/`, copied to a temp dir like S2's memory test).
- **The as-built behaviour is DESIGN.md §5.5** (rewritten in this change); read it
  rather than this summary.
- **Deviations from this plan's proposed API (all in DESIGN.md):**
  - `PhpPool::start(PhpConfig, impl LogSink)` instead of `PhpRuntime::start`; PHP is
    initialised on a **dedicated runtime thread** (`php-runtime`), not the calling
    thread. That thread also runs the watchdog and is the one that calls
    `runtime_shutdown`, so `start`/`shutdown` may be called from any thread (no
    "same thread as init" constraint for S8 under tokio). §5.1's lifecycle table
    now says "runtime thread" instead of "main thread".
  - Beyond-capacity `execute` calls **queue** (FIFO); the hard timeout starts when a
    thread picks the message up.
  - The watchdog runs on the runtime thread, independent of the execute future being
    polled (a dropped future cannot leak capacity). Queued messages whose future was
    dropped are skipped.
  - `unhealthy` = `abandoned_total ≥ max_abandoned_threads` (lifetime count, sticky)
    or a failed thread spawn. `PoolHealth` also has `abandoned` (still stuck) and
    `recycled_total`.
  - `shutdown(grace)` is **blocking** (S8: `spawn_blocking`) and idempotent; it
    returns `ShutdownReport { abandoned_at_shutdown, abandoned_threads,
    runtime_shut_down }`. While closing, every request's deadline is cut to the end
    of the grace period, so the watchdog enforces grace and hard timeout alike.
  - Recycling is jittered (PR review): each thread draws its limit uniformly from
    `[max - max/10, max]`, so threads started together don't all retire at once.
  - A thread whose request returned `Bootstrap` (failed `php_request_startup`) is
    replaced. This path is not covered by a test (can't make request startup fail on
    demand).
- **Acceptance evidence** (PHP 8.4.26 and 8.5.11, toolchain image, debug Rust build):
  - `pool` (8 threads): 10,000 concurrent echo requests (binary key/value, null and
    duplicate headers, varying partition/offset/attempt) all returned byte-identical
    with their own coordinates, in ~0.65–0.7 s; 8 × 1 s sleeps finished in 1.00 s;
    16 × 0.5 s sleeps (2 × capacity) queued and finished at 0.50 s / 1.00 s; 4
    busy loops with `set_time_limit(1)` → `SoftTimeout` while 4 concurrent 1.5 s
    sleeps on the other threads succeeded, threads reused afterwards; log lines
    carry the message context; second `start` → `AlreadyStarted` (zero hard timeout →
    `InvalidConfig`); shutdown with 8 in-flight 1 s requests (grace 5 s) completed
    all of them in ~1.01 s, then `runtime_shut_down: true`, and `execute` answers
    `Bootstrap`.
  - `hard_timeout` (2 threads, hard 1 s, max abandoned 2): a `usleep(3 s)` with no
    soft timeout → `HardTimeout` at 1.00 s while the other thread served ~2,000
    requests; replacement live immediately (2 × 0.5 s sleeps in parallel while the
    abandoned thread still blocks); the abandoned thread exits after its request
    (`abandoned` back to 0). Second abandonment → `unhealthy` via `health_watch`,
    pool still serving. Shutdown (grace 300 ms) with a stuck request → it gets
    `HardTimeout` after 300 ms, report `{ abandoned_at_shutdown: 1,
    abandoned_threads: 2, runtime_shut_down: false }`.
  - `recycling` (4 threads, 10 requests/thread): 1,000 concurrent requests all
    correct; `recycled_total` within the exact bound `9·r ≤ 1000 < 10·r + 4·10`
    (limits drawn in 9..=10); clean shutdown afterwards.
  - Found and fixed on 8.5: callers could receive `HardTimeout` before `health()`
    showed the abandonment. Callers are now answered after the replacement is
    spawned and health published; 15 consecutive runs of the three suites on 8.5
    passed afterwards.
- `cargo fmt --check`, `cargo clippy --workspace --all-targets --all-features -- -D
  warnings` clean; `cargo test --workspace --all-features` passes on 8.4 and 8.5.
- Run times: `pool` ~7 s, `hard_timeout` ~5 s, `recycling` < 1 s. A substring argument
  filters cases (`cargo test -p k4p-php --test pool -- soft`); `K4P_TEST_ECHO_LOG=1`
  echoes PHP log lines.
- **Gotchas for S8:** see S8's *Notes from other steps*.

**Notes from other steps:**
- From S2: read S2's handoff notes first. In short: `k4p_php_sys::runtime_init` is
  not thread-safe and PHP cannot be restarted in a process, so guard it with a
  process-wide flag (and tests need `harness = false`, see
  `crates/k4p-php-sys/tests/common/mod.rs`). A worker thread must call
  `thread_init()` before and `thread_shutdown()` after its requests (recycling =
  `thread_shutdown()` + exit; verified that a fresh thread works after another one
  was shut down). `execute()` returning `Err(Bootstrap(..))` with "unusable" means
  `php_request_startup()` failed: replace that thread. The log callback runs
  synchronously on the PHP worker thread, so per-message context (consumer/topic/
  partition/offset) can come from a Rust thread-local set around `execute()`.
  Soft timeouts use per-thread `SIGRTMIN` timers (`timer_create` with
  `SIGEV_THREAD_ID`): never block `SIGRTMIN` in PHP threads. A soft-timed-out request
  is a normal `Err(SoftTimeout)` and the thread stays usable.

---

## S4 — Opcache, preload & per-request cost under ZTS

**Depends on:** S2 (can run in parallel with S3)

**Goal:** prove opcache, the JIT and `opcache.preload` work with the custom SAPI
under ZTS, establish the default INI, and measure per-request overhead so we can
make honest throughput claims.

**Tasks**
- Verify opcache is active for SAPI `kafka4php` on PHP 8.5 (`opcache_get_status()`
  from a fixture; hits increase across requests), and inactive on PHP 8.4 (accepted
  limitation, DESIGN.md §5.1). Preload and JIT work is 8.5-only.
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

**Notes from other steps:**
- From S2: **benchmark from the container's own filesystem, not `/work`.** On Docker
  Desktop for Windows the repo bind mount makes every `open()`/`stat()` cost
  milliseconds: an empty request took ~5 ms with the entrypoint under `/work` and
  ~0.05 ms (opcache on or off) from a copy in `/tmp`. Copy fixtures to a temp dir as
  `crates/k4p-php-sys/tests/memory.rs` does.
- From S2, **decided:** on **PHP 8.4 OPcache is inactive under our SAPI, and that
  is an accepted limitation — do not work around it.** `ext/opcache/ZendAccelerator.c`
  `accel_find_sapi()` (8.4.26) only enables OPcache for `apache`, `fastcgi`,
  `cli-server`, `cgi-fcgi`, `fpm-fcgi`, `fpmi-fcgi`, `apache2handler`, `litespeed`,
  `uwsgi`, `fuzzer`, `frankenphp`, `ngx-php` (and `cli`/`phpdbg` with
  `enable_cli`). With `sapi_module.name = "kafka4php"` it is loaded
  (`extension_loaded('Zend OPcache')`) but `opcache_get_status()` returns false. On
  8.5 the list is gone and OPcache is active (hits increase per request). So for S4:
  the opcache, preload and JIT tasks and tests apply to **PHP 8.5**. On 8.4, add a
  test asserting OPcache is *inactive* (so a PHP change is noticed), and benchmark
  8.4 without OPcache only. The default INI must not break 8.4. DESIGN.md §5.1
  records the limitation.
- From S2: on 8.5, `opcache.enable` defaults to 1 for the `kafka4php` SAPI (OPcache
  is built in; 8.4 loads it via `conf.d/docker-php-ext-opcache.ini`). Note
  `opcache.file_update_protection` (2 s): freshly written fixtures are not cached for
  2 s; the memory test sets it to 0. The SAPI's own INI defaults and the INI
  precedence are in DESIGN.md §5.1.1; S2 imposes nothing else (PHP defaults apply:
  `max_execution_time=30`, `memory_limit=128M`). The `ini_defaults` hook in
  `csrc/k4p_sapi.c` (`k4p_sapi_ini_defaults`) is the place for overridable
  defaults if you want the default INI below user ini files.
- From S2: `opcache.preload` runs during `php_module_startup()` on the runtime thread,
  outside any request: `Kafka4php\Native\message()` throws `\LogicException` there.

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

**Notes from other steps:**
- From S3: build the executor with `k4p_php::PhpPool::start(PhpConfig, TracingLogSink)`
  (map `[php]`/`[php.ini]`/`[timeouts]`: `[php.ini]` → `ini_entries` as `key=value`
  lines; `threads`, `max_requests_per_thread`, `max_abandoned_threads` are `NonZero*`,
  `hard_timeout` must be > 0 — validate these and "hard > max_execution_time" yourself
  for exit 78). `StartError::Init` → exit 70. Any thread may call `start`/`shutdown`.
  `shutdown(grace)` blocks: call it via `spawn_blocking` *after* the host has drained,
  using what is left of `shutdown_grace`. Pool-unhealthy restart (§7.3, exit 75):
  `pool.health_watch().wait_for(|h| h.unhealthy)`. `/healthz`: `pool.health()`.
  PHP log records carry topic/partition/offset/attempt but **not the consumer name**
  (`k4p-php` doesn't know it): derive it from `delivery_topic` if you need the
  `consumer` field on PHP lines (§7.4). Pool events (abandoned thread etc.) are
  `tracing` events from target `k4p_php::…`; PHP output uses target `php`.

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

**Notes from other steps:**
- From S2 (decision): no OPcache on PHP 8.4 under SAPI `kafka4php` (DESIGN.md §5.1). The
  example app's preload script and any OPcache/preload E2E checks only apply to the 8.5
  image; the 8.4 E2E run must not depend on OPcache.

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

**Notes from other steps:**
- From S2 (decision): document in the PHP compatibility page (and the README) that
  PHP 8.4 runs **without OPcache** (and so without preload / JIT) because PHP 8.4
  only enables OPcache for a fixed list of SAPI names; PHP 8.5 has full OPcache.
  Recommend the 8.5 image when performance matters. DESIGN.md §5.1.
