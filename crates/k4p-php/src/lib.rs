//! The PHP side of kafka4php: the embedded PHP runtime and its thread pool, with the
//! hard-timeout watchdog and per-thread recycling. Implements [`k4p_core::Executor`].
//! DESIGN.md §5.5.
//!
//! Threads:
//!
//! - **runtime thread** (`php-runtime`, module `supervisor`): starts PHP, then supervises
//!   (watchdog, replacing threads), then shuts PHP down. Never runs a request.
//! - **PHP threads** (`php-<n>`, module `worker`): run one request at a time from a shared
//!   FIFO queue. No zval crosses threads: messages go in and outcomes come out as
//!   owned Rust values.
//!
//! All of them coordinate through `state::Shared`.

mod config;
mod log;
mod pool;
mod state;
mod supervisor;
mod worker;

pub use config::{PhpConfig, StartError};
pub use k4p_core::Executor;
pub use log::{LogLevel, LogRecord, LogSink, LogSource, MessageContext, TracingLogSink};
pub use pool::PhpPool;
pub use state::PoolHealth;
pub use supervisor::ShutdownReport;
