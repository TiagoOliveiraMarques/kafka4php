//! Pool configuration and start errors.

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use k4p_php_sys::{InitError, RuntimeConfig};

/// Configuration of the PHP runtime and its thread pool (DESIGN.md §5.5, §7.1 `[php]`
/// and `[timeouts]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhpConfig {
    /// Entry script executed once per message.
    pub entrypoint: PathBuf,
    /// php.ini path; `None` = PHP's default search. conf.d is always scanned.
    pub ini_file: Option<PathBuf>,
    /// INI overrides as `key=value` lines, applied after all ini files.
    pub ini_entries: String,
    /// PHP threads (= `Executor::capacity()`).
    pub threads: NonZeroUsize,
    /// Upper bound of requests after which a thread is replaced by a fresh one. Each
    /// thread draws its own limit in `[max - max/10, max]`, so they retire at
    /// different times.
    pub max_requests_per_thread: NonZeroU64,
    /// Watchdog: a request running longer is answered `HardTimeout` and its thread
    /// abandoned. Must be non-zero.
    pub hard_timeout: Duration,
    /// Abandoned threads (over the pool's lifetime) after which it reports unhealthy.
    pub max_abandoned_threads: NonZeroUsize,
}

impl PhpConfig {
    pub(crate) fn runtime(&self) -> RuntimeConfig {
        RuntimeConfig {
            entrypoint: self.entrypoint.clone(),
            ini_file: self.ini_file.clone(),
            ini_entries: self.ini_entries.clone(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), StartError> {
        if self.hard_timeout.is_zero() {
            return Err(StartError::InvalidConfig("hard_timeout must be > 0".into()));
        }
        Ok(())
    }
}

/// Why [`PhpPool::start`](crate::PhpPool::start) failed.
#[derive(Debug)]
pub enum StartError {
    /// The configuration is invalid; nothing was started.
    InvalidConfig(String),
    /// A pool was already started in this process (PHP can start only once).
    AlreadyStarted,
    /// PHP itself failed to start; details went to the log sink.
    Init(InitError),
    /// The runtime thread could not be spawned.
    Spawn(std::io::Error),
}

impl fmt::Display for StartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(m) => write!(f, "invalid PHP pool config: {m}"),
            Self::AlreadyStarted => f.write_str("PHP pool already started in this process"),
            Self::Init(e) => write!(f, "PHP runtime init failed: {e}"),
            Self::Spawn(e) => write!(f, "could not spawn the PHP runtime thread: {e}"),
        }
    }
}

impl std::error::Error for StartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Init(e) => Some(e),
            Self::Spawn(e) => Some(e),
            Self::InvalidConfig(_) | Self::AlreadyStarted => None,
        }
    }
}
