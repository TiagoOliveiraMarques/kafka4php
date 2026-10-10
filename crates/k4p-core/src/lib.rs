//! Shared contract between the Kafka side (`k4p-host`) and the PHP side (`k4p-php`).
//!
//! See DESIGN.md §4. Changing these types affects every crate — do so deliberately
//! and update the design document in the same change.
//!
//! This crate has no PHP and no Kafka dependencies.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[cfg(feature = "testing")]
pub mod testing;

/// Kafka headers: ordered, duplicate keys allowed (Kafka semantics). A `None` value
/// is a header with a null value, distinct from an empty one.
pub type Headers = Vec<(String, Option<Vec<u8>>)>;

/// A message handed to PHP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundMessage {
    /// Logical topic: the original topic, even when delivered via a retry topic.
    pub topic: String,
    /// Topic it was actually consumed from (`== topic` unless delivered via retry).
    pub delivery_topic: String,
    /// Partition of `delivery_topic`.
    pub partition: i32,
    /// Offset in `delivery_topic`.
    pub offset: i64,
    /// Record timestamp, unix milliseconds.
    pub timestamp_ms: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    /// User headers only; `k4p-*` headers are stripped by the host.
    pub headers: Headers,
    /// 1 on first delivery, incremented on each retry.
    pub attempt: u32,
}

/// A message the handler wants produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundMessage {
    pub topic: String,
    /// `None` = default partitioner (by key).
    pub partition: Option<i32>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Headers,
}

/// What the handler decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandlerResult {
    /// Commit, produce nothing.
    NoContent,
    /// Produce all (acks=all), then commit. An empty vec is equivalent to `NoContent`.
    Content(Vec<OutboundMessage>),
    /// Send the original message to the DLQ, then commit.
    Fail {
        reason: String,
        error_class: Option<String>,
    },
    /// Send the original message to the next retry tier, then commit.
    RetryLater { reason: String },
}

/// Why no [`HandlerResult`] could be obtained.
///
/// The `Display` string is used as the retry reason when the host maps an error to
/// `RetryLater` (DESIGN.md §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionError {
    /// The script finished without calling `result()`.
    NoResult,
    /// `result()` was called with a malformed array.
    InvalidResult(String),
    UncaughtException {
        class: String,
        message: String,
    },
    /// `E_ERROR` & friends, including `memory_limit` exhaustion.
    FatalError(String),
    /// `max_execution_time` hit (PHP-level timer).
    SoftTimeout,
    /// The watchdog fired; the PHP thread was abandoned.
    HardTimeout,
    /// The runtime or thread could not execute the message at all.
    Bootstrap(String),
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoResult => f.write_str("no result: script finished without calling result()"),
            Self::InvalidResult(msg) => write!(f, "invalid result: {msg}"),
            Self::UncaughtException { class, message } => {
                write!(f, "uncaught exception {class}: {message}")
            }
            Self::FatalError(msg) => write!(f, "fatal error: {msg}"),
            Self::SoftTimeout => f.write_str("soft timeout: max_execution_time exceeded"),
            Self::HardTimeout => f.write_str("hard timeout: PHP thread abandoned"),
            Self::Bootstrap(msg) => write!(f, "bootstrap error: {msg}"),
        }
    }
}

impl std::error::Error for ExecutionError {}

/// The result of executing one message.
pub type Outcome = Result<HandlerResult, ExecutionError>;

/// An owned, `Send` future. Same shape as `futures::future::BoxFuture`, defined here
/// so this crate needs no dependencies.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Executes messages. Implemented by the PHP pool (`k4p-php`) and by
/// `testing::FakeExecutor`; driven by the dispatcher (`k4p-host`).
pub trait Executor: Send + Sync + 'static {
    /// Execute one message as one PHP request. Resolves when the request finishes
    /// or the hard timeout fires. Must not be called more than `capacity()` times
    /// concurrently; the caller (dispatcher) enforces this.
    fn execute(&self, msg: InboundMessage) -> BoxFuture<'static, Outcome>;

    /// Number of messages that may execute concurrently (= live PHP threads).
    fn capacity(&self) -> usize;
}

impl<E: Executor + ?Sized> Executor for Arc<E> {
    fn execute(&self, msg: InboundMessage) -> BoxFuture<'static, Outcome> {
        (**self).execute(msg)
    }

    fn capacity(&self) -> usize {
        (**self).capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_error_display_is_a_useful_retry_reason() {
        let cases = [
            (
                ExecutionError::NoResult,
                "no result: script finished without calling result()",
            ),
            (
                ExecutionError::InvalidResult("missing key 'type'".into()),
                "invalid result: missing key 'type'",
            ),
            (
                ExecutionError::UncaughtException {
                    class: "RuntimeException".into(),
                    message: "db down".into(),
                },
                "uncaught exception RuntimeException: db down",
            ),
            (
                ExecutionError::FatalError("Allowed memory size exhausted".into()),
                "fatal error: Allowed memory size exhausted",
            ),
            (
                ExecutionError::SoftTimeout,
                "soft timeout: max_execution_time exceeded",
            ),
            (
                ExecutionError::HardTimeout,
                "hard timeout: PHP thread abandoned",
            ),
            (
                ExecutionError::Bootstrap("ts_resource failed".into()),
                "bootstrap error: ts_resource failed",
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(err.to_string(), expected);
        }
    }

    #[test]
    fn execution_error_is_std_error() {
        let err: Box<dyn std::error::Error + Send + Sync> = Box::new(ExecutionError::HardTimeout);
        assert!(err.source().is_none());
    }

    struct Constant;

    impl Executor for Constant {
        fn execute(&self, _msg: InboundMessage) -> BoxFuture<'static, Outcome> {
            Box::pin(async { Ok(HandlerResult::NoContent) })
        }

        fn capacity(&self) -> usize {
            3
        }
    }

    #[tokio::test]
    async fn arc_dyn_executor_delegates() {
        let exec: Arc<dyn Executor> = Arc::new(Constant);
        assert_eq!(exec.capacity(), 3);
        let msg = InboundMessage {
            topic: "t".into(),
            delivery_topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp_ms: None,
            key: None,
            value: None,
            headers: Vec::new(),
            attempt: 1,
        };
        assert_eq!(exec.execute(msg).await, Ok(HandlerResult::NoContent));
    }
}
