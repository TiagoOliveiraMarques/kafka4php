//! PHP log output (script stdout, PHP errors, SAPI diagnostics) → [`LogSink`].
//!
//! The C log callback runs synchronously on the PHP thread that produced the line
//! (DESIGN.md §5.1.1), so the message being executed is known there: workers set it
//! in a thread-local around each request and every record carries it.

use std::cell::RefCell;
use std::ffi::{CStr, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::OnceLock;

use k4p_core::InboundMessage;
use k4p_php_sys::ffi;

/// Receives every line PHP logs. Called on PHP threads, synchronously; implementations
/// must be thread-safe and should be quick (the request waits). Panics are swallowed.
pub trait LogSink: Send + Sync + 'static {
    fn log(&self, record: &LogRecord<'_>);
}

/// One line of PHP output.
#[derive(Debug, Clone, Copy)]
pub struct LogRecord<'a> {
    pub level: LogLevel,
    pub source: LogSource,
    /// Not NUL-terminated, line terminator stripped, may be invalid UTF-8.
    pub message: &'a [u8],
    /// The message being executed when the line was produced (`None` during runtime
    /// init/shutdown).
    pub context: Option<&'a MessageContext>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
}

/// Where a line came from (DESIGN.md §5.1.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogSource {
    /// Script output: `echo`, `print`, `var_dump`, … (one record per line).
    Stdout,
    /// PHP's error log: warnings, notices, fatal errors, uncaught exceptions, `error_log()`.
    Error,
    /// Diagnostics of the SAPI itself.
    Sapi,
}

impl LogSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Error => "error",
            Self::Sapi => "sapi",
        }
    }
}

/// Identifies the message a log line belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageContext {
    pub topic: String,
    pub delivery_topic: String,
    pub partition: i32,
    pub offset: i64,
    pub attempt: u32,
}

impl MessageContext {
    pub(crate) fn of(msg: &InboundMessage) -> Self {
        Self {
            topic: msg.topic.clone(),
            delivery_topic: msg.delivery_topic.clone(),
            partition: msg.partition,
            offset: msg.offset,
            attempt: msg.attempt,
        }
    }
}

/// Forwards PHP output to `tracing` (target `php`, field `source = "php"`), with the
/// `stream` (`stdout` / `error` / `sapi`) and the message's topic, partition, offset
/// and attempt. Formatting is up to the subscriber.
#[derive(Debug, Clone, Copy, Default)]
pub struct TracingLogSink;

macro_rules! php_event {
    ($level:expr, $($fields:tt)*) => {
        match $level {
            LogLevel::Error => tracing::error!(target: "php", $($fields)*),
            LogLevel::Warn => tracing::warn!(target: "php", $($fields)*),
            LogLevel::Info => tracing::info!(target: "php", $($fields)*),
        }
    };
}

impl LogSink for TracingLogSink {
    fn log(&self, r: &LogRecord<'_>) {
        let message = String::from_utf8_lossy(r.message);
        let stream = r.source.as_str();
        match r.context {
            Some(c) => php_event!(
                r.level,
                source = "php",
                stream,
                topic = %c.topic,
                delivery_topic = %c.delivery_topic,
                partition = c.partition,
                offset = c.offset,
                attempt = c.attempt,
                "{message}"
            ),
            None => php_event!(r.level, source = "php", stream, "{message}"),
        }
    }
}

/// The process-wide sink (PHP, and so its log callback, starts once per process).
static SINK: OnceLock<Box<dyn LogSink>> = OnceLock::new();

thread_local! {
    static CONTEXT: RefCell<Option<MessageContext>> = const { RefCell::new(None) };
}

/// Installs the sink; called once, before the runtime starts.
pub(crate) fn install(sink: Box<dyn LogSink>) {
    assert!(SINK.set(sink).is_ok(), "log sink installed twice");
}

/// Runs `f` (one request for `msg`) with `msg` as the log context of this thread.
pub(crate) fn with_context<T>(msg: &InboundMessage, f: impl FnOnce() -> T) -> T {
    CONTEXT.set(Some(MessageContext::of(msg)));
    let out = f();
    CONTEXT.set(None);
    out
}

/// The `k4p_log_fn` given to the C side.
pub(crate) unsafe extern "C" fn callback(
    _user_data: *mut c_void,
    level: ffi::k4p_log_level,
    source: *const c_char,
    msg: *const u8,
    len: usize,
) {
    // SAFETY: the C side passes a static NUL-terminated `source` and `len` bytes at
    // `msg` (valid for this call only).
    let (source, message) = unsafe {
        (
            CStr::from_ptr(source).to_bytes(),
            if len == 0 {
                &[][..]
            } else {
                std::slice::from_raw_parts(msg, len)
            },
        )
    };
    let level = match level {
        ffi::k4p_log_level::K4P_LOG_ERROR => LogLevel::Error,
        ffi::k4p_log_level::K4P_LOG_WARN => LogLevel::Warn,
        _ => LogLevel::Info,
    };
    let source = match source {
        b"stdout" => LogSource::Stdout,
        b"error" => LogSource::Error,
        _ => LogSource::Sapi,
    };
    // Must not unwind into C.
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let Some(sink) = SINK.get() else { return };
        CONTEXT.with_borrow(|context| {
            sink.log(&LogRecord {
                level,
                source,
                message,
                context: context.as_ref(),
            });
        });
    }));
}
