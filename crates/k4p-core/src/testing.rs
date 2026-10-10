//! Test helpers for crates that drive an [`Executor`] (feature `testing`).
//!
//! [`FakeExecutor`] stands in for the PHP pool in `k4p-host` tests: it returns
//! scripted outcomes, can take time to "execute", records every message it receives
//! and tracks how many executions are in flight.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use crate::{BoxFuture, Executor, HandlerResult, InboundMessage, Outcome};

type OutcomeFn = dyn Fn(&InboundMessage) -> Outcome + Send + Sync;
type DelayFn = dyn Fn(&InboundMessage) -> Duration + Send + Sync;

/// Identifies one Kafka record: `(delivery_topic, partition, offset)`.
pub type RecordId = (String, i32, i64);

/// A scriptable [`Executor`]. Cheap to clone; clones share all state, so a test can
/// keep one handle and give another to the code under test.
///
/// Outcome resolution, first match wins:
/// 1. an outcome registered for the exact record with [`on_record`](Self::on_record)
///    (matched on `delivery_topic`, `partition`, `offset`);
/// 2. the closure set with [`with_outcome_fn`](Self::with_outcome_fn);
/// 3. the default outcome, `Ok(NoContent)` unless changed with
///    [`with_default`](Self::with_default).
///
/// Messages are recorded, and counted as in flight, when `execute` is called; they
/// stop being in flight when the returned future completes or is dropped.
#[derive(Clone)]
pub struct FakeExecutor {
    inner: Arc<Inner>,
}

struct Inner {
    capacity: usize,
    default: Outcome,
    outcome_fn: Option<Box<OutcomeFn>>,
    delay_fn: Option<Box<DelayFn>>,
    by_record: Mutex<HashMap<RecordId, Outcome>>,
    received: Mutex<Vec<InboundMessage>>,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

impl FakeExecutor {
    /// A fake that reports `capacity` and answers `Ok(NoContent)` immediately.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                capacity,
                default: Ok(HandlerResult::NoContent),
                outcome_fn: None,
                delay_fn: None,
                by_record: Mutex::new(HashMap::new()),
                received: Mutex::new(Vec::new()),
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
            }),
        }
    }

    /// Outcome for messages with no more specific script.
    pub fn with_default(self, outcome: Outcome) -> Self {
        self.configure(|inner| inner.default = outcome)
    }

    /// Decide the outcome per message with a closure (e.g. by `attempt` or `value`).
    pub fn with_outcome_fn(
        self,
        f: impl Fn(&InboundMessage) -> Outcome + Send + Sync + 'static,
    ) -> Self {
        self.configure(|inner| inner.outcome_fn = Some(Box::new(f)))
    }

    /// Every execution takes `delay` before resolving.
    pub fn with_delay(self, delay: Duration) -> Self {
        self.with_delay_fn(move |_| delay)
    }

    /// Execution time per message, decided by a closure.
    pub fn with_delay_fn(
        self,
        f: impl Fn(&InboundMessage) -> Duration + Send + Sync + 'static,
    ) -> Self {
        self.configure(|inner| inner.delay_fn = Some(Box::new(f)))
    }

    /// Script the outcome for one record. May be called at any time, also while the
    /// fake is in use; replaces an earlier script for the same record.
    pub fn on_record(&self, delivery_topic: &str, partition: i32, offset: i64, outcome: Outcome) {
        lock(&self.inner.by_record).insert((delivery_topic.to_owned(), partition, offset), outcome);
    }

    /// All messages received so far, in the order `execute` was called.
    pub fn received(&self) -> Vec<InboundMessage> {
        lock(&self.inner.received).clone()
    }

    /// Ids of all messages received so far, in the order `execute` was called.
    pub fn received_ids(&self) -> Vec<RecordId> {
        lock(&self.inner.received).iter().map(record_id).collect()
    }

    /// Executions currently in flight.
    pub fn in_flight(&self) -> usize {
        self.inner.in_flight.load(Ordering::SeqCst)
    }

    /// Highest number of executions that were in flight at the same time.
    pub fn max_in_flight(&self) -> usize {
        self.inner.max_in_flight.load(Ordering::SeqCst)
    }

    fn configure(self, f: impl FnOnce(&mut Inner)) -> Self {
        let mut inner = Arc::try_unwrap(self.inner)
            .unwrap_or_else(|_| panic!("configure FakeExecutor before cloning it"));
        f(&mut inner);
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl Inner {
    fn outcome_for(&self, msg: &InboundMessage) -> Outcome {
        if let Some(outcome) = lock(&self.by_record).get(&record_id(msg)) {
            return outcome.clone();
        }
        match &self.outcome_fn {
            Some(f) => f(msg),
            None => self.default.clone(),
        }
    }
}

impl Executor for FakeExecutor {
    fn execute(&self, msg: InboundMessage) -> BoxFuture<'static, Outcome> {
        let inner = &self.inner;
        let outcome = inner.outcome_for(&msg);
        let delay = inner.delay_fn.as_ref().map_or(Duration::ZERO, |f| f(&msg));
        lock(&inner.received).push(msg);
        let now = inner.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        inner.max_in_flight.fetch_max(now, Ordering::SeqCst);
        let guard = InFlight(Arc::clone(inner));

        Box::pin(async move {
            let _guard = guard;
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            outcome
        })
    }

    fn capacity(&self) -> usize {
        self.inner.capacity
    }
}

impl fmt::Debug for FakeExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FakeExecutor")
            .field("capacity", &self.inner.capacity)
            .field("received", &lock(&self.inner.received).len())
            .field("in_flight", &self.in_flight())
            .field("max_in_flight", &self.max_in_flight())
            .finish_non_exhaustive()
    }
}

/// Decrements the in-flight counter when an execution finishes or is dropped.
struct InFlight(Arc<Inner>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A first-delivery message (`attempt` 1, not via a retry topic) with no key,
/// value, headers or timestamp. Adjust fields with struct update syntax.
pub fn inbound(topic: &str, partition: i32, offset: i64) -> InboundMessage {
    InboundMessage {
        topic: topic.to_owned(),
        delivery_topic: topic.to_owned(),
        partition,
        offset,
        timestamp_ms: None,
        key: None,
        value: None,
        headers: Vec::new(),
        attempt: 1,
    }
}

fn record_id(msg: &InboundMessage) -> RecordId {
    (msg.delivery_topic.clone(), msg.partition, msg.offset)
}

/// Locks ignoring poisoning: a panicking test thread must not hide later assertions.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::*;
    use crate::ExecutionError;

    fn retry(reason: &str) -> Outcome {
        Ok(HandlerResult::RetryLater {
            reason: reason.into(),
        })
    }

    #[tokio::test]
    async fn defaults_to_no_content() {
        let fake = FakeExecutor::new(1);
        assert_eq!(
            fake.execute(inbound("t", 0, 0)).await,
            Ok(HandlerResult::NoContent)
        );
    }

    #[tokio::test]
    async fn scripted_outcomes_follow_precedence() {
        let fake = FakeExecutor::new(1)
            .with_default(Err(ExecutionError::NoResult))
            .with_outcome_fn(|msg| match msg.partition {
                1 => retry("from closure"),
                _ => Err(ExecutionError::SoftTimeout),
            });
        fake.on_record("t", 1, 7, Err(ExecutionError::HardTimeout));

        // Exact record beats the closure.
        assert_eq!(
            fake.execute(inbound("t", 1, 7)).await,
            Err(ExecutionError::HardTimeout)
        );
        // Same offset on another partition or topic is a different record.
        assert_eq!(
            fake.execute(inbound("t", 1, 8)).await,
            retry("from closure")
        );
        assert_eq!(
            fake.execute(inbound("other", 1, 7)).await,
            retry("from closure")
        );
        // The closure beats the default.
        assert_eq!(
            fake.execute(inbound("t", 0, 7)).await,
            Err(ExecutionError::SoftTimeout)
        );

        let no_fn = FakeExecutor::new(1).with_default(Err(ExecutionError::NoResult));
        assert_eq!(
            no_fn.execute(inbound("t", 0, 0)).await,
            Err(ExecutionError::NoResult)
        );
    }

    #[tokio::test]
    async fn on_record_matches_delivery_topic_and_can_be_rescripted() {
        let fake = FakeExecutor::new(1);
        let shared = fake.clone();
        fake.on_record("t.retry.5s", 0, 3, retry("first"));
        let via_retry = InboundMessage {
            delivery_topic: "t.retry.5s".into(),
            attempt: 2,
            ..inbound("t", 0, 3)
        };
        assert_eq!(shared.execute(via_retry.clone()).await, retry("first"));

        shared.on_record("t.retry.5s", 0, 3, Ok(HandlerResult::NoContent));
        assert_eq!(fake.execute(via_retry).await, Ok(HandlerResult::NoContent));
    }

    #[tokio::test]
    async fn outcome_fn_sees_the_message() {
        let fake = FakeExecutor::new(1).with_outcome_fn(|msg| {
            if msg.attempt < 3 {
                retry("not yet")
            } else {
                Ok(HandlerResult::NoContent)
            }
        });
        for (attempt, expected) in [
            (1, retry("not yet")),
            (2, retry("not yet")),
            (3, Ok(HandlerResult::NoContent)),
        ] {
            let msg = InboundMessage {
                attempt,
                ..inbound("t", 0, 0)
            };
            assert_eq!(fake.execute(msg).await, expected);
        }
    }

    #[tokio::test]
    async fn records_received_messages_in_call_order() {
        let fake = FakeExecutor::new(4);
        let a = InboundMessage {
            key: Some(b"k\0\xff".to_vec()),
            ..inbound("a", 0, 1)
        };
        let b = inbound("b", 2, 5);
        let fut_a = fake.execute(a.clone());
        let fut_b = fake.execute(b.clone());
        // Recorded at call time, before the futures are polled.
        assert_eq!(fake.received(), vec![a, b]);
        fut_b.await.unwrap();
        fut_a.await.unwrap();
        assert_eq!(
            fake.received_ids(),
            vec![("a".to_owned(), 0, 1), ("b".to_owned(), 2, 5)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_delay() {
        let fake = FakeExecutor::new(1).with_delay(Duration::from_secs(5));
        let start = Instant::now();
        fake.execute(inbound("t", 0, 0)).await.unwrap();
        assert_eq!(start.elapsed(), Duration::from_secs(5));
    }

    #[tokio::test(start_paused = true)]
    async fn per_message_delay_and_concurrent_executions() {
        let fake = FakeExecutor::new(3)
            .with_delay_fn(|msg| Duration::from_secs(u64::try_from(msg.offset).unwrap()));
        let start = Instant::now();
        let futs: Vec<_> = (1..=3).map(|o| fake.execute(inbound("t", 0, o))).collect();
        assert_eq!(fake.in_flight(), 3);

        let handles: Vec<_> = futs
            .into_iter()
            .map(|f| {
                tokio::spawn(async move {
                    f.await.unwrap();
                    Instant::now()
                })
            })
            .collect();
        let mut finished = Vec::new();
        for h in handles {
            finished.push(h.await.unwrap() - start);
        }
        // Executions overlap: the slowest one decides the total time.
        assert_eq!(finished, [1, 2, 3].map(Duration::from_secs).to_vec());
        assert_eq!(start.elapsed(), Duration::from_secs(3));
        assert_eq!(fake.in_flight(), 0);
        assert_eq!(fake.max_in_flight(), 3);
    }

    #[tokio::test]
    async fn capacity_is_reported_and_in_flight_tracked() {
        let fake = FakeExecutor::new(8);
        assert_eq!(fake.capacity(), 8);
        assert_eq!(fake.clone().capacity(), 8);
        assert_eq!(FakeExecutor::new(1).capacity(), 1);

        let first = fake.execute(inbound("t", 0, 0));
        let second = fake.execute(inbound("t", 0, 1));
        assert_eq!((fake.in_flight(), fake.max_in_flight()), (2, 2));
        drop(first); // a dropped (cancelled) execution is no longer in flight
        assert_eq!((fake.in_flight(), fake.max_in_flight()), (1, 2));
        second.await.unwrap();
        assert_eq!((fake.in_flight(), fake.max_in_flight()), (0, 2));
    }

    #[test]
    #[should_panic(expected = "configure FakeExecutor before cloning it")]
    fn configuring_a_shared_fake_panics() {
        let fake = FakeExecutor::new(1);
        let _other = fake.clone();
        let _ = fake.with_delay(Duration::from_secs(1));
    }
}
