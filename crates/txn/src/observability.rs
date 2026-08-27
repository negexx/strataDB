//! Bounded operational events for one in-process [`crate::Dataset`] handle.
//!
//! This is deliberately not a durable audit log.  It gives an embedding
//! application a structured, redacted hand-off point for its own exporter.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// The operation categories emitted by the transaction engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationalEventKind {
    DatasetCreated,
    DatasetOpened,
    TransactionBegan,
    TransactionCommitted,
    TransactionConflict,
    TransactionFailed,
    LifecycleSucceeded,
    LifecycleFailed,
}

/// The outcome attached to an operational event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationalEventOutcome {
    Succeeded,
    Conflict,
    Failed,
}

/// One redacted event.  Sequence IDs are unique and ordered only within one
/// shared `Dataset` handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationalEvent {
    pub sequence: u64,
    pub kind: OperationalEventKind,
    pub outcome: OperationalEventOutcome,
}

/// Optional allow-list selectors used by [`OperationalEventLog::snapshot`] and
/// [`OperationalEventLog::drain`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OperationalEventFilter {
    pub kind: Option<OperationalEventKind>,
    pub outcome: Option<OperationalEventOutcome>,
}

impl OperationalEventFilter {
    fn matches(self, event: &OperationalEvent) -> bool {
        self.kind.is_none_or(|kind| kind == event.kind)
            && self.outcome.is_none_or(|outcome| outcome == event.outcome)
    }
}

/// Fixed-capacity event journal shared by clones of one dataset handle.
pub struct OperationalEventLog {
    capacity: usize,
    events: Mutex<VecDeque<OperationalEvent>>,
    next_sequence: AtomicU64,
    dropped: AtomicU64,
}

impl OperationalEventLog {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            events: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
            next_sequence: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }
    }

    pub(crate) fn record(&self, kind: OperationalEventKind, outcome: OperationalEventOutcome) {
        let mut events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Allocate the sequence while holding the same mutex that orders
        // insertion.  Otherwise concurrent recorders could publish sequence
        // 1 before sequence 0 and violate the journal's ordering contract.
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let event = OperationalEvent {
            sequence,
            kind,
            outcome,
        };
        if events.len() == self.capacity {
            events.pop_front();
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        events.push_back(event);
    }

    pub(crate) fn snapshot(&self, filter: OperationalEventFilter) -> Vec<OperationalEvent> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|event| filter.matches(event))
            .copied()
            .collect()
    }

    pub(crate) fn drain(&self, filter: OperationalEventFilter) -> Vec<OperationalEvent> {
        let mut events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut drained = Vec::new();
        let mut retained = VecDeque::with_capacity(events.len());
        while let Some(event) = events.pop_front() {
            if filter.matches(&event) {
                drained.push(event);
            } else {
                retained.push_back(event);
            }
        }
        *events = retained;
        drained
    }

    pub(crate) fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn bounded_journal_orders_filters_and_counts_evictions() {
        let log = OperationalEventLog::new(2);
        log.record(
            OperationalEventKind::TransactionBegan,
            OperationalEventOutcome::Succeeded,
        );
        log.record(
            OperationalEventKind::TransactionConflict,
            OperationalEventOutcome::Conflict,
        );
        log.record(
            OperationalEventKind::TransactionCommitted,
            OperationalEventOutcome::Succeeded,
        );

        assert_eq!(log.dropped_count(), 1);
        assert_eq!(
            log.snapshot(OperationalEventFilter::default()),
            vec![
                OperationalEvent {
                    sequence: 1,
                    kind: OperationalEventKind::TransactionConflict,
                    outcome: OperationalEventOutcome::Conflict,
                },
                OperationalEvent {
                    sequence: 2,
                    kind: OperationalEventKind::TransactionCommitted,
                    outcome: OperationalEventOutcome::Succeeded,
                },
            ]
        );
        assert_eq!(
            log.drain(OperationalEventFilter {
                kind: Some(OperationalEventKind::TransactionConflict),
                outcome: None,
            }),
            vec![OperationalEvent {
                sequence: 1,
                kind: OperationalEventKind::TransactionConflict,
                outcome: OperationalEventOutcome::Conflict,
            }]
        );
        assert_eq!(log.snapshot(OperationalEventFilter::default()).len(), 1);
    }

    #[test]
    fn concurrent_recorders_publish_in_sequence_order() {
        let log = Arc::new(OperationalEventLog::new(800));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let log = Arc::clone(&log);
                scope.spawn(move || {
                    for _ in 0..100 {
                        log.record(
                            OperationalEventKind::TransactionBegan,
                            OperationalEventOutcome::Succeeded,
                        );
                    }
                });
            }
        });

        let events = log.snapshot(OperationalEventFilter::default());
        assert_eq!(events.len(), 800);
        assert!(
            events
                .windows(2)
                .all(|pair| pair[0].sequence < pair[1].sequence)
        );
    }
}
