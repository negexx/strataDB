//! Admission control between transaction preparation and lifecycle execution.
//!
//! A transaction owns a [`PreparationLease`] from the first instruction of
//! `Transaction::commit` until publication, typed failure, or panic unwind.
//! Each individual mutating lifecycle operation obtains
//! [`LifecycleExclusiveGuard`] before `Dataset.commit_lock`; this module
//! deliberately never takes that lock. `Dataset::maintain` invokes several
//! such operations sequentially, so it does not retain one guard for the
//! composite run and writers may interleave between phases.

#[cfg(loom)]
use loom::sync::{Condvar, Mutex};
#[cfg(not(loom))]
use std::sync::{Condvar, Mutex};

#[derive(Default)]
struct State {
    waiting_executors: usize,
    active_preparations: usize,
    executor_active: bool,
}

/// Shared writer-preferring coordinator for commit preparation and lifecycle
/// execution for one lifecycle operation. A queued executor prevents later
/// preparations from entering so continuous commits cannot starve that
/// exclusive operation; it does not make a multi-operation maintenance run
/// atomic.
#[derive(Default)]
pub(crate) struct LifecycleCoordinator {
    state: Mutex<State>,
    wake: Condvar,
}

impl LifecycleCoordinator {
    /// Acquires a lease that admits one transaction to prepare and publish a
    /// commit. The lease must live through the entire commit scope.
    pub(crate) fn acquire_preparation(&self) -> PreparationLease<'_> {
        let mut state = self.lock();
        while state.executor_active || state.waiting_executors > 0 {
            state = self.wait(state);
        }
        assert!(
            state.active_preparations < usize::MAX,
            "lifecycle preparation lease count overflow"
        );
        state.active_preparations += 1;
        PreparationLease { coordinator: self }
    }

    /// Acquires exclusive lifecycle access after every admitted preparation
    /// has completed. Lifecycle execution acquires this before
    /// `Dataset.commit_lock`.
    pub(crate) fn acquire_exclusive(&self) -> LifecycleExclusiveGuard<'_> {
        let mut state = self.lock();
        assert!(
            state.waiting_executors < usize::MAX,
            "waiting lifecycle executor count overflow"
        );
        state.waiting_executors += 1;
        while state.executor_active || state.active_preparations > 0 {
            state = self.wait(state);
        }
        state.waiting_executors -= 1;
        state.executor_active = true;
        LifecycleExclusiveGuard { coordinator: self }
    }

    #[cfg(test)]
    pub(crate) fn wait_for_executor_to_queue(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let state = self.lock();
            if state.waiting_executors > 0 {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "exclusive acquisition never joined the writer-preference queue"
            );
            drop(state);
            std::thread::yield_now();
        }
    }

    #[cfg(not(loom))]
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(loom)]
    fn lock(&self) -> loom::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(not(loom))]
    fn wait<'a>(
        &self,
        state: std::sync::MutexGuard<'a, State>,
    ) -> std::sync::MutexGuard<'a, State> {
        self.wake
            .wait(state)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(loom)]
    fn wait<'a>(
        &self,
        state: loom::sync::MutexGuard<'a, State>,
    ) -> loom::sync::MutexGuard<'a, State> {
        self.wake
            .wait(state)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// RAII admission lease for one in-flight transaction preparation.
pub(crate) struct PreparationLease<'a> {
    coordinator: &'a LifecycleCoordinator,
}

impl Drop for PreparationLease<'_> {
    fn drop(&mut self) {
        let mut state = self.coordinator.lock();
        assert!(
            state.active_preparations > 0,
            "preparation lease drop must match preparation admission"
        );
        state.active_preparations -= 1;
        if state.active_preparations == 0 {
            self.coordinator.wake.notify_all();
        }
    }
}

/// RAII guard for exclusive lifecycle execution.
pub(crate) struct LifecycleExclusiveGuard<'a> {
    coordinator: &'a LifecycleCoordinator,
}

impl Drop for LifecycleExclusiveGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.coordinator.lock();
        assert!(
            state.executor_active,
            "exclusive lifecycle guard drop must match exclusive acquisition"
        );
        state.executor_active = false;
        self.coordinator.wake.notify_all();
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    use super::LifecycleCoordinator;

    const WAIT: Duration = Duration::from_millis(100);
    const DEADLINE: Duration = Duration::from_secs(2);

    #[test]
    fn exclusive_acquisition_waits_for_the_last_preparation_lease() {
        // Break caught: allowing lifecycle execution to begin while any commit
        // can still create a prepared file races execution with publication.
        let coordinator = Arc::new(LifecycleCoordinator::default());
        let preparation = coordinator.acquire_preparation();
        let (exclusive_acquired_tx, exclusive_acquired_rx) = mpsc::channel();
        let exclusive_coordinator = Arc::clone(&coordinator);
        let exclusive = std::thread::spawn(move || {
            let _guard = exclusive_coordinator.acquire_exclusive();
            exclusive_acquired_tx.send(()).unwrap();
        });

        assert!(
            exclusive_acquired_rx.recv_timeout(WAIT).is_err(),
            "exclusive access must wait until every preparation lease drops"
        );
        drop(preparation);
        exclusive_acquired_rx.recv_timeout(DEADLINE).unwrap();
        exclusive.join().unwrap();
    }

    #[test]
    fn waiting_exclusive_acquisition_blocks_new_preparation_admission() {
        // Break caught: admitting a new preparation after an executor is
        // waiting starves lifecycle execution under continuous commits.
        let coordinator = Arc::new(LifecycleCoordinator::default());
        let first_preparation = coordinator.acquire_preparation();
        let (exclusive_acquired_tx, exclusive_acquired_rx) = mpsc::channel();
        let exclusive_coordinator = Arc::clone(&coordinator);
        let exclusive = std::thread::spawn(move || {
            let _guard = exclusive_coordinator.acquire_exclusive();
            exclusive_acquired_tx.send(()).unwrap();
        });
        coordinator.wait_for_executor_to_queue();

        let (preparation_acquired_tx, preparation_acquired_rx) = mpsc::channel();
        let preparation_coordinator = Arc::clone(&coordinator);
        let later_preparation = std::thread::spawn(move || {
            let _lease = preparation_coordinator.acquire_preparation();
            preparation_acquired_tx.send(()).unwrap();
        });

        assert!(
            preparation_acquired_rx.recv_timeout(WAIT).is_err(),
            "a queued executor must block later preparation admission"
        );
        drop(first_preparation);
        exclusive_acquired_rx.recv_timeout(DEADLINE).unwrap();
        exclusive.join().unwrap();
        preparation_acquired_rx.recv_timeout(DEADLINE).unwrap();
        later_preparation.join().unwrap();
    }

    #[test]
    fn preparation_and_exclusive_guards_release_on_all_scope_exits() {
        // Break caught: a lease leaked on a normal return, typed error, or
        // panic would permanently block the opposite lifecycle operation.
        fn return_after_preparation(coordinator: &LifecycleCoordinator) -> Result<(), ()> {
            let _lease = coordinator.acquire_preparation();
            Err(())
        }

        fn return_after_exclusive(coordinator: &LifecycleCoordinator) -> Result<(), ()> {
            let _guard = coordinator.acquire_exclusive();
            Err(())
        }

        let coordinator = LifecycleCoordinator::default();

        let preparation = coordinator.acquire_preparation();
        drop(preparation);
        let exclusive = coordinator.acquire_exclusive();
        drop(exclusive);

        assert_eq!(return_after_preparation(&coordinator), Err(()));
        drop(coordinator.acquire_exclusive());

        assert_eq!(return_after_exclusive(&coordinator), Err(()));
        drop(coordinator.acquire_preparation());

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _lease = coordinator.acquire_preparation();
                panic!("test preparation panic");
            }))
            .is_err()
        );
        drop(coordinator.acquire_exclusive());

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _guard = coordinator.acquire_exclusive();
                panic!("test exclusive panic");
            }))
            .is_err()
        );
        drop(coordinator.acquire_preparation());
    }
}

#[cfg(all(test, loom))]
#[allow(clippy::unwrap_used)]
mod loom_tests {
    use loom::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::LifecycleCoordinator;

    struct Activity {
        preparations: AtomicUsize,
        exclusive: AtomicUsize,
    }

    #[test]
    fn preparation_and_exclusive_execution_never_overlap() {
        loom::model(|| {
            let coordinator = Arc::new(LifecycleCoordinator::default());
            let activity = Arc::new(Activity {
                preparations: AtomicUsize::new(0),
                exclusive: AtomicUsize::new(0),
            });
            let exclusive_finished = Arc::new(AtomicUsize::new(0));
            let first_preparation = coordinator.acquire_preparation();
            activity.preparations.fetch_add(1, Ordering::SeqCst);

            let exclusive_coordinator = Arc::clone(&coordinator);
            let exclusive_activity = Arc::clone(&activity);
            let exclusive_completed = Arc::clone(&exclusive_finished);
            let exclusive = loom::thread::spawn(move || {
                let _guard = exclusive_coordinator.acquire_exclusive();
                assert_eq!(
                    exclusive_activity.preparations.load(Ordering::SeqCst),
                    0,
                    "exclusive lifecycle execution must wait for preparation"
                );
                assert_eq!(
                    exclusive_activity.exclusive.fetch_add(1, Ordering::SeqCst),
                    0,
                    "at most one lifecycle executor may be active"
                );
                loom::thread::yield_now();
                assert_eq!(
                    exclusive_activity.preparations.load(Ordering::SeqCst),
                    0,
                    "preparation overlapped exclusive lifecycle execution"
                );
                exclusive_activity.exclusive.fetch_sub(1, Ordering::SeqCst);
                exclusive_completed.store(1, Ordering::SeqCst);
            });

            loop {
                let state = coordinator.lock();
                let executor_is_waiting = state.waiting_executors > 0;
                drop(state);
                if executor_is_waiting {
                    break;
                }
                loom::thread::yield_now();
            }

            let later_coordinator = Arc::clone(&coordinator);
            let later_activity = Arc::clone(&activity);
            let later_exclusive_finished = Arc::clone(&exclusive_finished);
            let later_preparation = loom::thread::spawn(move || {
                let _lease = later_coordinator.acquire_preparation();
                assert_eq!(
                    later_exclusive_finished.load(Ordering::SeqCst),
                    1,
                    "a queued executor must complete before later preparation admission"
                );
                assert_eq!(
                    later_activity.exclusive.load(Ordering::SeqCst),
                    0,
                    "later preparation must not overlap exclusive lifecycle execution"
                );
            });

            activity.preparations.fetch_sub(1, Ordering::SeqCst);
            drop(first_preparation);
            exclusive.join().unwrap();
            later_preparation.join().unwrap();
        });
    }
}
