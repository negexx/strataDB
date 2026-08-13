# Phase 3 Age Retention Implementation Plan

1. Add a backward-compatible manifest commit timestamp with a zero default for
   legacy state.
2. Add failing tests covering age pruning, active-snapshot protection, and the
   latest-version window.
3. Implement typed age authority and deletion under the existing lifecycle and
   commit locks.
4. Add the bounded maintenance operation that compacts, retains, vacuums, and
   reports final inventory evidence.
5. Run formatting, workspace checks, strict clippy, targeted runtime tests in
   GitHub Actions, and the lifecycle benchmark before declaring the slice
   complete.

No cross-process coordination, serializability, or unknown-object deletion is
part of this plan.
