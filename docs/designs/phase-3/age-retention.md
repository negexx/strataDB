# Phase 3 Age Retention and Conditional Bounds

Strata exposes age retention as an explicit shared-handle operation. Each newly
published manifest records a UTC microsecond commit timestamp; older manifests
without that field are treated conservatively and are never age-pruned.

`Dataset::prune_manifests_by_age(AgeRetentionPolicy)` protects the current
manifest, the requested latest-version window, and every active snapshot owned
by the handle. It deletes only eligible historical manifest objects while
holding lifecycle exclusivity and the publication lock.

`Dataset::maintain(LifecycleMaintenancePolicy)` composes compaction, age
retention, recognized-object vacuum, and a final inventory. It reports
`storage_bound_met` rather than claiming a universal bound: active snapshots,
retained history, or unsupported object types can keep physical growth above a
requested limit and remain visible in the report.

Independent openers and cross-process retention are intentionally excluded;
that boundary remains reserved for Phase 4.
