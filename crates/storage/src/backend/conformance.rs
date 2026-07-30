//! A backend-agnostic conformance suite for any [`crate::backend::Backend`]
//! impl. Run against `LocalFs` in this milestone; run again unmodified
//! against `S3Backend`/`InMemory` in a later milestone — see
//! `docs/superpowers/specs/2026-07-30-phase9-object-storage-backend-design.md`
//! §4. Not a `#[cfg(test)] mod tests` itself: it's a reusable function
//! `mod tests` blocks in this crate call into, kept in its own file because
//! it's meant to outlive this milestone's own tests.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::backend::{Backend, ObjectMeta};
use crate::error::StorageError;

/// Runs the full conformance suite against a fresh backend from
/// `make_backend`. Called once per assertion group so each group starts
/// from an empty backend, rather than sharing state across assertions.
pub(crate) fn run(make_backend: impl Fn() -> Box<dyn Backend>) {
    put_then_get_round_trips(&*make_backend());
    get_range_reads_a_byte_span(&*make_backend());
    put_if_absent_succeeds_once_then_collides(&*make_backend());
    list_finds_a_put_key_under_its_prefix(&*make_backend());
    delete_removes_a_key(&*make_backend());
}

fn put_then_get_round_trips(backend: &dyn Backend) {
    backend.put("conformance/a.bin", b"hello").unwrap();
    assert_eq!(backend.get("conformance/a.bin").unwrap(), b"hello");
}

fn get_range_reads_a_byte_span(backend: &dyn Backend) {
    backend.put("conformance/range.bin", b"0123456789").unwrap();
    assert_eq!(
        backend.get_range("conformance/range.bin", 2..5).unwrap(),
        b"234"
    );
}

fn put_if_absent_succeeds_once_then_collides(backend: &dyn Backend) {
    backend
        .put_if_absent("conformance/once.bin", b"first")
        .unwrap();
    let result = backend.put_if_absent("conformance/once.bin", b"second");
    assert!(
        matches!(result, Err(StorageError::AlreadyExists(_))),
        "expected AlreadyExists, got {result:?}"
    );
    assert_eq!(backend.get("conformance/once.bin").unwrap(), b"first");
}

fn list_finds_a_put_key_under_its_prefix(backend: &dyn Backend) {
    backend.put("conformance/listed/x.bin", b"x").unwrap();
    let listed = backend.list("conformance/listed/").unwrap();
    assert_eq!(
        listed,
        vec![ObjectMeta {
            key: "conformance/listed/x.bin".to_string(),
            size: 1
        }]
    );
}

fn delete_removes_a_key(backend: &dyn Backend) {
    backend.put("conformance/gone.bin", b"x").unwrap();
    backend.delete("conformance/gone.bin").unwrap();
    assert!(backend.get("conformance/gone.bin").is_err());
}
