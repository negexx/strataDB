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
    list_orders_lexicographically_across_a_directory_boundary(&*make_backend());
    put_overwrites_with_truncate_and_replace_semantics(&*make_backend());
    empty_payload_round_trips(&*make_backend());
    get_on_a_never_written_key_errors(&*make_backend());
    list_with_empty_prefix_returns_everything_put_so_far(&*make_backend());
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

/// A real object store's `list` is a flat, lexicographic-by-key scan, not a
/// directory traversal — a backend that (like a naive tree-walk) returns
/// results in traversal order rather than sorted-by-key order would pass
/// every other single-directory test here while still diverging from S3.
/// `"a/z.bin"` sorts before `"b/a.bin"` lexicographically but would be
/// visited second by a traversal that lists `"b/"` before `"a/"`.
fn list_orders_lexicographically_across_a_directory_boundary(backend: &dyn Backend) {
    backend.put("conformance/sort-b/a.bin", b"b").unwrap();
    backend.put("conformance/sort-a/z.bin", b"a").unwrap();

    let listed = backend.list("conformance/sort-").unwrap();

    let keys: Vec<&str> = listed.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(
        keys,
        vec!["conformance/sort-a/z.bin", "conformance/sort-b/a.bin"],
        "list must return keys in lexicographic order, got {keys:?}"
    );
}

/// `put`'s documented contract is truncate-and-replace, not append or
/// merge — a second `put` to the same key must fully replace the first
/// payload.
fn put_overwrites_with_truncate_and_replace_semantics(backend: &dyn Backend) {
    backend.put("conformance/overwrite.bin", b"first").unwrap();
    backend.put("conformance/overwrite.bin", b"second").unwrap();

    assert_eq!(backend.get("conformance/overwrite.bin").unwrap(), b"second");
}

/// A zero-length payload is a legitimate object, not an error case or a
/// stand-in for "missing" — both a full `get` and a zero-length
/// `get_range` must round-trip it as `Ok(vec![])`.
fn empty_payload_round_trips(backend: &dyn Backend) {
    backend.put("conformance/empty.bin", b"").unwrap();

    assert_eq!(backend.get("conformance/empty.bin").unwrap(), b"");
    assert_eq!(
        backend.get_range("conformance/empty.bin", 0..0).unwrap(),
        b""
    );
}

/// `get` on a key that was never written must error on its own, not just
/// incidentally as a side effect of a prior `delete` in the same group.
fn get_on_a_never_written_key_errors(backend: &dyn Backend) {
    assert!(backend.get("conformance/never-written.bin").is_err());
}

/// `list("")` must return every object put so far — the semantics M4's
/// `strata vacuum` orphan-object GC depends on to enumerate the whole
/// keyspace.
fn list_with_empty_prefix_returns_everything_put_so_far(backend: &dyn Backend) {
    backend.put("conformance/list-all-1.bin", b"1").unwrap();
    backend.put("conformance/list-all-2.bin", b"2").unwrap();

    let listed = backend.list("").unwrap();
    let keys: Vec<&str> = listed.iter().map(|m| m.key.as_str()).collect();

    assert!(
        keys.contains(&"conformance/list-all-1.bin"),
        "list(\"\") must include conformance/list-all-1.bin, got {keys:?}"
    );
    assert!(
        keys.contains(&"conformance/list-all-2.bin"),
        "list(\"\") must include conformance/list-all-2.bin, got {keys:?}"
    );
}
