"""Behavioral tests for the checked unsafe-code admission inventory.

These tests exercise temporary Rust trees through the checker instead of
inspecting the checker source. Each test names a policy violation that must
fail closed.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from check_unsafe_inventory import (  # noqa: E402
    discover_rust_files,
    load_inventory,
    scan_rust_source,
    validate_inventory,
)


class UnsafeInventoryTest(unittest.TestCase):
    def write_tree(self, source: str, sites: list[dict]) -> tuple[Path, Path, tempfile.TemporaryDirectory[str]]:
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        (root / "src").mkdir()
        (root / "src" / "sample.rs").write_text(source, encoding="utf-8")
        inventory = root / "inventory.json"
        inventory.write_text(json.dumps({"schema_version": 1, "sites": sites}), encoding="utf-8")
        return root, inventory, temporary

    @staticmethod
    def site(
        identifier: str, kind: str, *, path: str = "src/sample.rs", safety: str = "The fixture maintains the stated invariant."
    ) -> dict:
        evidence = ["sample::tests::exercises_the_construct"]
        if kind != "block":
            evidence = ["review-only: sample invariant reviewed with its caller"]
        return {
            "id": identifier,
            "path": path,
            "kind": kind,
            "owner": "sample maintainers",
            "safety": safety,
            "evidence": evidence,
        }

    def assert_fails(self, source: str, sites: list[dict], expected: str) -> None:
        root, inventory, temporary = self.write_tree(source, sites)
        self.addCleanup(temporary.cleanup)
        self.assertTrue(
            any(expected in error for error in validate_inventory(root, inventory)),
            expected,
        )

    def test_valid_marked_block_function_and_impl_form_a_bijection(self) -> None:
        source = """// SAFETY[TEST-BLOCK]: The fixture block is intentionally empty.\nunsafe {}\n\n// SAFETY[TEST-FUNCTION]: Callers uphold the fixture precondition.\nunsafe fn sample() {}\n\nstruct Sample;\n// SAFETY[TEST-IMPL]: Sample has no state that invalidates Send.\nunsafe impl Send for Sample {}\n"""
        sites = [
            self.site("TEST-BLOCK", "block", safety="The fixture block is intentionally empty."),
            self.site("TEST-FUNCTION", "function", safety="Callers uphold the fixture precondition."),
            self.site("TEST-IMPL", "impl", safety="Sample has no state that invalidates Send."),
        ]
        root, inventory, temporary = self.write_tree(source, sites)
        self.addCleanup(temporary.cleanup)
        self.assertEqual(validate_inventory(root, inventory), [])

    def test_unmarked_unsafe_construct_fails(self) -> None:
        self.assert_fails("unsafe {}\n", [], "unmarked unsafe block")

    def test_marker_without_inventory_entry_fails(self) -> None:
        self.assert_fails(
            "// SAFETY[TEST-BLOCK]: The fixture block is intentionally empty.\nunsafe {}\n",
            [],
            "marker has no inventory entry",
        )

    def test_stale_inventory_entry_fails(self) -> None:
        self.assert_fails("", [self.site("TEST-BLOCK", "block")], "stale inventory entry")

    def test_duplicate_marker_fails(self) -> None:
        self.assert_fails(
            "// SAFETY[TEST-BLOCK]: First empty fixture block.\nunsafe {}\n\n// SAFETY[TEST-BLOCK]: Second empty fixture block.\nunsafe {}\n",
            [self.site("TEST-BLOCK", "block")],
            "duplicate marker",
        )

    def test_path_mismatch_fails(self) -> None:
        self.assert_fails(
            "// SAFETY[TEST-BLOCK]: The fixture block is intentionally empty.\nunsafe {}\n",
            [self.site("TEST-BLOCK", "block", path="src/other.rs")],
            "path mismatch",
        )

    def test_kind_mismatch_fails(self) -> None:
        self.assert_fails(
            "// SAFETY[TEST-BLOCK]: The fixture block is intentionally empty.\nunsafe {}\n",
            [self.site("TEST-BLOCK", "function")],
            "kind mismatch",
        )

    def test_missing_owner_fails(self) -> None:
        site = self.site("TEST-BLOCK", "block")
        site["owner"] = ""
        self.assert_fails(
            "// SAFETY[TEST-BLOCK]: The fixture block is intentionally empty.\nunsafe {}\n",
            [site],
            "owner must be a non-empty string",
        )

    def test_missing_safety_fails(self) -> None:
        site = self.site("TEST-BLOCK", "block")
        site["safety"] = ""
        self.assert_fails(
            "// SAFETY[TEST-BLOCK]: The fixture block is intentionally empty.\nunsafe {}\n",
            [site],
            "safety must be a non-empty string",
        )

    def test_missing_evidence_fails(self) -> None:
        site = self.site("TEST-BLOCK", "block")
        site["evidence"] = []
        self.assert_fails(
            "// SAFETY[TEST-BLOCK]: The fixture block is intentionally empty.\nunsafe {}\n",
            [site],
            "evidence must be a non-empty array of non-empty strings",
        )

    def test_comments_strings_raw_strings_bytes_and_chars_are_ignored(self) -> None:
        source = r'''// unsafe {}
/* nested /* unsafe fn ignored() {} */ unsafe impl Send for Ignored {} */
const NORMAL: &str = "unsafe {}";
const RAW: &str = r#"unsafe fn ignored() {}"#;
const BYTE: &[u8] = b"unsafe impl Send for Ignored {}";
const BYTE_RAW: &[u8] = br##"unsafe {}"##;
const CHARACTER: char = 'u';
// SAFETY[TEST-BLOCK]: The fixture block is intentionally empty.
unsafe {}
'''
        sites = [self.site("TEST-BLOCK", "block", safety="The fixture block is intentionally empty.")]
        root, inventory, temporary = self.write_tree(source, sites)
        self.addCleanup(temporary.cleanup)
        self.assertEqual(validate_inventory(root, inventory), [])

    def test_unknown_unsafe_form_fails_closed(self) -> None:
        self.assert_fails("unsafe trait Marker {}\n", [], "unknown unsafe form")

    def test_repository_inventory_has_43_blocks_15_declarations_and_exact_six_files(self) -> None:
        root = SCRIPTS.parent
        inventory = root / "docs" / "audit" / "phase-3" / "unsafe-inventory.json"
        self.assertEqual(validate_inventory(root, inventory), [])
        sites = [
            site
            for path in discover_rust_files(root)
            for site in scan_rust_source(path.relative_to(root).as_posix(), path.read_text(encoding="utf-8"))
        ]
        self.assertEqual(sum(site.kind == "block" for site in sites), 43)
        self.assertEqual(sum(site.kind in {"function", "impl"} for site in sites), 15)
        self.assertEqual(
            {site.path for site in sites},
            {
                "crates/index/src/node_layout.rs",
                "crates/index/src/node.rs",
                "crates/index/src/node_table.rs",
                "crates/index/src/segment_format.rs",
                "bench/benches/lifecycle_bench.rs",
                "bench/benches/segment_recall_bench.rs",
            },
        )
        self.assertEqual(len(load_inventory(inventory)), 58)


if __name__ == "__main__":
    unittest.main()
