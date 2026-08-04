import tempfile
import unittest
import subprocess
import sys
from pathlib import Path

import summarize


class SummarizeTests(unittest.TestCase):
    def _write_complete_configured_matrix(self, artifact: Path, fixture_evidence: str) -> None:
        config = f"""workload_signature=synthetic-seed-20260801-dim512-hnsw-M16-efc100-efsearch32-k10
seed=20260801
fixture_evidence={fixture_evidence}
manifest_points=1,10,20,40,80,160
manifest_warmup_runs=1
manifest_repetitions=5
segment_rows=256
segment_queries=16
segment_dimension=512
segment_k=10
segment_ef_search=32
segment_m=16
segment_ef_construction=100
segment_max_layer=16
segment_points=1,2,4,8,16,32,64
segment_warmup_runs=1
segment_repetitions=5
lifecycle_rows=64
lifecycle_batch_rows=1
lifecycle_pins=0,1,4,16,64
lifecycle_warmup_runs=1
lifecycle_repetitions=5
command_manifest=manifest
command_segment=segment
command_lifecycle=lifecycle
"""
        for label in ("before", "after"):
            directory = artifact / label
            directory.mkdir()
            (directory / "config.env").write_text(
                f"label={label}\nrevision={'a' if label == 'before' else 'b'}\n"
                "lockfile_sha256=lock\n" + config,
                encoding="utf-8",
            )
            for point in (1, 10, 20, 40, 80, 160):
                (directory / f"manifest_growth_{point}.log").write_text(
                    f"manifest growth â€” {point} sequential commits, one data file each\n"
                    f"input: deterministic id-only rows; commits={point}; buckets=20; warmup runs excluded=1; measured repetitions=5\n"
                    "median commit-sequence wall: 1.000 ms; p95: 1.100 ms; sample variance: 0.100 ms^2\n"
                    "median newest manifest: 712 bytes; p95: 713 bytes; sample variance: 0.100 bytes^2\n",
                    encoding="utf-8",
                )
                (directory / f"manifest_growth_{point}.time").write_text(
                    "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
                )
            segment = [
                "loaded 256 rows from synthetic seed=20260801; input hash=abc123",
                "computing exact ground truth for 16 queries...",
                "==== recall vs segment count â€” 256 rows x 512-dim, k=10, ef_search=32 ====",
                "production HNSW parameters: M=16, ef_construction=100, max_layer=16",
                "query policy: 1 full unfiltered+filtered warmup sweep(s), then 5 measured sweep(s) per K",
            ]
            for mode in ("unfiltered", "filtered"):
                segment.append(f"{mode} query results (median / p95 over measured repetitions):")
                for point in (1, 2, 4, 8, 16, 32, 64):
                    segment.append(f"{point:4d}  1.0000 / 1.0000     10.0 / 11.0     100 / 90")
            (directory / "segment_recall.log").write_text("\n".join(segment), encoding="utf-8")
            (directory / "segment_recall.time").write_text(
                "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
            )
            lifecycle = []
            for pins in (0, 1, 4, 16, 64):
                for repetition in range(1, 6):
                    lifecycle.extend(
                        [
                            "================ StrataDB lifecycle â€” 64 rows x 512-dim ================",
                            "ingest+commit                    20.00ms       2.0MB       3.0MB      -1.0MB  fsync",
                            "                            64 commits, 64 rows/s, 1.00 ms/commit",
                            "8 threads x 3 = 24 commits, 24/s",
                            "input hash=abc123",
                            f"measurement             : measured repetition {repetition}/5; pins={pins}",
                            f"{pins} retained handles ({pins} distinct snapshots; operational current snapshot excluded);",
                            "loading 64 rows (512-dim) from synthetic seed=20260801; input hash=abc123",
                            "newest manifest bytes  : 200",
                        ]
                    )
            (directory / "lifecycle.log").write_text("\n".join(lifecycle), encoding="utf-8")
            (directory / "lifecycle.time").write_text(
                "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
            )
            if fixture_evidence == "not-requested":
                (directory / "fixture_segment_recall.status").write_text(
                    "fixture_status=not-requested\n", encoding="utf-8"
                )

    def _write_fixture_evidence(
        self, artifact: Path, label: str, *, path: str | None = None, input_hash: str = "f1a2ce123"
    ) -> None:
        directory = artifact / label
        fixture_path = path or f"/worktrees/{label}/bench/data/dbpedia-openai-100k.parquet"
        config = {
            "label": label,
            "revision": "a" if label == "before" else "b",
            "lockfile_sha256": "lock",
            "source": "fixture",
            **summarize.FIXTURE_PROVENANCE,
            "fixture_worktree_path": fixture_path,
            "fixture_source": f"fixture {fixture_path}",
            "fixture_input_hash": input_hash,
            "segment_rows": "256",
            "segment_queries": "16",
            "segment_dimension": "512",
            "segment_k": "10",
            "segment_ef_search": "32",
            "segment_m": "16",
            "segment_ef_construction": "100",
            "segment_max_layer": "16",
            "segment_points": "1,2,4,8,16,32,64",
            "segment_warmup_runs": "1",
            "segment_repetitions": "5",
        }
        (directory / "fixture_segment_recall.env").write_text(
            "".join(f"{key}={value}\n" for key, value in config.items()), encoding="utf-8"
        )
        log = [
            f"label={label}",
            f"revision={config['revision']}",
            "lockfile_sha256=lock",
            "benchmark=fixture_segment_recall",
            *(f"{key}={value}" for key, value in summarize.FIXTURE_PROVENANCE.items()),
            f"fixture_worktree_path={fixture_path}",
            f"loaded 256 rows from fixture {fixture_path}; input hash={input_hash}",
            "computing exact ground truth for 16 queries...",
            "==== recall vs segment count â€” 256 rows x 512-dim, k=10, ef_search=32 ====",
            "production HNSW parameters: M=16, ef_construction=100, max_layer=16",
            "query policy: 1 full unfiltered+filtered warmup sweep(s), then 5 measured sweep(s) per K",
        ]
        for mode in ("unfiltered", "filtered"):
            log.append(f"{mode} query results (median / p95 over measured repetitions):")
            for point in (1, 2, 4, 8, 16, 32, 64):
                log.append(f"{point:4d}  1.0000 / 1.0000     10.0 / 11.0     100 / 90")
        (directory / "fixture_segment_recall.log").write_text("\n".join(log), encoding="utf-8")
        (directory / "fixture_segment_recall.time").write_text(
            "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
        )
        (directory / "fixture_segment_recall.status").write_text(
            "fixture_status=complete\n", encoding="utf-8"
        )

    def test_fixture_provenance_rejects_missing_pinned_sha256(self):
        config = {
            "source": "fixture",
            "fixture_repo": "Qdrant/dbpedia-entities-openai3-text-embedding-3-small-512-100K",
            "fixture_revision": "56e6849a3d0f7913e56b475bf92c0064c93b576d",
            "fixture_file": "data/train-00000-of-00001.parquet",
            "fixture_size_bytes": "363758493",
        }
        with self.assertRaisesRegex(ValueError, "fixture_sha256"):
            summarize.validate_fixture_provenance(config)

    def test_collects_fixture_segment_metrics_with_pinned_provenance(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "before"
            directory.mkdir()
            config = {
                "label": "before",
                "revision": "revision",
                "lockfile_sha256": "lock",
                "source": "fixture",
                **summarize.FIXTURE_PROVENANCE,
            }
            (directory / "fixture_segment_recall.env").write_text(
                "".join(f"{key}={value}\n" for key, value in config.items()), encoding="utf-8"
            )
            (directory / "fixture_segment_recall.log").write_text(
                "loaded 64 rows from fixture /tmp/dbpedia-openai-100k.parquet; input hash=abc123\n"
                "unfiltered query results (median / p95 over measured repetitions):\n"
                "   1   1.0000 /  1.0000     10.0 /    11.0    100 /   90\n",
                encoding="utf-8",
            )
            (directory / "fixture_segment_recall.time").write_text(
                "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
            )

            records = summarize.summarize_directory(Path(root))

            self.assertTrue(records)
            self.assertEqual({row["benchmark"] for row in records}, {"fixture_segment_recall"})
            self.assertTrue(all(row["config"]["source"] == "fixture" for row in records))

    def test_collects_manifest_and_segment_metrics_and_delta(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            (artifact / "before").mkdir()
            (artifact / "after").mkdir()
            for label, wall, latency in (("before", "100.000", "10.0"), ("after", "80.000", "8.0")):
                (artifact / label / "manifest_growth.log").write_text(
                    f"median commit-sequence wall: {wall} ms; p95: {wall} ms\n"
                    "median newest manifest: 712 bytes; p95: 712 bytes\n",
                    encoding="utf-8",
                )
                (artifact / label / "segment_recall.log").write_text(
                    "unfiltered query results (median / p95 over measured repetitions):\n"
                    f"   1   1.0000 /  1.0000     {latency} /    {latency}    100 /   100\n",
                    encoding="utf-8",
                )
                (artifact / label / "manifest_growth.time").write_text(
                    "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
                )

            records = summarize.summarize_directory(artifact)
            deltas = summarize.compute_deltas(records)

            manifest = next(
                row for row in records if row["label"] == "after" and row["metric"] == "wall_ms"
            )
            self.assertEqual(manifest["value"], 80.0)
            self.assertEqual(manifest["max_rss_kb"], 1234)
            latency_delta = next(
                row for row in deltas if row["metric"] == "segment_k1_unfiltered_us_per_query_median"
            )
            self.assertEqual(latency_delta["delta"], -2.0)
            self.assertEqual(latency_delta["relative_delta"], -0.2)

    def test_lifecycle_ignores_warmup_and_summarizes_measured_phase(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "before"
            directory.mkdir()
            block = """
================ StrataDB lifecycle — 4 rows x 512-dim ================
ingest+commit                    10.00ms       1.0MB       2.0MB       0.0MB  fsync
measurement             : excluded warmup 1/1; pins=0
newest manifest bytes  : 100
================ StrataDB lifecycle — 4 rows x 512-dim ================
ingest+commit                    20.00ms       2.0MB       3.0MB      -1.0MB  fsync
measurement             : measured repetition 1/5; pins=0
newest manifest bytes  : 200
"""
            (directory / "lifecycle.log").write_text(block, encoding="utf-8")
            (directory / "lifecycle.time").write_text(
                "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
            )
            rows = summarize._lifecycle("before", directory, {})
            wall = next(row for row in rows if row["metric"] == "lifecycle_pins0_ingest_commit_wall_ms")
            self.assertEqual(wall["value"], 20.0)
            self.assertEqual(wall["samples"], 1)
            live = next(row for row in rows if row["metric"] == "lifecycle_pins0_ingest_commit_live_delta_mb")
            self.assertEqual(live["value"], -1.0)

    def test_validation_rejects_an_incomplete_matrix(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            records = [
                {
                    "label": "before",
                    "benchmark": "manifest_growth",
                    "metric": "wall_ms",
                    "value": 1.0,
                    "max_rss_kb": 1,
                }
            ]
            with self.assertRaises(ValueError):
                summarize.validate_records(records, artifact)

    def test_cli_writes_machine_readable_outputs(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root) / "before"
            artifact.mkdir()
            (artifact / "manifest_growth.log").write_text(
                "median commit-sequence wall: 1.000 ms; p95: 1.000 ms\n"
                "median newest manifest: 712 bytes; p95: 712 bytes\n",
                encoding="utf-8",
            )
            jsonl = Path(root) / "summary.jsonl"
            csv = Path(root) / "deltas.csv"
            result = subprocess.run(
                [sys.executable, str(Path(summarize.__file__)), str(Path(root)), "--jsonl", str(jsonl), "--csv", str(csv)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(jsonl.is_file())
            self.assertTrue(csv.is_file())

    def test_validation_accepts_the_complete_configured_matrix(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            config = """workload_signature=synthetic-seed-20260801-dim512-hnsw-M16-efc100-efsearch32-k10
seed=20260801
fixture_evidence=not-requested
manifest_points=1,10,20,40,80,160
manifest_warmup_runs=1
manifest_repetitions=5
segment_rows=256
segment_queries=16
segment_dimension=512
segment_k=10
segment_ef_search=32
segment_m=16
segment_ef_construction=100
segment_max_layer=16
segment_points=1,2,4,8,16,32,64
segment_warmup_runs=1
segment_repetitions=5
lifecycle_rows=64
lifecycle_batch_rows=1
lifecycle_pins=0,1,4,16,64
lifecycle_warmup_runs=1
lifecycle_repetitions=5
command_manifest=manifest
command_segment=segment
command_lifecycle=lifecycle
"""
            for label in ("before", "after"):
                directory = artifact / label
                directory.mkdir()
                (directory / "fixture_segment_recall.status").write_text(
                    "fixture_status=not-requested\n", encoding="utf-8"
                )
                (directory / "config.env").write_text(
                    f"label={label}\nrevision={'a' if label == 'before' else 'b'}\n"
                    "lockfile_sha256=lock\n" + config,
                    encoding="utf-8",
                )
                for point in (1, 10, 20, 40, 80, 160):
                    (directory / f"manifest_growth_{point}.log").write_text(
                        f"manifest growth — {point} sequential commits, one data file each\n"
                        f"input: deterministic id-only rows; commits={point}; buckets=20; warmup runs excluded=1; measured repetitions=5\n"
                        "median commit-sequence wall: 1.000 ms; p95: 1.100 ms; sample variance: 0.100 ms^2\n"
                        "median newest manifest: 712 bytes; p95: 713 bytes; sample variance: 0.100 bytes^2\n",
                        encoding="utf-8",
                    )
                    (directory / f"manifest_growth_{point}.time").write_text(
                        "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
                    )
                segment = [
                    "loaded 256 rows from synthetic seed=20260801; input hash=abc123",
                    "computing exact ground truth for 16 queries...",
                    "==== recall vs segment count — 256 rows x 512-dim, k=10, ef_search=32 ====",
                    "production HNSW parameters: M=16, ef_construction=100, max_layer=16",
                    "query policy: 1 full unfiltered+filtered warmup sweep(s), then 5 measured sweep(s) per K",
                ]
                for mode in ("unfiltered", "filtered"):
                    segment.append(f"{mode} query results (median / p95 over measured repetitions):")
                    for point in (1, 2, 4, 8, 16, 32, 64):
                        segment.append(f"{point:4d}  1.0000 / 1.0000     10.0 / 11.0     100 / 90")
                (directory / "segment_recall.log").write_text("\n".join(segment), encoding="utf-8")
                (directory / "segment_recall.time").write_text(
                    "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
                )
                lifecycle = []
                for pins in (0, 1, 4, 16, 64):
                    for repetition in range(1, 6):
                        lifecycle.extend(
                            [
                                "================ StrataDB lifecycle — 64 rows x 512-dim ================",
                                "ingest+commit                    20.00ms       2.0MB       3.0MB      -1.0MB  fsync",
                                "                            64 commits, 64 rows/s, 1.00 ms/commit",
                                "8 threads x 3 = 24 commits, 24/s",
                                "input hash=abc123",
                                f"measurement             : measured repetition {repetition}/5; pins={pins}",
                                f"{pins} retained handles ({pins} distinct snapshots; operational current snapshot excluded);",
                                "loading 64 rows (512-dim) from synthetic seed=20260801; input hash=abc123",
                                "newest manifest bytes  : 200",
                            ]
                        )
                (directory / "lifecycle.log").write_text("\n".join(lifecycle), encoding="utf-8")
                (directory / "lifecycle.time").write_text(
                    "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
                )
            records = summarize.summarize_directory(artifact)
            summarize.validate_records(records, artifact)
            self.assertTrue(summarize.compute_deltas(records))
            before_lifecycle = artifact / "before" / "lifecycle.log"
            before_lifecycle.write_text(
                before_lifecycle.read_text(encoding="utf-8").replace(
                    "input hash=abc123", "input hash=abc123\ninput hash=def456", 1
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_accepts_complete_before_after_fixture_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)

            records = summarize.summarize_directory(artifact)

            summarize.validate_records(records, artifact)
            self.assertTrue(
                any(row["benchmark"] == "fixture_segment_recall" for row in records)
            )

    def test_validation_rejects_fixture_input_hash_mismatch_between_revisions(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            self._write_fixture_evidence(artifact, "before", input_hash="f1a2ce123")
            self._write_fixture_evidence(artifact, "after", input_hash="d4e5fa678")

            records = summarize.summarize_directory(artifact)

            with self.assertRaisesRegex(
                ValueError, "before/after fixture_segment_recall input hashes differ"
            ):
                summarize.validate_records(records, artifact)

    def test_validation_rejects_requested_fixture_evidence_when_all_artifacts_are_missing(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")

            with self.assertRaisesRegex(ValueError, "fixture_segment_recall"):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_requested_fixture_evidence_when_an_artifact_is_missing(self):
        for filename in (
            "fixture_segment_recall.env",
            "fixture_segment_recall.log",
            "fixture_segment_recall.time",
            "fixture_segment_recall.status",
        ):
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as root:
                artifact = Path(root)
                self._write_complete_configured_matrix(artifact, "requested")
                for label in ("before", "after"):
                    self._write_fixture_evidence(artifact, label)
                (artifact / "before" / filename).unlink()

                with self.assertRaisesRegex(ValueError, "fixture_segment_recall"):
                    summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_fixture_sidecar_identity_mismatches(self):
        for key, replacement in (
            ("source", "synthetic"),
            ("fixture_worktree_path", "/unexpected/fixture.parquet"),
            ("fixture_input_hash", "differenthash"),
        ):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as root:
                artifact = Path(root)
                self._write_complete_configured_matrix(artifact, "requested")
                for label in ("before", "after"):
                    self._write_fixture_evidence(artifact, label)
                sidecar = artifact / "before" / "fixture_segment_recall.env"
                original = (
                    "fixture"
                    if key == "source"
                    else "/worktrees/before/bench/data/dbpedia-openai-100k.parquet"
                    if key == "fixture_worktree_path"
                    else "f1a2ce123"
                )
                sidecar.write_text(
                    sidecar.read_text(encoding="utf-8").replace(
                        f"{key}={original}\n", f"{key}={replacement}\n", 1
                    ),
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(ValueError, key):
                    summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_missing_or_empty_fixture_sidecar_input_hash(self):
        for label in ("before", "after"):
            for replacement in (None, ""):
                with self.subTest(label=label, replacement=replacement), tempfile.TemporaryDirectory() as root:
                    artifact = Path(root)
                    self._write_complete_configured_matrix(artifact, "requested")
                    for fixture_label in ("before", "after"):
                        self._write_fixture_evidence(artifact, fixture_label)
                    sidecar = artifact / label / "fixture_segment_recall.env"
                    sidecar_text = sidecar.read_text(encoding="utf-8")
                    replacement_line = "" if replacement is None else "fixture_input_hash=\n"
                    sidecar.write_text(
                        sidecar_text.replace(
                            "fixture_input_hash=f1a2ce123\n", replacement_line, 1
                        ),
                        encoding="utf-8",
                    )

                    with self.assertRaisesRegex(
                        ValueError, rf"{label}: fixture_input_hash must be non-empty"
                    ):
                        summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_truncated_fixture_filtered_metrics(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)
            fixture_log = artifact / "before" / "fixture_segment_recall.log"
            final_filtered_row = "  64  1.0000 / 1.0000     10.0 / 11.0     100 / 90"
            before, separator, after = fixture_log.read_text(encoding="utf-8").rpartition(final_filtered_row)
            fixture_log.write_text(
                before + after if separator else fixture_log.read_text(encoding="utf-8"),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "segment_k64_filtered"):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_conflicting_duplicate_fixture_metadata(self):
        conflicts = {
            "source": "loaded 256 rows from fixture /unexpected/fixture.parquet; input hash=f1a2ce123",
            "path": "fixture_worktree_path=/unexpected/fixture.parquet",
            "shape": "==== recall vs segment count — 256 rows x 999-dim, k=10, ef_search=32 ====",
            "hash": "loaded 256 rows from fixture /worktrees/before/bench/data/dbpedia-openai-100k.parquet; input hash=differenthash",
        }
        for kind, duplicate in conflicts.items():
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as root:
                artifact = Path(root)
                self._write_complete_configured_matrix(artifact, "requested")
                for label in ("before", "after"):
                    self._write_fixture_evidence(artifact, label)
                fixture_log = artifact / "before" / "fixture_segment_recall.log"
                fixture_log.write_text(
                    fixture_log.read_text(encoding="utf-8") + f"\n{duplicate}\n",
                    encoding="utf-8",
                )

                with self.assertRaises(ValueError):
                    summarize.validate_records(summarize.summarize_directory(artifact), artifact)


if __name__ == "__main__":
    unittest.main()
